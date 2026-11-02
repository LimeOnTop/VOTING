// Package app wires dependencies and manages their lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/LimeOnTop/VOTING/cmd/config"
	"github.com/LimeOnTop/VOTING/internal/auth"
	"github.com/LimeOnTop/VOTING/internal/cache"
	"github.com/LimeOnTop/VOTING/internal/client"
	"github.com/LimeOnTop/VOTING/internal/controller"
	"github.com/LimeOnTop/VOTING/internal/observability"
	"github.com/LimeOnTop/VOTING/internal/repository"
	"github.com/LimeOnTop/VOTING/internal/service"
	"github.com/LimeOnTop/VOTING/internal/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	startup, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancelStartup()
	primary, err := openPool(startup, cfg.Database.URL, cfg.Database)
	if err != nil {
		return fmt.Errorf("open metadata database: %w", err)
	}
	defer primary.Close()
	pools := []*pgxpool.Pool{primary}
	byDSN := map[string]*pgxpool.Pool{cfg.Database.URL: primary}
	shards := make([]*pgxpool.Pool, 0, len(cfg.Database.VoteURLs))
	defer func() {
		for _, pool := range pools[1:] {
			pool.Close()
		}
	}()
	for _, dsn := range cfg.Database.VoteURLs {
		pool, ok := byDSN[dsn]
		if !ok {
			pool, err = openPool(startup, dsn, cfg.Database)
			if err != nil {
				return fmt.Errorf("open vote database: %w", err)
			}
			pools = append(pools, pool)
			byDSN[dsn] = pool
		}
		shards = append(shards, pool)
	}
	if cfg.Mode == "migrate" {
		for _, pool := range pools {
			if err = repository.Migrate(startup, pool, cfg.QueryTimeout); err != nil {
				return fmt.Errorf("migrate database: %w", err)
			}
		}
		return nil
	}
	clock := systemClock{}
	metrics := observability.NewMetrics()
	metadata := repository.NewPollRepository(primary)
	cachedPolls, err := cache.NewPollRepository(metadata, cache.PollOptions{Capacity: cfg.Cache.Capacity, Shards: cfg.Cache.Shards, TTL: cfg.Cache.PollTTL, NegativeTTL: cfg.Cache.NegativeTTL, LoadTimeout: cfg.Cache.LoadTimeout, MaxLoads: cfg.Cache.MaxLoads}, clock, metrics)
	if err != nil {
		return fmt.Errorf("initialize metadata cache: %w", err)
	}
	pollService, err := service.NewPollService(cachedPolls, clock, auth.IDs{}, service.PollOptions{QueryTimeout: cfg.QueryTimeout, DefaultPageSize: cfg.HTTP.DefaultPageSize, MaxPageSize: cfg.HTTP.MaxPageSize})
	if err != nil {
		return fmt.Errorf("initialize poll service: %w", err)
	}
	if err = pollService.Warm(startup, cfg.WarmPollIDs); err != nil {
		return fmt.Errorf("warm poll metadata: %w", err)
	}
	votes, err := repository.NewVoteRepository(shards, repository.VoteOptions{CounterShards: cfg.Database.CounterShards, ReadConcurrency: cfg.Database.ReadConcurrency, QueryTimeout: cfg.ProjectionTimeout})
	if err != nil {
		return fmt.Errorf("initialize vote repository: %w", err)
	}
	kafkaOptions := client.KafkaOptions{Brokers: cfg.Kafka.Brokers, Topic: cfg.Kafka.Topic, Group: cfg.Kafka.Group, TLS: cfg.Kafka.TLS, Username: cfg.Kafka.Username, Password: cfg.Kafka.Password, DialTimeout: cfg.Kafka.DialTimeout, ReadTimeout: cfg.Kafka.ReadTimeout, WriteTimeout: cfg.Kafka.WriteTimeout, ProducerBatchWait: cfg.Kafka.ProducerBatchWait, ConsumerBatchWait: cfg.Kafka.ConsumerBatchWait, ProducerBatchSize: cfg.Kafka.ProducerBatchSize, ConsumerBatchSize: cfg.Kafka.ConsumerBatchSize, MinBytes: cfg.Kafka.MinBytes, MaxBytes: cfg.Kafka.MaxBytes}
	life, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.Mode == "worker" {
		consumer, e := client.NewVoteConsumer(kafkaOptions)
		if e != nil {
			return fmt.Errorf("initialize Kafka consumer: %w", e)
		}
		defer closeLogged(logger, "Kafka consumer", consumer.Close)
		health, e := service.NewHealthService([]usecase.HealthCheck{metadata, consumer}, service.HealthOptions{Interval: cfg.HealthInterval, Timeout: cfg.HealthTimeout})
		if e != nil {
			return fmt.Errorf("initialize worker health: %w", e)
		}
		go health.Run(life)
		defer health.Stop()
		metricsServer := startMetrics(cancel, cfg, metrics, health, logger)
		defer stopServer(ctx, metricsServer, cfg.ShutdownTimeout, logger)
		projector, e := service.NewProjectionService(pollService, votes, consumer, clock, metrics, logger, service.ProjectionOptions{QueryTimeout: cfg.ProjectionTimeout, RetryBackoff: cfg.RetryBackoff, MaxFutureSkew: cfg.FutureSkew})
		if e != nil {
			return fmt.Errorf("initialize projection service: %w", e)
		}
		err = projector.Run(life)
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("run vote projector: %w", err)
		}
		return nil
	}
	publisher, err := client.NewVotePublisher(kafkaOptions)
	if err != nil {
		return fmt.Errorf("initialize Kafka publisher: %w", err)
	}
	defer closeLogged(logger, "Kafka publisher", publisher.Close)
	identity, err := auth.NewAnonymousIdentity(cfg.SigningKey, auth.IDs{})
	if err != nil {
		return fmt.Errorf("initialize anonymous identity: %w", err)
	}
	readCache, err := cache.NewVoteReadRepository(votes, cache.ResultOptions{Capacity: cfg.Cache.Capacity, Shards: cfg.Cache.Shards, ResultsTTL: cfg.Cache.ResultsTTL, LoadTimeout: cfg.Cache.LoadTimeout, MaxLoads: cfg.Cache.MaxLoads}, clock, metrics)
	if err != nil {
		return fmt.Errorf("initialize results cache: %w", err)
	}
	admission, err := cache.NewAdmission(cfg.Cache.DedupCapacity, cfg.Cache.Shards, clock)
	if err != nil {
		return fmt.Errorf("initialize vote admission: %w", err)
	}
	votingService, err := service.NewVoteService(pollService, publisher, admission, identity, clock, metrics, service.VoteOptions{EnqueueTimeout: cfg.EnqueueTimeout, MaxInFlight: cfg.HTTP.MaxInFlight, DedupRetention: cfg.Cache.DedupRetention})
	if err != nil {
		return fmt.Errorf("initialize voting service: %w", err)
	}
	resultsService, err := service.NewResultsService(pollService, readCache, cfg.QueryTimeout)
	if err != nil {
		return fmt.Errorf("initialize results service: %w", err)
	}
	// Projection shard failures must not stop durable intake.
	health, err := service.NewHealthService([]usecase.HealthCheck{metadata, publisher}, service.HealthOptions{Interval: cfg.HealthInterval, Timeout: cfg.HealthTimeout})
	if err != nil {
		return fmt.Errorf("initialize API health: %w", err)
	}
	httpController, err := controller.NewController(pollService, votingService, resultsService, identity, health, metrics, logger, controller.Options{AdminToken: cfg.AdminToken, WebDir: cfg.HTTP.WebDir, CookieName: cfg.HTTP.CookieName, CookieMaxAge: cfg.HTTP.CookieMaxAge, MetadataMaxAge: cfg.HTTP.MetadataMaxAge, RequestTimeout: cfg.HTTP.RequestTimeout, SecureCookie: cfg.HTTP.SecureCookie, MaxBodyBytes: cfg.HTTP.MaxBodyBytes})
	if err != nil {
		return fmt.Errorf("initialize HTTP controller: %w", err)
	}
	defer closeLogged(logger, "web root", httpController.Close)
	go health.Run(life)
	defer health.Stop()
	metricsServer := startMetrics(cancel, cfg, metrics, health, logger)
	defer stopServer(ctx, metricsServer, cfg.ShutdownTimeout, logger)
	server := newServer(cfg.HTTP.Address, httpController.Handler(), cfg)
	done := make(chan error, 1)
	go func() {
		logger.Info("HTTP listening", "address", cfg.HTTP.Address, "vote_shards", len(shards))
		done <- server.ListenAndServe()
	}()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve API: %w", err)
	case <-life.Done():
		cancel()
		health.Stop()
		shutdown, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancelShutdown()
		if err = server.Shutdown(shutdown); err != nil {
			return fmt.Errorf("HTTP shutdown: %w", err)
		}
		return nil
	}
}
func newServer(address string, handler http.Handler, cfg config.Config) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout, ReadTimeout: cfg.HTTP.ReadTimeout, WriteTimeout: cfg.HTTP.WriteTimeout, IdleTimeout: cfg.HTTP.IdleTimeout, MaxHeaderBytes: cfg.HTTP.MaxHeaderBytes}
}
func startMetrics(cancel context.CancelFunc, cfg config.Config, metrics *observability.Metrics, health usecase.HealthStatus, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !health.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := newServer(cfg.HTTP.MetricsAddress, mux, cfg)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
			cancel()
		}
	}()
	return server
}
func stopServer(parent context.Context, server *http.Server, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Warn("metrics shutdown", "error", err)
	}
}
func closeLogged(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Warn("resource close", "resource", name, "error", err)
	}
}
