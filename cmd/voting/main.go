package main

import (
	"context"
	"errors"
	"github.com/LimeOnTop/VOTING/internal/broker"
	"github.com/LimeOnTop/VOTING/internal/config"
	"github.com/LimeOnTop/VOTING/internal/httpapi"
	"github.com/LimeOnTop/VOTING/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	boot, cancel := context.WithTimeout(ctx, 15*time.Second)
	s, err := store.Open(boot, c.DatabaseURL)
	cancel()
	if err != nil {
		return err
	}
	defer s.Pool.Close()
	if c.Mode == "migrate" {
		return s.Migrate(ctx)
	}
	metrics := http.NewServeMux()
	metrics.Handle("GET /metrics", promhttp.Handler())
	metrics.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	metrics.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		probe, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if s.Pool.Ping(probe) != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	ms := &http.Server{Addr: c.MetricsAddr, Handler: metrics, ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if e := ms.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", e)
			stop()
		}
	}()
	defer ms.Close()
	if c.Mode == "worker" {
		err = broker.Run(ctx, c, s)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	p := broker.New(c)
	defer p.Writer.Close()
	api := &httpapi.Server{Repo: s, Queue: p, AdminToken: c.AdminToken, SigningKey: c.SigningKey, SecureCookie: c.SecureCookie, WebDir: c.WebDir}
	handler := api.Handler()
	api.SetReady(false)
	go api.Monitor(ctx, func(probe context.Context) error {
		if e := s.Pool.Ping(probe); e != nil {
			return e
		}
		return p.Ping(probe)
	})
	srv := &http.Server{Addr: c.Addr, Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 12 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { slog.Info("http listening", "addr", c.Addr); done <- srv.ListenAndServe() }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		api.SetReady(false)
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	}
}
