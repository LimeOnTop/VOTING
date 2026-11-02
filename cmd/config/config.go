// Package config reads and validates environment settings.
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Database struct {
	URL                                                     string
	VoteURLs, VoteShardIDs                                  []string
	WriteConcurrency                                        int
	MaxConnections, MinConnections                          int32
	MaxLifetime, MaxIdleTime, StatementTimeout, LockTimeout time.Duration
	ReadConcurrency, CounterShards                          int
}
type Kafka struct {
	Brokers                                                                      []string
	Topic, Group, Username, Password                                             string
	TLS                                                                          bool
	DialTimeout, ReadTimeout, WriteTimeout, ProducerBatchWait, ConsumerBatchWait time.Duration
	ProducerBatchSize, ConsumerBatchSize, MinBytes, MaxBytes                     int
}
type Cache struct {
	Capacity, Shards, MaxLoads, DedupCapacity                     int
	PollTTL, NegativeTTL, ResultsTTL, LoadTimeout, DedupRetention time.Duration
}
type HTTP struct {
	Address, MetricsAddress, WebDir, CookieName                                                             string
	SecureCookie                                                                                            bool
	MaxBodyBytes                                                                                            int64
	MaxInFlight, DefaultPageSize, MaxPageSize                                                               int
	CookieMaxAge, MetadataMaxAge, RequestTimeout, ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	MaxHeaderBytes                                                                                          int
}
type Config struct {
	FinalizePollID                                                                                                                            string
	FinalizeConfirmDrained                                                                                                                    bool
	Mode, AdminToken, SigningKey                                                                                                              string
	WarmPollIDs                                                                                                                               []string
	Database                                                                                                                                  Database
	Kafka                                                                                                                                     Kafka
	Cache                                                                                                                                     Cache
	HTTP                                                                                                                                      HTTP
	QueryTimeout, EnqueueTimeout, ProjectionTimeout, RetryBackoff, FutureSkew, HealthInterval, HealthTimeout, StartupTimeout, ShutdownTimeout time.Duration
}

type reader struct {
	lookup func(string) (string, bool)
	err    error
}

func (r *reader) text(key, fallback string) string {
	if value, ok := r.lookup(key); ok && value != "" {
		return value
	}
	return fallback
}
func (r *reader) duration(key, fallback string) time.Duration {
	value, err := time.ParseDuration(r.text(key, fallback))
	if err != nil || value <= 0 {
		r.invalid(key)
		return 0
	}
	return value
}
func (r *reader) integer(key, fallback string) int {
	value, err := strconv.Atoi(r.text(key, fallback))
	if err != nil || value < 1 {
		r.invalid(key)
		return 0
	}
	return value
}
func (r *reader) int32(key, fallback string, minimum int64) int32 {
	value, err := strconv.ParseInt(r.text(key, fallback), 10, 32)
	if err != nil || value < minimum {
		r.invalid(key)
		return 0
	}
	return int32(value)
}
func (r *reader) boolean(key, fallback string) bool {
	value, err := strconv.ParseBool(r.text(key, fallback))
	if err != nil {
		r.invalid(key)
	}
	return value
}
func (r *reader) invalid(key string) {
	if r.err == nil {
		r.err = fmt.Errorf("invalid configuration: %s", key)
	}
}
func list(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
func Load() (Config, error) { return LoadWithLookup(os.LookupEnv) }
func LoadWithLookup(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("environment lookup is required")
	}
	r := &reader{lookup: lookup}
	c := Config{}
	c.Mode = r.text("MODE", "api")
	c.AdminToken = r.text("ADMIN_TOKEN", "")
	c.SigningKey = r.text("SIGNING_KEY", "")
	c.WarmPollIDs = list(r.text("WARM_POLL_IDS", ""))
	c.Database = Database{URL: r.text("DATABASE_URL", ""), MaxConnections: r.int32("DB_MAX_CONNECTIONS", "20", 1), MinConnections: r.int32("DB_MIN_CONNECTIONS", "2", 0), MaxLifetime: r.duration("DB_MAX_LIFETIME", "30m"), MaxIdleTime: r.duration("DB_MAX_IDLE_TIME", "5m"), StatementTimeout: r.duration("DB_STATEMENT_TIMEOUT", "5s"), LockTimeout: r.duration("DB_LOCK_TIMEOUT", "1s"), WriteConcurrency: r.integer("DB_WRITE_CONCURRENCY", "4"), ReadConcurrency: r.integer("DB_READ_CONCURRENCY", "8"), CounterShards: r.integer("COUNTER_SHARDS", "256")}
	if value := r.text("VOTE_DATABASE_URLS", ""); value != "" {
		if err := json.Unmarshal([]byte(value), &c.Database.VoteURLs); err != nil {
			r.invalid("VOTE_DATABASE_URLS")
		}
	} else {
		c.Database.VoteURLs = []string{c.Database.URL}
	}
	c.Database.VoteShardIDs = list(r.text("VOTE_SHARD_IDS", ""))
	if len(c.Database.VoteShardIDs) == 0 && len(c.Database.VoteURLs) == 1 {
		c.Database.VoteShardIDs = []string{"primary"}
	}
	c.FinalizePollID = r.text("FINALIZE_POLL_ID", "")
	c.FinalizeConfirmDrained = r.boolean("FINALIZE_CONFIRM_DRAINED", "false")
	c.Kafka = Kafka{Brokers: list(r.text("KAFKA_BROKERS", "localhost:19092")), Topic: r.text("KAFKA_TOPIC", "votes.v1"), Group: r.text("KAFKA_GROUP", "voting-projector-v1"), Username: r.text("KAFKA_USER", ""), Password: r.text("KAFKA_PASSWORD", ""), TLS: r.boolean("KAFKA_TLS", "false"), DialTimeout: r.duration("KAFKA_DIAL_TIMEOUT", "3s"), ReadTimeout: r.duration("KAFKA_READ_TIMEOUT", "5s"), WriteTimeout: r.duration("KAFKA_WRITE_TIMEOUT", "5s"), ProducerBatchWait: r.duration("KAFKA_PRODUCER_BATCH_WAIT", "5ms"), ConsumerBatchWait: r.duration("KAFKA_CONSUMER_BATCH_WAIT", "25ms"), ProducerBatchSize: r.integer("KAFKA_PRODUCER_BATCH_SIZE", "1000"), ConsumerBatchSize: r.integer("KAFKA_CONSUMER_BATCH_SIZE", "500"), MinBytes: r.integer("KAFKA_FETCH_MIN_BYTES", "1"), MaxBytes: r.integer("KAFKA_FETCH_MAX_BYTES", "10000000")}
	c.Cache = Cache{DedupCapacity: r.integer("DEDUP_CAPACITY", "1000000"), DedupRetention: r.duration("DEDUP_RETENTION", "5m"), Capacity: r.integer("CACHE_CAPACITY", "10000"), Shards: r.integer("CACHE_SHARDS", "64"), MaxLoads: r.integer("CACHE_MAX_LOADS", "32"), PollTTL: r.duration("CACHE_POLL_TTL", "5m"), NegativeTTL: r.duration("CACHE_NEGATIVE_TTL", "2s"), ResultsTTL: r.duration("CACHE_RESULTS_TTL", "1s"), LoadTimeout: r.duration("CACHE_LOAD_TIMEOUT", "2s")}
	c.HTTP = HTTP{Address: r.text("ADDR", ":8080"), MetricsAddress: r.text("METRICS_ADDR", ":9090"), WebDir: r.text("WEB_DIR", "web/dist"), CookieName: r.text("COOKIE_NAME", "voting_id"), SecureCookie: r.boolean("COOKIE_SECURE", "true"), CookieMaxAge: r.duration("COOKIE_MAX_AGE", "720h"), MetadataMaxAge: r.duration("METADATA_MAX_AGE", "30s"), MaxBodyBytes: int64(r.integer("HTTP_MAX_BODY_BYTES", "8192")), MaxInFlight: r.integer("VOTE_MAX_IN_FLIGHT", "1024"), DefaultPageSize: r.integer("POLL_PAGE_SIZE", "100"), MaxPageSize: r.integer("POLL_MAX_PAGE_SIZE", "1000"), RequestTimeout: r.duration("HTTP_REQUEST_TIMEOUT", "10s"), ReadHeaderTimeout: r.duration("HTTP_READ_HEADER_TIMEOUT", "3s"), ReadTimeout: r.duration("HTTP_READ_TIMEOUT", "10s"), WriteTimeout: r.duration("HTTP_WRITE_TIMEOUT", "12s"), IdleTimeout: r.duration("HTTP_IDLE_TIMEOUT", "60s"), MaxHeaderBytes: r.integer("HTTP_MAX_HEADER_BYTES", "16384")}
	c.QueryTimeout = r.duration("QUERY_TIMEOUT", "2s")
	c.EnqueueTimeout = r.duration("ENQUEUE_TIMEOUT", "6s")
	c.ProjectionTimeout = r.duration("PROJECTION_TIMEOUT", "10s")
	c.RetryBackoff = r.duration("PROJECTION_RETRY_BACKOFF", "250ms")
	c.FutureSkew = r.duration("VOTE_FUTURE_SKEW", "1m")
	c.HealthInterval = r.duration("HEALTH_INTERVAL", "3s")
	c.HealthTimeout = r.duration("HEALTH_TIMEOUT", "2s")
	c.StartupTimeout = r.duration("STARTUP_TIMEOUT", "15s")
	c.ShutdownTimeout = r.duration("SHUTDOWN_TIMEOUT", "20s")
	if r.err != nil {
		return Config{}, r.err
	}
	if c.Mode != "api" && c.Mode != "worker" && c.Mode != "migrate" && c.Mode != "finalize" {
		return Config{}, fmt.Errorf("MODE must be api, worker, migrate or finalize")
	}
	if c.Mode == "api" && (len(c.AdminToken) < 24 || len(c.SigningKey) < 32) {
		return Config{}, fmt.Errorf("ADMIN_TOKEN >=24 bytes and SIGNING_KEY >=32 bytes are required for api")
	}
	urls := append([]string{c.Database.URL}, c.Database.VoteURLs...)
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
			return Config{}, fmt.Errorf("DATABASE_URL and VOTE_DATABASE_URLS must contain PostgreSQL connection URLs")
		}
	}
	if len(c.Database.VoteURLs) == 0 {
		return Config{}, fmt.Errorf("VOTE_DATABASE_URLS cannot be empty")
	}
	if c.Mode == "finalize" && (c.FinalizePollID == "" || !c.FinalizeConfirmDrained) {
		return Config{}, fmt.Errorf("finalize requires FINALIZE_POLL_ID and FINALIZE_CONFIRM_DRAINED=true")
	}
	if len(c.Database.VoteShardIDs) != len(c.Database.VoteURLs) {
		return Config{}, fmt.Errorf("VOTE_SHARD_IDS must name every vote database")
	}
	shardIDs := map[string]bool{}
	for _, id := range c.Database.VoteShardIDs {
		if id == "" || strings.ContainsAny(id, "\x00\r\n") || shardIDs[id] {
			return Config{}, fmt.Errorf("VOTE_SHARD_IDS must be nonempty and distinct")
		}
		shardIDs[id] = true
	}
	seen := map[string]bool{}
	for _, dsn := range c.Database.VoteURLs {
		if seen[dsn] {
			return Config{}, fmt.Errorf("VOTE_DATABASE_URLS must name distinct shard databases")
		}
		seen[dsn] = true
	}
	if c.Database.MinConnections > c.Database.MaxConnections || c.Database.CounterShards > 1<<15 || c.Database.StatementTimeout < time.Millisecond || c.Database.LockTimeout < time.Millisecond {
		return Config{}, fmt.Errorf("invalid database pool or counter shard limits")
	}
	if c.Cache.Shards > c.Cache.Capacity || c.Cache.Shards > c.Cache.DedupCapacity {
		return Config{}, fmt.Errorf("CACHE_SHARDS must not exceed CACHE_CAPACITY")
	}
	if c.HTTP.MaxPageSize < c.HTTP.DefaultPageSize || c.HTTP.MetadataMaxAge > c.Cache.PollTTL || c.HTTP.WriteTimeout < c.HTTP.RequestTimeout {
		return Config{}, fmt.Errorf("inconsistent HTTP pagination/cache/deadline limits")
	}
	if len(c.Kafka.Brokers) == 0 || c.Kafka.MaxBytes < c.Kafka.MinBytes {
		return Config{}, fmt.Errorf("invalid Kafka broker or fetch limits")
	}
	if (c.Kafka.Username == "") != (c.Kafka.Password == "") || c.Kafka.Username != "" && !c.Kafka.TLS {
		return Config{}, fmt.Errorf("Kafka SASL PLAIN credentials must be complete and require TLS")
	}
	return c, nil
}
