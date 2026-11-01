package observability

import (
	"net/http"
	"time"

	"github.com/LimeOnTop/VOTING/internal/usecase"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry      *prometheus.Registry
	requests      *prometheus.CounterVec
	enqueues      *prometheus.CounterVec
	latency       prometheus.Histogram
	projected     *prometheus.CounterVec
	lastProcessed prometheus.Gauge
	cache         *prometheus.CounterVec
}

var _ usecase.VoteObserver = (*Metrics)(nil)
var _ usecase.ProjectionObserver = (*Metrics)(nil)
var _ usecase.CacheObserver = (*Metrics)(nil)
var _ usecase.HTTPObserver = (*Metrics)(nil)

func NewMetrics() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry(), requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_http_requests_total", Help: "HTTP responses by route and status."}, []string{"route", "status"}), enqueues: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_enqueue_total", Help: "Kafka acknowledgement outcomes, including retries and duplicates."}, []string{"outcome"}), latency: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "voting_enqueue_duration_seconds", Help: "Time waiting for Kafka acknowledgement.", Buckets: prometheus.DefBuckets}), projected: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_worker_events_total", Help: "Projection outcomes (cross-shard partial retry can be observed as duplicate)."}, []string{"outcome"}), lastProcessed: prometheus.NewGauge(prometheus.GaugeOpts{Name: "voting_worker_last_processed_timestamp_seconds", Help: "Most recent successfully committed projection batch."}), cache: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_cache_access_total", Help: "Bounded cache hit/miss/load/error outcomes."}, []string{"cache", "outcome"})}
	m.registry.MustRegister(m.requests, m.enqueues, m.latency, m.projected, m.lastProcessed, m.cache, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
func (m *Metrics) Enqueued(elapsed time.Duration, success bool) {
	outcome := "failed"
	if success {
		outcome = "accepted"
	}
	m.enqueues.WithLabelValues(outcome).Inc()
	m.latency.Observe(elapsed.Seconds())
}
func (m *Metrics) Projected(counted, duplicate int) {
	m.projected.WithLabelValues("counted").Add(float64(counted))
	m.projected.WithLabelValues("duplicate").Add(float64(duplicate))
	m.lastProcessed.Set(float64(time.Now().Unix()))
}
func (m *Metrics) Retried()                         { m.projected.WithLabelValues("retry").Inc() }
func (m *Metrics) CacheAccess(name, outcome string) { m.cache.WithLabelValues(name, outcome).Inc() }
func (m *Metrics) HTTPRequest(route string, status int) {
	m.requests.WithLabelValues(route, http.StatusText(status)).Inc()
}
