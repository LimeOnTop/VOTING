package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"github.com/LimeOnTop/VOTING/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Repository interface {
	Get(context.Context, string) (domain.Poll, error)
	Create(context.Context, domain.Poll) error
	List(context.Context) ([]domain.Poll, error)
	Receipt(context.Context, string, string) ([]string, error)
	Results(context.Context, string) (domain.Result, error)
}
type Publisher interface {
	Publish(context.Context, domain.Vote) error
	Ping(context.Context) error
}
type cached struct {
	p     domain.Poll
	until time.Time
}
type Server struct {
	Repo                           Repository
	Queue                          Publisher
	AdminToken, SigningKey, WebDir string
	SecureCookie                   bool
	cache                          sync.Map
	slots                          chan struct{}
	ready                          atomic.Bool
}

var Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_http_requests_total", Help: "HTTP responses by bounded route and status."}, []string{"route", "status"})
var Accepted = prometheus.NewCounter(prometheus.CounterOpts{Name: "voting_accepted_total", Help: "Durably enqueued requests, including duplicates."})
var Latency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "voting_enqueue_duration_seconds", Help: "Kafka acknowledgement latency.", Buckets: prometheus.DefBuckets})

func init() { prometheus.MustRegister(Requests, Accepted, Latency) }
func (s *Server) Handler() http.Handler {
	s.slots = make(chan struct{}, 1024)
	s.ready.Store(true)
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "alive"}) })
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			fail(w, 503, "draining")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	m.HandleFunc("GET /api/session", s.session)
	m.HandleFunc("GET /api/polls/{id}", s.getPoll)
	m.HandleFunc("POST /api/polls/{id}/votes", s.vote)
	m.HandleFunc("GET /api/polls/{id}/receipt", s.receipt)
	m.HandleFunc("GET /api/admin/polls", s.admin(s.list))
	m.HandleFunc("POST /api/admin/polls", s.admin(s.create))
	m.HandleFunc("GET /api/admin/polls/{id}/results", s.admin(s.results))
	m.HandleFunc("/", s.static)
	return s.middleware(m)
}
func (s *Server) SetReady(v bool) { s.ready.Store(v) }
func (s *Server) Monitor(ctx context.Context, ping func(context.Context) error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := ping(probe)
		cancel()
		s.SetReady(err == nil)
		select {
		case <-ctx.Done():
			s.SetReady(false)
			return
		case <-ticker.C:
		}
	}
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]string{"error": msg})
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return errors.New("ожидается application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return errors.New("некорректный JSON")
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return errors.New("ожидается один JSON объект")
	}
	return nil
}
func (s *Server) get(ctx context.Context, id string) (domain.Poll, error) {
	now := time.Now()
	if c, ok := s.cache.Load(id); ok && now.Before(c.(cached).until) {
		return c.(cached).p, nil
	}
	p, err := s.Repo.Get(ctx, id)
	if err == nil {
		if n := s.cacheSize(); n > 1000 {
			s.cache.Range(func(k, v any) bool { s.cache.Delete(k); return false })
		}
		s.cache.Store(id, cached{p, now.Add(10 * time.Second)})
	}
	return p, err
}
func (s *Server) cacheSize() int {
	n := 0
	s.cache.Range(func(_, _ any) bool { n++; return true })
	return n
}
func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if _, ok := s.identity(r); !ok {
		http.SetCookie(w, &http.Cookie{Name: "voting_id", Value: s.cookieValue(), Path: "/", HttpOnly: true, Secure: s.SecureCookie, SameSite: http.SameSiteLaxMode, MaxAge: 30 * 24 * 3600})
	}
	respond(w, 200, map[string]bool{"ready": true})
}
func (s *Server) getPoll(w http.ResponseWriter, r *http.Request) {
	p, err := s.get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.repoError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=5")
	respond(w, 200, p)
}
func (s *Server) vote(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !s.ready.Load() {
		fail(w, 503, "сервис временно недоступен")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		fail(w, 429, "попробуйте ещё раз")
		return
	}
	id, ok := s.identity(r)
	if !ok {
		fail(w, 428, "сначала разрешите cookie и откройте страницу голосования")
		return
	}
	var in struct {
		Options []string `json:"options"`
	}
	if err := decode(w, r, &in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	p, err := s.get(ctx, r.PathValue("id"))
	if err != nil {
		s.repoError(w, err)
		return
	}
	now := time.Now().UTC()
	if err = p.ValidateVote(in.Options, now); err != nil {
		fail(w, 422, err.Error())
		return
	}
	sort.Strings(in.Options)
	v := domain.Vote{Version: 1, PollID: p.ID, VoterKey: s.voterKey(p.ID, id), Options: in.Options, AcceptedAt: now}
	start := time.Now()
	err = s.Queue.Publish(ctx, v)
	Latency.Observe(time.Since(start).Seconds())
	if err != nil {
		slog.Error("enqueue failed", "error", err)
		w.Header().Set("Retry-After", "1")
		fail(w, 503, "не удалось подтвердить запись; повторите тот же запрос")
		return
	}
	Accepted.Inc()
	respond(w, 202, map[string]string{"status": "accepted", "receipt_url": "/api/polls/" + p.ID + "/receipt"})
}
func (s *Server) receipt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	id, ok := s.identity(r)
	if !ok {
		fail(w, 428, "cookie отсутствует")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	opts, err := s.Repo.Receipt(ctx, r.PathValue("id"), s.voterKey(r.PathValue("id"), id))
	if err != nil {
		s.repoError(w, err)
		return
	}
	if opts == nil {
		respond(w, 200, map[string]string{"status": "pending_or_not_submitted"})
		return
	}
	respond(w, 200, map[string]any{"status": "counted", "options": opts})
}
func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var in domain.CreatePoll
	if err := decode(w, r, &in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	p, err := domain.NewPoll(in, time.Now())
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	if err = s.Repo.Create(r.Context(), p); err != nil {
		s.repoError(w, err)
		return
	}
	respond(w, 201, p)
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	p, err := s.Repo.List(r.Context())
	if err != nil {
		s.repoError(w, err)
		return
	}
	respond(w, 200, p)
}
func (s *Server) results(w http.ResponseWriter, r *http.Request) {
	p, err := s.Repo.Results(r.Context(), r.PathValue("id"))
	if err != nil {
		s.repoError(w, err)
		return
	}
	respond(w, 200, p)
}
func (s *Server) repoError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		fail(w, 404, "опрос не найден")
		return
	}
	slog.Error("storage request failed", "error", err)
	fail(w, 503, "хранилище временно недоступно")
}
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		fail(w, 405, "метод не поддерживается")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		fail(w, 404, "маршрут не найден")
		return
	}
	path := filepath.Join(s.WebDir, filepath.Clean("/"+r.URL.Path))
	st, err := os.Stat(path)
	if err == nil && !st.IsDir() {
		if strings.Contains(r.URL.Path, "/assets/") {
			w.Header().Set("Cache-Control", "public,max-age=31536000,immutable")
		}
		http.ServeFile(w, r, path)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFile(w, r, filepath.Join(s.WebDir, "index.html"))
}

type tracked struct {
	http.ResponseWriter
	status int
}

func (t *tracked) WriteHeader(n int) { t.status = n; t.ResponseWriter.WriteHeader(n) }
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		if r.Method != "GET" && r.Method != "HEAD" {
			origin := r.Header.Get("Origin")
			if origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
					fail(w, 403, "чужой источник запроса")
					return
				}
			}
			if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
				fail(w, 403, "чужой источник запроса")
				return
			}
		}
		tw := &tracked{w, 200}
		defer func() {
			if e := recover(); e != nil {
				slog.Error("request panic")
				if tw.status == 200 {
					fail(tw, 500, "внутренняя ошибка")
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			Requests.WithLabelValues(route, http.StatusText(tw.status)).Inc()
		}()
		next.ServeHTTP(tw, r)
	})
}
