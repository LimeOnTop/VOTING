package httpapi

import (
	"context"
	"errors"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type repo struct{ p domain.Poll }

func (r repo) Get(context.Context, string) (domain.Poll, error)          { return r.p, nil }
func (r repo) Create(context.Context, domain.Poll) error                 { return nil }
func (r repo) List(context.Context) ([]domain.Poll, error)               { return []domain.Poll{r.p}, nil }
func (r repo) Receipt(context.Context, string, string) ([]string, error) { return nil, nil }
func (r repo) Results(context.Context, string) (domain.Result, error)    { return domain.Result{}, nil }

type publisher struct {
	err   error
	calls int
}

func (p *publisher) Publish(context.Context, domain.Vote) error { p.calls++; return p.err }
func (p *publisher) Ping(context.Context) error                 { return p.err }
func setup(t *testing.T) (*Server, http.Handler, *publisher, *http.Cookie) {
	t.Helper()
	now := time.Now()
	p, _ := domain.NewPoll(domain.CreatePoll{Question: "Question", Type: "single", Options: []string{"A", "B"}, OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(time.Minute)}, now)
	q := &publisher{}
	s := &Server{Repo: repo{p}, Queue: q, AdminToken: strings.Repeat("t", 32), SigningKey: strings.Repeat("s", 32)}
	h := s.Handler()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/session", nil))
	return s, h, q, rr.Result().Cookies()[0]
}
func TestVoteSecurityAndAcknowledgement(t *testing.T) {
	_, h, q, c := setup(t)
	for _, tc := range []struct {
		name, body, origin string
		cookie             *http.Cookie
		status             int
	}{{"valid", `{"options":["1"]}`, "", c, 202}, {"missing cookie", `{"options":["1"]}`, "", nil, 428}, {"forged cookie", `{"options":["1"]}`, "", &http.Cookie{Name: "voting_id", Value: "forged"}, 428}, {"cross site", `{"options":["1"]}`, "https://evil.test", c, 403}, {"invalid option", `{"options":["3"]}`, "", c, 422}, {"multiple single", `{"options":["1","2"]}`, "", c, 422}, {"unknown field", `{"options":["1"],"voter_key":"spoof"}`, "", c, 400}, {"trailing JSON", `{"options":["1"]}{}`, "", c, 400}} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/polls/test/votes", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.cookie != nil {
				r.AddCookie(tc.cookie)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("%d: %s", w.Code, w.Body.String())
			}
		})
	}
	if q.calls != 1 {
		t.Fatalf("invalid request reached queue: %d", q.calls)
	}
	q.err = errors.New("broker down")
	r := httptest.NewRequest("POST", "/api/polls/test/votes", strings.NewReader(`{"options":["1"]}`))
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(c)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("unpersisted vote acknowledged: %d", w.Code)
	}
}
func TestAdminAndPollScopedIdentity(t *testing.T) {
	s, h, _, _ := setup(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/admin/polls", nil))
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	r := httptest.NewRequest("GET", "/api/admin/polls", nil)
	r.Header.Set("Authorization", "Bearer "+s.AdminToken)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	if s.voterKey("a", "browser") == s.voterKey("b", "browser") {
		t.Fatal("identity linkable across polls")
	}
}
