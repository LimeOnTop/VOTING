package controller

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type pollStub struct{ usecase.Poll }
type votingStub struct{ usecase.Voting }
type resultsStub struct{ usecase.Results }
type identityStub struct{ usecase.AnonymousIdentity }

func (identityStub) NewToken() (string, error) { return "signed-token", nil }

type healthy struct{}

func (healthy) Ready() bool { return true }

type observer struct{}

func (observer) HTTPRequest(string, int) {}
func TestHTTPBoundaries(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("app-shell"), 0600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "private")
	if err := os.WriteFile(outside, []byte("private-material"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	c, err := NewController(pollStub{}, votingStub{}, resultsStub{}, identityStub{}, healthy{}, observer{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AdminToken: strings.Repeat("a", 24), WebDir: root, CookieName: "voting_id", CookieMaxAge: time.Hour, MetadataMaxAge: time.Second, RequestTimeout: time.Second, MaxBodyBytes: 128, SecureCookie: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	handler := c.Handler()
	for _, route := range []string{"/api/admin/polls", "/api/admin/polls/id/results"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, route, nil)
		req.Header.Set("Authorization", "Bearer invalid")
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusUnauthorized || res.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("admin guard: %d %v", res.Code, res.Header())
		}
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/session", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	cookies := res.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || !cookies[0].Secure || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatalf("unsafe cookie: %v", cookies)
	}
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/admin/polls", strings.NewReader("{}"))
	req.Header.Set("Origin", "https://other.example")
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 24))
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("cross origin accepted: %d", res.Code)
	}
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/escape", nil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if strings.Contains(res.Body.String(), "private-material") {
		t.Fatal("static root escaped")
	}
}
func TestStrictJSONBoundary(t *testing.T) {
	c := &Controller{options: Options{MaxBodyBytes: 32}}
	for _, body := range []string{`{"unknown":1}`, `{} {}`, strings.Repeat(" ", 33) + `{}`} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if err := c.decode(httptest.NewRecorder(), req, &struct{}{}); err == nil {
			t.Fatalf("invalid input accepted: %q", body)
		}
	}
}

func TestAdmissionErrorsHaveDistinctHTTPStatuses(t *testing.T) {
	c := &Controller{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, tc := range []struct {
		err    error
		status int
		retry  string
	}{{apperr.ErrAlreadyVoted, 409, ""}, {apperr.ErrVotePending, 425, "1"}, {apperr.ErrOverloaded, 429, "1"}} {
		w := httptest.NewRecorder()
		c.error(w, tc.err)
		if w.Code != tc.status || w.Header().Get("Retry-After") != tc.retry {
			t.Fatalf("status=%d retry=%s", w.Code, w.Header().Get("Retry-After"))
		}
	}
}
