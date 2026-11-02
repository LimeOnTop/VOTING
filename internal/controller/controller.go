// Package controller translates HTTP into application ports.
package controller

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type Options struct {
	AdminToken, WebDir, CookieName               string
	CookieMaxAge, MetadataMaxAge, RequestTimeout time.Duration
	SecureCookie                                 bool
	MaxBodyBytes                                 int64
}
type Controller struct {
	polls     usecase.Poll
	voting    usecase.Voting
	results   usecase.Results
	identity  usecase.AnonymousIdentity
	health    usecase.HealthStatus
	observer  usecase.HTTPObserver
	logger    *slog.Logger
	options   Options
	adminHash [sha256.Size]byte
	root      *os.Root
}

func NewController(polls usecase.Poll, voting usecase.Voting, results usecase.Results, identity usecase.AnonymousIdentity, health usecase.HealthStatus, observer usecase.HTTPObserver, logger *slog.Logger, options Options) (*Controller, error) {
	if polls == nil || voting == nil || results == nil || identity == nil || health == nil || observer == nil || logger == nil || len(options.AdminToken) < 24 || options.CookieName == "" || options.CookieMaxAge < time.Second || options.MetadataMaxAge < 0 || options.MaxBodyBytes < 1 || options.RequestTimeout <= 0 {
		return nil, fmt.Errorf("invalid controller dependencies or options")
	}
	root, err := os.OpenRoot(options.WebDir)
	if err != nil {
		return nil, fmt.Errorf("open web bundle: %w", err)
	}
	return &Controller{polls: polls, voting: voting, results: results, identity: identity, health: health, observer: observer, logger: logger, options: options, adminHash: sha256.Sum256([]byte(options.AdminToken)), root: root}, nil
}
func (c *Controller) Close() error { return c.root.Close() }
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		c.respond(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !c.health.Ready() {
			c.fail(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		c.respond(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /api/v1/session", c.session)
	mux.HandleFunc("GET /api/v1/polls/{id}", c.getPoll)
	mux.HandleFunc("POST /api/v1/polls/{id}/votes", c.vote)
	mux.HandleFunc("GET /api/v1/polls/{id}/receipt", c.receipt)
	mux.HandleFunc("GET /api/v1/admin/polls", c.admin(c.list))
	mux.HandleFunc("POST /api/v1/admin/polls", c.admin(c.create))
	mux.HandleFunc("GET /api/v1/admin/polls/{id}/results", c.admin(c.getResults))
	mux.HandleFunc("/", c.static)
	return c.middleware(mux)
}
func (c *Controller) respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		c.logger.Debug("response encoding interrupted", "error", err)
	}
}
func (c *Controller) fail(w http.ResponseWriter, status int, message string) {
	c.respond(w, status, ErrorResponse{Error: message})
}
func (c *Controller) error(w http.ResponseWriter, err error) {
	var validation *apperr.Validation
	switch {
	case errors.As(err, &validation):
		c.fail(w, http.StatusUnprocessableEntity, validation.Error())
	case errors.Is(err, apperr.ErrNotFound):
		c.fail(w, http.StatusNotFound, apperr.ErrNotFound.Error())
	case errors.Is(err, apperr.ErrInvalidIdentity):
		c.fail(w, http.StatusPreconditionRequired, apperr.ErrInvalidIdentity.Error())
	case errors.Is(err, apperr.ErrAlreadyVoted):
		c.fail(w, http.StatusConflict, apperr.ErrAlreadyVoted.Error())
	case errors.Is(err, apperr.ErrVotePending):
		w.Header().Set("Retry-After", "1")
		c.fail(w, http.StatusTooEarly, apperr.ErrVotePending.Error())
	case errors.Is(err, apperr.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		c.fail(w, http.StatusTooManyRequests, apperr.ErrOverloaded.Error())
	default:
		c.logger.Error("application request failed", "error", err)
		w.Header().Set("Retry-After", "1")
		c.fail(w, http.StatusServiceUnavailable, "не удалось подтвердить операцию; повторите запрос")
	}
}
func (c *Controller) decode(w http.ResponseWriter, r *http.Request, value any) error {
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" {
		return errors.New("ожидается application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, c.options.MaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(value); err != nil {
		return errors.New("некорректный JSON")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("ожидается один JSON объект")
	}
	return nil
}
func (c *Controller) browserID(r *http.Request) (string, error) {
	cookie, err := r.Cookie(c.options.CookieName)
	if err != nil {
		return "", apperr.ErrInvalidIdentity
	}
	id, err := c.identity.ParseToken(cookie.Value)
	if err != nil {
		return "", fmt.Errorf("parse browser identity: %w", err)
	}
	return id, nil
}

// @Summary Начать анонимную сессию
// @Description Устанавливает cookie участника, если она отсутствует или недействительна.
// @Tags Session
// @Success 200 {object} SessionResponse
// @Failure 503 {object} ErrorResponse
// @Router /session [get]
func (c *Controller) session(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if _, err := c.browserID(r); err != nil {
		token, err := c.identity.NewToken()
		if err != nil {
			c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
			return
		}
		http.SetCookie(w, &http.Cookie{Name: c.options.CookieName, Value: token, Path: "/", HttpOnly: true, Secure: c.options.SecureCookie, SameSite: http.SameSiteLaxMode, MaxAge: int(c.options.CookieMaxAge / time.Second)})
	}
	c.respond(w, http.StatusOK, SessionResponse{Ready: true})
}

// @Summary Получить опрос
// @Tags Polls
// @Param id path string true "Идентификатор опроса"
// @Success 200 {object} entity.Poll
// @Failure 404,429,503 {object} ErrorResponse
// @Router /polls/{id} [get]
func (c *Controller) getPoll(w http.ResponseWriter, r *http.Request) {
	poll, err := c.polls.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(c.options.MetadataMaxAge/time.Second)))
	c.respond(w, http.StatusOK, poll)
}

// @Summary Отправить ответ
// @Description Принимает ответ в обработку. Первый учтённый ответ участника сохраняется без возможности изменения.
// @Tags Votes
// @Security BrowserCookie
// @Param id path string true "Идентификатор опроса"
// @Param answer body VoteRequest true "Выбранные варианты"
// @Success 202 {object} VoteResponse
// @Failure 400,403,409,422,425,428,429,503 {object} ErrorResponse
// @Header 425,429,503 {string} Retry-After "Задержка перед повторной попыткой в секундах"
// @Router /polls/{id}/votes [post]
func (c *Controller) vote(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !c.health.Ready() {
		c.error(w, apperr.ErrUnavailable)
		return
	}
	browser, err := c.browserID(r)
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	var input VoteRequest
	if err = c.decode(w, r, &input); err != nil {
		c.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = c.voting.Accept(r.Context(), r.PathValue("id"), browser, input.Options); err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	c.respond(w, http.StatusAccepted, VoteResponse{Status: "accepted", ReceiptURL: "/api/v1/polls/" + r.PathValue("id") + "/receipt"})
}

// @Summary Получить подтверждение приёма
// @Description Статус accepted подтверждает приём ответа, а не завершение подсчёта.
// @Tags Votes
// @Security BrowserCookie
// @Param id path string true "Идентификатор опроса"
// @Success 200 {object} ReceiptResponse
// @Failure 404,428,503 {object} ErrorResponse
// @Router /polls/{id}/receipt [get]
func (c *Controller) receipt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	browser, err := c.browserID(r)
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	options, err := c.voting.Receipt(r.Context(), r.PathValue("id"), browser)
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	if options == nil {
		c.respond(w, http.StatusOK, ReceiptResponse{Status: "not_submitted_or_forgotten"})
		return
	}
	c.respond(w, http.StatusOK, ReceiptResponse{Status: "accepted", Options: options})
}

// @Summary Создать опрос
// @Description Публикует опрос с неизменяемыми вариантами и временем проведения.
// @Tags Admin
// @Security AdminBearer
// @Param poll body CreatePollRequest true "Новый опрос"
// @Success 201 {object} entity.Poll
// @Failure 400,401,403,422,503 {object} ErrorResponse
// @Router /admin/polls [post]
func (c *Controller) create(w http.ResponseWriter, r *http.Request) {
	var input CreatePollRequest
	if err := c.decode(w, r, &input); err != nil {
		c.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	poll, err := c.polls.Create(r.Context(), entity.CreatePoll{Question: input.Question, Type: input.Type, Options: input.Options, OpensAt: input.OpensAt, ClosesAt: input.ClosesAt})
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	c.respond(w, http.StatusCreated, poll)
}

// @Summary Получить список опросов
// @Tags Admin
// @Security AdminBearer
// @Param limit query int false "Размер страницы" default(100) minimum(1) maximum(1000)
// @Param offset query int false "Смещение" default(0) minimum(0)
// @Success 200 {array} entity.Poll
// @Failure 400,401,422,503 {object} ErrorResponse
// @Router /admin/polls [get]
func (c *Controller) list(w http.ResponseWriter, r *http.Request) {
	limit, err := paginationInt(r, "limit")
	if err != nil {
		c.fail(w, http.StatusBadRequest, "некорректный limit")
		return
	}
	offset, err := paginationInt(r, "offset")
	if err != nil {
		c.fail(w, http.StatusBadRequest, "некорректный offset")
		return
	}
	polls, err := c.polls.GetAll(r.Context(), limit, offset)
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	c.respond(w, http.StatusOK, polls)
}
func paginationInt(r *http.Request, name string) (int, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}
	return strconv.Atoi(value)
}

// @Summary Получить результаты опроса
// @Description Возвращает число участников и суммы по вариантам. Результаты обновляются асинхронно.
// @Tags Admin
// @Security AdminBearer
// @Param id path string true "Идентификатор опроса"
// @Success 200 {object} entity.Result
// @Failure 401,404,429,503 {object} ErrorResponse
// @Router /admin/polls/{id}/results [get]
func (c *Controller) getResults(w http.ResponseWriter, r *http.Request) {
	result, err := c.results.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		c.error(w, fmt.Errorf("%s: %w", r.Pattern, err))
		return
	}
	c.respond(w, http.StatusOK, result)
}
func (c *Controller) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		hash := sha256.Sum256([]byte(token))
		if !bearer || !hmac.Equal(hash[:], c.adminHash[:]) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			c.fail(w, http.StatusUnauthorized, "нужен ключ администратора")
			return
		}
		next(w, r)
	}
}
func (c *Controller) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		c.fail(w, http.StatusMethodNotAllowed, "метод не поддерживается")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		c.fail(w, http.StatusNotFound, "маршрут не найден")
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	fileSystem := c.root.FS()
	info, err := fs.Stat(fileSystem, name)
	if err == nil && !info.IsDir() {
		if strings.HasPrefix(name, "assets/") {
			w.Header().Set("Cache-Control", "public,max-age=31536000,immutable")
		}
		http.ServeFileFS(w, r, fileSystem, name) //nolint:gosec // G703: os.Root.FS confines all paths and symlinks to the configured web root.
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFileFS(w, r, fileSystem, "index.html")
}

type trackedResponse struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *trackedResponse) WriteHeader(status int) {
	if w.written {
		return
	}
	w.written = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *trackedResponse) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
func (w *trackedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (c *Controller) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracked := &trackedResponse{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				c.logger.Error("request panic")
				if !tracked.written {
					c.fail(tracked, http.StatusInternalServerError, "внутренняя ошибка")
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			c.observer.HTTPRequest(route, tracked.status)
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			origin := r.Header.Get("Origin")
			if origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
					c.fail(tracked, http.StatusForbidden, "чужой источник запроса")
					return
				}
			}
			if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
				c.fail(tracked, http.StatusForbidden, "чужой источник запроса")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), c.options.RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		next.ServeHTTP(tracked, r)
	})
}
