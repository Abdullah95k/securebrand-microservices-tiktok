// Package api implements tks's REST API (/v1) on schema tt: accounts (by
// user id, username or secUid) with their username history, snapshots and
// posts; posts with their metrics, comments and media; jobs; registrations
// on the schedule; erasure. The contract is api/openapi/tks.v1.yaml, kept in
// step with the router by a test.
//
// It is internal/api (fbs's) on tks's tables and envelope (docs/tiktok ADR
// 0007 §13); the keys, scopes and rate limits are the same, on schema tt's
// api_clients.
package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-redis/redis_rate/v10"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// Server holds the API's dependencies. DB and ReadDB must be pools pinned to
// schema tt (ttstore.Open).
type Server struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool // writes
	ReadDB   *pgxpool.Pool // reads (replica when configured)
	Redis    *redis.Client
	Dispatch *dispatch.Dispatcher
	// Now is the clock for new schedules (tests); time.Now by default.
	Now func() time.Time
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Handler builds the router.
func (s *Server) Handler() http.Handler {
	if s.ReadDB == nil {
		s.ReadDB = s.DB
	}
	auth := &authenticator{db: s.DB, defaultRate: s.Cfg.API.DefaultRatePerMinute, cache: map[string]*Client{}}
	if s.Redis != nil {
		auth.limiter = redis_rate.NewLimiter(s.Redis)
	}
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, requestLog)
	timeout := s.requestTimeout()
	r.Route("/v1", func(r chi.Router) {
		r.Use(auth.middleware)
		// Results stream for as long as the job's data takes; every other
		// request is cut off after http.write_timeout.
		r.Get("/jobs/{id}/results", require(ScopeDataRead, s.jobResults))
		r.Group(func(r chi.Router) {
			r.Use(middleware.Timeout(timeout))
			s.routes(r)
		})
	})
	return r
}

// requestTimeout bounds an API request (http.write_timeout, 60s if unset).
func (s *Server) requestTimeout() time.Duration {
	if s.Cfg != nil && s.Cfg.HTTP.WriteTimeout > 0 {
		return s.Cfg.HTTP.WriteTimeout
	}
	return 60 * time.Second
}

// routes are the API routes under the request timeout.
func (s *Server) routes(r chi.Router) {
	r.Post("/jobs", require(ScopeJobsWrite, s.createJob))
	r.Get("/jobs", require(ScopeDataRead, s.listJobs))
	r.Get("/jobs/{id}", require(ScopeDataRead, s.getJob))
	r.Post("/jobs/{id}/cancel", require(ScopeJobsWrite, s.cancelJob))

	r.Post("/accounts", require(ScopeAccountsWrite, s.registerAccount))
	r.Get("/accounts", require(ScopeDataRead, s.listAccounts))
	r.Get("/accounts/{account}", require(ScopeDataRead, s.getAccount))
	r.Patch("/accounts/{account}", require(ScopeAccountsWrite, s.updateAccount))
	r.Delete("/accounts/{account}", require(ScopeAccountsWrite, s.deleteAccount))
	r.Get("/accounts/{account}/usernames", require(ScopePIIRead, s.accountUsernames))
	r.Get("/accounts/{account}/snapshots", require(ScopeDataRead, s.accountSnapshots))
	r.Get("/accounts/{account}/posts", require(ScopeDataRead, s.listPosts))

	r.Get("/posts/{post}", require(ScopeDataRead, s.getPost))
	r.Get("/posts/{post}/comments", require(ScopeDataRead, s.listComments))
	r.Get("/posts/{post}/metrics", require(ScopeDataRead, s.postMetrics))
	r.Get("/posts/{post}/media", require(ScopeDataRead, s.postMedia))
	r.Get("/music/{music}", require(ScopeDataRead, s.getMusic))
	r.Get("/media/{media}", require(ScopeDataRead, s.getMedia))

	r.Post("/erasure-requests", require(ScopeAdmin, s.createErasure))
	r.Get("/erasure-requests/{id}", require(ScopeAdmin, s.getErasure))

	r.Get("/admin/operations", require(ScopeAdmin, s.listOperations))
	r.Put("/admin/operations/{name}", require(ScopeAdmin, s.putOperation))
	r.Get("/admin/canary/runs", require(ScopeAdmin, s.canaryRuns))
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

// Unwrap lets http.ResponseController reach the connection's deadlines.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		metrics.APIRequests.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
		if sw.code >= 500 {
			slog.Error("api request", "method", r.Method, "route", route, "code", sw.code, "dur_ms", time.Since(start).Milliseconds())
		}
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// problem writes an RFC 9457 problem document.
func problem(w http.ResponseWriter, code int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": title, "status": code, "detail": detail})
}

func serverError(w http.ResponseWriter, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, http.StatusNotFound, "not found", "no such resource")
		return
	}
	slog.Error("api error", "err", err)
	problem(w, http.StatusInternalServerError, "internal error", "unexpected error")
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func limitParam(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return 50
	}
	return min(n, 500)
}

// cursorPos is an opaque keyset position.
type cursorPos struct {
	T  time.Time `json:"t"`
	ID int64     `json:"i"`
}

func encodeCursor(t time.Time, id int64) string {
	b, _ := json.Marshal(cursorPos{T: t, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

// cursorParam reads ?cursor=; a cursor that does not decode is a 400, not
// the first page again (a client would loop).
func cursorParam(w http.ResponseWriter, r *http.Request) (*cursorPos, bool) {
	s := r.URL.Query().Get("cursor")
	if s == "" {
		return nil, true
	}
	var c cursorPos
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid cursor", "cursor must be a next_cursor value from this API")
		return nil, false
	}
	return &c, true
}

func pathInt(r *http.Request, name string) (int64, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	return v, err == nil && v > 0
}

func boolParam(r *http.Request, name string) bool {
	v := strings.ToLower(r.URL.Query().Get(name))
	return v == "1" || v == "true" || v == "yes"
}

// timeParam reads an RFC 3339 query parameter.
func timeParam(w http.ResponseWriter, r *http.Request, name string) (time.Time, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return time.Time{}, true
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid "+name, err.Error())
		return time.Time{}, false
	}
	return t, true
}
