// Package api implements the fbs REST API (/v1). The contract is described
// in api/openapi/fbs.v1.yaml.
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

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Server holds API dependencies.
type Server struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool // writes
	ReadDB   *pgxpool.Pool // reads (replica when configured)
	Redis    *redis.Client
	Dispatch *dispatch.Dispatcher
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
	r.Use(middleware.RequestID, middleware.Recoverer, requestLog, middleware.Timeout(60*time.Second))
	r.Route("/v1", func(r chi.Router) {
		r.Use(auth.middleware)
		r.Post("/jobs", require(ScopeJobsWrite, s.createJob))
		r.Get("/jobs", require(ScopeDataRead, s.listJobs))
		r.Get("/jobs/{id}", require(ScopeDataRead, s.getJob))
		r.Post("/jobs/{id}/cancel", require(ScopeJobsWrite, s.cancelJob))
		r.Get("/jobs/{id}/results", require(ScopeDataRead, s.jobResults))

		r.Post("/pages", require(ScopePagesWrite, s.registerPage))
		r.Get("/pages", require(ScopeDataRead, s.listPages))
		r.Get("/pages/{page}", require(ScopeDataRead, s.getPage))
		r.Patch("/pages/{page}", require(ScopePagesWrite, s.updatePage))
		r.Delete("/pages/{page}", require(ScopePagesWrite, s.deletePage))
		r.Get("/pages/{page}/posts", require(ScopeDataRead, s.listPosts))
		r.Get("/pages/{page}/snapshots", require(ScopeDataRead, s.pageSnapshots))

		r.Get("/posts/{post}", require(ScopeDataRead, s.getPost))
		r.Get("/posts/{post}/comments", require(ScopeDataRead, s.listComments))
		r.Get("/posts/{post}/metrics", require(ScopeDataRead, s.postMetrics))
		r.Get("/posts/{post}/media", require(ScopeDataRead, s.postMedia))
		r.Get("/media/{media}", require(ScopeDataRead, s.getMedia))

		r.Post("/erasure-requests", require(ScopeAdmin, s.createErasure))
		r.Get("/erasure-requests/{id}", require(ScopeAdmin, s.getErasure))

		r.Get("/admin/queries", require(ScopeAdmin, s.listQueries))
		r.Put("/admin/queries/{name}", require(ScopeAdmin, s.putQuery))
		r.Get("/admin/registry-sync/runs", require(ScopeAdmin, s.registrySyncRuns))
	})
	return r
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

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

// Cursor is an opaque keyset position.
type cursorPos struct {
	T  time.Time `json:"t"`
	ID int64     `json:"i"`
}

func encodeCursor(t time.Time, id int64) string {
	b, _ := json.Marshal(cursorPos{T: t, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursorPos, bool) {
	if s == "" {
		return cursorPos{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursorPos{}, false
	}
	var c cursorPos
	if json.Unmarshal(b, &c) != nil {
		return cursorPos{}, false
	}
	return c, true
}

func strconvItoa(i int) string { return strconv.Itoa(i) }

func pathInt(r *http.Request, name string) (int64, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	return v, err == nil
}

func boolParam(r *http.Request, name string) bool {
	v := strings.ToLower(r.URL.Query().Get(name))
	return v == "1" || v == "true" || v == "yes"
}
