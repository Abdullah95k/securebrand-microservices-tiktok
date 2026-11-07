// Package httpserver provides the operations server every binary exposes:
// /metrics, /healthz (liveness) and /readyz (readiness checks).
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// Check reports whether a dependency is usable.
type Check func(ctx context.Context) error

// Ops is the operations HTTP server.
type Ops struct {
	addr   string
	mu     sync.RWMutex
	checks map[string]Check

	// Set by Early: the server already listening, its result, and whether
	// the binary is still opening its dependencies.
	srv      *http.Server
	served   chan error
	starting atomic.Bool

	// The last run of the readiness checks, reused for CheckEvery.
	lastMu  sync.Mutex
	lastAt  time.Time
	lastErr error
}

// CheckEvery bounds how often /readyz runs its checks: between runs it
// answers with the last result. A probe every 10 s that pinged
// PostgreSQL each time kept every pod's pooled server connection busy, so
// PgBouncer could never close it as idle (the master, 2026-11-07).
var CheckEvery = time.Minute

func NewOps(addr string) *Ops { return &Ops{addr: addr, checks: map[string]Check{}} }

// AddCheck registers a readiness check.
func (o *Ops) AddCheck(name string, c Check) {
	o.mu.Lock()
	o.checks[name] = c
	o.mu.Unlock()
}

func (o *Ops) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if o.starting.Load() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		if err := o.ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// ready runs the readiness checks, or returns their last result when they
// ran less than CheckEvery ago.
func (o *Ops) ready(ctx context.Context) error {
	o.lastMu.Lock()
	defer o.lastMu.Unlock()
	if !o.lastAt.IsZero() && time.Since(o.lastAt) < CheckEvery {
		return o.lastErr
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	o.mu.RLock()
	defer o.mu.RUnlock()
	o.lastErr = nil
	for name, c := range o.checks {
		if err := c(ctx); err != nil {
			o.lastErr = errors.New(name + ": " + err.Error())
			break
		}
	}
	o.lastAt = time.Now()
	return o.lastErr
}

// Early starts serving before the binary opens its dependencies, so
// /healthz (liveness) answers while, say, the database is still being
// retried at start-up; /readyz fails with "starting" until Run is called,
// which the binary does once its dependencies are open and its checks
// added. Without Early, Run starts the server itself.
func (o *Ops) Early() {
	o.starting.Store(true)
	o.srv = newServer(o.addr, o.Handler())
	o.served = make(chan error, 1)
	go func() { o.served <- o.srv.ListenAndServe() }()
	slog.Info("http listening (starting)", "addr", o.addr)
}

// Run serves until ctx is done.
func (o *Ops) Run(ctx context.Context) error {
	if o.srv == nil {
		return Serve(ctx, o.addr, o.Handler())
	}
	o.starting.Store(false)
	return wait(ctx, o.srv, o.served)
}

// Option configures the server Serve runs.
type Option func(*http.Server)

// WithReadTimeout bounds reading a whole request, body included.
func WithReadTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		if d > 0 {
			s.ReadTimeout = d
		}
	}
}

// Serve runs an HTTP server and shuts it down gracefully when ctx is done.
// Idle keep-alive connections are closed after 2 minutes, so clients that
// hold connections open (authenticated or not) can't exhaust descriptors.
func Serve(ctx context.Context, addr string, h http.Handler, opts ...Option) error {
	srv := newServer(addr, h)
	for _, o := range opts {
		o(srv)
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("http listening", "addr", addr)
	return wait(ctx, srv, errc)
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
}

// wait returns when srv stops by itself (errc) or, once ctx is done, after
// shutting it down gracefully.
func wait(ctx context.Context, srv *http.Server, errc <-chan error) error {
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
