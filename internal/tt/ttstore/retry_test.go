package ttstore_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Errors that pass once other clients let go of their connections are
// retried at start; others are not.
func TestRetryable(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "53300", Message: "remaining connection slots are reserved for roles with the SUPERUSER attribute"}, true},
		{&pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, true},
		{fmt.Errorf("ttstore: ping: %w", &pgconn.PgError{Code: "08P01", Message: "server login has been failing, try again later (server_login_retry)"}), true},
		{errors.New("FATAL: no more connections allowed (max_client_conn)"), true},
		{&pgconn.PgError{Code: "08P01", Message: "query_wait_timeout"}, true},
		{&pgconn.PgError{Code: "28P01", Message: "password authentication failed"}, false},
		{&pgconn.PgError{Code: "3D000", Message: "database does not exist"}, false},
	} {
		if got := ttstore.Retryable(c.err); got != c.want {
			t.Errorf("Retryable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// A role the server gives no connection slot (53300, as when every service
// starts at once) is retried until it gets one, instead of failing.
func TestOpenWaitsForASlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dsn := testenv.EmptyPostgres(t)
	admin, err := ttstore.Open(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	role := fmt.Sprintf("tks_slot_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'slot' CONNECTION LIMIT 0`, role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "slot")
	go func() {
		time.Sleep(2 * time.Second)
		_, _ = admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s CONNECTION LIMIT -1`, role))
	}()
	start := time.Now()
	pool, err := ttstore.Open(ctx, u.String(), 1)
	if err != nil {
		t.Fatalf("open failed instead of waiting for a slot: %v", err)
	}
	pool.Close()
	if time.Since(start) < 2*time.Second {
		t.Errorf("connected after %s, before a slot was free", time.Since(start))
	}
}

// A server that accepts the connection and never answers (a connection
// proxy queueing clients while the database is full) costs one attempt,
// not the whole start-up: the attempt times out and is retried.
func TestOpenRetriesASilentServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c // held open, never answered
		}
	}()
	defer func() {
		for len(accepted) > 0 {
			(<-accepted).Close()
		}
	}()
	defer func(a, r time.Duration) { ttstore.AttemptTimeout, ttstore.StartRetryFor = a, r }(ttstore.AttemptTimeout, ttstore.StartRetryFor)
	ttstore.AttemptTimeout, ttstore.StartRetryFor = 300*time.Millisecond, 2*time.Second
	_, err = ttstore.Open(context.Background(), "postgres://u:p@"+ln.Addr().String()+"/db?sslmode=disable", 1)
	if err == nil {
		t.Fatal("opened a server that never answers")
	}
	if n := len(accepted); n < 2 {
		t.Errorf("%d attempts, want the silent one retried", n)
	}
}

// OpenWaiting keeps retrying past StartRetryFor, for as long as it is told.
func TestOpenWaitingOutlastsStartRetry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	defer func() {
		for len(accepted) > 0 {
			(<-accepted).Close()
		}
	}()
	defer func(a, r time.Duration) { ttstore.AttemptTimeout, ttstore.StartRetryFor = a, r }(ttstore.AttemptTimeout, ttstore.StartRetryFor)
	ttstore.AttemptTimeout, ttstore.StartRetryFor = 200*time.Millisecond, 100*time.Millisecond
	start := time.Now()
	if _, err := ttstore.OpenWaiting(context.Background(), "postgres://u:p@"+ln.Addr().String()+"/db?sslmode=disable", 1, 3500*time.Millisecond); err == nil {
		t.Fatal("opened a server that never answers")
	}
	if d := time.Since(start); d < 2*time.Second {
		t.Errorf("gave up after %s, want it to keep waiting past StartRetryFor", d)
	}
	if n := len(accepted); n < 3 {
		t.Errorf("%d attempts, want it to keep retrying", n)
	}
}

// A role whose default search_path is tt (production: the platform's setup
// job) gets no session-level SET, which PgBouncer's transaction pooling
// would leave on the pooled server session; a role without one is moved to
// tt with a SET.
func TestSearchPathOnlySetWhenNeeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dsn := testenv.EmptyPostgres(t)
	admin, err := ttstore.Open(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	source := func(pool interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) (schema, src string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT coalesce(current_schema(), ''), (SELECT source FROM pg_settings WHERE name = 'search_path')`).
			Scan(&schema, &src); err != nil {
			t.Fatal(err)
		}
		return schema, src
	}
	var db string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		t.Fatal(err)
	}
	role := fmt.Sprintf("tks_path_%d", time.Now().UnixNano())
	for _, q := range []string{
		`CREATE SCHEMA IF NOT EXISTS tt`,
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'path'`, role),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA tt TO %s`, role),
		fmt.Sprintf(`ALTER ROLE %s IN DATABASE %s SET search_path = tt`, role, db),
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA tt CASCADE`)
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP OWNED BY %s; DROP ROLE IF EXISTS %s`, role, role))
	})
	if s, src := source(admin); s != "tt" || src != "session" {
		t.Errorf("a role with no default: schema %q from %q, want tt set by the session", s, src)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "path")
	pool, err := ttstore.Open(ctx, u.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if s, src := source(pool); s != "tt" || src == "session" {
		t.Errorf("a role defaulting to tt: schema %q from %q, want tt with no session SET", s, src)
	}
}
