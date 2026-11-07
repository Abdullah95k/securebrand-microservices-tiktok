package ttstore_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

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
