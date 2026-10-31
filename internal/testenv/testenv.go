// Package testenv gives integration tests access to real infrastructure.
// Tests skip unless the IGS_TEST_* variables are set (make test-integration
// starts the containers and sets them).
package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// Postgres creates a fresh database for the test, migrates it and returns
// a pool. Requires IGS_TEST_PG_DSN (a DSN to an admin database).
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("IGS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("IGS_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	admin, err := pg.Open(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("fbs_test_%d", rand.Int64N(1<<40))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	testDSN := replaceDB(dsn, name)
	pool, err := pg.Open(ctx, testDSN, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	if err := store.MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func replaceDB(dsn, name string) string {
	q := ""
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		dsn, q = dsn[:i], dsn[i:]
	}
	i := strings.LastIndexByte(dsn, '/')
	return dsn[:i+1] + name + q
}

// Redis returns a client on a dedicated DB index, flushed. Requires
// IGS_TEST_REDIS_ADDR.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("IGS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IGS_TEST_REDIS_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: 1 + rand.IntN(14)})
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Kafka returns the broker list. Requires IGS_TEST_KAFKA_BROKERS.
func Kafka(t testing.TB) []string {
	t.Helper()
	b := os.Getenv("IGS_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("IGS_TEST_KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

// TopicPrefix returns a unique prefix so tests sharing a broker never see
// each other's records.
func TopicPrefix() string { return fmt.Sprintf("t%d.", rand.Int64N(1<<40)) }
