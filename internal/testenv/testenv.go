// Package testenv gives integration tests access to real infrastructure.
// Tests skip unless the TKS_TEST_* variables are set (make test-integration
// starts the containers and sets them).
package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
)

// Postgres creates a fresh database for the test, migrates it and returns
// a pool. Requires TKS_TEST_PG_DSN (a DSN to an admin database).
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TKS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TKS_TEST_PG_DSN not set")
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

// Redis returns a client on a database of its own (1-15), flushed.
// Requires TKS_TEST_REDIS_ADDR. A database is claimed through a key in
// database 0, so tests running at once (go test runs packages in parallel)
// never share one; it is released at cleanup, or after 30 minutes.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("TKS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TKS_TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	db, claim := 0, ""
	for deadline := time.Now().Add(5 * time.Minute); db == 0; time.Sleep(200 * time.Millisecond) {
		for _, n := range rand.Perm(15) {
			key := fmt.Sprintf("tks:testenv:db:%d", n+1)
			ok, err := admin.SetNX(ctx, key, os.Getpid(), 30*time.Minute).Result()
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				db, claim = n+1, key
				break
			}
		}
		if db == 0 && time.Now().After(deadline) {
			t.Fatal("testenv: no free Redis database")
		}
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	if err := c.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = admin.Del(context.Background(), claim).Err()
		_ = admin.Close()
	})
	return c
}

// Kafka returns the broker list. Requires TKS_TEST_KAFKA_BROKERS.
func Kafka(t testing.TB) []string {
	t.Helper()
	b := os.Getenv("TKS_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TKS_TEST_KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

// TopicPrefix returns a unique prefix so tests sharing a broker never see
// each other's records.
func TopicPrefix() string { return fmt.Sprintf("t%d.", rand.Int64N(1<<40)) }
