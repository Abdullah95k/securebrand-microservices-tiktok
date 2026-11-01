// Package ttstore opens tks's Postgres schema and migrates it (docs/tiktok
// ADR 0007 §4). Every table lives in schema tt; a pool from Open pins
// search_path to tt, so unqualified names (the copied code's) resolve
// there and nowhere else, and goose keeps tt's version table at
// tt.goose_db_version, apart from any other service's. In testing, tks has
// its own database (tks) on fbs's Postgres server; CheckSchema, run by the
// binaries at start, refuses a pool that would read another schema.
package ttstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	ttmigrations "github.com/abdullah95k/securebrand-microservices-tiktok/migrations/tt"
)

const (
	// Schema holds every tks table.
	Schema = "tt"
	// VersionTable is goose's version table for schema tt.
	VersionTable = Schema + ".goose_db_version"
)

// Tables are schema tt's partitioned tables: posts and comments by the
// post's month, account snapshots by month, metrics by day.
var Tables = store.Tables{
	Monthly: []string{"posts", "comments", "account_snapshots"},
	Daily:   []string{"post_metrics"},
	LZ4: map[string][]string{
		"posts":    {"caption", "text_ranges", "labels"},
		"comments": {"text", "text_ranges"},
	},
}

// Open creates a pool whose connections all use search_path tt, and checks
// it can connect.
func Open(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("ttstore: parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	// A runtime parameter is sent with every connection's startup, so no
	// connection of the pool ever sees another search_path. A DSN's own
	// search_path is overridden.
	cfg.ConnConfig.RuntimeParams["search_path"] = Schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ttstore: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ttstore: ping: %w", err)
	}
	return pool, nil
}

// ErrWrongSchema is returned by CheckSchema.
var ErrWrongSchema = errors.New("ttstore: not tks's schema")

// CheckSchema verifies that pool's connections resolve names in schema tt
// and that tt has been migrated. The binaries call it at start.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var cur *string
	var migrated bool
	err := pool.QueryRow(ctx, `SELECT current_schema(), to_regclass($1) IS NOT NULL`, VersionTable).Scan(&cur, &migrated)
	if err != nil {
		return fmt.Errorf("ttstore: check schema: %w", err)
	}
	if cur == nil || *cur != Schema {
		got := "none"
		if cur != nil {
			got = *cur
		}
		return fmt.Errorf("%w: current schema is %s, want %s (open the pool with ttstore.Open)", ErrWrongSchema, got, Schema)
	}
	if !migrated {
		return fmt.Errorf("%w: %s missing (run tksctl migrate up)", ErrWrongSchema, VersionTable)
	}
	return nil
}

func provider(ctx context.Context, pool *pgxpool.Pool) (*goose.Provider, *sql.DB, error) {
	// goose creates its version table first, so the schema must exist.
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{Schema}.Sanitize()); err != nil {
		return nil, nil, fmt.Errorf("ttstore: create schema: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, ttmigrations.FS, goose.WithTableName(VersionTable))
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ttstore: goose: %w", err)
	}
	return p, db, nil
}

// MigrateUp applies every pending migration of schema tt.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("ttstore: migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back one migration.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Down(ctx); err != nil {
		return fmt.Errorf("ttstore: migrate down: %w", err)
	}
	return nil
}

// MigrateDownTo rolls back every migration newer than version (0: all).
func MigrateDownTo(ctx context.Context, pool *pgxpool.Pool, version int64) error {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.DownTo(ctx, version); err != nil {
		return fmt.Errorf("ttstore: migrate down to %d: %w", version, err)
	}
	return nil
}

// MigrateStatus returns "version state path" lines.
func MigrateStatus(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	st, err := p.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(st))
	for _, s := range st {
		out = append(out, fmt.Sprintf("%05d %s %s", s.Source.Version, s.State, s.Source.Path))
	}
	return out, nil
}

// EnsurePartitions creates the missing partitions of tt's tables inside
// the window.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, w store.PartitionWindow) (int, error) {
	return Tables.Ensure(ctx, pool, now, w)
}

// Registry is schema tt's operation registry: fbs's fb_queries and
// fb_client_params under tks's names (ADR 0006 §3).
var Registry = registry.Tables{Queries: "operations", Params: "client_params"}
