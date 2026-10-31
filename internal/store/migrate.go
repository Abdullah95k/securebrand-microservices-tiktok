// Package store holds database lifecycle helpers shared by several binaries:
// migrations and partition maintenance.
package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/abdullah95k/securebrand-microservices-igs/migrations"
)

func gooseProvider(pool *pgxpool.Pool) (*goose.Provider, *sql.DB, error) {
	db := stdlib.OpenDBFromPool(pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("store: goose: %w", err)
	}
	return p, db, nil
}

// MigrateUp applies all pending migrations.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := gooseProvider(pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back one migration.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := gooseProvider(pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Down(ctx); err != nil {
		return fmt.Errorf("store: migrate down: %w", err)
	}
	return nil
}

// MigrateStatus returns "version: applied" lines.
func MigrateStatus(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	p, db, err := gooseProvider(pool)
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
