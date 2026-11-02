// Command tks-persister writes tks's public records (tt.*.v1) to schema tt
// of tks's database, setting aside what cannot be written to
// tks.persist.dlq (docs/tiktok ADR 0007 §3, §4).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func main() {
	fs := flag.NewFlagSet("tks-persister", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-persister", cfg.Log.Level, cfg.Log.Format)
	if err := runPersister(context.Background(), cfg); err != nil {
		slog.Error("tks-persister exited", "err", err)
		os.Exit(1)
	}
}

func runPersister(ctx context.Context, cfg *config.Config) error {
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	c, err := app.NewPersister(cfg, db, 250*time.Millisecond, nil)
	if err != nil {
		return err
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, ops.Run, c.Run)
}
