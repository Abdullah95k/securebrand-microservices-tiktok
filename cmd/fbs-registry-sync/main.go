// Command fbs-registry-sync follows Facebook's doc_id rotations: every
// registry_sync.interval it scans the canary pages' JS bundles for new
// doc_ids, checks them and the active ones with real requests on the
// canary pages, and promotes a working replacement (internal/registrysync).
// Run a single replica; runs are serialised by a Postgres advisory lock.
//
//	fbs-registry-sync [--config file]          run every interval
//	fbs-registry-sync [--config file] --once   one run; exit 1 on error, 3 when degraded
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scheduler"
)

func main() {
	fs := flag.NewFlagSet("fbs-registry-sync", flag.ExitOnError)
	once := fs.Bool("once", false, "run once, print the report and exit")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-registry-sync", cfg.Log.Level, cfg.Log.Format)
	code, err := runSync(context.Background(), cfg, *once)
	if err != nil {
		slog.Error("fbs-registry-sync exited", "err", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runSync(ctx context.Context, cfg *config.Config, once bool) (int, error) {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	s, reg, err := app.NewRegistrySync(ctx, cfg, db, nil)
	if err != nil {
		return 0, err
	}
	if err := s.InitMetrics(ctx); err != nil {
		slog.Warn("registry-sync: last run unknown", "err", err)
	}
	sync := func(ctx context.Context) (registrysync.Report, error) {
		ctx, cancel := context.WithTimeout(ctx, cfg.RegistrySync.Timeout)
		defer cancel()
		if err := reg.Reload(ctx); err != nil {
			return registrysync.Report{}, err
		}
		return s.Run(ctx)
	}
	if once {
		rep, err := sync(ctx)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		switch {
		case err != nil:
			return 0, err
		case rep.Status == registrysync.StatusDegraded:
			return 3, nil
		}
		return 0, nil
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	return 0, run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return scheduler.Every(ctx, "registry-sync", cfg.RegistrySync.Interval, func(ctx context.Context) error {
			_, err := sync(ctx)
			return err
		})
	})
}
