// Command tks-canary follows TikTok's changes to the web calls tks makes
// (build brief M8; docs/runbooks/tks-canary.md): every tt.canary.interval
// it calls each variant of the crawler's operations on the canary
// accounts, with their signed or unsigned twins, and promotes a working
// variant when the active one stops answering, or an unsigned one that
// answers where the active one needs a signature (internal/tt/canary).
// Run a single replica; runs are serialised by a Postgres advisory lock.
//
//	tks-canary [--config file]          run now, then every interval
//	tks-canary [--config file] --once   one run; exit 1 on error, 3 when degraded
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/canary"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/cost"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func main() {
	fs := flag.NewFlagSet("tks-canary", flag.ExitOnError)
	once := fs.Bool("once", false, "run once, print the report and exit")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-canary", cfg.Log.Level, cfg.Log.Format)
	code, err := runCanary(context.Background(), cfg, *once)
	if err != nil {
		slog.Error("tks-canary exited", "err", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runCanary(ctx context.Context, cfg *config.Config, once bool) (int, error) {
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, cfg.Postgres.MaxConns)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return 0, err
	}
	c, err := app.NewCanary(ctx, cfg, db, app.CanaryOptions{})
	if err != nil {
		return 0, err
	}
	defer c.Close()
	if err := c.InitMetrics(ctx); err != nil {
		slog.Warn("canary: last run unknown", "err", err)
	}
	check := func(ctx context.Context) (canary.Report, error) {
		ctx, cancel := context.WithTimeout(ctx, cfg.TT.Canary.Timeout)
		defer cancel()
		if err := c.Registry.Reload(ctx); err != nil {
			return canary.Report{}, err
		}
		return c.Run(ctx)
	}
	if once {
		rep, err := check(ctx)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		switch {
		case err != nil:
			return 0, err
		case rep.Status == canary.StatusDegraded:
			return 3, nil
		}
		return 0, nil
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	loops := []run.Func{ops.Run}
	if c.Signer != nil {
		// The signer pages' browser time, priced by tks's cost (A5).
		rdb, err := connect.Redis(ctx, cfg)
		if err != nil {
			return 0, err
		}
		defer func() { _ = rdb.Close() }()
		loops = append(loops, func(ctx context.Context) error {
			return cost.RunBrowserTime(ctx, rdb, cfg.Kafka.TopicPrefix, c.Signer, time.Minute)
		})
	}
	return 0, run.Group(ctx, append(loops, func(ctx context.Context) error {
		t := time.NewTicker(cfg.TT.Canary.Interval)
		defer t.Stop()
		for {
			// A failed run is in canary_runs and the metrics; the next one
			// tries again.
			if _, err := check(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("canary: run failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	})...)
}
