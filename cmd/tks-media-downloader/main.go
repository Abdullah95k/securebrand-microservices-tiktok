// Command tks-media-downloader downloads TikTok media files into tks's
// bucket, announces each on tt.media.ready.v1, and records the consumers'
// acknowledgements (tt.media.processed.v1) and the deletions another service
// makes (tt.media.deleted.v1). It deletes nothing and sets no TTL (build
// brief D5; docs/tiktok ADR 0007 §15). Tasks come only when
// tt.media.include is on (off by default).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func main() {
	fs := flag.NewFlagSet("tks-media-downloader", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-media-downloader", cfg.Log.Level, cfg.Log.Format)
	if err := runMedia(context.Background(), cfg); err != nil {
		slog.Error("tks-media-downloader exited", "err", err)
		os.Exit(1)
	}
}

func runMedia(ctx context.Context, cfg *config.Config) error {
	// /healthz answers while the database is still being retried; /readyz
	// waits for ops.Run.
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.Early()
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	s3, err := app.OpenS3(ctx, cfg, cfg.S3.MediaBucket)
	if err != nil {
		return err
	}
	m, err := app.NewMedia(ctx, cfg, app.MediaOptions{DB: db, S3: s3})
	if err != nil {
		return err
	}
	defer m.Producer.Close()
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, append([]run.Func{ops.Run}, m.Components...)...)
}
