// Command fbs-media-janitor deletes temporary media after the analysis
// service acknowledges it, and enforces the TTL and orphan cleanup.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

func main() {
	fs := flag.NewFlagSet("fbs-media-janitor", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-media-janitor", cfg.Log.Level, cfg.Log.Format)
	if err := runJanitor(context.Background(), cfg); err != nil {
		slog.Error("fbs-media-janitor exited", "err", err)
		os.Exit(1)
	}
}

func runJanitor(ctx context.Context, cfg *config.Config) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL, cfg.S3.MediaBucket)
	if err != nil {
		return err
	}
	kcfg, err := app.KafkaClient(cfg, "-janitor")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	j := &media.Janitor{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer,
		RequiredConsumers: cfg.Media.RequiredConsumers, MaxRetryableFailures: cfg.Media.MaxRetryableFailures, TTL: cfg.Media.TTL}
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-media-janitor",
		Topics: []string{topics.MediaProcessd}, MaxInflight: 32, DrainTimeout: 20 * time.Second}, j.Handle)
	if err != nil {
		return err
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, ops.Run, c.Run,
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "media-sweep", cfg.Media.SweepInterval, j.Sweep)
		},
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "media-orphans", cfg.Media.OrphanInterval, j.Orphans)
		},
	)
}
