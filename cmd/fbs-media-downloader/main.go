// Command fbs-media-downloader downloads discovered media into temporary
// object storage and announces it on fb.media.ready.v1.
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
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

func main() {
	fs := flag.NewFlagSet("fbs-media-downloader", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-media-downloader", cfg.Log.Level, cfg.Log.Format)
	if err := runDownloader(context.Background(), cfg); err != nil {
		slog.Error("fbs-media-downloader exited", "err", err)
		os.Exit(1)
	}
}

func runDownloader(ctx context.Context, cfg *config.Config) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL, cfg.S3.MediaBucket)
	if err != nil {
		return err
	}
	media.Lifecycle(ctx, s3, cfg.S3.MediaBucket, cfg.Media.TTL)
	kcfg, err := app.KafkaClient(cfg, "-media")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	d := &media.Downloader{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer, HTTP: media.NewHTTPClient(),
		TTL: cfg.Media.TTL, MaxBytes: cfg.Media.MaxBytes, Presign: cfg.Media.Presign, PresignTTL: cfg.Media.PresignTTL}
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-media-downloader",
		Topics: []string{topics.TaskMedia}, MaxInflight: cfg.Media.Concurrency, DrainTimeout: 60 * time.Second}, d.Handle)
	if err != nil {
		return err
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, ops.Run, c.Run)
}
