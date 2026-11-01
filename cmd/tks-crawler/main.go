// Command tks-crawler runs tks's crawl stages (account: post lists and the
// account; post: comments; thread: replies) and the retry relay as Kafka
// consumers on tks.task.*, publishing tt.*.v1 (docs/tiktok build brief §4).
// Run one Deployment per stage with --stages, or every stage in one process
// for small setups. Comments and replies need the browser signer
// (tt.signer.mode browser: Chromium in the image).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func main() {
	fs := flag.NewFlagSet("tks-crawler", flag.ExitOnError)
	stagesFlag := fs.String("stages", strings.Join(app.Stages, ","), "comma separated stages: "+strings.Join(app.Stages, ","))
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-crawler", cfg.Log.Level, cfg.Log.Format)
	if err := runCrawler(context.Background(), cfg, strings.Split(*stagesFlag, ",")); err != nil {
		slog.Error("tks-crawler exited", "err", err)
		os.Exit(1)
	}
}

func runCrawler(ctx context.Context, cfg *config.Config, stages []string) error {
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	rdb, err := connect.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := connect.Kafka(cfg, "-crawler")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	var raw *minio.Client
	if cfg.TT.CaptureRaw != "none" {
		if raw, err = s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL, cfg.S3.RawBucket); err != nil {
			return err
		}
	}
	c, err := app.NewCrawler(ctx, cfg, db, rdb, producer, app.CrawlerOptions{Stages: stages, RawS3: raw})
	if err != nil {
		return err
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("kafka", producer.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, append([]run.Func{ops.Run}, c.Components...)...)
}
