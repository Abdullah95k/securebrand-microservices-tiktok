// Command fbs-crawler runs the crawl stages (page timeline, post comments,
// reply threads) and the retry relay as Kafka consumers. Run one Deployment
// per stage with --stages, or all stages in one process for small setups.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
)

func main() {
	fs := flag.NewFlagSet("fbs-crawler", flag.ExitOnError)
	stagesFlag := fs.String("stages", "", "comma separated stages: page,post,thread,retry (default crawler.stages)")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-crawler", cfg.Log.Level, cfg.Log.Format)
	var stages []string
	if *stagesFlag != "" {
		stages = strings.Split(*stagesFlag, ",")
	}
	if err := runCrawler(context.Background(), cfg, stages); err != nil {
		slog.Error("fbs-crawler exited", "err", err)
		os.Exit(1)
	}
}

func runCrawler(ctx context.Context, cfg *config.Config, stages []string) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := app.KafkaClient(cfg, "-crawler")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	var raw *minio.Client
	if cfg.FB.CaptureRaw != "none" {
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
