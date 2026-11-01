// Command tks-api serves tks's REST API (/v1, api/openapi/tks.v1.yaml) on
// schema tt: accounts, posts, metrics, comments, jobs, registrations and
// erasure requests (docs/tiktok ADR 0007 §13). The scheduler carries out
// the erasures; the API only queues them.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func main() {
	fs := flag.NewFlagSet("tks-api", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-api", cfg.Log.Level, cfg.Log.Format)
	if err := serve(context.Background(), cfg); err != nil {
		slog.Error("tks-api exited", "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, cfg *config.Config) error {
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	read := db
	if cfg.Postgres.ReadDSN != "" {
		if read, err = ttstore.Open(ctx, cfg.Postgres.ReadDSN, cfg.Postgres.MaxConns); err != nil {
			return err
		}
		defer read.Close()
	}
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := app.KafkaClient(cfg, "-api")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	srv := &api.Server{Cfg: cfg, DB: db, ReadDB: read, Redis: rdb,
		Dispatch: &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: producer}}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return httpserver.Serve(ctx, cfg.HTTP.Addr, srv.Handler(), httpserver.WithReadTimeout(cfg.HTTP.ReadTimeout))
	})
}
