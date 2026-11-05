package main

import (
	"context"
	"flag"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dlq"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/stage"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// dlqReplay sends dead letters back to their topics (internal/tt/dlq):
// tasks readied to run again (done mark cleared, retry budget reset, job
// reopened), or records the persister set aside, as they were.
func dlqReplay(ctx context.Context, cfg *config.Config, args []string) (*dlq.Result, error) {
	fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
	from := fs.String("topic", topics.DLQ, "dead letters to replay: "+topics.DLQ+" (tasks) or "+topics.PersistQuarantine+
		" (records the persister set aside, replayed as they are)")
	reason := fs.String("reason", "", "only replay dead letters with this x-reason header")
	max := fs.Int("max", 1000, "maximum records replayed")
	idle := fs.Duration("idle", 10*time.Second, "stop after this long without records")
	_ = fs.Parse(args)
	kcfg, err := app.KafkaClient(cfg, "-tksctl")
	if err != nil {
		return nil, err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rp := &stage.Replayer{Ckpt: checkpoint.New(rdb), Jobs: jobs.New(rdb), DB: db}
	res, err := dlq.Replay(ctx, kcfg.Opts(), cfg.Kafka.TopicPrefix, p, rp,
		dlq.Options{Topic: *from, Reason: *reason, Max: *max, Idle: *idle})
	return &res, err
}
