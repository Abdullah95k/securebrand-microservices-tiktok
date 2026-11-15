package main

import (
	"context"
	"flag"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

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
	// The platform's managed Kafka grants tks's user produce and consume
	// on tt.* and tks.* only, without cluster permissions; a replay refused
	// there with CLUSTER_AUTHORIZATION_FAILED (2026-11-15). A replayed task
	// does not need idempotent writes: a task sent twice is skipped by its
	// done marker, and the persister's writes are idempotent.
	noIdem := fs.Bool("no-idempotent", false, "produce without idempotent writes (no cluster IdempotentWrite permission needed)")
	_ = fs.Parse(args)
	kcfg, err := app.KafkaClient(cfg, "-tksctl")
	if err != nil {
		return nil, err
	}
	var extra []kgo.Opt
	if *noIdem {
		extra = append(extra, kgo.DisableIdempotentWrite())
	}
	p, err := kafka.NewProducer(kcfg, extra...)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 2)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rp := &stage.Replayer{Ckpt: checkpoint.New(rdb), Jobs: jobs.New(rdb), DB: db}
	res, err := dlq.Replay(ctx, kcfg.Opts(), cfg.Kafka.TopicPrefix, p, rp,
		dlq.Options{Topic: *from, Reason: *reason, Max: *max, Idle: *idle})
	return &res, err
}
