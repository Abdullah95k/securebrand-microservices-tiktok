package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// contentExport is `tksctl content export`: the content.* events of what
// was stored before tt.content was switched on (or of any window Kafka
// still holds), sent through the persister's own emitter. Safe beside the
// live persister and safe to run again (content.Export).
func contentExport(ctx context.Context, cfg *config.Config, args []string) (*content.ExportResult, error) {
	fs := flag.NewFlagSet("content export", flag.ExitOnError)
	since := fs.String("since", "", "start at records from this time (RFC 3339) or this long ago (e.g. 24h); empty: everything the tt.*.v1 topics still hold (168 h)")
	batch := fs.Int("batch", 500, "records per emit")
	idle := fs.Duration("idle", 30*time.Second, "fail after this long without a record while partitions are unfinished")
	_ = fs.Parse(args)
	from, err := parseSince(*since, time.Now())
	if err != nil {
		return nil, err
	}
	kcfg, err := app.KafkaClient(cfg, "-tksctl")
	if err != nil {
		return nil, err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 4)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	e := &content.Emitter{DB: db, Producer: p, Options: conformance.Options{Production: cfg.TT.Content.Production}}
	slog.Info("content export: reading tt.*.v1 up to the persister's offsets", "since", from)
	res, err := content.Export(ctx, kcfg.Opts(), cfg.Kafka.TopicPrefix, e, content.ExportOptions{
		Topics: topics.Persisted, Group: ttpersist.Group, New: ttpersist.Sink{}.New, Since: from, Batch: *batch, Idle: *idle})
	return &res, err
}

// parseSince reads --since: empty (the zero time), an RFC 3339 time, or a
// duration before now.
func parseSince(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return now.Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("--since %q: want an RFC 3339 time or a duration such as 24h", s)
}
