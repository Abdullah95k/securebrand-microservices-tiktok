package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"

	protosrc "github.com/abdullah95k/securebrand-microservices-tiktok/api/proto"
	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/schemareg"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// migrateResult is what `tksctl migrate up` reports.
type migrateResult struct {
	Schema     string   `json:"schema"`
	Status     []string `json:"status"`
	Operations int      `json:"operations_seeded,omitempty"`
	Partitions int      `json:"partitions_created,omitempty"`
}

// migrate runs schema tt's migrations on postgres.dsn (database tks), then,
// on up, seeds the operation registry from tt.operation_seed and creates
// the partitions the backfill window and the months ahead need.
func migrate(ctx context.Context, cfg *config.Config, sub string) (*migrateResult, error) {
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, 4)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	res := &migrateResult{Schema: ttstore.Schema}
	switch sub {
	case "down":
		err = ttstore.MigrateDown(ctx, db)
	case "up":
		err = migrateUp(ctx, cfg, db, res)
	}
	if err != nil {
		return nil, err
	}
	if res.Status, err = ttstore.MigrateStatus(ctx, db); err != nil {
		return nil, err
	}
	return res, nil
}

func migrateUp(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, res *migrateResult) error {
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		return err
	}
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	b, err := configs.FS.ReadFile(cfg.TT.OperationSeed)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(b)
	if err != nil {
		return err
	}
	if err := registry.ApplySeedTo(ctx, db, ttstore.Registry, seed); err != nil {
		return err
	}
	res.Operations = len(seed.Queries)
	res.Partitions, err = ttstore.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{
		MonthsBack: cfg.TT.Freshness.BackfillDays/30 + 1, MonthsAhead: cfg.Scheduler.PartitionMonthsAhead, DaysBack: 1, DaysAhead: 14})
	return err
}

// topicsDoc is configs/tt/topics.yaml.
type topicsDoc struct {
	Replicas int16             `yaml:"replicas"`
	Topics   []kafka.TopicSpec `yaml:"topics"`
}

func readTopics() (topicsDoc, error) {
	var doc topicsDoc
	b, err := configs.FS.ReadFile("tt/topics.yaml")
	if err != nil {
		return doc, err
	}
	err = yaml.Unmarshal(b, &doc)
	return doc, err
}

// kafkaApply creates tks's topics, or grows their partitions and retention.
func kafkaApply(ctx context.Context, cfg *config.Config, args []string) (map[string]int, error) {
	fs := flag.NewFlagSet("kafka apply", flag.ExitOnError)
	pcap := fs.Int("partitions-cap", 0, "cap partition counts (local dev)")
	repl := fs.Int("replicas", 0, "override replication factor")
	_ = fs.Parse(args)
	doc, err := readTopics()
	if err != nil {
		return nil, err
	}
	if *repl > 0 {
		doc.Replicas = int16(*repl)
	}
	for i := range doc.Topics {
		if *pcap > 0 && doc.Topics[i].Partitions > int32(*pcap) {
			doc.Topics[i].Partitions = int32(*pcap)
		}
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
	if err := kafka.EnsureTopics(ctx, p.Client(), cfg.Kafka.TopicPrefix, doc.Topics, doc.Replicas); err != nil {
		return nil, err
	}
	return map[string]int{"topics_applied": len(doc.Topics)}, nil
}

// schemasApply registers each public topic's value schema as
// <prefix><topic>-value (and the files they import), BACKWARD_TRANSITIVE.
func schemasApply(ctx context.Context, cfg *config.Config, check bool) ([]schemareg.Result, error) {
	sr := cfg.Kafka.SchemaRegistry
	if sr.URL == "" {
		return nil, errors.New("kafka.schema_registry.url is not set")
	}
	c := &schemareg.Client{URL: sr.URL, Username: sr.Username, Password: sr.Password}
	res, err := schemareg.Apply(ctx, c, protosrc.FS, cfg.Kafka.TopicPrefix, topics.ValueSchemas, check)
	if err != nil {
		return res, fmt.Errorf("schemas: %w", err)
	}
	return res, nil
}
