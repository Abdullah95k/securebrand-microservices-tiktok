package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

func migrate(ctx context.Context, cfg *config.Config, sub string) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 4)
	if err != nil {
		return err
	}
	defer db.Close()
	switch sub {
	case "status":
		lines, err := store.MigrateStatus(ctx, db)
		if err != nil {
			return err
		}
		fmt.Println(strings.Join(lines, "\n"))
		return nil
	case "down":
		return store.MigrateDown(ctx, db)
	}
	if err := store.MigrateUp(ctx, db); err != nil {
		return err
	}
	b, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(b)
	if err != nil {
		return err
	}
	if err := registry.ApplySeed(ctx, db, seed); err != nil {
		return err
	}
	n, err := store.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{
		MonthsBack: cfg.Limits.MaxAgeCapDays/30 + 1, MonthsAhead: cfg.Scheduler.PartitionMonthsAhead, DaysBack: 1, DaysAhead: 14})
	if err != nil {
		return err
	}
	fmt.Printf("migrated; registry seeded (%d queries); %d partitions created\n", len(seed.Queries), n)
	return nil
}

func kafkaApply(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("kafka apply", flag.ExitOnError)
	pcap := fs.Int("partitions-cap", 0, "cap partition counts (local dev)")
	repl := fs.Int("replicas", 0, "override replication factor")
	_ = fs.Parse(args)
	b, err := configs.FS.ReadFile("topics.yaml")
	if err != nil {
		return err
	}
	var doc struct {
		Replicas int16             `yaml:"replicas"`
		Topics   []kafka.TopicSpec `yaml:"topics"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return err
	}
	if *repl > 0 {
		doc.Replicas = int16(*repl)
	}
	for i := range doc.Topics {
		if *pcap > 0 && doc.Topics[i].Partitions > int32(*pcap) {
			doc.Topics[i].Partitions = int32(*pcap)
		}
	}
	kcfg, err := app.KafkaClient(cfg, "-fbctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	if err := kafka.EnsureTopics(ctx, p.Client(), cfg.Kafka.TopicPrefix, doc.Topics, doc.Replicas); err != nil {
		return err
	}
	fmt.Printf("%d topics applied\n", len(doc.Topics))
	return nil
}

func apikeyCreate(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
	name := fs.String("name", "", "client name")
	scopes := fs.String("scopes", "jobs:write,pages:write,data:read", "comma separated scopes")
	rate := fs.Int("rate", 0, "requests per minute (0 = default)")
	_ = fs.Parse(args)
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	key, id, err := api.CreateClient(ctx, db, *name, strings.Split(*scopes, ","), *rate)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"client_id": id, "name": *name, "api_key": key, "note": "store this key now; it cannot be shown again"})
	return nil
}

func apikeyRevoke(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("apikey revoke", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix (the part after fbs_)")
	_ = fs.Parse(args)
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	tag, err := db.Exec(ctx, `UPDATE api_clients SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL`, *prefix)
	if err != nil {
		return err
	}
	fmt.Printf("%d key(s) revoked\n", tag.RowsAffected())
	return nil
}

func registryList(ctx context.Context, cfg *config.Config) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	qs, err := registry.List(ctx, db)
	if err != nil {
		return err
	}
	printJSON(qs)
	return nil
}

func registrySet(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("registry set", flag.ExitOnError)
	file := fs.String("file", "", "JSON file with a registry query (name, doc_id, friendly_name, variables, parser, ...)")
	_ = fs.Parse(args)
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var q registry.Query
	if err := json.Unmarshal(b, &q); err != nil {
		return err
	}
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := registry.Upsert(ctx, db, q); err != nil {
		return err
	}
	fmt.Printf("registry entry %s/%s saved\n", q.Name, q.DocID)
	return nil
}

// dlqReplay re-produces dead-lettered tasks to their original topics.
func dlqReplay(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
	reason := fs.String("reason", "", "only replay this x-reason")
	max := fs.Int("max", 1000, "maximum records")
	idle := fs.Duration("idle", 10*time.Second, "stop after this long without records")
	_ = fs.Parse(args)
	kcfg, err := app.KafkaClient(cfg, "-fbctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	cl, err := kgo.NewClient(append(kcfg.Opts(), kgo.ConsumerGroup(cfg.Kafka.TopicPrefix+"igs-dlq-replay"),
		kgo.ConsumeTopics(cfg.Kafka.TopicPrefix+topics.DLQ), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())...)
	if err != nil {
		return err
	}
	defer cl.Close()
	n, skipped := 0, 0
	for n < *max {
		pctx, cancel := context.WithTimeout(ctx, *idle)
		f := cl.PollRecords(pctx, 100)
		cancel()
		recs := f.Records()
		if len(recs) == 0 {
			break
		}
		var out []*kgo.Record
		for _, r := range recs {
			if *reason != "" && kafka.HeaderValue(r, kafka.HeaderReason) != *reason {
				skipped++
				continue
			}
			orig := kafka.HeaderValue(r, kafka.HeaderOrigTopic)
			if orig == "" {
				skipped++
				continue
			}
			out = append(out, &kgo.Record{Topic: p.Topic(strings.TrimPrefix(orig, cfg.Kafka.TopicPrefix)), Key: r.Key, Value: r.Value})
		}
		if err := p.ProduceSync(ctx, out...); err != nil {
			return err
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil {
			return err
		}
		n += len(out)
	}
	fmt.Printf("replayed %d task(s), skipped %d\n", n, skipped)
	return nil
}

// mediaAck simulates the analysis service: it acknowledges every ready
// media item (local development and end-to-end tests).
func mediaAck(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("media ack", flag.ExitOnError)
	consumer := fs.String("consumer", "analysis", "consumer name")
	status := fs.String("status", "processed", "processed | failed_permanent | failed_retryable")
	max := fs.Int("max", 10000, "maximum acks")
	_ = fs.Parse(args)
	st, ok := fbv1.ProcessStatus_value["PROCESS_STATUS_"+strings.ToUpper(*status)]
	if !ok {
		return fmt.Errorf("unknown status %q", *status)
	}
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query(ctx, `SELECT media_id::text FROM media WHERE status = 'ready' LIMIT $1`, *max)
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	kcfg, err := app.KafkaClient(cfg, "-fbctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	b := p.NewBatch()
	for _, id := range ids {
		if err := b.AddProto(ctx, topics.MediaProcessd, id, &fbv1.MediaProcessed{EventId: id + ":" + *consumer, MediaId: id,
			Consumer: *consumer, Status: fbv1.ProcessStatus(st), ProcessedAt: timestamppb.Now()}); err != nil {
			return err
		}
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	fmt.Printf("acknowledged %d media item(s)\n", len(ids))
	return nil
}

func fixturesScrub(args []string) error {
	fs := flag.NewFlagSet("fixtures scrub", flag.ExitOnError)
	dir := fs.String("dir", "", "fixture directory (response.ndjson|response.html + meta.json)")
	keep := fs.String("keep-ids", "", "comma separated ids to keep (page ids)")
	_ = fs.Parse(args)
	return fixtures.ScrubDir(*dir, strings.Split(*keep, ","))
}
