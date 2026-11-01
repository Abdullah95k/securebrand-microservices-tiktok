package main

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// `tksctl migrate up` migrates schema tt, seeds the operations and creates
// partitions; running it again changes nothing; down rolls back one step.
func TestMigrate(t *testing.T) {
	cfg := testConfig(t)
	cfg.Postgres.DSN = testenv.EmptyPostgres(t)
	ctx := context.Background()
	res, err := migrate(ctx, cfg, "up")
	if err != nil {
		t.Fatal(err)
	}
	if res.Schema != "tt" || res.Operations == 0 || res.Partitions == 0 || len(res.Status) == 0 {
		t.Fatalf("%+v", res)
	}
	again, err := migrate(ctx, cfg, "up")
	if err != nil {
		t.Fatal(err)
	}
	if again.Partitions != 0 || again.Operations != res.Operations {
		t.Fatalf("not idempotent: %+v", again)
	}
	if _, err := migrate(ctx, cfg, "down"); err != nil {
		t.Fatal(err)
	}
	if _, err := migrate(ctx, cfg, "up"); err != nil {
		t.Fatal(err)
	}
}

// `tksctl kafka apply` creates every topic of configs/tt/topics.yaml under
// the topic prefix, with the partition cap.
func TestKafkaApply(t *testing.T) {
	cfg := testConfig(t)
	cfg.Kafka.Brokers = testenv.Kafka(t)
	cfg.Kafka.TopicPrefix = testenv.TopicPrefix()
	ctx := context.Background()
	out, err := kafkaApply(ctx, cfg, []string{"--partitions-cap", "2", "--replicas", "1"})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := readTopics()
	if err != nil {
		t.Fatal(err)
	}
	if out["topics_applied"] != len(doc.Topics) {
		t.Fatalf("%v", out)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	names := make([]string, 0, len(doc.Topics))
	for _, tp := range doc.Topics {
		names = append(names, cfg.Kafka.TopicPrefix+tp.Name)
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(context.Background(), names...) })
	td, err := adm.ListTopics(ctx, names...)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		d, ok := td[n]
		if !ok || d.Err != nil || len(d.Partitions) != 2 {
			t.Errorf("%s: %+v", n, d)
		}
	}
	if _, ok := td[cfg.Kafka.TopicPrefix+topics.Comment]; !ok {
		t.Fatal("tt.comment.v1 missing")
	}
}

func TestSchemasApplyNeedsRegistry(t *testing.T) {
	cfg := testConfig(t)
	cfg.Kafka.SchemaRegistry.URL = ""
	if _, err := schemasApply(context.Background(), cfg, true); err == nil {
		t.Fatal("no registry URL accepted")
	}
	cfg.Kafka.SchemaRegistry.URL = testenv.SchemaRegistry(t)
	cfg.Kafka.TopicPrefix = testenv.TopicPrefix()
	res, err := schemasApply(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(topics.ValueSchemas)+2 {
		t.Fatalf("%d results", len(res))
	}
}
