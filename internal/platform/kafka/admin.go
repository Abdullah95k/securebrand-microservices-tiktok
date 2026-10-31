package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicSpec describes a topic to create or update.
type TopicSpec struct {
	Name       string        `yaml:"name"`
	Partitions int32         `yaml:"partitions"`
	Retention  time.Duration `yaml:"retention"`
	Replicas   int16         `yaml:"replicas"`
}

// EnsureTopics creates missing topics and raises partition counts and
// retention on existing ones. It never deletes or shrinks.
func EnsureTopics(ctx context.Context, cl *kgo.Client, prefix string, specs []TopicSpec, defaultReplicas int16) error {
	adm := kadm.NewClient(cl)
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = prefix + s.Name
	}
	existing, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("kafka: list topics: %w", err)
	}
	for _, s := range specs {
		name := prefix + s.Name
		repl := s.Replicas
		if repl == 0 {
			repl = defaultReplicas
		}
		cfgs := map[string]*string{
			"cleanup.policy":   ptr("delete"),
			"compression.type": ptr("producer"),
		}
		if s.Retention > 0 {
			cfgs["retention.ms"] = ptr(strconv.FormatInt(s.Retention.Milliseconds(), 10))
		}
		td, ok := existing[name]
		if !ok || td.Err != nil {
			resp, err := adm.CreateTopic(ctx, s.Partitions, repl, cfgs, name)
			if err != nil {
				return fmt.Errorf("kafka: create %s: %w", name, err)
			}
			if resp.Err != nil {
				return fmt.Errorf("kafka: create %s: %w", name, resp.Err)
			}
			continue
		}
		if int32(len(td.Partitions)) < s.Partitions {
			if _, err := adm.UpdatePartitions(ctx, int(s.Partitions), name); err != nil {
				return fmt.Errorf("kafka: grow %s: %w", name, err)
			}
		}
		if s.Retention > 0 {
			alter := []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: cfgs["retention.ms"]}}
			if _, err := adm.AlterTopicConfigs(ctx, alter, name); err != nil {
				return fmt.Errorf("kafka: alter %s: %w", name, err)
			}
		}
	}
	return nil
}

// GroupCaughtUp reports whether group has committed offsets at or beyond the
// given end offsets for every listed partition.
func GroupCaughtUp(ctx context.Context, cl *kgo.Client, group string, ends kadm.ListedOffsets) (bool, error) {
	adm := kadm.NewClient(cl)
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return false, err
	}
	caught := true
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Err != nil || lo.Offset <= 0 {
			return
		}
		c, ok := committed.Lookup(lo.Topic, lo.Partition)
		if !ok || c.Err != nil || c.At < lo.Offset {
			caught = false
		}
	})
	return caught, nil
}

// EndOffsets returns the current end offsets of topics.
func EndOffsets(ctx context.Context, cl *kgo.Client, topics ...string) (kadm.ListedOffsets, error) {
	return kadm.NewClient(cl).ListEndOffsets(ctx, topics...)
}

func ptr(s string) *string { return &s }
