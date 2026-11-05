package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dlq"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/stage"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// TestDeadLetterReplay: while the signer's signatures are refused, the
// comment tasks give up (one attempt here) and go to tks.task.dlq, and the
// backfills end partial. Once signing works again (a deploy), the replay
// tksctl dlq replay runs readies and sends them back: every job runs again, is
// finalised completed, and the store equals fakett's world. A second
// replay has nothing left to send.
func TestDeadLetterReplay(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 51, Accounts: 2, PostsPerAccount: 6, MaxComments: 12, MaxReplies: 3, PostSpacing: 30 * time.Hour},
		Config: func(c *config.Config) { c.Crawler.MaxAttempts = 1 }})
	ctx := context.Background()
	accs := s.World.Accounts()
	sc := newSched(s)
	sc.register(t, accs...)
	s.Signer.Refuse.Store(true)
	dead := int64(0)
	for ref, j := range sc.tick(t) {
		if j.status != "partial" || j.counters["tasks_failed"] == 0 {
			t.Fatalf("%s: job %+v, want partial with failed tasks", ref, j)
		}
		dead += j.counters["tasks_failed"]
	}
	if n := count(t, s, `SELECT count(*) FROM comments`); n != 0 {
		t.Fatalf("%d comments stored from refused calls", n)
	}

	// The fix is a deploy: new crawlers, whose operation breakers are
	// closed (the refusals opened the signed comments' one).
	s.Signer.Refuse.Store(false)
	s.RestartCrawler(t)
	replay := func() dlq.Result {
		t.Helper()
		rp := &stage.Replayer{Ckpt: checkpoint.New(s.Redis), Jobs: s.Tracker, DB: s.DB}
		res, err := dlq.Replay(ctx, []kgo.Opt{kgo.SeedBrokers(s.Cfg.Kafka.Brokers...)}, s.Cfg.Kafka.TopicPrefix, s.Producer, rp,
			dlq.Options{Topic: topics.DLQ, Idle: 3 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := replay()
	if int64(res.Replayed) != dead || int64(res.Reopened) != dead || res.Skipped != 0 {
		t.Fatalf("replay %+v, want %d tasks replayed and reopened", res, dead)
	}
	ids := []string{}
	rows, err := s.DB.Query(ctx, `SELECT job_id::text FROM jobs WHERE status = 'running'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != len(accs) {
		t.Fatalf("%d jobs running again, want %d", len(ids), len(accs))
	}
	s.Wait(t, 3*time.Minute, ids...)
	deadline := time.Now().Add(time.Minute)
	for count(t, s, `SELECT count(*) FROM jobs WHERE status = 'running'`) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("replayed jobs not finalised")
		}
		if err := sc.fin.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE status <> 'completed'`); n != 0 {
		t.Errorf("%d jobs not completed after the replay", n)
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
	if res := replay(); res.Replayed != 0 {
		t.Errorf("second replay %+v, want nothing", res)
	}
}
