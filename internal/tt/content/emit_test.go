package content

import (
	"context"
	"errors"
	"testing"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

type fakeProducer struct {
	recs []*kgo.Record
	fail bool
}

func (f *fakeProducer) Topic(base string) string { return base }

func (f *fakeProducer) ProduceSync(_ context.Context, recs ...*kgo.Record) error {
	if f.fail {
		return errors.New("broker down")
	}
	f.recs = append(f.recs, recs...)
	return nil
}

// take returns the records produced since the last call, by topic.
func (f *fakeProducer) take() map[string][]*kgo.Record {
	out := map[string][]*kgo.Record{}
	for _, r := range f.recs {
		out[r.Topic] = append(out[r.Topic], r)
	}
	f.recs = nil
	return out
}

func newEmitter(t *testing.T) (*Emitter, *fakeProducer) {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	p := &fakeProducer{}
	return &Emitter{DB: db, Producer: p}, p
}

func withMetrics(p *ttv1.Post) []proto.Message {
	return []proto.Message{p, &ttv1.PostMetrics{Obs: p.Obs, PostId: p.PostId, UserId: p.UserId, Counts: p.Counts}}
}

func counts(got map[string][]*kgo.Record) map[string]int {
	out := map[string]int{}
	for k, v := range got {
		out[k] = len(v)
	}
	return out
}

func TestGate(t *testing.T) {
	ctx := context.Background()
	e, p := newEmitter(t)
	step := func(name string, msgs []proto.Message, want map[string]int) map[string][]*kgo.Record {
		t.Helper()
		if err := e.Emit(ctx, msgs); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := p.take()
		for _, tp := range []string{topicPost, topicMetrics} {
			if len(got[tp]) != want[tp] {
				t.Fatalf("%s: %s sent %d, want %d (all: %v)", name, tp, len(got[tp]), want[tp], counts(got))
			}
		}
		return got
	}
	first := step("first read", withMetrics(post(t0, "caption", 100)), map[string]int{topicPost: 1, topicMetrics: 1})
	for _, r := range first[topicPost] {
		if string(r.Key) != "tt:post:7400000000000000001" || headerOf(r, HeaderContract) == "" || headerOf(r, HeaderType) != "securebrand.content.v1.Post" {
			t.Fatalf("record key %q headers %v", r.Key, r.Headers)
		}
	}
	step("redelivered", withMetrics(post(t0, "caption", 100)), nil)
	step("counts changed", withMetrics(post(t0.Add(time.Hour), "caption", 200)), map[string]int{topicMetrics: 1})
	step("nothing changed", withMetrics(post(t0.Add(2*time.Hour), "caption", 200)), nil)
	step("a day later", withMetrics(post(t0.Add(26*time.Hour), "caption", 200)), map[string]int{topicMetrics: 1})
	step("older read", withMetrics(post(t0.Add(-time.Hour), "other caption", 300)), nil)
	got := step("caption edited", withMetrics(post(t0.Add(27*time.Hour), "new caption", 200)), map[string]int{topicPost: 1})
	var m contentv1.Post
	if err := proto.Unmarshal(got[topicPost][0].Value, &m); err != nil {
		t.Fatal(err)
	}
	if !m.Flags.GetEdited() || !m.Flags.GetEditedAt().GetAt().AsTime().Equal(t0.Add(27*time.Hour)) {
		t.Fatalf("flags %v", m.Flags)
	}

	// A produce that fails rolls the gate back: the retry sends the same
	// event, with the same id.
	p.fail = true
	if err := e.Emit(ctx, withMetrics(post(t0.Add(28*time.Hour), "third caption", 200))); err == nil {
		t.Fatal("a failed produce passed")
	}
	p.fail = false
	got = step("retried", withMetrics(post(t0.Add(28*time.Hour), "third caption", 200)), map[string]int{topicPost: 1})
	again := []proto.Message{post(t0.Add(28*time.Hour), "third caption", 200)}
	evs := latest(Map(again, nil))
	evs[0].Seal()
	if err := proto.Unmarshal(got[topicPost][0].Value, &m); err != nil {
		t.Fatal(err)
	}
	if m.Envelope.EventId != evs[0].Msg.(*contentv1.Post).Envelope.EventId {
		t.Fatal("the retry's event id differs from the first attempt's")
	}
}

// A comment deleted and seen again is sent each time; a reply's counts go
// on content.metrics.v1 keyed by the post.
func TestCommentLifecycle(t *testing.T) {
	ctx := context.Background()
	e, p := newEmitter(t)
	c := comment(t0, 0, nil, nil)
	if err := e.Emit(ctx, []proto.Message{c}); err != nil {
		t.Fatal(err)
	}
	got := p.take()
	if len(got[topicComment]) != 1 || len(got[topicMetrics]) != 1 || string(got[topicMetrics][0].Key) != "tt:post:7400000000000000001" {
		t.Fatalf("first read: %v", counts(got))
	}
	del := &ttv1.Comment{Obs: obs(t0.Add(time.Hour)), CommentId: c.CommentId, PostId: c.PostId, Status: ttv1.CommentStatus_COMMENT_STATUS_DELETED}
	if err := e.Emit(ctx, []proto.Message{del}); err != nil {
		t.Fatal(err)
	}
	got = p.take()
	var m contentv1.Comment
	if len(got[topicComment]) != 1 || proto.Unmarshal(got[topicComment][0].Value, &m) != nil ||
		!m.Envelope.Partial || m.Status.Availability != contentv1.Availability_AVAILABILITY_DELETED {
		t.Fatalf("deletion: %v %v", counts(got), m.Status)
	}
	back := comment(t0.Add(2*time.Hour), 0, nil, nil)
	if err := e.Emit(ctx, []proto.Message{back}); err != nil {
		t.Fatal(err)
	}
	got = p.take()
	if len(got[topicComment]) != 1 || proto.Unmarshal(got[topicComment][0].Value, &m) != nil ||
		m.Status.Availability != contentv1.Availability_AVAILABILITY_VISIBLE || m.Flags.GetEdited() {
		t.Fatalf("seen again: %v %v %v", counts(got), m.Status, m.Flags)
	}
}

func headerOf(r *kgo.Record, k string) string {
	for _, h := range r.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}
