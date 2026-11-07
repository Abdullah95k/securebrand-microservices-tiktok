package content

import (
	"context"
	"testing"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

func exportNew(topic string) proto.Message {
	switch topic {
	case "tt.post.v1":
		return &ttv1.Post{}
	case "tt.post_metrics.v1":
		return &ttv1.PostMetrics{}
	case "tt.comment.v1":
		return &ttv1.Comment{}
	}
	return nil
}

// The export sends what the tt.*.v1 topics hold up to the persister's
// committed offsets, through the change gate: a second run, or a run after
// the live persister sent the same records, sends nothing again, and the
// event ids are the persister's.
func TestExport(t *testing.T) {
	ctx := context.Background()
	brokers := testenv.Kafka(t)
	prefix := testenv.TopicPrefix()
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	tps := []string{"tt.post.v1", "tt.post_metrics.v1", "tt.comment.v1"}
	var specs []kafka.TopicSpec
	for _, n := range tps {
		specs = append(specs, kafka.TopicSpec{Name: n, Partitions: 2, Retention: time.Hour})
	}
	if err := kafka.EnsureTopics(ctx, cl, prefix, specs, 1); err != nil {
		t.Fatal(err)
	}

	// Two posts with metrics and a comment, then a later read of the
	// first post that the persister has not reached (past its offset).
	p1, p2 := post(t0, "caption", 100), post(t0, "second", 5)
	p2.PostId++
	c := comment(t0, 0, nil, nil)

	var recs []*kgo.Record
	add := func(topic string, m proto.Message, part int32) {
		b, _ := proto.Marshal(m)
		recs = append(recs, &kgo.Record{Topic: prefix + topic, Partition: part, Value: b})
	}
	for _, p := range []*ttv1.Post{p1, p2} {
		for _, m := range withMetrics(p) {
			topic := "tt.post.v1"
			if _, ok := m.(*ttv1.PostMetrics); ok {
				topic = "tt.post_metrics.v1"
			}
			add(topic, m, 0)
		}
	}
	add("tt.comment.v1", c, 1)
	pc, err := kgo.NewClient(append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if err := pc.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// The persister has committed everything produced so far.
	commit := kadm.Offsets{}
	for _, r := range recs {
		if o, ok := commit.Lookup(r.Topic, r.Partition); !ok || o.At < r.Offset+1 {
			commit.Add(kadm.Offset{Topic: r.Topic, Partition: r.Partition, At: r.Offset + 1, LeaderEpoch: -1})
		}
	}
	later := post(t0.Add(time.Hour), "edited caption", 100)
	b, _ := proto.Marshal(later)
	if err := pc.ProduceSync(ctx, &kgo.Record{Topic: prefix + "tt.post.v1", Partition: 0, Value: b}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// It commits nothing past them: the later read is the persister's.
	if cr, err := kadm.NewClient(pc).CommitOffsets(ctx, prefix+"tks-tt-persister", commit); err != nil || cr.Error() != nil {
		t.Fatal(err, cr.Error())
	}

	e, p := newEmitter(t)
	o := ExportOptions{Topics: append(tps, "tt.subtitle.v1"), Group: "tks-tt-persister", New: exportNew, Batch: 2, Idle: 10 * time.Second}
	res, err := Export(ctx, opts, prefix, e, o)
	if err != nil {
		t.Fatal(err)
	}
	got := p.take()
	if len(res.Missing) != 1 || res.Missing[0] != "tt.subtitle.v1" {
		t.Fatalf("missing topics %v", res.Missing)
	}
	if res.Read["tt.post.v1"] != 2 || res.Read["tt.post_metrics.v1"] != 2 || res.Read["tt.comment.v1"] != 1 {
		t.Fatalf("read %v", res.Read)
	}
	if len(got[topicPost]) != 2 || len(got[topicComment]) != 1 || res.Sent != int64(len(p.all(got))) || res.Invalid != 0 {
		t.Fatalf("sent %v, result %+v", counts(got), res)
	}
	var m contentv1.Post
	for _, r := range got[topicPost] {
		if err := proto.Unmarshal(r.Value, &m); err != nil {
			t.Fatal(err)
		}
		if m.Flags.GetEdited() {
			t.Fatal("the later read past the persister's offset was exported")
		}
		if headerOf(r, HeaderType) != "securebrand.content.v1.Post" || headerOf(r, HeaderContract) == "" {
			t.Fatalf("headers %v", r.Headers)
		}
	}
	// Its event id is the one the persister sends for the same record.
	want := latest(Map([]proto.Message{p1}, nil, nil))[0]
	want.Seal()
	found := false
	for _, r := range got[topicPost] {
		_ = proto.Unmarshal(r.Value, &m)
		found = found || m.Envelope.EventId == want.Msg.(*contentv1.Post).Envelope.EventId
	}
	if !found {
		t.Fatal("the export's event id differs from the persister's")
	}

	// Again: everything is held back by the gate.
	res, err = Export(ctx, opts, prefix, e, o)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.take(); len(got) != 0 || res.Sent != 0 || res.Unchanged == 0 {
		t.Fatalf("second run sent %v, result %+v", counts(got), res)
	}

	// --since after every record: nothing is read.
	o.Since = time.Now().Add(time.Hour)
	if res, err = Export(ctx, opts, prefix, e, o); err != nil || len(res.Read) != 0 {
		t.Fatalf("since the future: %+v %v", res, err)
	}
}

func (f *fakeProducer) all(by map[string][]*kgo.Record) []*kgo.Record {
	var out []*kgo.Record
	for _, rs := range by {
		out = append(out, rs...)
	}
	return out
}
