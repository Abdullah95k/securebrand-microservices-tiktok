package persist

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// A record that does not decode, or that Postgres refuses for what it
// holds, is set aside in the quarantine topic and the rest of its batch is
// written; before M2 the persister retried such a batch forever (and
// wrote an undecodable record as far as it decoded). Many refusals in one
// batch are a systematic error: the batch then fails, and is retried,
// instead of setting the stream's data aside.
func TestBadRecordsAreSetAside(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	kcfg := kafka.ClientConfig{Brokers: brokers, ClientID: "persist-test", TopicPrefix: prefix, Linger: time.Millisecond}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if err := kafka.EnsureTopics(ctx, p.Client(), prefix, []kafka.TopicSpec{{Name: topics.PersistQuarantine, Partitions: 1}}, 1); err != nil {
		t.Fatal(err)
	}
	c, err := NewConsumer(kcfg, &Writer{DB: db}, 100, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.cl.Close)
	q, err := kgo.NewClient(append(kcfg.Opts(), kgo.ConsumeTopics(prefix+topics.PersistQuarantine),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	quarantined := func(want int) []*kgo.Record {
		t.Helper()
		var got []*kgo.Record
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		for len(got) < want+1 && pctx.Err() == nil { // read past want to see none more
			got = append(got, q.PollFetches(pctx).Records()...)
		}
		if len(got) != want {
			t.Fatalf("%d records set aside, want %d", len(got), want)
		}
		return got
	}

	// Postgres refuses comment 11 and those from 20 on for their data
	// (22023). (A NUL character, the refusal seen in practice, is now
	// stripped before writing: TestBatchesThatPostgresWouldRefuse.)
	if _, err := db.Exec(ctx, `CREATE FUNCTION refuse_some() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.comment_id = 11 OR NEW.comment_id >= 20 THEN RAISE EXCEPTION 'refused' USING ERRCODE = '22023'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER refuse_some BEFORE INSERT ON comments FOR EACH ROW EXECUTE FUNCTION refuse_some()`); err != nil {
		t.Fatal(err)
	}
	created := timestamppb.New(now.Add(-time.Hour))
	offset := int64(0)
	rec := func(topic string, m proto.Message) *kgo.Record {
		offset++
		b, _ := proto.Marshal(m)
		return &kgo.Record{Topic: prefix + topic, Key: []byte("k"), Value: b, Offset: offset}
	}
	comment := func(id int64, text string) *fbv1.Comment {
		return &fbv1.Comment{Obs: obs(now, text), CommentId: id, PostId: 1, PageId: 9, PostCreatedAt: created, Text: text,
			Author: &fbv1.Author{AuthorKey: []byte{byte(id)}}}
	}
	bad := &kgo.Record{Topic: prefix + topics.Post, Key: []byte("k"), Value: []byte{0xff, 0xff}, Offset: 99}
	b := c.decode([]*kgo.Record{
		rec(topics.Post, &fbv1.Post{Obs: obs(now, "p"), PostId: 1, PageId: 9, CreatedAt: created}),
		bad,
		rec(topics.Comment, comment(10, "kept")),
		rec(topics.Comment, comment(11, "refused")),
		rec(topics.Comment, comment(12, "kept too")),
	})
	if err := c.persist(ctx, b); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]int{
		`SELECT count(*) FROM posts WHERE post_id = 1`:                   1,
		`SELECT count(*) FROM comments WHERE comment_id IN (10, 12)`:     2,
		`SELECT count(*) FROM comments WHERE comment_id = 11`:            0,
		`SELECT count(*) FROM posts WHERE post_id <> 1 OR page_id <> 9`:  0, // nothing of the undecodable record
		`SELECT count(*) FROM comment_authors WHERE author_key = '\x0b'`: 0,
	} {
		var n int
		if err := db.QueryRow(ctx, q).Scan(&n); err != nil || n != want {
			t.Fatalf("%s = %d (%v), want %d", q, n, err, want)
		}
	}
	got := quarantined(2)
	reasons := map[string]string{}
	for _, r := range got {
		reasons[kafka.HeaderValue(r, kafka.HeaderReason)] = kafka.HeaderValue(r, kafka.HeaderError)
		if kafka.HeaderValue(r, kafka.HeaderOrigTopic) == "" {
			t.Fatal("no original topic")
		}
	}
	if !strings.HasPrefix(reasons["refused"], "sqlstate 22023") || reasons["undecodable"] == "" || string(got[0].Value) != string(bad.Value) {
		t.Fatalf("set aside: %v", reasons)
	}
	// A batch written again (after some other failure) sets nothing aside twice.
	if err := c.persist(ctx, b); err != nil {
		t.Fatal(err)
	}
	quarantined(0)

	// More refusals than maxRefused in one batch: no bad record but a
	// systematic error; the batch fails and is retried by the caller.
	var many []*kgo.Record
	for i := range maxRefused + 1 {
		many = append(many, rec(topics.Comment, comment(int64(20+i), "refused "+string(rune('a'+i)))))
	}
	err = c.persist(ctx, c.decode(many))
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a systematic error must fail the batch: %v", err)
	}
	quarantined(maxRefused)
}

// Only errors a retry can never fix count as a refused record: a race
// between writers (unique violation) or a partition not created yet (check
// violation, joined with the reason creating it failed) are retried.
func TestIsDataError(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code} }
	for err, want := range map[error]bool{
		pg("22023"): true,  // a refused value
		pg("22P05"): true,  // a NUL character
		pg("23502"): true,  // a NOT NULL field missing
		pg("54000"): true,  // a value too large for its index
		pg("23505"): false, // two writers racing for a unique value
		pg("23514"): false, // no partition for the row
		errors.Join(pg("23514"), errors.New("lock timeout")): false,
		pg("40001"):                    false,
		errors.New("connection reset"): false,
	} {
		if got := isDataError(err); got != want {
			t.Errorf("%v: %v, want %v", err, got, want)
		}
	}
}
