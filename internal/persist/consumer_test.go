package persist

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

// rowSink writes each record (an id) as one row of table rows, a batch in
// one transaction, as a schema's sink does.
type rowSink struct{ db *pgxpool.Pool }

func (rowSink) Topics() []string { return []string{"rows"} }

func (rowSink) New(topic string) proto.Message {
	if topic != "rows" {
		return nil
	}
	return &wrapperspb.Int64Value{}
}

func (rowSink) Entity(string) string { return "row" }

func (s rowSink) Write(ctx context.Context, msgs []proto.Message) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, m := range msgs {
		if _, err := tx.Exec(ctx, `INSERT INTO rows VALUES ($1) ON CONFLICT DO NOTHING`, m.(*wrapperspb.Int64Value).Value); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

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
	// Postgres refuses row 11 and those from 20 on for their data (22023).
	if _, err := db.Exec(ctx, `CREATE TABLE rows (id bigint PRIMARY KEY);
		CREATE FUNCTION refuse_some() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.id = 11 OR NEW.id >= 20 THEN RAISE EXCEPTION 'refused' USING ERRCODE = '22023'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER refuse_some BEFORE INSERT ON rows FOR EACH ROW EXECUTE FUNCTION refuse_some()`); err != nil {
		t.Fatal(err)
	}
	const quarantine = "test.persist.dlq"
	prefix := testenv.TopicPrefix()
	kcfg := kafka.ClientConfig{Brokers: brokers, ClientID: "persist-test", TopicPrefix: prefix, Linger: time.Millisecond}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if err := kafka.EnsureTopics(ctx, p.Client(), prefix, []kafka.TopicSpec{{Name: quarantine, Partitions: 1}}, 1); err != nil {
		t.Fatal(err)
	}
	c, err := NewSinkConsumer(kcfg, rowSink{db}, SinkConfig{Group: "test-persister", Quarantine: quarantine}, 100, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.cl.Close)
	q, err := kgo.NewClient(append(kcfg.Opts(), kgo.ConsumeTopics(prefix+quarantine),
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

	offset := int64(0)
	rec := func(id int64) *kgo.Record {
		offset++
		b, _ := proto.Marshal(wrapperspb.Int64(id))
		return &kgo.Record{Topic: prefix + "rows", Key: []byte("k"), Value: b, Offset: offset}
	}
	bad := &kgo.Record{Topic: prefix + "rows", Key: []byte("k"), Value: []byte{0xff, 0xff}, Offset: 99}
	b := c.decode([]*kgo.Record{rec(1), bad, rec(10), rec(11), rec(12)})
	if err := c.persist(ctx, b); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	rows, err := db.Query(ctx, `SELECT id FROM rows ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 10 || ids[2] != 12 {
		t.Fatalf("written %v, want [1 10 12]", ids)
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
		many = append(many, rec(int64(20+i)))
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

func TestErrClass(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{&pgconn.PgError{Code: "57014"}, "57"},
		{fmt.Errorf("write comments: %w", &pgconn.PgError{Code: "40P01"}), "40"},
		{errors.New("kafka: produce failed"), "other"},
		{&pgconn.PgError{}, "other"},
	} {
		if got := errClass(c.err); got != c.want {
			t.Errorf("errClass(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
