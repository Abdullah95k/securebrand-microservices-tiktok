package content

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	contracts "github.com/abdullah95k/securebrand-contracts"
	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	coveragev1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/coverage/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"
	mediav1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/media/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// Producer sends records and waits for every acknowledgement.
type Producer interface {
	ProduceSync(ctx context.Context, recs ...*kgo.Record) error
	Topic(base string) string
}

var _ Producer = (*kafka.Producer)(nil)

// Emitter publishes the content.* events of a batch of tt.*.v1 records the
// persister has written. DB is schema tt's pool (ttstore.Open).
type Emitter struct {
	DB       *pgxpool.Pool
	Producer Producer
	// Options are the contract's conformance options every event is
	// checked with before it leaves; one that fails is counted and logged,
	// never sent.
	Options conformance.Options
	// Counts, when set, also tallies the outcomes for the caller.
	Counts *SendCounts
}

// Map maps a batch of tt.*.v1 records to contract events. owners gives the
// stored account of each post's user id, for its URL and Post.account;
// subtitles the stored subtitle tracks of each post, for Post.captions.
func Map(msgs []proto.Message, owners map[int64]*Owner, subtitles map[int64][]*ttv1.Subtitle) []*Event {
	var out []*Event
	for _, m := range msgs {
		switch m := m.(type) {
		case *ttv1.Account:
			out = append(out, Account(m)...)
		case *ttv1.Post:
			out = append(out, Post(m, owners[m.UserId], captionTracks(subtitles[m.PostId]))...)
		case *ttv1.PostSubtitles:
			if e := PostCaptions(m, subtitles[m.PostId]); e != nil {
				out = append(out, e)
			}
		case *ttv1.PostMetrics:
			out = append(out, PostMetrics(m)...)
		case *ttv1.Comment:
			out = append(out, Comment(m)...)
		case *ttv1.Music:
			out = append(out, Audio(m)...)
		case *ttv1.Media:
			out = append(out, Media(m)...)
		}
	}
	return out
}

// latest keeps the newest event per gate key (an older one could never
// pass the gate), sorted by key so concurrent batches lock gate rows in
// the same order.
func latest(es []*Event) []*Event {
	by := map[string]*Event{}
	for _, e := range es {
		if e.Observed.IsZero() {
			continue
		}
		if prev, ok := by[e.Gate]; !ok || e.Observed.After(prev.Observed) {
			by[e.Gate] = e
		}
	}
	out := make([]*Event, 0, len(by))
	for _, e := range by {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b *Event) int { return strings.Compare(a.Gate, b.Gate) })
	return out
}

// Emit maps msgs, passes them through the change gate and produces the
// events that passed, all before the gate's transaction commits.
func (e *Emitter) Emit(ctx context.Context, msgs []proto.Message) error {
	owners, err := e.owners(ctx, msgs)
	if err != nil {
		return fmt.Errorf("content: owners: %w", err)
	}
	subs, err := e.storedSubtitles(ctx, msgs)
	if err != nil {
		return fmt.Errorf("content: subtitles: %w", err)
	}
	return e.Send(ctx, Map(msgs, owners, subs))
}

// Send passes events through the change gate and produces those that
// passed, all before the gate's transaction commits: a failed produce
// rolls the gate back, so the caller's retry sends them again (with the
// same event ids).
func (e *Emitter) Send(ctx context.Context, all []*Event) error {
	evs := latest(all)
	if len(evs) == 0 {
		return nil
	}
	// One order for every sender: the finalizer sends several jobs'
	// coverage at once, and gate rows locked in different orders deadlock.
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].Gate < evs[j].Gate })
	return pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
		passed, err := gate(ctx, tx, evs)
		if err != nil {
			return fmt.Errorf("content: gate: %w", err)
		}
		recs := make([]*kgo.Record, 0, len(passed))
		sent := map[string]int{}
		for _, ev := range evs {
			edited, ok := passed[ev.Gate]
			if !ok {
				metrics.ContentEvents.WithLabelValues(ev.Topic, "unchanged").Inc()
				if e.Counts != nil {
					e.Counts.Unchanged++
				}
				continue
			}
			if edited != nil {
				ev.Edited(*edited)
			}
			ev.Seal()
			if err := Check(ev.Msg, e.Options); err != nil {
				metrics.ContentEvents.WithLabelValues(ev.Topic, "invalid").Inc()
				if e.Counts != nil {
					e.Counts.Invalid++
				}
				slog.Warn("content: event breaks the contract, not sent", "topic", ev.Topic, "id", ev.Key, "err", err)
				continue
			}
			rec, err := Record(e.Producer, ev.Topic, ev.Key, ev.Msg)
			if err != nil {
				return err
			}
			recs = append(recs, rec)
			sent[ev.Topic]++
		}
		if err := e.Producer.ProduceSync(ctx, recs...); err != nil {
			return fmt.Errorf("content: produce: %w", err)
		}
		for t, n := range sent {
			metrics.ContentEvents.WithLabelValues(t, "sent").Add(float64(n))
			if e.Counts != nil {
				e.Counts.Sent += int64(n)
			}
		}
		return nil
	})
}

var wire = proto.MarshalOptions{Deterministic: true}

// Record builds a contract record for topic (the producer's prefix
// applied): the deterministic wire form, with the contract's headers.
func Record(p Producer, topic, key string, m proto.Message) (*kgo.Record, error) {
	b, err := wire.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("content: marshal %s: %w", topic, err)
	}
	return &kgo.Record{Topic: p.Topic(topic), Key: []byte(key), Value: b, Headers: []kgo.RecordHeader{
		{Key: HeaderType, Value: []byte(proto.MessageName(m))},
		{Key: HeaderContract, Value: []byte(contracts.Version)},
		{Key: kafka.HeaderContentType, Value: []byte(kafka.ContentTypeProto)},
	}}, nil
}

// Check runs the contract's conformance check for a content message.
func Check(m proto.Message, opt conformance.Options) error {
	switch m := m.(type) {
	case *contentv1.Account:
		return conformance.Account(m, opt)
	case *contentv1.AccountSnapshot:
		return conformance.AccountSnapshot(m, opt)
	case *contentv1.Post:
		return conformance.Post(m, opt)
	case *contentv1.Comment:
		return conformance.Comment(m, opt)
	case *contentv1.Metrics:
		return conformance.Metrics(m, opt)
	case *contentv1.Media:
		return conformance.Media(m, opt)
	case *contentv1.Audio:
		return conformance.Envelope(m.GetEnvelope(), "audio", opt).Err()
	case *mediav1.MediaReady:
		return conformance.MediaReady(m, opt)
	case *mediav1.MediaProcessed:
		return conformance.MediaProcessed(m, opt)
	case *mediav1.MediaDeleted:
		return conformance.MediaDeleted(m, opt)
	case *discoveryv1.Candidate:
		return conformance.Candidate(m, opt)
	case *coveragev1.JobCoverage:
		return checkCoverage(m)
	}
	return fmt.Errorf("content: no check for %T", m)
}

// owners reads the stored accounts of the batch's posts.
func (e *Emitter) owners(ctx context.Context, msgs []proto.Message) (map[int64]*Owner, error) {
	var ids []int64
	for _, m := range msgs {
		if p, ok := m.(*ttv1.Post); ok && p.UserId != 0 {
			ids = append(ids, p.UserId)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	rows, err := e.DB.Query(ctx, `SELECT user_id, coalesce(username, ''), coalesce(nickname, ''), sec_uid, verified
		FROM accounts WHERE user_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]*Owner{}
	for rows.Next() {
		var id int64
		o := &Owner{}
		if err := rows.Scan(&id, &o.Username, &o.Nickname, &o.SecUID, &o.Verified); err != nil {
			return nil, err
		}
		out[id] = o
	}
	return out, rows.Err()
}

// gate records the batch's events in tt.content_state and returns those
// that pass, each with its edited time (nil: never edited). An event
// passes when it is newer than the last one sent on its stream and its
// content hash differs, or, for counts, the last one sent is a day older.
// A text that differs from the last one seen sets edited_at.
func gate(ctx context.Context, tx pgx.Tx, evs []*Event) (map[string]*time.Time, error) {
	ids := make([]string, len(evs))
	hashes := make([][]byte, len(evs))
	texts := make([][]byte, len(evs))
	ats := make([]time.Time, len(evs))
	for i, ev := range evs {
		ids[i], hashes[i], texts[i], ats[i] = ev.Gate, ev.Hash, ev.TextHash, ev.Observed
	}
	rows, err := tx.Query(ctx, `
		INSERT INTO content_state AS t (id, content_hash, text_hash, observed_at)
		SELECT * FROM unnest($1::text[], $2::bytea[], $3::bytea[], $4::timestamptz[])
		ON CONFLICT (id) DO UPDATE SET content_hash = EXCLUDED.content_hash,
			text_hash = COALESCE(EXCLUDED.text_hash, t.text_hash), observed_at = EXCLUDED.observed_at,
			edited_at = CASE WHEN EXCLUDED.text_hash IS NOT NULL AND t.text_hash IS NOT NULL
				AND EXCLUDED.text_hash <> t.text_hash THEN EXCLUDED.observed_at ELSE t.edited_at END
		WHERE t.observed_at < EXCLUDED.observed_at
		  AND (t.content_hash <> EXCLUDED.content_hash
		    OR (starts_with(EXCLUDED.id, 'metrics/') AND t.observed_at <= EXCLUDED.observed_at - interval '1 day'))
		RETURNING id, edited_at`, ids, hashes, texts, ats)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*time.Time{}
	for rows.Next() {
		var id string
		var edited *time.Time
		if err := rows.Scan(&id, &edited); err != nil {
			return nil, err
		}
		out[id] = edited
	}
	return out, rows.Err()
}
