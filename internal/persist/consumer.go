package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// Group is the persister's consumer group; the job finaliser waits for its
// committed offsets.
const Group = "tks-persister"

// Consumer reads entity topics in batches and writes them.
type Consumer struct {
	cl       *kgo.Client
	w        *Writer
	prefix   string
	maxBatch int
	linger   time.Duration
}

func NewConsumer(cfg kafka.ClientConfig, w *Writer, maxBatch int, linger time.Duration) (*Consumer, error) {
	ts := make([]string, len(topics.Entity))
	for i, t := range topics.Entity {
		ts[i] = cfg.TopicPrefix + t
	}
	cl, err := kgo.NewClient(append(cfg.Opts(),
		kgo.ConsumerGroup(cfg.TopicPrefix+Group),
		kgo.ConsumeTopics(ts...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)...)
	if err != nil {
		return nil, err
	}
	if maxBatch <= 0 {
		maxBatch = 2000
	}
	return &Consumer{cl: cl, w: w, prefix: cfg.TopicPrefix, maxBatch: maxBatch, linger: linger}, nil
}

// Run consumes until ctx is done. A batch is committed only after every
// record in it was written.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		var recs []*kgo.Record
		deadline := time.Now().Add(c.linger)
		for len(recs) < c.maxBatch {
			pctx, cancel := context.WithDeadline(ctx, deadline)
			f := c.cl.PollRecords(pctx, c.maxBatch-len(recs))
			cancel()
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
			f.EachError(func(t string, p int32, err error) {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					slog.Warn("persist: fetch error", "topic", t, "partition", p, "err", err)
				}
			})
			recs = append(recs, f.Records()...)
			if time.Now().After(deadline) || (len(recs) > 0 && f.NumRecords() == 0) {
				break
			}
		}
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}
		b := c.decode(recs)
		if err := c.persist(ctx, b); err != nil {
			if ctx.Err() != nil {
				// Shutting down: the batch is not committed and is read again.
				c.cl.AllowRebalance()
				return nil
			}
			slog.Error("persist: batch failed; retrying", "records", len(recs), "err", err)
			// Re-write the same batch until it succeeds: never commit past
			// unwritten data.
			for ctx.Err() == nil {
				time.Sleep(2 * time.Second)
				if err = c.persist(ctx, b); err == nil {
					break
				}
				slog.Error("persist: batch retry failed", "err", err)
			}
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
		}
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			slog.Warn("persist: commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}

// entry is one record of a batch with its decoded message.
type entry struct {
	rec *kgo.Record
	msg proto.Message
}

// batch is one poll's records, decoded once. A record that does not decode,
// or that Postgres refuses for what it holds, is set aside: produced to the
// quarantine topic (topics.PersistQuarantine) and skipped. A batch retried
// after another failure skips what it set aside before, instead of setting
// it aside twice.
type batch struct {
	entries     []entry
	undecodable []aside
	aside       map[*kgo.Record]bool
	refused     int
}

// aside is a record to set aside, and why.
type aside struct {
	rec            *kgo.Record
	reason, detail string
}

// maxRefused is how many records of one batch may be set aside as refused
// by Postgres. More is no bad record but a systematic error (a code or
// schema change): the batch is then retried, stalling the persister (its
// lag alert fires) instead of setting the stream's data aside.
const maxRefused = 3

func (c *Consumer) decode(recs []*kgo.Record) *batch {
	b := &batch{aside: map[*kgo.Record]bool{}}
	for _, r := range recs {
		var m proto.Message
		switch strings.TrimPrefix(r.Topic, c.prefix) {
		case topics.Page:
			m = &fbv1.Page{}
		case topics.Post:
			m = &fbv1.Post{}
		case topics.Comment:
			m = &fbv1.Comment{}
		case topics.Media:
			m = &fbv1.Media{}
		default:
			continue
		}
		// A record that does not decode is set aside, not written as far
		// as it decoded (Unmarshal leaves the fields it read).
		if err := proto.Unmarshal(r.Value, m); err != nil {
			b.undecodable = append(b.undecodable, aside{rec: r, reason: "undecodable", detail: err.Error()})
			continue
		}
		b.entries = append(b.entries, entry{rec: r, msg: m})
	}
	return b
}

// persist sets aside the batch's undecodable records and writes the rest,
// setting aside a record Postgres refuses for what it holds (isDataError):
// found by halving the batch, it would otherwise hold up every record after
// it for good. Rows are upserted by observation time, so writing the halves
// apart is the same as writing them together.
func (c *Consumer) persist(ctx context.Context, b *batch) error {
	for _, a := range b.undecodable {
		if err := c.setAside(ctx, b, a); err != nil {
			return err
		}
	}
	todo := make([]entry, 0, len(b.entries))
	for _, e := range b.entries {
		if !b.aside[e.rec] {
			todo = append(todo, e)
		}
	}
	return c.writeBisect(ctx, b, todo)
}

func (c *Consumer) writeBisect(ctx context.Context, b *batch, es []entry) error {
	err := c.write(ctx, es)
	if !isDataError(err) {
		return err
	}
	if len(es) == 1 {
		if b.refused >= maxRefused {
			return fmt.Errorf("persist: more than %d records of one batch refused; not setting aside more: %w", maxRefused, err)
		}
		var pgErr *pgconn.PgError
		errors.As(err, &pgErr)
		if err := c.setAside(ctx, b, aside{rec: es[0].rec, reason: "refused", detail: "sqlstate " + pgErr.Code}); err != nil {
			return err
		}
		b.refused++
		return nil
	}
	mid := len(es) / 2
	if err := c.writeBisect(ctx, b, es[:mid]); err != nil {
		return err
	}
	return c.writeBisect(ctx, b, es[mid:])
}

// isDataError reports Postgres refusing a record for what it holds:
// writing the same record again can never succeed. That is a data exception
// (SQLSTATE class 22), a NOT NULL field it lacks (23502), or a value too
// large for its index (54000). Other integrity errors can pass: a unique
// violation between two writers racing, the check violation of a partition
// not created yet (withPartitions).
func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "23502" || pgErr.Code == "54000"
}

// setAside produces a record to the quarantine topic, as it was read, with
// where it came from and why (never its content in the log: it holds
// personal data). An operator replays it once the cause is fixed: fbctl
// dlq replay --topic <prefix>fbs.persist.dlq.
func (c *Consumer) setAside(ctx context.Context, b *batch, a aside) error {
	if b.aside[a.rec] {
		return nil
	}
	r := a.rec
	out := &kgo.Record{Topic: c.prefix + topics.PersistQuarantine, Key: r.Key, Value: r.Value, Headers: []kgo.RecordHeader{
		{Key: kafka.HeaderOrigTopic, Value: []byte(r.Topic)},
		{Key: kafka.HeaderReason, Value: []byte(a.reason)},
		{Key: kafka.HeaderError, Value: []byte(fmt.Sprintf("%s (partition %d, offset %d)", a.detail, r.Partition, r.Offset))},
	}}
	for _, h := range r.Headers {
		if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType {
			out.Headers = append(out.Headers, h)
		}
	}
	if err := c.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("persist: set aside: %w", err)
	}
	b.aside[r] = true
	entity := entityOf(strings.TrimPrefix(r.Topic, c.prefix))
	metrics.PersistQuarantined.WithLabelValues(entity, a.reason).Inc()
	slog.Error("persist: record set aside", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
		"reason", a.reason, "detail", a.detail)
	return nil
}

func entityOf(topic string) string {
	switch topic {
	case topics.Page:
		return "page"
	case topics.Post:
		return "post"
	case topics.Comment:
		return "comment"
	case topics.Media:
		return "media"
	}
	return "unknown"
}

func (c *Consumer) write(ctx context.Context, es []entry) error {
	var (
		pages    []*fbv1.Page
		posts    []*fbv1.Post
		comments []*fbv1.Comment
		media    []*fbv1.Media
	)
	for _, e := range es {
		switch m := e.msg.(type) {
		case *fbv1.Page:
			pages = append(pages, m)
		case *fbv1.Post:
			posts = append(posts, m)
		case *fbv1.Comment:
			comments = append(comments, m)
		case *fbv1.Media:
			media = append(media, m)
		}
	}
	// Order matters only for references the API joins on; posts before
	// comments keeps post_index ahead of comment lookups.
	if err := c.w.Pages(ctx, pages); err != nil {
		return err
	}
	if err := c.w.Posts(ctx, posts); err != nil {
		return err
	}
	if err := c.w.Comments(ctx, comments); err != nil {
		return err
	}
	if err := c.w.Media(ctx, media); err != nil {
		return fmt.Errorf("media: %w", err)
	}
	return nil
}
