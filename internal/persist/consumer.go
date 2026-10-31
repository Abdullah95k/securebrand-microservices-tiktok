package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Group is the persister's consumer group; the job finaliser waits for its
// committed offsets.
const Group = "igs-persister"

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
		if err := c.flush(ctx, recs); err != nil {
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
				if err = c.flush(ctx, recs); err == nil {
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

func (c *Consumer) flush(ctx context.Context, recs []*kgo.Record) error {
	var (
		pages    []*fbv1.Page
		posts    []*fbv1.Post
		comments []*fbv1.Comment
		media    []*fbv1.Media
	)
	for _, r := range recs {
		var err error
		switch r.Topic {
		case c.prefix + topics.Page:
			m := &fbv1.Page{}
			err = proto.Unmarshal(r.Value, m)
			pages = append(pages, m)
		case c.prefix + topics.Post:
			m := &fbv1.Post{}
			err = proto.Unmarshal(r.Value, m)
			posts = append(posts, m)
		case c.prefix + topics.Comment:
			m := &fbv1.Comment{}
			err = proto.Unmarshal(r.Value, m)
			comments = append(comments, m)
		case c.prefix + topics.Media:
			m := &fbv1.Media{}
			err = proto.Unmarshal(r.Value, m)
			media = append(media, m)
		}
		if err != nil {
			slog.Error("persist: undecodable record skipped", "topic", r.Topic, "offset", r.Offset, "err", err)
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
