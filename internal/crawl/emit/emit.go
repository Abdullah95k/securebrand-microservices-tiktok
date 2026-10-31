// Package emit publishes parsed entities on the public fb.* topics. It
// stamps each message with an Observation (deterministic content hash and
// event id) and applies the privacy rules (author pseudonymisation,
// suppression) before anything leaves the crawler.
package emit

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Source describes how entities were obtained.
type Source struct {
	JobID         string
	Query         string
	DocID         string
	ParserVersion uint32
	Mode          fbv1.CrawlMode
}

// Emitter publishes entities.
type Emitter struct {
	Producer     *kafka.Producer
	HMACKey      []byte
	Pseudonymize bool
	Suppression  *gdpr.Suppression
	Now          func() time.Time
}

func (e *Emitter) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now().UTC()
}

var marshal = proto.MarshalOptions{Deterministic: true}

// stamp computes the content hash over msg without its observation and
// sets a fresh Observation.
func (e *Emitter) stamp(src Source, entityKey string, msg proto.Message, set func(*fbv1.Observation)) {
	set(nil)
	b, _ := marshal.Marshal(hashable(msg))
	h := xxh3.Hash128(b).Bytes()
	ev := xxh3.Hash128(append([]byte(entityKey+"|"), h[:]...)).Bytes()
	var jobs []string
	if src.JobID != "" {
		jobs = []string{src.JobID}
	}
	set(&fbv1.Observation{
		EventId: hex.EncodeToString(ev[:]), ObservedAt: timestamppb.New(e.now()), JobIds: jobs,
		QueryName: src.Query, DocId: src.DocID, ParserVersion: src.ParserVersion, ContentHash: h[:], Mode: src.Mode,
	})
}

// Page publishes a page profile.
func (e *Emitter) Page(ctx context.Context, b *kafka.Batch, src Source, p *fbv1.Page) error {
	e.stamp(src, "page:"+strconv.FormatInt(p.PageId, 10), p, func(o *fbv1.Observation) { p.Obs = o })
	return b.AddProto(ctx, topics.Page, strconv.FormatInt(p.PageId, 10), p)
}

// Post publishes a post and an engagement sample.
func (e *Emitter) Post(ctx context.Context, b *kafka.Batch, src Source, p *fbv1.Post) error {
	e.stamp(src, "post:"+strconv.FormatInt(p.PostId, 10), p, func(o *fbv1.Observation) { p.Obs = o })
	key := strconv.FormatInt(p.PostId, 10)
	if err := b.AddProto(ctx, topics.Post, key, p); err != nil {
		return err
	}
	m := &fbv1.PostMetrics{
		PostId: p.PostId, PageId: p.PageId, PostCreatedAt: p.CreatedAt, CapturedAt: timestamppb.New(e.now()),
		Reactions: p.Reactions, CommentCount: p.CommentCount, ShareCount: p.ShareCount, ViewCount: p.ViewCount,
	}
	e.stamp(src, "post_metrics:"+key, m, func(o *fbv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.PostMetrics, key, m)
}

// Comment applies privacy rules and publishes a comment. It returns false
// when the comment was redacted (its media must not be downloaded).
func (e *Emitter) Comment(ctx context.Context, b *kafka.Batch, src Source, c *fbv1.Comment) (bool, error) {
	keep := e.Privatize(c)
	e.stamp(src, "comment:"+strconv.FormatInt(c.CommentId, 10), c, func(o *fbv1.Observation) { c.Obs = o })
	return keep, b.AddProto(ctx, topics.Comment, strconv.FormatInt(c.PostId, 10), c)
}

// CommentDeleted publishes that a known comment is no longer visible: a
// Comment with status DELETED and only its keys set. Its event id is the
// job's, so a comment deleted again after a re-sighting is a new event.
func (e *Emitter) CommentDeleted(ctx context.Context, b *kafka.Batch, src Source, c *fbv1.Comment) error {
	d := &fbv1.Comment{CommentId: c.CommentId, PostId: c.PostId, PageId: c.PageId, PostCreatedAt: c.PostCreatedAt,
		Depth: c.Depth, Status: fbv1.CommentStatus_COMMENT_STATUS_DELETED}
	e.stamp(src, "comment-deleted:"+strconv.FormatInt(c.CommentId, 10)+":"+src.JobID, d, func(o *fbv1.Observation) { d.Obs = o })
	return b.AddProto(ctx, topics.Comment, strconv.FormatInt(c.PostId, 10), d)
}

// Privatize sets the author key, applies pseudonymisation and redacts
// suppressed authors. It returns false if the comment was redacted.
func (e *Emitter) Privatize(c *fbv1.Comment) bool {
	if c.Author == nil {
		return true
	}
	c.Author.AuthorKey = gdpr.AuthorKey(e.HMACKey, c.Author.Id)
	if e.Suppression.AuthorSuppressed(c.Author.AuthorKey) {
		c.Author = &fbv1.Author{AuthorKey: c.Author.AuthorKey}
		c.Text, c.TextRanges, c.Attachments, c.Url = "", nil, nil, ""
		c.Redacted = true
		return false
	}
	if e.Pseudonymize {
		c.Author.Id, c.Author.Name, c.Author.Url, c.Author.AvatarUrl = "", "", "", ""
	}
	return true
}

// Media publishes discovered media metadata.
func (e *Emitter) Media(ctx context.Context, b *kafka.Batch, src Source, m *fbv1.Media) error {
	e.stamp(src, "media:"+m.MediaId, m, func(o *fbv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.Media, m.MediaId, m)
}
