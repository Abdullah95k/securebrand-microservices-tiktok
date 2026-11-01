// Package emit publishes parsed TikTok records on the public tt.* topics
// (docs/tiktok ADR 0007 §3). It is internal/crawl/emit for securebrand.tt.v1:
// each record is stamped with an Observation (a content hash and an event id
// that are the same for the same content), and the privacy rules (author
// keys, pseudonymisation, suppression; ADR 0007 §6) are applied before
// anything leaves the crawler.
package emit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// AuthorKey is a person's pseudonymous key: HMAC-SHA256 of "tt:user:<user
// id>" under tks's secret, domain-separated from fbs's and igs's keys (ADR
// 0007 §1). Never on the username or the secUid, which change.
func AuthorKey(secret []byte, userID int64) []byte {
	if userID == 0 {
		return nil
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("tt:user:" + strconv.FormatInt(userID, 10)))
	return m.Sum(nil)
}

// Source describes how records were obtained.
type Source struct {
	JobID string
	// Query and Variant are the registry operation (Observation.query_name
	// and doc_id).
	Query, Variant string
	ParserVersion  uint32
	Mode           commonv1.CrawlMode
}

// Emitter publishes records.
type Emitter struct {
	Producer     *kafka.Producer
	HMACKey      []byte
	Pseudonymize bool
	// Suppression holds erased authors and accounts (Suppression.ForAccounts:
	// keyed by the account's user id).
	Suppression *gdpr.Suppression
	Now         func() time.Time
}

func (e *Emitter) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now().UTC()
}

// Key returns a person's author key under the emitter's secret.
func (e *Emitter) Key(userID int64) []byte { return AuthorKey(e.HMACKey, userID) }

// AccountSuppressed reports whether an account was erased: nothing of it is
// published or read again.
func (e *Emitter) AccountSuppressed(userID int64) bool { return e.Suppression.PageSuppressed(userID) }

var marshal = proto.MarshalOptions{Deterministic: true}

// stamp computes the content hash over msg without its observation and sets
// a fresh Observation.
func (e *Emitter) stamp(src Source, entityKey string, msg proto.Message, set func(*commonv1.Observation)) {
	set(nil)
	b, _ := marshal.Marshal(hashable(msg))
	h := xxh3.Hash128(b).Bytes()
	ev := xxh3.Hash128(append([]byte(entityKey+"|"), h[:]...)).Bytes()
	var jobs []string
	if src.JobID != "" {
		jobs = []string{src.JobID}
	}
	set(&commonv1.Observation{
		EventId: hex.EncodeToString(ev[:]), ObservedAt: timestamppb.New(e.now()), JobIds: jobs, QueryName: src.Query,
		DocId: src.Variant, ParserVersion: src.ParserVersion, ContentHash: h[:], Mode: src.Mode,
		Platform: commonv1.Platform_PLATFORM_TIKTOK,
	})
}

// Account publishes an account, except an erased one.
func (e *Emitter) Account(ctx context.Context, b *kafka.Batch, src Source, a *ttv1.Account) error {
	if e.AccountSuppressed(a.UserId) {
		return nil
	}
	if len(a.AuthorKey) == 0 {
		a.AuthorKey = e.Key(a.UserId)
	}
	key := strconv.FormatInt(a.UserId, 10)
	e.stamp(src, "account:"+key, a, func(o *commonv1.Observation) { a.Obs = o })
	return b.AddProto(ctx, topics.Account, key, a)
}

// Post publishes a post and its counts at this read (tt.post_metrics.v1,
// captured now). A post of an erased account is not published.
func (e *Emitter) Post(ctx context.Context, b *kafka.Batch, src Source, p *ttv1.Post) error {
	if e.AccountSuppressed(p.UserId) {
		return nil
	}
	e.people(textPeople(p.TextRanges))
	key := strconv.FormatInt(p.PostId, 10)
	e.stamp(src, "post:"+key, p, func(o *commonv1.Observation) { p.Obs = o })
	if err := b.AddProto(ctx, topics.Post, key, p); err != nil {
		return err
	}
	if p.Counts == nil {
		return nil
	}
	m := &ttv1.PostMetrics{PostId: p.PostId, UserId: p.UserId, PostCreatedAt: p.CreatedAt,
		CapturedAt: timestamppb.New(e.now()), Counts: proto.Clone(p.Counts).(*ttv1.PostCounts)}
	e.stamp(src, "post_metrics:"+key, m, func(o *commonv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.PostMetrics, key, m)
}

// Music publishes a post's sound. Its author name is the creator's for an
// original sound: personal data, cleared when pseudonymising (ADR 0007 §6).
func (e *Emitter) Music(ctx context.Context, b *kafka.Batch, src Source, m *ttv1.Music) error {
	if m == nil || m.MusicId == 0 {
		return nil
	}
	if e.Pseudonymize {
		m.AuthorName = ""
	}
	key := strconv.FormatInt(m.MusicId, 10)
	e.stamp(src, "music:"+key, m, func(o *commonv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.Music, key, m)
}

// Media publishes media metadata, except of an erased account or author.
func (e *Emitter) Media(ctx context.Context, b *kafka.Batch, src Source, m *ttv1.Media) error {
	if e.AccountSuppressed(m.UserId) || e.Suppression.AuthorSuppressed(m.AuthorKey) {
		return nil
	}
	e.stamp(src, "media:"+m.MediaId, m, func(o *commonv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.Media, m.MediaId, m)
}

// Comment applies the privacy rules and publishes a comment. It returns
// false when the comment was redacted (its images must not be downloaded).
// A comment on an erased account's post is not published at all.
func (e *Emitter) Comment(ctx context.Context, b *kafka.Batch, src Source, c *ttv1.Comment) (bool, error) {
	if e.AccountSuppressed(c.UserId) {
		return false, nil
	}
	keep := e.Privatize(c)
	e.stamp(src, "comment:"+strconv.FormatInt(c.CommentId, 10), c, func(o *commonv1.Observation) { c.Obs = o })
	return keep, b.AddProto(ctx, topics.Comment, strconv.FormatInt(c.PostId, 10), c)
}

// CommentDeleted publishes that a known comment is no longer served: a
// Comment with status DELETED and only its keys set. Its event id is the
// job's, so a comment deleted again after a re-sighting is a new event.
func (e *Emitter) CommentDeleted(ctx context.Context, b *kafka.Batch, src Source, c *ttv1.Comment) error {
	d := &ttv1.Comment{CommentId: c.CommentId, PostId: c.PostId, UserId: c.UserId, PostCreatedAt: c.PostCreatedAt,
		Depth: c.Depth, ParentCommentId: c.ParentCommentId, Status: ttv1.CommentStatus_COMMENT_STATUS_DELETED}
	e.stamp(src, "comment-deleted:"+strconv.FormatInt(c.CommentId, 10)+":"+src.JobID, d, func(o *commonv1.Observation) { d.Obs = o })
	return b.AddProto(ctx, topics.Comment, strconv.FormatInt(c.PostId, 10), d)
}

// Privatize sets the author's key, pseudonymises the author and the people
// the text mentions when configured, and redacts a comment of an erased
// author. It returns false if the comment was redacted.
func (e *Emitter) Privatize(c *ttv1.Comment) bool {
	if c.Author != nil && len(c.Author.AuthorKey) == 0 {
		c.Author.AuthorKey = e.Key(c.Author.GetUserId())
	}
	if e.AccountSuppressed(c.UserId) || e.Suppression.AuthorSuppressed(c.GetAuthor().GetAuthorKey()) {
		Redact(c)
		return false
	}
	e.people(append(textPeople(c.TextRanges), c.Author))
	return true
}

// Redact strips a comment of an erased author to its keys and the author
// key, as an erasure leaves the stored row.
func Redact(c *ttv1.Comment) {
	c.Author = &ttv1.Person{AuthorKey: c.GetAuthor().GetAuthorKey()}
	c.Text, c.TextRanges, c.ImageMediaIds, c.Labels = "", nil, nil, nil
	c.Redacted = true
}

// people sets each person's author key, and clears what identifies them:
// everyone when pseudonymising, else erased authors (whose mentions keep
// only the key).
func (e *Emitter) people(ps []*ttv1.Person) {
	for _, p := range ps {
		if p == nil {
			continue
		}
		if len(p.AuthorKey) == 0 {
			p.AuthorKey = e.Key(p.GetUserId())
		}
		if e.Pseudonymize || e.Suppression.AuthorSuppressed(p.AuthorKey) {
			*p = ttv1.Person{AuthorKey: p.AuthorKey}
		}
	}
}

func textPeople(rs []*ttv1.TextRange) []*ttv1.Person {
	var out []*ttv1.Person
	for _, r := range rs {
		if r.Person != nil {
			out = append(out, r.Person)
		}
	}
	return out
}

// hashable is msg without what changes between two reads of the same
// content: the query of signed links (x-expires, x-signature, policy), a
// link's expiry and a metrics sample's capture time.
func hashable(msg proto.Message) proto.Message {
	c := proto.Clone(msg)
	switch m := c.(type) {
	case *ttv1.Media:
		m.SourceExpiresAt = nil
	case *ttv1.PostMetrics:
		m.CapturedAt = nil
	}
	stripQueries(c.ProtoReflect())
	return c
}

func stripQueries(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				switch fd.Kind() {
				case protoreflect.MessageKind:
					stripQueries(l.Get(i).Message())
				case protoreflect.StringKind:
					l.Set(i, protoreflect.ValueOfString(stableURL(l.Get(i).String())))
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			stripQueries(v.Message())
		case fd.Kind() == protoreflect.StringKind:
			if s := v.String(); s != stableURL(s) {
				m.Set(fd, protoreflect.ValueOfString(stableURL(s)))
			}
		}
		return true
	})
}

// stableURL drops a link's query: TikTok's CDN links are signed and
// expire, so the same image comes with a new query on every read.
func stableURL(s string) string {
	if !strings.Contains(s, "://") {
		return s
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		return s[:i]
	}
	return s
}
