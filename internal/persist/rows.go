// Package persist writes the public fb.* topics into Postgres in batches:
// dedupe within the batch, COPY into a temp table, then one upsert that is a
// no-op for unchanged rows (same content hash).
package persist

import (
	"encoding/json"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
)

func jsonOf(v any) []byte {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" || string(b) == "[]" || string(b) == "{}" {
		return nil
	}
	return b
}

func protoJSON(m proto.Message) []byte {
	b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}.Marshal(m)
	if err != nil || string(b) == "{}" {
		return nil
	}
	return b
}

func protoListJSON[T proto.Message](xs []T) []byte {
	if len(xs) == 0 {
		return nil
	}
	parts := make([]json.RawMessage, 0, len(xs))
	for _, x := range xs {
		parts = append(parts, protoJSON(x))
	}
	return jsonOf(parts)
}

func extraJSON(s string) []byte {
	if s == "" || !json.Valid([]byte(s)) {
		return nil
	}
	return []byte(s)
}

func observed(o *fbv1.Observation) time.Time {
	if o == nil || o.ObservedAt == nil {
		return time.Now().UTC()
	}
	return o.ObservedAt.AsTime()
}

func postType(t fbv1.PostType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "POST_TYPE_"))
}

func mediaKind(k fbv1.MediaKind) string {
	return strings.ToLower(strings.TrimPrefix(k.String(), "MEDIA_KIND_"))
}

func ptr[T any](v T) *T { return &v }

func nilIfZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// postRow returns the COPY row for posts (column order = postCols).
func postRow(p *fbv1.Post) []any {
	seen := observed(p.Obs)
	var reactionCount *int64
	var reactions []byte
	var complete *bool
	if p.Reactions != nil {
		reactionCount = ptr(p.Reactions.Total)
		reactions = jsonOf(p.Reactions.ByType)
		complete = ptr(p.Reactions.Complete)
	}
	var hash []byte
	if p.Obs != nil {
		hash = p.Obs.ContentHash
	}
	status := "visible"
	if p.Status == fbv1.PostStatus_POST_STATUS_UNAVAILABLE {
		status = "unavailable"
	}
	return []any{
		p.CreatedAt.AsTime(), p.PostId, p.PageId, nilIfEmpty(p.StoryId), nilIfEmpty(p.Permalink), postType(p.Type),
		nilIfEmpty(p.Text), protoListJSON(p.TextRanges), p.Hashtags, protoListJSON(p.Links), p.SharedPostId,
		nilIfEmpty(p.SharedPostUrl), p.IsPinned, p.IsPaidPartnership, nilIfEmpty(p.AuthorId), nilIfEmpty(p.AuthorName),
		reactionCount, reactions, complete, p.CommentCount, p.ShareCount, p.ViewCount, nilIfEmpty(p.FeedbackId),
		p.MediaIds, extraJSON(p.ExtraJson), hash, status, seen, seen,
	}
}

var postCols = []string{
	"post_created_at", "post_id", "page_id", "story_id", "permalink", "post_type",
	"text", "text_ranges", "hashtags", "links", "shared_post_id",
	"shared_post_url", "is_pinned", "is_paid_partnership", "author_id", "author_name",
	"reaction_count", "reactions", "reactions_complete", "comment_count_total", "share_count", "view_count", "feedback_id",
	"media_ids", "extra", "content_hash", "status", "first_seen_at", "last_seen_at",
}

var commentCols = []string{
	"post_created_at", "post_id", "comment_id", "page_id", "graph_id", "parent_comment_id", "root_comment_id", "depth",
	"author_key", "author_id", "text", "text_ranges", "attachments", "created_at", "edited", "is_op",
	"reaction_count", "reactions", "reply_count", "feedback_id", "url", "extra", "redacted", "content_hash",
	"first_seen_at", "last_seen_at",
}

func commentRow(c *fbv1.Comment) []any {
	seen := observed(c.Obs)
	var authorKey []byte
	var authorID *string
	if c.Author != nil {
		authorKey = c.Author.AuthorKey
		authorID = nilIfEmpty(c.Author.Id)
	}
	var reactionCount *int64
	var reactions []byte
	if c.Reactions != nil {
		reactionCount = ptr(c.Reactions.Total)
		reactions = jsonOf(c.Reactions.ByType)
	}
	var created *time.Time
	if c.CreatedAt != nil {
		created = ptr(c.CreatedAt.AsTime())
	}
	var hash []byte
	if c.Obs != nil {
		hash = c.Obs.ContentHash
	}
	text := nilIfEmpty(c.Text)
	return []any{
		c.PostCreatedAt.AsTime(), c.PostId, c.CommentId, c.PageId, nilIfEmpty(c.GraphId), c.ParentCommentId, c.RootCommentId,
		int16(c.Depth), authorKey, authorID, text, protoListJSON(c.TextRanges), protoListJSON(c.Attachments), created,
		c.Edited, c.IsOp, reactionCount, reactions, c.ReplyCount, nilIfEmpty(c.FeedbackId), nilIfEmpty(c.Url),
		extraJSON(c.ExtraJson), c.Redacted, hash, seen, seen,
	}
}

var mediaCols = []string{
	"media_id", "fb_media_id", "variant", "kind", "owner_type", "owner_id", "post_id", "page_id", "comment_id",
	"source_url", "source_expires_at", "width", "height", "duration_ms", "mime", "alt_text", "discovered_at",
}

func mediaRow(m *fbv1.Media) []any {
	var exp *time.Time
	if m.SourceExpiresAt != nil {
		exp = ptr(m.SourceExpiresAt.AsTime())
	}
	return []any{
		m.MediaId, m.FbMediaId, m.Variant, mediaKind(m.Kind), m.OwnerType, nilIfZero(m.OwnerId), nilIfZero(m.PostId),
		nilIfZero(m.PageId), m.CommentId, nilIfEmpty(m.SourceUrl), exp, int32OrNil(m.Width), int32OrNil(m.Height),
		nilIfZero(m.DurationMs), nilIfEmpty(m.Mime), nilIfEmpty(m.AltText), observed(m.Obs),
	}
}

func int32OrNil(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}
