package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// Post is the API view of a post.
type Post struct {
	PostID            int64           `json:"post_id"`
	PageID            int64           `json:"page_id"`
	CreatedAt         time.Time       `json:"created_at"`
	Type              string          `json:"type"`
	Permalink         *string         `json:"permalink,omitempty"`
	Text              *string         `json:"text,omitempty"`
	TextRanges        json.RawMessage `json:"text_ranges,omitempty"`
	Hashtags          []string        `json:"hashtags,omitempty"`
	Links             json.RawMessage `json:"links,omitempty"`
	SharedPostID      *int64          `json:"shared_post_id,omitempty"`
	IsPinned          *bool           `json:"is_pinned,omitempty"`
	IsPaidPartnership *bool           `json:"is_paid_partnership,omitempty"`
	ReactionCount     *int64          `json:"reaction_count,omitempty"`
	Reactions         json.RawMessage `json:"reactions,omitempty"`
	ReactionsComplete *bool           `json:"reactions_complete,omitempty"`
	CommentCount      *int64          `json:"comment_count,omitempty"`
	ShareCount        *int64          `json:"share_count,omitempty"`
	ViewCount         *int64          `json:"view_count,omitempty"`
	MediaIDs          []string        `json:"media_ids,omitempty"`
	CommentsCollected *int64          `json:"comments_collected,omitempty"`
	RepliesCollected  *int64          `json:"replies_collected,omitempty"`
	Completeness      *float32        `json:"completeness,omitempty"`
	CommentsState     string          `json:"comments_state"`
	Status            string          `json:"status"`
	FirstSeenAt       time.Time       `json:"first_seen_at"`
	LastSeenAt        time.Time       `json:"last_seen_at"`
}

const postSelect = `SELECT post_id, page_id, post_created_at, post_type, permalink, text, text_ranges, hashtags, links,
	shared_post_id, is_pinned, is_paid_partnership, reaction_count, reactions, reactions_complete, comment_count_total,
	share_count, view_count, media_ids, comments_collected, replies_collected, completeness, comments_state, status,
	first_seen_at, last_seen_at FROM posts`

func scanPost(row pgx.CollectableRow) (Post, error) {
	var p Post
	err := row.Scan(&p.PostID, &p.PageID, &p.CreatedAt, &p.Type, &p.Permalink, &p.Text, &p.TextRanges, &p.Hashtags, &p.Links,
		&p.SharedPostID, &p.IsPinned, &p.IsPaidPartnership, &p.ReactionCount, &p.Reactions, &p.ReactionsComplete, &p.CommentCount,
		&p.ShareCount, &p.ViewCount, &p.MediaIDs, &p.CommentsCollected, &p.RepliesCollected, &p.Completeness, &p.CommentsState,
		&p.Status, &p.FirstSeenAt, &p.LastSeenAt)
	return p, err
}

// queryPosts lists a page's posts newest first. seenSince filters on
// last_seen_at (job results); after/before bound post creation time.
func (s *Server) queryPosts(ctx context.Context, pageID int64, seenSince, since time.Time, cur *cursorPos, limit int, bySeen bool) ([]Post, error) {
	q := postSelect + ` WHERE page_id = $1`
	args := []any{pageID}
	if bySeen && !seenSince.IsZero() {
		args = append(args, seenSince)
		q += fmt.Sprintf(" AND last_seen_at >= $%d", len(args))
	}
	if !since.IsZero() {
		args = append(args, since)
		q += fmt.Sprintf(" AND post_created_at >= $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.T, cur.ID)
		q += fmt.Sprintf(" AND (post_created_at, post_id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY post_created_at DESC, post_id DESC LIMIT $%d", len(args))
	rows, err := s.ReadDB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanPost)
}

func (s *Server) listPosts(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolvePage(w, r)
	if !ok {
		return
	}
	var since time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid since", err.Error())
			return
		}
		since = t
	}
	var cur *cursorPos
	if c, ok := decodeCursor(r.URL.Query().Get("cursor")); ok {
		cur = &c
	}
	limit := limitParam(r)
	posts, err := s.queryPosts(r.Context(), id, time.Time{}, since, cur, limit, false)
	if err != nil {
		serverError(w, err)
		return
	}
	if t := r.URL.Query().Get("type"); t != "" {
		filtered := posts[:0]
		for _, p := range posts {
			if p.Type == t {
				filtered = append(filtered, p)
			}
		}
		posts = filtered
	}
	out := map[string]any{"posts": posts}
	if len(posts) == limit {
		last := posts[len(posts)-1]
		out["next_cursor"] = encodeCursor(last.CreatedAt, last.PostID)
	}
	writeJSON(w, http.StatusOK, out)
}

// postKey returns the partition key of a post.
func (s *Server) postKey(ctx context.Context, postID int64) (time.Time, error) {
	var t time.Time
	err := s.ReadDB.QueryRow(ctx, `SELECT post_created_at FROM post_index WHERE post_id = $1`, postID).Scan(&t)
	return t, err
}

func (s *Server) getPost(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	t, err := s.postKey(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), postSelect+` WHERE post_created_at = $1 AND post_id = $2`, t, id)
	if err != nil {
		serverError(w, err)
		return
	}
	p, err := pgx.CollectExactlyOneRow(rows, scanPost)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// Comment is the API view of a comment. Author identity fields require the
// pii:read scope; author_key is always present.
type Comment struct {
	CommentID       int64           `json:"comment_id"`
	PostID          int64           `json:"post_id"`
	ParentCommentID *int64          `json:"parent_comment_id,omitempty"`
	RootCommentID   *int64          `json:"root_comment_id,omitempty"`
	Depth           int16           `json:"depth"`
	AuthorKey       string          `json:"author_key,omitempty"`
	Author          *CommentAuthor  `json:"author,omitempty"`
	Text            *string         `json:"text,omitempty"`
	TextRanges      json.RawMessage `json:"text_ranges,omitempty"`
	Attachments     json.RawMessage `json:"attachments,omitempty"`
	CreatedAt       *time.Time      `json:"created_at,omitempty"`
	Edited          *bool           `json:"edited,omitempty"`
	IsOP            *bool           `json:"is_op,omitempty"`
	ReactionCount   *int64          `json:"reaction_count,omitempty"`
	Reactions       json.RawMessage `json:"reactions,omitempty"`
	ReplyCount      *int64          `json:"reply_count,omitempty"`
	URL             *string         `json:"url,omitempty"`
	// Status is visible or deleted (no longer returned by a full read of the
	// post since DeletedAt); a deleted comment keeps its last content.
	Status    string     `json:"status"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	// EditCount counts text changes seen across scrapes, the last at EditedAt.
	EditCount   int        `json:"edit_count"`
	EditedAt    *time.Time `json:"edited_at,omitempty"`
	Redacted    bool       `json:"redacted"`
	FirstSeenAt time.Time  `json:"first_seen_at"`
	LastSeenAt  time.Time  `json:"last_seen_at"`
}

type CommentAuthor struct {
	ID         *string `json:"id,omitempty"`
	Name       *string `json:"name,omitempty"`
	ProfileURL *string `json:"profile_url,omitempty"`
	AvatarURL  *string `json:"avatar_url,omitempty"`
	Type       *string `json:"type,omitempty"`
	Verified   *bool   `json:"verified,omitempty"`
}

func (s *Server) queryComments(ctx context.Context, postID int64, postCreated time.Time, parent *int64, depth *int, status string, cur *cursorPos, limit int, pii bool) ([]Comment, error) {
	q := `SELECT c.comment_id, c.post_id, c.parent_comment_id, c.root_comment_id, c.depth, c.author_key, c.text, c.text_ranges,
		c.attachments, c.created_at, c.edited, c.is_op, c.reaction_count, c.reactions, c.reply_count, c.url, c.status,
		c.deleted_at, c.edit_count, c.edited_at, c.redacted,
		c.first_seen_at, c.last_seen_at, a.author_id, a.name, a.profile_url, a.avatar_url, a.author_type, a.verified
		FROM comments c LEFT JOIN comment_authors a ON a.author_key = c.author_key AND NOT c.redacted
		WHERE c.post_created_at = $1 AND c.post_id = $2`
	args := []any{postCreated, postID}
	if parent != nil {
		args = append(args, *parent)
		q += fmt.Sprintf(" AND c.parent_comment_id = $%d", len(args))
	}
	if depth != nil {
		args = append(args, *depth)
		q += fmt.Sprintf(" AND c.depth = $%d", len(args))
	}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" AND c.status = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.T, cur.ID)
		q += fmt.Sprintf(" AND (coalesce(c.created_at, 'epoch'), c.comment_id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY coalesce(c.created_at, 'epoch'), c.comment_id LIMIT $%d", len(args))
	rows, err := s.ReadDB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Comment, error) {
		var c Comment
		var key []byte
		var a CommentAuthor
		err := row.Scan(&c.CommentID, &c.PostID, &c.ParentCommentID, &c.RootCommentID, &c.Depth, &key, &c.Text, &c.TextRanges,
			&c.Attachments, &c.CreatedAt, &c.Edited, &c.IsOP, &c.ReactionCount, &c.Reactions, &c.ReplyCount, &c.URL, &c.Status,
			&c.DeletedAt, &c.EditCount, &c.EditedAt, &c.Redacted, &c.FirstSeenAt, &c.LastSeenAt, &a.ID, &a.Name, &a.ProfileURL, &a.AvatarURL, &a.Type, &a.Verified)
		if len(key) > 0 {
			c.AuthorKey = hex.EncodeToString(key)
		}
		if pii && (a.ID != nil || a.Name != nil) {
			c.Author = &a
		} else if a.Type != nil {
			c.Author = &CommentAuthor{Type: a.Type, Verified: a.Verified}
		}
		return c, err
	})
}

func (s *Server) listComments(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	t, err := s.postKey(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	var parent *int64
	if v := r.URL.Query().Get("parent_id"); v != "" {
		var p int64
		if _, err := fmt.Sscan(v, &p); err != nil {
			problem(w, http.StatusBadRequest, "invalid parent_id", err.Error())
			return
		}
		parent = &p
	}
	var depth *int
	if v := r.URL.Query().Get("depth"); v != "" {
		var d int
		if _, err := fmt.Sscan(v, &d); err != nil {
			problem(w, http.StatusBadRequest, "invalid depth", err.Error())
			return
		}
		depth = &d
	}
	status := r.URL.Query().Get("status")
	if status != "" && status != "visible" && status != "deleted" {
		problem(w, http.StatusBadRequest, "invalid status", "status must be visible or deleted")
		return
	}
	var cur *cursorPos
	if c, ok := decodeCursor(r.URL.Query().Get("cursor")); ok {
		cur = &c
	}
	limit := limitParam(r)
	cs, err := s.queryComments(r.Context(), id, t, parent, depth, status, cur, limit, clientFrom(r.Context()).Has(ScopePIIRead))
	if err != nil {
		serverError(w, err)
		return
	}
	out := map[string]any{"comments": cs}
	if len(cs) == limit {
		last := cs[len(cs)-1]
		var ts time.Time
		if last.CreatedAt != nil {
			ts = *last.CreatedAt
		}
		out["next_cursor"] = encodeCursor(ts, last.CommentID)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) postMetrics(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, reaction_count, reactions, comment_count, share_count, view_count
		FROM post_metrics WHERE post_id = $1 ORDER BY captured_at DESC LIMIT $2`, id, limitParam(r))
	if err != nil {
		serverError(w, err)
		return
	}
	type sample struct {
		CapturedAt time.Time       `json:"captured_at"`
		Reactions  *int64          `json:"reaction_count,omitempty"`
		ByType     json.RawMessage `json:"reactions,omitempty"`
		Comments   *int64          `json:"comment_count,omitempty"`
		Shares     *int64          `json:"share_count,omitempty"`
		Views      *int64          `json:"view_count,omitempty"`
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sample, error) {
		var s sample
		return s, row.Scan(&s.CapturedAt, &s.Reactions, &s.ByType, &s.Comments, &s.Shares, &s.Views)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"metrics": out})
}

// Media is the API view of a media item and its download lifecycle.
type Media struct {
	MediaID      string     `json:"media_id"`
	FBMediaID    string     `json:"fb_media_id"`
	Variant      string     `json:"variant"`
	Kind         string     `json:"kind"`
	OwnerType    string     `json:"owner_type"`
	PostID       *int64     `json:"post_id,omitempty"`
	CommentID    *int64     `json:"comment_id,omitempty"`
	SourceURL    *string    `json:"source_url,omitempty"`
	SourceExpiry *time.Time `json:"source_expires_at,omitempty"`
	Width        *int32     `json:"width,omitempty"`
	Height       *int32     `json:"height,omitempty"`
	DurationMS   *int64     `json:"duration_ms,omitempty"`
	AltText      *string    `json:"alt_text,omitempty"`
	Status       string     `json:"status"`
	S3Key        *string    `json:"s3_key,omitempty"`
	SizeBytes    *int64     `json:"size_bytes,omitempty"`
	SHA256       string     `json:"sha256,omitempty"`
	DeleteAfter  *time.Time `json:"delete_after,omitempty"`
	DiscoveredAt time.Time  `json:"discovered_at"`
	ReadyAt      *time.Time `json:"ready_at,omitempty"`
	DeletedAt    *time.Time `json:"deleted_at,omitempty"`
}

const mediaSelect = `SELECT media_id::text, fb_media_id, variant, kind, owner_type, post_id, comment_id, source_url,
	source_expires_at, width, height, duration_ms, alt_text, status, s3_key, size_bytes, sha256, delete_after, discovered_at,
	ready_at, deleted_at FROM media`

func scanMedia(row pgx.CollectableRow) (Media, error) {
	var m Media
	var sha []byte
	err := row.Scan(&m.MediaID, &m.FBMediaID, &m.Variant, &m.Kind, &m.OwnerType, &m.PostID, &m.CommentID, &m.SourceURL,
		&m.SourceExpiry, &m.Width, &m.Height, &m.DurationMS, &m.AltText, &m.Status, &m.S3Key, &m.SizeBytes, &sha,
		&m.DeleteAfter, &m.DiscoveredAt, &m.ReadyAt, &m.DeletedAt)
	if len(sha) > 0 {
		m.SHA256 = hex.EncodeToString(sha)
	}
	return m, err
}

func (s *Server) postMedia(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), mediaSelect+` WHERE post_id = $1 ORDER BY discovered_at LIMIT $2`, id, limitParam(r))
	if err != nil {
		serverError(w, err)
		return
	}
	out, err := pgx.CollectRows(rows, scanMedia)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"media": out})
}

func (s *Server) getMedia(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "media")
	if len(id) != 36 || strings.Count(id, "-") != 4 {
		problem(w, http.StatusBadRequest, "invalid media id", "media id must be a UUID")
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), mediaSelect+` WHERE media_id = $1`, id)
	if err != nil {
		serverError(w, err)
		return
	}
	m, err := pgx.CollectExactlyOneRow(rows, scanMedia)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}
