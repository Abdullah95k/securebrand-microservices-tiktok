package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PostCounts are a post's latest counts (exact, from statsV2; rounded by
// TikTok past 10,000: T31). Their history is /posts/{post}/metrics.
type PostCounts struct {
	Plays    *int64 `json:"play_count,omitempty"`
	Likes    *int64 `json:"like_count,omitempty"`
	Comments *int64 `json:"comment_count,omitempty"`
	Shares   *int64 `json:"share_count,omitempty"`
	Saves    *int64 `json:"save_count,omitempty"`
	Reposts  *int64 `json:"repost_count,omitempty"`
}

// Post is the API view of a post (a video or a photo post). The caption is
// the account's own content and not gated (ADR 0007 §6); the people it
// mentions are, and the link, which names the account's username.
type Post struct {
	PostID           int64           `json:"post_id,string"`
	UserID           int64           `json:"user_id,string"`
	CreatedAt        time.Time       `json:"created_at"`
	Kind             string          `json:"kind"`
	URL              *string         `json:"url,omitempty"`
	Caption          *string         `json:"caption,omitempty"`
	TextRanges       json.RawMessage `json:"text_ranges,omitempty"`
	Hashtags         []string        `json:"hashtags,omitempty"`
	DurationMS       *int64          `json:"duration_ms,omitempty"`
	Width            *int32          `json:"width,omitempty"`
	Height           *int32          `json:"height,omitempty"`
	CoverMediaID     *string         `json:"cover_media_id,omitempty"`
	PhotoCount       *int32          `json:"photo_count,omitempty"`
	MusicID          *int64          `json:"music_id,omitempty,string"`
	IsPinned         *bool           `json:"is_pinned,omitempty"`
	IsAd             *bool           `json:"is_ad,omitempty"`
	Labels           json.RawMessage `json:"labels,omitempty"`
	CommentsDisabled *bool           `json:"comments_disabled,omitempty"`
	Region           *string         `json:"region,omitempty"`
	TextLanguage     *string         `json:"text_language,omitempty"`
	DiscoveredVia    string          `json:"discovered_via"`
	Counts           PostCounts      `json:"counts"`
	// Comment coverage of the last comment read (ADR 0007 §5).
	CommentsCollected *int64     `json:"comments_collected,omitempty"`
	RepliesCollected  *int64     `json:"replies_collected,omitempty"`
	Completeness      *float32   `json:"completeness,omitempty"`
	CommentsState     string     `json:"comments_state"`
	CommentsSyncedAt  *time.Time `json:"comments_synced_at,omitempty"`
	Status            string     `json:"status"`
	FirstSeenAt       time.Time  `json:"first_seen_at"`
	LastSeenAt        time.Time  `json:"last_seen_at"`
}

const postSelect = `SELECT p.post_id, p.user_id, p.created_at, p.kind, a.username, p.caption, p.text_ranges, p.hashtags,
	p.duration_ms, p.width, p.height, p.cover_media_id::text, p.photo_count, p.music_id, p.is_pinned, p.is_ad, p.labels,
	p.comments_disabled, p.region, p.text_language, p.discovered_via, p.play_count, p.like_count, p.comment_count,
	p.share_count, p.save_count, p.repost_count, p.comments_collected, p.replies_collected, p.completeness, p.comments_state,
	p.comments_synced_at, p.status, p.first_seen_at, p.last_seen_at
	FROM posts p LEFT JOIN accounts a ON a.user_id = p.user_id`

// scanPost reads a postSelect row; without pii, the link and who the
// caption mentions are left out.
func scanPost(pii bool) pgx.RowToFunc[Post] {
	return func(row pgx.CollectableRow) (Post, error) {
		var p Post
		var username *string
		c := &p.Counts
		err := row.Scan(&p.PostID, &p.UserID, &p.CreatedAt, &p.Kind, &username, &p.Caption, &p.TextRanges, &p.Hashtags,
			&p.DurationMS, &p.Width, &p.Height, &p.CoverMediaID, &p.PhotoCount, &p.MusicID, &p.IsPinned, &p.IsAd, &p.Labels,
			&p.CommentsDisabled, &p.Region, &p.TextLanguage, &p.DiscoveredVia, &c.Plays, &c.Likes, &c.Comments, &c.Shares,
			&c.Saves, &c.Reposts, &p.CommentsCollected, &p.RepliesCollected, &p.Completeness, &p.CommentsState,
			&p.CommentsSyncedAt, &p.Status, &p.FirstSeenAt, &p.LastSeenAt)
		if err != nil {
			return p, err
		}
		if pii && username != nil && *username != "" {
			u := PostURL(*username, p.Kind, p.PostID)
			p.URL = &u
		}
		if !pii {
			p.TextRanges = scrubRanges(p.TextRanges)
		}
		return p, nil
	}
}

// PostURL is a post's link: built from the account's current username and
// the post's id (ADR 0007 §1), never stored.
func PostURL(username, kind string, postID int64) string {
	path := "video"
	if kind == "photo" {
		path = "photo"
	}
	return "https://www.tiktok.com/@" + username + "/" + path + "/" + strconv.FormatInt(postID, 10)
}

// scrubRanges leaves of each mention its place and the person's author key
// (as the writer stores an erased person's): who it names needs pii:read.
// Text ranges are stored with protojson's field names (persist.rangesJSON).
// Anything it cannot read comes back as nil.
func scrubRanges(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var rs []map[string]any
	if err := json.Unmarshal(raw, &rs); err != nil {
		return nil
	}
	for _, r := range rs {
		p, ok := r["person"].(map[string]any)
		if !ok {
			continue
		}
		r["person"] = map[string]any{"author_key": p["author_key"]}
		if r["type"] == "mention" {
			delete(r, "name")
		}
	}
	out, err := json.Marshal(rs)
	if err != nil {
		return nil
	}
	return out
}

// postQuery selects an account's posts, newest first.
type postQuery struct {
	userID    int64
	seenSince time.Time // last_seen_at at or after (job results)
	since     time.Time // created at or after
	kind      string
	cur       *cursorPos
	limit     int
}

func (s *Server) queryPosts(ctx context.Context, q postQuery, pii bool) ([]Post, error) {
	sql := postSelect + ` WHERE p.user_id = $1`
	args := []any{q.userID}
	add := func(cond string, vs ...any) {
		args = append(args, vs...)
		n := make([]any, len(vs))
		for i := range vs {
			n[i] = len(args) - len(vs) + i + 1
		}
		sql += " AND " + fmt.Sprintf(cond, n...)
	}
	if q.kind != "" {
		add("p.kind = $%d", q.kind)
	}
	if !q.seenSince.IsZero() {
		add("p.last_seen_at >= $%d", q.seenSince)
	}
	if !q.since.IsZero() {
		add("p.created_at >= $%d", q.since)
	}
	if q.cur != nil {
		add("(p.created_at, p.post_id) < ($%d, $%d)", q.cur.T, q.cur.ID)
	}
	args = append(args, q.limit)
	sql += fmt.Sprintf(" ORDER BY p.created_at DESC, p.post_id DESC LIMIT $%d", len(args))
	rows, err := s.ReadDB.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanPost(pii))
}

func (s *Server) listPosts(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolveAccount(w, r)
	if !ok {
		return
	}
	since, ok := timeParam(w, r, "since")
	if !ok {
		return
	}
	cur, ok := cursorParam(w, r)
	if !ok {
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind != "" && kind != "video" && kind != "photo" {
		problem(w, http.StatusBadRequest, "invalid kind", "kind must be video or photo")
		return
	}
	limit := limitParam(r)
	posts, err := s.queryPosts(r.Context(), postQuery{userID: id, since: since, kind: kind, cur: cur, limit: limit},
		clientFrom(r.Context()).Has(ScopePIIRead))
	if err != nil {
		serverError(w, err)
		return
	}
	if posts == nil {
		posts = []Post{}
	}
	out := map[string]any{"posts": posts}
	if len(posts) == limit {
		last := posts[len(posts)-1]
		out["next_cursor"] = encodeCursor(last.CreatedAt, last.PostID)
	}
	writeJSON(w, http.StatusOK, out)
}

// postKey returns a post's partition key (post_index).
func (s *Server) postKey(w http.ResponseWriter, r *http.Request) (int64, time.Time, bool) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return 0, time.Time{}, false
	}
	var t time.Time
	if err := s.ReadDB.QueryRow(r.Context(), `SELECT created_at FROM post_index WHERE post_id = $1`, id).Scan(&t); err != nil {
		serverError(w, err)
		return 0, time.Time{}, false
	}
	return id, t, true
}

func (s *Server) getPost(w http.ResponseWriter, r *http.Request) {
	id, t, ok := s.postKey(w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), postSelect+` WHERE p.created_at = $1 AND p.post_id = $2`, t, id)
	if err != nil {
		serverError(w, err)
		return
	}
	p, err := pgx.CollectExactlyOneRow(rows, scanPost(clientFrom(r.Context()).Has(ScopePIIRead)))
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// Comment is the API view of a top-level comment or a reply. The text and
// the author's identity are personal data and need pii:read (ADR 0007 §6);
// author_key is always there, so a client can group a person's comments
// without knowing who they are.
type Comment struct {
	CommentID        int64          `json:"comment_id,string"`
	PostID           int64          `json:"post_id,string"`
	ParentCommentID  *int64         `json:"parent_comment_id,omitempty,string"`
	ReplyToCommentID *int64         `json:"reply_to_comment_id,omitempty,string"`
	Depth            int16          `json:"depth"`
	AuthorKey        string         `json:"author_key,omitempty"`
	Author           *CommentAuthor `json:"author,omitempty"`
	Text             *string        `json:"text,omitempty"`
	TextRanges       json.RawMessage `json:"text_ranges,omitempty"`
	CreatedAt        *time.Time     `json:"created_at,omitempty"`
	LikeCount        *int64         `json:"like_count,omitempty"`
	ReplyCount       *int64         `json:"reply_count,omitempty"`
	LikedByCreator   *bool          `json:"liked_by_creator,omitempty"`
	PinnedByCreator  *bool          `json:"pinned_by_creator,omitempty"`
	HasImages        *bool          `json:"has_images,omitempty"`
	ImageMediaIDs    []string       `json:"image_media_ids,omitempty"`
	Labels           []string       `json:"labels,omitempty"`
	Language         *string        `json:"language,omitempty"`
	// Status is visible or deleted (a whole read of the post no longer
	// returned it, at DeletedAt); a deleted comment keeps its last content.
	Status    string     `json:"status"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	// EditCount counts text changes seen across reads, the last at EditedAt.
	EditCount   int        `json:"edit_count"`
	EditedAt    *time.Time `json:"edited_at,omitempty"`
	Redacted    bool       `json:"redacted"`
	FirstSeenAt time.Time  `json:"first_seen_at"`
	LastSeenAt  time.Time  `json:"last_seen_at"`
}

// CommentAuthor is who wrote a comment (pii:read; empty when stored
// pseudonymized).
type CommentAuthor struct {
	UserID    *int64  `json:"user_id,omitempty,string"`
	SecUID    *string `json:"sec_uid,omitempty"`
	Username  *string `json:"username,omitempty"`
	Nickname  *string `json:"nickname,omitempty"`
	AvatarURL *string `json:"avatar_url,omitempty"`
	Verified  *bool   `json:"verified,omitempty"`
}

// commentQuery selects a post's comments, oldest first.
type commentQuery struct {
	postID      int64
	postCreated time.Time
	parent      *int64
	depth       *int
	status      string
	cur         *cursorPos
	limit       int
}

func commentCursor(c Comment) *cursorPos {
	// An undated comment sorts as 'epoch' (queryComments).
	ts := time.Unix(0, 0).UTC()
	if c.CreatedAt != nil {
		ts = *c.CreatedAt
	}
	return &cursorPos{T: ts, ID: c.CommentID}
}

func (s *Server) queryComments(ctx context.Context, q commentQuery, pii bool) ([]Comment, error) {
	sql := `SELECT c.comment_id, c.post_id, c.parent_comment_id, c.reply_to_comment_id, c.depth, c.author_key, c.text,
		c.text_ranges, c.created_at, c.like_count, c.reply_count, c.liked_by_creator, c.pinned_by_creator, c.has_images,
		c.image_media_ids::text[], c.labels, c.language, c.status, c.deleted_at, c.edit_count, c.edited_at, c.redacted,
		c.first_seen_at, c.last_seen_at, coalesce(c.author_user_id, p.user_id), p.sec_uid, p.username, p.nickname,
		p.avatar_url, p.verified
		FROM comments c LEFT JOIN people p ON p.author_key = c.author_key AND NOT c.redacted
		WHERE c.post_created_at = $1 AND c.post_id = $2`
	args := []any{q.postCreated, q.postID}
	if q.parent != nil {
		args = append(args, *q.parent)
		sql += fmt.Sprintf(" AND c.parent_comment_id = $%d", len(args))
	}
	if q.depth != nil {
		args = append(args, *q.depth)
		sql += fmt.Sprintf(" AND c.depth = $%d", len(args))
	}
	if q.status != "" {
		args = append(args, q.status)
		sql += fmt.Sprintf(" AND c.status = $%d", len(args))
	}
	if q.cur != nil {
		args = append(args, q.cur.T, q.cur.ID)
		sql += fmt.Sprintf(" AND (coalesce(c.created_at, 'epoch'), c.comment_id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, q.limit)
	sql += fmt.Sprintf(" ORDER BY coalesce(c.created_at, 'epoch'), c.comment_id LIMIT $%d", len(args))
	rows, err := s.ReadDB.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Comment, error) {
		var c Comment
		var key []byte
		var a CommentAuthor
		err := row.Scan(&c.CommentID, &c.PostID, &c.ParentCommentID, &c.ReplyToCommentID, &c.Depth, &key, &c.Text, &c.TextRanges,
			&c.CreatedAt, &c.LikeCount, &c.ReplyCount, &c.LikedByCreator, &c.PinnedByCreator, &c.HasImages, &c.ImageMediaIDs,
			&c.Labels, &c.Language, &c.Status, &c.DeletedAt, &c.EditCount, &c.EditedAt, &c.Redacted, &c.FirstSeenAt,
			&c.LastSeenAt, &a.UserID, &a.SecUID, &a.Username, &a.Nickname, &a.AvatarURL, &a.Verified)
		if len(key) > 0 {
			c.AuthorKey = hex.EncodeToString(key)
		}
		switch {
		case !pii:
			c.Text, c.TextRanges, c.ImageMediaIDs = nil, nil, nil
		case !c.Redacted && (a.UserID != nil || a.Username != nil || a.Nickname != nil):
			c.Author = &a
		}
		return c, err
	})
}

func (s *Server) listComments(w http.ResponseWriter, r *http.Request) {
	id, t, ok := s.postKey(w, r)
	if !ok {
		return
	}
	q := commentQuery{postID: id, postCreated: t, limit: limitParam(r)}
	if v := r.URL.Query().Get("parent_id"); v != "" {
		p, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid parent_id", "parent_id must be a comment id")
			return
		}
		q.parent = &p
	}
	if v := r.URL.Query().Get("depth"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil || d < 0 || d > 1 {
			problem(w, http.StatusBadRequest, "invalid depth", "depth must be 0 (top level) or 1 (replies)")
			return
		}
		q.depth = &d
	}
	q.status = r.URL.Query().Get("status")
	if q.status != "" && q.status != "visible" && q.status != "deleted" {
		problem(w, http.StatusBadRequest, "invalid status", "status must be visible or deleted")
		return
	}
	if q.cur, ok = cursorParam(w, r); !ok {
		return
	}
	cs, err := s.queryComments(r.Context(), q, clientFrom(r.Context()).Has(ScopePIIRead))
	if err != nil {
		serverError(w, err)
		return
	}
	if cs == nil {
		cs = []Comment{}
	}
	out := map[string]any{"comments": cs}
	if len(cs) == q.limit {
		c := commentCursor(cs[len(cs)-1])
		out["next_cursor"] = encodeCursor(c.T, c.ID)
	}
	writeJSON(w, http.StatusOK, out)
}

// postMetrics is a post's counts history, newest first: one row per change
// (ADR 0007 §11).
func (s *Server) postMetrics(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	cur, ok := cursorParam(w, r)
	if !ok {
		return
	}
	before := time.Now().Add(time.Hour)
	if cur != nil {
		before = cur.T
	}
	limit := limitParam(r)
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, play_count, like_count, comment_count, share_count, save_count,
		repost_count FROM post_metrics WHERE post_id = $1 AND captured_at < $2 ORDER BY captured_at DESC LIMIT $3`, id, before, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	type sample struct {
		CapturedAt time.Time `json:"captured_at"`
		PostCounts
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sample, error) {
		var x sample
		c := &x.PostCounts
		return x, row.Scan(&x.CapturedAt, &c.Plays, &c.Likes, &c.Comments, &c.Shares, &c.Saves, &c.Reposts)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []sample{}
	}
	resp := map[string]any{"metrics": out}
	if len(out) == limit {
		resp["next_cursor"] = encodeCursor(out[len(out)-1].CapturedAt, 0)
	}
	writeJSON(w, http.StatusOK, resp)
}

// Media is the API view of a media item and its download lifecycle. A
// person's avatar or comment image (author_key set) shows its link only
// with pii:read.
type Media struct {
	MediaID       string     `json:"media_id"`
	SourceMediaID string     `json:"source_media_id"`
	Kind          string     `json:"kind"`
	Variant       string     `json:"variant"`
	UserID        *int64     `json:"user_id,omitempty,string"`
	PostID        *int64     `json:"post_id,omitempty,string"`
	MusicID       *int64     `json:"music_id,omitempty,string"`
	CommentID     *int64     `json:"comment_id,omitempty,string"`
	Position      *int32     `json:"position,omitempty"`
	AuthorKey     string     `json:"author_key,omitempty"`
	SourceURL     *string    `json:"source_url,omitempty"`
	SourceExpiry  *time.Time `json:"source_expires_at,omitempty"`
	Width         *int32     `json:"width,omitempty"`
	Height        *int32     `json:"height,omitempty"`
	DurationMS    *int64     `json:"duration_ms,omitempty"`
	Bitrate       *int64     `json:"bitrate,omitempty"`
	MIME          *string    `json:"mime,omitempty"`
	Status        string     `json:"status"`
	S3Bucket      *string    `json:"s3_bucket,omitempty"`
	S3Key         *string    `json:"s3_key,omitempty"`
	SizeBytes     *int64     `json:"size_bytes,omitempty"`
	SHA256        string     `json:"sha256,omitempty"`
	DeleteAfter   *time.Time `json:"delete_after,omitempty"`
	DiscoveredAt  time.Time  `json:"discovered_at"`
	StoredAt      *time.Time `json:"stored_at,omitempty"`
	ReadyAt       *time.Time `json:"ready_at,omitempty"`
	DeletedAt     *time.Time `json:"deleted_at,omitempty"`
}

const mediaSelect = `SELECT media_id::text, source_media_id, kind, variant, user_id, post_id, music_id, comment_id, position,
	author_key, source_url, source_expires_at, width, height, duration_ms, bitrate, mime, status, s3_bucket, s3_key, size_bytes,
	sha256, delete_after, discovered_at, stored_at, ready_at, deleted_at FROM media`

func scanMedia(pii bool) pgx.RowToFunc[Media] {
	return func(row pgx.CollectableRow) (Media, error) {
		var m Media
		var sha, key []byte
		err := row.Scan(&m.MediaID, &m.SourceMediaID, &m.Kind, &m.Variant, &m.UserID, &m.PostID, &m.MusicID, &m.CommentID,
			&m.Position, &key, &m.SourceURL, &m.SourceExpiry, &m.Width, &m.Height, &m.DurationMS, &m.Bitrate, &m.MIME, &m.Status,
			&m.S3Bucket, &m.S3Key, &m.SizeBytes, &sha, &m.DeleteAfter, &m.DiscoveredAt, &m.StoredAt, &m.ReadyAt, &m.DeletedAt)
		if len(sha) > 0 {
			m.SHA256 = hex.EncodeToString(sha)
		}
		if len(key) > 0 {
			m.AuthorKey = hex.EncodeToString(key)
			if !pii {
				m.SourceURL = nil
			}
		}
		if !pii && m.Kind == "avatar" {
			m.SourceURL = nil
		}
		return m, err
	}
}

func (s *Server) postMedia(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "post")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid post id", "post id must be numeric")
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), mediaSelect+` WHERE post_id = $1 AND comment_id IS NULL
		ORDER BY kind, position NULLS FIRST, variant LIMIT $2`, id, limitParam(r))
	if err != nil {
		serverError(w, err)
		return
	}
	out, err := pgx.CollectRows(rows, scanMedia(clientFrom(r.Context()).Has(ScopePIIRead)))
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []Media{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"media": out})
}

func (s *Server) getMedia(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "media"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid media id", "media id must be a UUID")
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), mediaSelect+` WHERE media_id = $1`, id)
	if err != nil {
		serverError(w, err)
		return
	}
	m, err := pgx.CollectExactlyOneRow(rows, scanMedia(clientFrom(r.Context()).Has(ScopePIIRead)))
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// getMusic is a sound. An original sound is named after its creator, so
// its author's name needs pii:read.
func (s *Server) getMusic(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(r, "music")
	if !ok {
		problem(w, http.StatusBadRequest, "invalid music id", "music id must be numeric")
		return
	}
	var m struct {
		MusicID      int64     `json:"music_id,string"`
		Title        *string   `json:"title,omitempty"`
		AuthorName   *string   `json:"author_name,omitempty"`
		Original     *bool     `json:"original,omitempty"`
		DurationMS   *int64    `json:"duration_ms,omitempty"`
		Album        *string   `json:"album,omitempty"`
		CoverMediaID *string   `json:"cover_media_id,omitempty"`
		Copyrighted  *bool     `json:"copyrighted,omitempty"`
		Commerce     *bool     `json:"commerce,omitempty"`
		FirstSeenAt  time.Time `json:"first_seen_at"`
		LastSeenAt   time.Time `json:"last_seen_at"`
	}
	err := s.ReadDB.QueryRow(r.Context(), `SELECT music_id, title, author_name, original, duration_ms, album,
		cover_media_id::text, copyrighted, commerce, first_seen_at, last_seen_at FROM music WHERE music_id = $1`, id).
		Scan(&m.MusicID, &m.Title, &m.AuthorName, &m.Original, &m.DurationMS, &m.Album, &m.CoverMediaID, &m.Copyrighted,
			&m.Commerce, &m.FirstSeenAt, &m.LastSeenAt)
	if err != nil {
		serverError(w, err)
		return
	}
	if !clientFrom(r.Context()).Has(ScopePIIRead) {
		m.AuthorName = nil
	}
	writeJSON(w, http.StatusOK, m)
}
