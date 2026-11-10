package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Repost is the API view of one post on an account's reposts tab
// (discovery's reposts source, M19; owner D-047). TikTok shows no repost
// time: the times are when tks first and last saw it on the tab. The post
// itself is at /v1/posts/{post}.
type Repost struct {
	PostID       int64     `json:"post_id,string"`
	AuthorUserID int64     `json:"author_user_id,string"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
}

// RepostTab is the tab's read state (its discovery target).
type RepostTab struct {
	State      string     `json:"state"`
	StatusCode *int64     `json:"status_code,omitempty"`
	LastReadAt *time.Time `json:"last_read_at,omitempty"`
	NextReadAt *time.Time `json:"next_read_at,omitempty"`
	Reads      int64      `json:"reads"`
	LastStop   *string    `json:"last_stop,omitempty"`
}

// accountReposts serves GET /v1/accounts/{account}/reposts: the posts the
// account reposted, the newest first seen first.
func (s *Server) accountReposts(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolveAccount(w, r)
	if !ok {
		return
	}
	cur, ok := cursorParam(w, r)
	if !ok {
		return
	}
	before, beforeID := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), int64(1<<63-1)
	if cur != nil {
		before, beforeID = cur.T, cur.ID
	}
	limit := limitParam(r)
	ctx := r.Context()
	var tab *RepostTab
	var t RepostTab
	err := s.ReadDB.QueryRow(ctx, `SELECT state, status_code, last_read_at, next_read_at, reads, last_stop
		FROM discovery_targets WHERE source = 'repost' AND key = $1`, strconv.FormatInt(id, 10)).
		Scan(&t.State, &t.StatusCode, &t.LastReadAt, &t.NextReadAt, &t.Reads, &t.LastStop)
	switch {
	case err == nil:
		tab = &t
	case !errors.Is(err, pgx.ErrNoRows):
		serverError(w, err)
		return
	}
	rows, err := s.ReadDB.Query(ctx, `SELECT post_id, author_user_id, first_seen_at, last_seen_at FROM reposts
		WHERE user_id = $1 AND (first_seen_at, post_id) < ($2, $3)
		ORDER BY first_seen_at DESC, post_id DESC LIMIT $4`, id, before, beforeID, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Repost, error) {
		var p Repost
		return p, row.Scan(&p.PostID, &p.AuthorUserID, &p.FirstSeenAt, &p.LastSeenAt)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []Repost{}
	}
	resp := map[string]any{"reposts": out}
	if tab != nil {
		resp["tab"] = tab
	}
	if len(out) == limit {
		last := out[len(out)-1]
		resp["next_cursor"] = encodeCursor(last.FirstSeenAt, last.PostID)
	}
	writeJSON(w, http.StatusOK, resp)
}
