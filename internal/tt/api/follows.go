package api

import (
	"encoding/hex"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// Follow is the API view of one person on a monitored account's following
// or followers list (discovery part D, ADR 0007 §23). Who the person is
// needs pii:read; author_key is always there.
type Follow struct {
	AuthorKey string         `json:"author_key"`
	Person    *CommentAuthor `json:"person,omitempty"`
	// What the list showed of the person when last seen.
	Private        *bool     `json:"private,omitempty"`
	VideoCount     *int64    `json:"video_count,omitempty"`
	FollowerCount  *int64    `json:"follower_count,omitempty"`
	FollowingCount *int64    `json:"following_count,omitempty"`
	FirstSeenAt    time.Time `json:"first_seen_at"`
	LastSeenAt     time.Time `json:"last_seen_at"`
}

// FollowList is a list's read state.
type FollowList struct {
	State      string     `json:"state"`
	StatusCode *int64     `json:"status_code,omitempty"`
	LastReadAt *time.Time `json:"last_read_at,omitempty"`
	NextReadAt time.Time  `json:"next_read_at"`
	DeepDone   bool       `json:"read_to_end"`
	Total      *int64     `json:"total,omitempty"`
	LastStop   *string    `json:"last_stop,omitempty"`
}

// accountFollows serves GET /v1/accounts/{account}/follows: the people on
// one of the account's lists (direction following or follower), the
// newest first seen first.
func (s *Server) accountFollows(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolveAccount(w, r)
	if !ok {
		return
	}
	dir := r.URL.Query().Get("direction")
	switch dir {
	case "":
		dir = "follower"
	case "following", "follower":
	default:
		problem(w, http.StatusBadRequest, "invalid direction", "direction must be following or follower")
		return
	}
	cur, ok := cursorParam(w, r)
	if !ok {
		return
	}
	// No cursor: everything (a link's time is its observation's, which
	// need not be before this server's clock).
	before, beforeID := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), int64(1<<63-1)
	if cur != nil {
		before, beforeID = cur.T, cur.ID
	}
	limit := limitParam(r)
	ctx := r.Context()
	var list *FollowList
	var l FollowList
	err := s.ReadDB.QueryRow(ctx, `SELECT state, status_code, last_read_at, next_read_at, deep_done, total, last_stop
		FROM follow_lists WHERE user_id = $1 AND direction = $2`, id, dir).
		Scan(&l.State, &l.StatusCode, &l.LastReadAt, &l.NextReadAt, &l.DeepDone, &l.Total, &l.LastStop)
	switch {
	case err == nil:
		list = &l
	case err != pgx.ErrNoRows:
		serverError(w, err)
		return
	}
	rows, err := s.ReadDB.Query(ctx, `SELECT f.author_key, f.person_user_id, p.sec_uid, p.username, p.nickname, p.avatar_url, p.verified,
			f.person_private, f.person_videos, f.person_followers, f.person_following, f.first_seen_at, f.last_seen_at
		FROM follows f LEFT JOIN people p ON p.author_key = f.author_key
		WHERE f.user_id = $1 AND f.direction = $2 AND (f.first_seen_at, f.person_user_id) < ($3, $4)
		ORDER BY f.first_seen_at DESC, f.person_user_id DESC LIMIT $5`, id, dir, before, beforeID, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	pii := clientFrom(ctx).Has(ScopePIIRead)
	var last int64
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Follow, error) {
		var f Follow
		var key []byte
		var a CommentAuthor
		err := row.Scan(&key, &a.UserID, &a.SecUID, &a.Username, &a.Nickname, &a.AvatarURL, &a.Verified,
			&f.Private, &f.VideoCount, &f.FollowerCount, &f.FollowingCount, &f.FirstSeenAt, &f.LastSeenAt)
		f.AuthorKey = hex.EncodeToString(key)
		if a.UserID != nil {
			last = *a.UserID
		}
		if pii {
			f.Person = &a
		}
		return f, err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []Follow{}
	}
	resp := map[string]any{"direction": dir, "follows": out}
	if list != nil {
		resp["list"] = list
	}
	if len(out) == limit {
		resp["next_cursor"] = encodeCursor(out[len(out)-1].FirstSeenAt, last)
	}
	writeJSON(w, http.StatusOK, resp)
}
