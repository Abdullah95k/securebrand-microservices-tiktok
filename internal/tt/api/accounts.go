package api

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/erasure"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Account is the API view of a TikTok account. TikTok's ids are JSON
// strings: most do not fit a JavaScript number. The username, nickname,
// bio, bio link and avatar are personal data and need pii:read (ADR 0007
// §6); the ids, flags and counts do not.
type Account struct {
	UserID             int64      `json:"user_id,string"`
	SecUID             string     `json:"sec_uid"`
	Username           *string    `json:"username,omitempty"`
	Nickname           *string    `json:"nickname,omitempty"`
	Bio                *string    `json:"bio,omitempty"`
	BioLink            *string    `json:"bio_link,omitempty"`
	AvatarURL          *string    `json:"avatar_url,omitempty"`
	AvatarMediaID      *string    `json:"avatar_media_id,omitempty"`
	Verified           *bool      `json:"verified,omitempty"`
	Private            *bool      `json:"private,omitempty"`
	Organization       *bool      `json:"organization,omitempty"`
	Language           *string    `json:"language,omitempty"`
	CreatedAt          *time.Time `json:"created_at,omitempty"`
	UsernameModifiedAt *time.Time `json:"username_modified_at,omitempty"`
	NicknameModifiedAt *time.Time `json:"nickname_modified_at,omitempty"`
	FollowerCount      *int64     `json:"follower_count,omitempty"`
	FollowingCount     *int64     `json:"following_count,omitempty"`
	HeartCount         *int64     `json:"heart_count,omitempty"`
	VideoCount         *int64     `json:"video_count,omitempty"`
	LikesGivenCount    *int64     `json:"likes_given_count,omitempty"`
	FriendCount        *int64     `json:"friend_count,omitempty"`
	AuthorKey          string     `json:"author_key"`
	// Status is active, private, unavailable or banned.
	Status             string     `json:"status"`
	FirstSeenAt        time.Time  `json:"first_seen_at"`
	LastSeenAt         time.Time  `json:"last_seen_at"`
	ProfileRefreshedAt *time.Time `json:"profile_refreshed_at,omitempty"`
	PageReadAt         *time.Time `json:"page_read_at,omitempty"`
}

const accountSelect = `SELECT user_id, sec_uid, username, nickname, bio, bio_link, avatar_url, avatar_media_id::text, verified,
	private, organization, language, created_at, username_modified_at, nickname_modified_at, follower_count, following_count,
	heart_count, video_count, likes_given_count, friend_count, author_key, status, first_seen_at, last_seen_at,
	profile_refreshed_at, page_read_at FROM accounts`

func (s *Server) accountByID(ctx context.Context, id int64, pii bool) (*Account, error) {
	var a Account
	var key []byte
	err := s.ReadDB.QueryRow(ctx, accountSelect+` WHERE user_id = $1`, id).Scan(&a.UserID, &a.SecUID, &a.Username, &a.Nickname,
		&a.Bio, &a.BioLink, &a.AvatarURL, &a.AvatarMediaID, &a.Verified, &a.Private, &a.Organization, &a.Language, &a.CreatedAt,
		&a.UsernameModifiedAt, &a.NicknameModifiedAt, &a.FollowerCount, &a.FollowingCount, &a.HeartCount, &a.VideoCount,
		&a.LikesGivenCount, &a.FriendCount, &key, &a.Status, &a.FirstSeenAt, &a.LastSeenAt, &a.ProfileRefreshedAt, &a.PageReadAt)
	if err != nil {
		return nil, err
	}
	a.AuthorKey = hex.EncodeToString(key)
	if !pii {
		a.Username, a.Nickname, a.Bio, a.BioLink, a.AvatarURL, a.AvatarMediaID = nil, nil, nil, nil, nil, nil
	}
	return &a, nil
}

// accountRef reads the {account} path parameter: id:<user id>,
// sec:<secUid>, u:<username>, @username, a username or a profile link
// (URL-escaped). A bare number is a username: TikTok allows all-digit
// usernames, so a user id needs its id: prefix.
func accountRef(w http.ResponseWriter, r *http.Request) (string, bool) {
	// chi matches the escaped path, so a link's slashes arrive escaped.
	raw, err := url.PathUnescape(chi.URLParam(r, "account"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid account", err.Error())
		return "", false
	}
	ref, err := task.NormalizeRef(raw)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid account", err.Error())
		return "", false
	}
	return ref, true
}

// resolveAccount maps the {account} path parameter to a stored account
// (by user id, secUid, or a current or former username: dispatch.Resolve).
func (s *Server) resolveAccount(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	ref, ok := accountRef(w, r)
	if !ok {
		return 0, "", false
	}
	a, found, err := dispatch.Resolve(r.Context(), s.ReadDB, ref)
	if err != nil {
		serverError(w, err)
		return 0, ref, false
	}
	if !found {
		problem(w, http.StatusNotFound, "unknown account", "tks has not read this account yet")
		return 0, ref, false
	}
	return a.UserID, ref, true
}

// scheduleRef returns the schedule row an account is registered under: the
// reference itself, or, for an account tks has read, any row of its user
// id (an account registered by username and asked for by secUid or id is
// one schedule).
func (s *Server) scheduleRef(ctx context.Context, ref string, userID int64) (string, bool, error) {
	var got string
	err := s.DB.QueryRow(ctx, `SELECT account_ref FROM account_schedule WHERE account_ref = $1
		OR ($2::bigint IS NOT NULL AND user_id = $2) ORDER BY account_ref = $1 DESC, created_at LIMIT 1`, ref, nilIfZero(userID)).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return ref, false, nil
	}
	return got, err == nil, err
}

func nilIfZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

type scheduleReq struct {
	Enabled *bool `json:"enabled,omitempty"`
	// IntervalMinutes is the time between checks: tt.freshness.list_interval
	// (2 h 45 min) unless set; the build brief's delay target assumes it.
	IntervalMinutes *int `json:"interval_minutes,omitempty"`
}

// Interval bounds: no faster than every 30 minutes, no slower than monthly.
const (
	minIntervalMinutes = 30
	maxIntervalMinutes = 30 * 24 * 60
)

func (q *scheduleReq) interval() (*int, error) {
	if q == nil || q.IntervalMinutes == nil {
		return nil, nil
	}
	if m := *q.IntervalMinutes; m < minIntervalMinutes || m > maxIntervalMinutes {
		return nil, errors.New("interval_minutes must be between 30 and 43200")
	}
	v := *q.IntervalMinutes * 60
	return &v, nil
}

type registerReq struct {
	Account  string       `json:"account"`
	Schedule *scheduleReq `json:"schedule,omitempty"`
	// Options apply to every scheduled job of the account; the first
	// registration's stay.
	Options *CrawlOptions `json:"options,omitempty"`
}

// registerAccount puts an account on the schedule, due now: the planner's
// first job for an account never read is its backfill (365 days or to its
// first post), every later one a check every interval. Registering again is
// safe: the schedule is kept (enabled if it was not, an explicit interval
// only shortens it) and no second backfill starts.
func (s *Server) registerAccount(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Account)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid account", err.Error())
		return
	}
	explicit, err := req.Schedule.interval()
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid schedule", err.Error())
		return
	}
	var opts CrawlOptions
	if req.Options != nil {
		opts = *req.Options
	}
	if err := s.validate(opts); err != nil {
		problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	ctx := r.Context()
	acc, known, err := dispatch.Resolve(ctx, s.DB, ref)
	if err != nil {
		serverError(w, err)
		return
	}
	var uid int64
	if known {
		uid = acc.UserID
	}
	if s.refuseErased(w, r, ref, uid) {
		return
	}
	if !known && task.ScopeFromRef(ref).UserId != 0 {
		problem(w, http.StatusUnprocessableEntity, "unknown account",
			"an id: reference needs an account tks has read; register it by username or secUid")
		return
	}
	schedRef, _, err := s.scheduleRef(ctx, ref, uid)
	if err != nil {
		serverError(w, err)
		return
	}
	interval := int(s.Cfg.TT.Freshness.ListInterval.Seconds())
	if explicit != nil {
		interval = *explicit
	}
	enabled := req.Schedule == nil || req.Schedule.Enabled == nil || *req.Schedule.Enabled
	optsJSON, _ := json.Marshal(opts)
	c := clientFrom(r.Context())
	var created bool
	var v scheduleView
	err = pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `INSERT INTO account_schedule AS s (account_ref, user_id, enabled, interval_seconds, next_run_at,
				options, max_age_days, max_posts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (account_ref) DO UPDATE SET enabled = s.enabled OR EXCLUDED.enabled,
				interval_seconds = CASE WHEN $9 THEN least(s.interval_seconds, EXCLUDED.interval_seconds) ELSE s.interval_seconds END,
				user_id = coalesce(s.user_id, EXCLUDED.user_id)
			RETURNING old.account_ref IS NULL`,
			schedRef, nilIfZero(uid), enabled, interval, s.now(), optsJSON, opts.MaxAgeDays, opts.MaxPosts, explicit != nil).Scan(&created)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO account_registrations (api_client_id, account_ref) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			c.ID, schedRef); err != nil {
			return err
		}
		v, err = scanSchedule(tx.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM account_schedule s WHERE s.account_ref = $1`, schedRef))
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"account_ref": schedRef, "created": created, "schedule": v})
}

// refuseErased answers 409 for an erased account (by its stored user id,
// or the id of an id: reference: an erasure deletes the names that other
// references would need) and reports whether it did.
func (s *Server) refuseErased(w http.ResponseWriter, r *http.Request, ref string, uid int64) bool {
	if uid == 0 {
		uid = task.ScopeFromRef(ref).UserId
	}
	if uid == 0 {
		return false
	}
	var erased bool
	if err := s.DB.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM suppression WHERE subject_type = 'account' AND subject_key = $1)`,
		strconv.FormatInt(uid, 10)).Scan(&erased); err != nil {
		serverError(w, err)
		return true
	}
	if erased {
		problem(w, http.StatusConflict, "erased account", "this account was erased and is not collected again")
	}
	return erased
}

type scheduleView struct {
	AccountRef      string          `json:"account_ref"`
	UserID          *int64          `json:"user_id,omitempty,string"`
	Enabled         bool            `json:"enabled"`
	IntervalSeconds int             `json:"interval_seconds"`
	NextRunAt       time.Time       `json:"next_run_at"`
	LastRunAt       *time.Time      `json:"last_run_at,omitempty"`
	LastSuccessAt   *time.Time      `json:"last_success_at,omitempty"`
	Options         json.RawMessage `json:"options,omitempty"`
	Failures        int             `json:"consecutive_failures"`
	PostsPerDay     *float64        `json:"posts_per_day,omitempty"`
	LastPostAt      *time.Time      `json:"last_post_at,omitempty"`
	Walks           json.RawMessage `json:"walks,omitempty"`
	LastJobID       *string         `json:"last_job_id,omitempty"`
}

const scheduleColumns = `s.account_ref, s.user_id, s.enabled, s.interval_seconds, s.next_run_at, s.last_run_at, s.last_success_at,
	s.options, s.consecutive_failures, s.posts_per_day, s.last_post_at, s.walks, s.last_job_id::text`

func scanSchedule(row pgx.Row) (scheduleView, error) {
	var v scheduleView
	err := row.Scan(&v.AccountRef, &v.UserID, &v.Enabled, &v.IntervalSeconds, &v.NextRunAt, &v.LastRunAt, &v.LastSuccessAt,
		&v.Options, &v.Failures, &v.PostsPerDay, &v.LastPostAt, &v.Walks, &v.LastJobID)
	return v, err
}

// accountItem is a registered account in a list: its schedule, and the
// stored account once read.
type accountItem struct {
	scheduleView
	Status   *string `json:"status,omitempty"`
	Username *string `json:"username,omitempty"`
}

// listAccounts lists the caller's registered accounts (every account for
// an admin) by reference, cursor-paged.
func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	c := clientFrom(r.Context())
	pii := c.Has(ScopePIIRead)
	after := ""
	if v := r.URL.Query().Get("cursor"); v != "" {
		b, err := base64.RawURLEncoding.DecodeString(v)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid cursor", "cursor must be a next_cursor value from this API")
			return
		}
		after = string(b)
	}
	q := `SELECT ` + scheduleColumns + `, a.status, a.username FROM account_schedule s LEFT JOIN accounts a ON a.user_id = s.user_id`
	args := []any{after}
	where := ` WHERE s.account_ref > $1`
	if !c.Has(ScopeAdmin) {
		args = append(args, c.ID)
		q += ` JOIN account_registrations g ON g.account_ref = s.account_ref AND g.api_client_id = $2`
	}
	limit := limitParam(r)
	args = append(args, limit)
	q += where + ` ORDER BY s.account_ref LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.ReadDB.Query(r.Context(), q, args...)
	if err != nil {
		serverError(w, err)
		return
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (accountItem, error) {
		var it accountItem
		v := &it.scheduleView
		err := row.Scan(&v.AccountRef, &v.UserID, &v.Enabled, &v.IntervalSeconds, &v.NextRunAt, &v.LastRunAt, &v.LastSuccessAt,
			&v.Options, &v.Failures, &v.PostsPerDay, &v.LastPostAt, &v.Walks, &v.LastJobID, &it.Status, &it.Username)
		if !pii {
			it.Username = nil
		}
		return it, err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []accountItem{}
	}
	resp := map[string]any{"accounts": out}
	if len(out) == limit {
		resp["next_cursor"] = base64.RawURLEncoding.EncodeToString([]byte(out[len(out)-1].AccountRef))
	}
	writeJSON(w, http.StatusOK, resp)
}

// getAccount returns the stored account and its schedule; an account
// registered but not read yet has only its schedule.
func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	ref, ok := accountRef(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	acc, found, err := dispatch.Resolve(ctx, s.ReadDB, ref)
	if err != nil {
		serverError(w, err)
		return
	}
	out := map[string]any{"account_ref": ref}
	var uid int64
	if found {
		uid = acc.UserID
		a, err := s.accountByID(ctx, uid, clientFrom(ctx).Has(ScopePIIRead))
		if err != nil {
			serverError(w, err)
			return
		}
		out["account"] = a
	}
	schedRef, registered, err := s.scheduleRef(ctx, ref, uid)
	if err != nil {
		serverError(w, err)
		return
	}
	if registered {
		v, err := scanSchedule(s.ReadDB.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM account_schedule s WHERE s.account_ref = $1`, schedRef))
		if err != nil {
			serverError(w, err)
			return
		}
		out["schedule"] = v
	}
	if !found && !registered {
		problem(w, http.StatusNotFound, "unknown account", "tks has not read this account and it is not registered")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type updateReq struct {
	scheduleReq
	Options *CrawlOptions `json:"options,omitempty"`
}

// updateAccount changes an account's schedule. The schedule is shared by
// every client registered to the account: only a registered client (or an
// admin) may change it, and while others are registered too it may only be
// checked more (enabled, a shorter interval), as a second registration
// does; disabling it, checking it less often or changing its options is
// the admin's.
func (s *Server) updateAccount(w http.ResponseWriter, r *http.Request) {
	ref, ok := accountRef(w, r)
	if !ok {
		return
	}
	var req updateReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	interval, err := req.interval()
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid schedule", err.Error())
		return
	}
	var opts []byte
	if req.Options != nil {
		if err := s.validate(*req.Options); err != nil {
			problem(w, http.StatusBadRequest, "invalid options", err.Error())
			return
		}
		opts, _ = json.Marshal(req.Options)
	}
	ctx := r.Context()
	var uid int64
	if acc, found, err := dispatch.Resolve(ctx, s.DB, ref); err != nil {
		serverError(w, err)
		return
	} else if found {
		uid = acc.UserID
	}
	schedRef, registered, err := s.scheduleRef(ctx, ref, uid)
	if err != nil {
		serverError(w, err)
		return
	}
	if !registered {
		problem(w, http.StatusNotFound, "not registered", "the account is not registered")
		return
	}
	c := clientFrom(ctx)
	if !c.Has(ScopeAdmin) {
		var mine bool
		var others, current int
		if err := s.DB.QueryRow(ctx, `SELECT coalesce(bool_or(g.api_client_id = $1), false),
				count(DISTINCT g.api_client_id) FILTER (WHERE g.api_client_id <> $1), min(s.interval_seconds)
			FROM account_schedule s LEFT JOIN account_registrations g ON g.account_ref = s.account_ref
			WHERE s.account_ref = $2`, c.ID, schedRef).Scan(&mine, &others, &current); err != nil {
			serverError(w, err)
			return
		}
		if !mine {
			problem(w, http.StatusNotFound, "not registered", "the account is not registered by this client")
			return
		}
		less := (req.Enabled != nil && !*req.Enabled) || (interval != nil && *interval > current) || opts != nil
		if others > 0 && less {
			problem(w, http.StatusConflict, "shared schedule",
				"other clients are registered to this account: only an admin may disable it, check it less often or change its options")
			return
		}
	}
	var maxAge *int
	var maxPosts *int64
	if req.Options != nil {
		maxAge, maxPosts = req.Options.MaxAgeDays, req.Options.MaxPosts
	}
	if _, err := s.DB.Exec(ctx, `UPDATE account_schedule SET enabled = coalesce($2, enabled),
			interval_seconds = coalesce($3, interval_seconds), options = coalesce($4, options),
			max_age_days = CASE WHEN $4::jsonb IS NULL THEN max_age_days ELSE $5 END,
			max_posts = CASE WHEN $4::jsonb IS NULL THEN max_posts ELSE $6 END
		WHERE account_ref = $1`, schedRef, req.Enabled, interval, opts, maxAge, maxPosts); err != nil {
		serverError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteAccount unregisters the caller; the schedule is disabled once no
// client is registered. With ?purge=true (admin) it also erases the
// account (an erasure request: its data deleted, its user id suppressed).
func (s *Server) deleteAccount(w http.ResponseWriter, r *http.Request) {
	ref, ok := accountRef(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	c := clientFrom(ctx)
	purge := boolParam(r, "purge")
	if purge && !c.Has(ScopeAdmin) {
		problem(w, http.StatusForbidden, "forbidden", "purge requires the admin scope")
		return
	}
	acc, found, err := dispatch.Resolve(ctx, s.DB, ref)
	if err != nil {
		serverError(w, err)
		return
	}
	var uid int64
	if found {
		uid = acc.UserID
	}
	schedRef, registered, err := s.scheduleRef(ctx, ref, uid)
	if err != nil {
		serverError(w, err)
		return
	}
	if registered {
		err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `DELETE FROM account_registrations WHERE api_client_id = $1 AND account_ref = $2`, c.ID, schedRef); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE account_schedule SET enabled = false WHERE account_ref = $1
				AND NOT EXISTS (SELECT 1 FROM account_registrations WHERE account_ref = $1)`, schedRef)
			return err
		})
		if err != nil {
			serverError(w, err)
			return
		}
	}
	if !purge {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !found {
		problem(w, http.StatusNotFound, "unknown account", "tks has not read this account: erase it by user id")
		return
	}
	e, err := s.insertErasure(ctx, c, erasure.SubjectAccount, nil, uid, map[string]any{"account": ref})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, e)
}

// accountUsernames is the account's username history, newest first
// (pii:read).
func (s *Server) accountUsernames(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolveAccount(w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT username, first_seen_at, last_seen_at FROM account_usernames
		WHERE user_id = $1 ORDER BY last_seen_at DESC, username LIMIT $2`, id, limitParam(r))
	if err != nil {
		serverError(w, err)
		return
	}
	type name struct {
		Username    string    `json:"username"`
		FirstSeenAt time.Time `json:"first_seen_at"`
		LastSeenAt  time.Time `json:"last_seen_at"`
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (name, error) {
		var n name
		return n, row.Scan(&n.Username, &n.FirstSeenAt, &n.LastSeenAt)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []name{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"usernames": out})
}

// accountSnapshots is the account's history (one row when its names, counts
// or flags changed), newest first; names need pii:read.
func (s *Server) accountSnapshots(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolveAccount(w, r)
	if !ok {
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
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, username, nickname, follower_count, following_count, heart_count,
		video_count, verified, private FROM account_snapshots WHERE user_id = $1 AND captured_at < $2
		ORDER BY captured_at DESC LIMIT $3`, id, before, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	type snap struct {
		CapturedAt     time.Time `json:"captured_at"`
		Username       *string   `json:"username,omitempty"`
		Nickname       *string   `json:"nickname,omitempty"`
		FollowerCount  *int64    `json:"follower_count,omitempty"`
		FollowingCount *int64    `json:"following_count,omitempty"`
		HeartCount     *int64    `json:"heart_count,omitempty"`
		VideoCount     *int64    `json:"video_count,omitempty"`
		Verified       *bool     `json:"verified,omitempty"`
		Private        *bool     `json:"private,omitempty"`
	}
	pii := clientFrom(r.Context()).Has(ScopePIIRead)
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (snap, error) {
		var x snap
		err := row.Scan(&x.CapturedAt, &x.Username, &x.Nickname, &x.FollowerCount, &x.FollowingCount, &x.HeartCount,
			&x.VideoCount, &x.Verified, &x.Private)
		if !pii {
			x.Username, x.Nickname = nil, nil
		}
		return x, err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		out = []snap{}
	}
	resp := map[string]any{"snapshots": out}
	if len(out) == limit {
		resp["next_cursor"] = encodeCursor(out[len(out)-1].CapturedAt, 0)
	}
	writeJSON(w, http.StatusOK, resp)
}
