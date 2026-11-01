package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// CrawlOptions are the per-job and per-account crawl settings the API
// accepts. Stored with a registration, the planner applies them to every
// scheduled job of the account (scheduler.applyOptions).
type CrawlOptions struct {
	MaxAgeDays          *int    `json:"max_age_days,omitempty"`
	MaxPosts            *int64  `json:"max_posts,omitempty"`
	MaxCommentsPerPost  *int64  `json:"max_comments_per_post,omitempty"`
	IncludeComments     *bool   `json:"include_comments,omitempty"`
	IncludeReplies      *bool   `json:"include_replies,omitempty"`
	IncludeMedia        *bool   `json:"include_media,omitempty"`
	IncludeCommentMedia *bool   `json:"include_comment_media,omitempty"`
	VideoPolicy         *string `json:"video_policy,omitempty"`
}

// validate checks the options against the configured caps.
func (s *Server) validate(o CrawlOptions) error {
	if o.MaxAgeDays != nil && (*o.MaxAgeDays <= 0 || *o.MaxAgeDays > s.Cfg.Limits.MaxAgeCapDays) {
		return errors.New("max_age_days must be between 1 and the configured cap (limits.max_age_cap_days)")
	}
	if o.MaxPosts != nil && *o.MaxPosts < 0 {
		return errors.New("max_posts must not be negative")
	}
	if o.MaxCommentsPerPost != nil && *o.MaxCommentsPerPost < 0 {
		return errors.New("max_comments_per_post must not be negative")
	}
	if o.VideoPolicy != nil {
		switch *o.VideoPolicy {
		case "none", "thumbnail", "sd", "hd":
		default:
			return errors.New("video_policy must be none, thumbnail, sd or hd")
		}
	}
	return nil
}

// limits applies options over the defaults of tks's config (task.
// DefaultLimits: comments, replies and media as tt: sets them).
func (s *Server) limits(o CrawlOptions, backfill bool) (*tkstaskv1.Limits, error) {
	if err := s.validate(o); err != nil {
		return nil, err
	}
	l := task.DefaultLimits(s.Cfg.Limits, s.Cfg.TT, backfill)
	if o.MaxAgeDays != nil {
		l.MaxAgeSeconds = int64(*o.MaxAgeDays) * 86400
	}
	if o.MaxPosts != nil {
		l.MaxPosts = *o.MaxPosts
	}
	if o.MaxCommentsPerPost != nil {
		l.MaxCommentsPerPost = *o.MaxCommentsPerPost
	}
	for dst, v := range map[*bool]*bool{&l.IncludeComments: o.IncludeComments, &l.IncludeReplies: o.IncludeReplies,
		&l.IncludeMedia: o.IncludeMedia, &l.IncludeCommentMedia: o.IncludeCommentMedia} {
		if v != nil {
			*dst = *v
		}
	}
	if o.VideoPolicy != nil {
		l.VideoPolicy = task.ParseVideoPolicy(*o.VideoPolicy)
	}
	return l, nil
}

type createJobReq struct {
	Account string `json:"account"`
	// auto (default): incremental when the account's list was read before,
	// else full (back tt.freshness.backfill_days); refresh is a metrics
	// walk back max_age_days.
	Mode string `json:"mode,omitempty"`
	CrawlOptions
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Account)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid account", err.Error())
		return
	}
	switch req.Mode {
	case "", dispatch.ModeAuto, dispatch.ModeFull, dispatch.ModeIncremental, dispatch.ModeRefresh:
	default:
		problem(w, http.StatusBadRequest, "invalid mode", "mode must be auto, full, incremental or refresh")
		return
	}
	if s.refuseErased(w, r, ref, 0) {
		return
	}
	lim, err := s.limits(req.CrawlOptions, req.Mode == dispatch.ModeFull)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	c := clientFrom(r.Context())
	j, created, err := s.Dispatch.Create(r.Context(), dispatch.Request{
		ClientID: &c.ID, Kind: dispatch.KindOnDemand, Lane: tkstaskv1.Lane_LANE_P0, Mode: req.Mode, AccountRef: ref,
		Limits: lim, IdempotencyKey: r.Header.Get("Idempotency-Key"), Params: map[string]any{"account": req.Account},
		MaxRunningInLane: s.Cfg.API.MaxRunningP0Jobs,
	}, s.Cfg.API.MaxRunningP0JobsPerClient)
	switch {
	case errors.Is(err, dispatch.ErrUnknownAccount):
		problem(w, http.StatusUnprocessableEntity, "unknown account",
			"an id: reference needs an account tks has read; use the username or the secUid")
		return
	case errors.Is(err, dispatch.ErrTooManyJobs):
		w.Header().Set("Retry-After", "30")
		problem(w, http.StatusTooManyRequests, "too many running jobs", err.Error())
		return
	case errors.Is(err, dispatch.ErrLaneFull):
		w.Header().Set("Retry-After", "30")
		problem(w, http.StatusTooManyRequests, "on-demand capacity reached", err.Error())
		return
	case err != nil:
		serverError(w, err)
		return
	}
	code := http.StatusAccepted
	if !created {
		code = http.StatusOK
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID.String())
	writeJSON(w, code, j)
}

func (s *Server) jobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid job id", err.Error())
		return uuid.Nil, false
	}
	return id, true
}

// ownJob loads job id if the caller may access it as asked (see
// dispatch.Access); admins may do anything. Any other job is reported as
// not found.
func (s *Server) ownJob(w http.ResponseWriter, r *http.Request, a dispatch.Access) (*dispatch.Job, bool) {
	id, ok := s.jobID(w, r)
	if !ok {
		return nil, false
	}
	var client *uuid.UUID
	if c := clientFrom(r.Context()); !c.Has(ScopeAdmin) {
		client = &c.ID
	}
	j, err := s.Dispatch.GetFor(r.Context(), id, client, a)
	if err != nil {
		serverError(w, err)
		return nil, false
	}
	return j, true
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, ok := s.ownJob(w, r, dispatch.Read)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	before := time.Now().Add(time.Second)
	cur, ok := cursorParam(w, r)
	if !ok {
		return
	}
	if cur != nil {
		before = cur.T
	}
	var ref string
	if p := r.URL.Query().Get("account"); p != "" {
		var err error
		if ref, err = task.NormalizeRef(p); err != nil {
			problem(w, http.StatusBadRequest, "invalid account", err.Error())
			return
		}
	}
	c := clientFrom(r.Context())
	var owner *uuid.UUID
	if !c.Has(ScopeAdmin) {
		owner = &c.ID
	}
	limit := limitParam(r)
	js, err := s.Dispatch.List(r.Context(), owner, ref, r.URL.Query().Get("status"), before, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	if js == nil {
		js = []*dispatch.Job{}
	}
	out := map[string]any{"jobs": js}
	if len(js) == limit {
		out["next_cursor"] = encodeCursor(js[len(js)-1].CreatedAt, 0)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	j, ok := s.ownJob(w, r, dispatch.Write)
	if !ok {
		return
	}
	if err := s.Dispatch.Cancel(r.Context(), j.ID); err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": j.ID.String(), "status": "cancelling"})
}

// jobResults streams the job's data as NDJSON: one {"type":"account"}, then
// the account's posts seen during the job, each followed by its comments
// when include=comments, and a last {"type":"end"} (or {"type":"error"}).
func (s *Server) jobResults(w http.ResponseWriter, r *http.Request) {
	j, ok := s.ownJob(w, r, dispatch.Read)
	if !ok {
		return
	}
	if j.UserID == nil {
		problem(w, http.StatusConflict, "no data yet", "the job has not resolved its account yet")
		return
	}
	withComments := r.URL.Query().Get("include") == "comments"
	pii := clientFrom(r.Context()).Has(ScopePIIRead)
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	// The stream has no overall timeout, but each post (with its comments)
	// must reach the client within the request timeout: one that stops
	// reading is cut off instead of holding the handler forever.
	rc := http.NewResponseController(w)
	timeout := s.requestTimeout()
	broken := false
	send := func(v any) bool {
		if !broken && enc.Encode(v) != nil {
			broken = true
		}
		return !broken
	}
	next := func() bool {
		_ = rc.Flush()
		_ = rc.SetWriteDeadline(time.Now().Add(timeout))
		return !broken && r.Context().Err() == nil
	}
	next()
	if a, err := s.accountByID(r.Context(), *j.UserID, pii); err == nil {
		send(map[string]any{"type": "account", "account": a})
	}
	// Posts and comments are read and written in batches, so a large
	// account streams instead of filling memory.
	const batch = 500
	fail := func(err error) {
		slog.Error("api: job results", "job", j.ID, "err", err)
		send(map[string]any{"type": "error", "error": "the results stopped early; retry the request"})
	}
	var cur *cursorPos
	for {
		posts, err := s.queryPosts(r.Context(), postQuery{userID: *j.UserID, seenSince: j.CreatedAt.Add(-time.Minute), cur: cur, limit: batch}, pii)
		if err != nil {
			fail(err)
			return
		}
		for _, p := range posts {
			if !next() {
				return
			}
			send(map[string]any{"type": "post", "post": p})
			if withComments {
				var ccur *cursorPos
				for {
					cs, err := s.queryComments(r.Context(), commentQuery{postID: p.PostID, postCreated: p.CreatedAt, cur: ccur, limit: batch}, pii)
					if err != nil {
						fail(err)
						return
					}
					for _, c := range cs {
						send(map[string]any{"type": "comment", "comment": c})
					}
					if len(cs) < batch || !next() {
						break
					}
					ccur = commentCursor(cs[len(cs)-1])
				}
			}
		}
		if len(posts) < batch {
			break
		}
		last := posts[len(posts)-1]
		cur = &cursorPos{T: last.CreatedAt, ID: last.PostID}
	}
	send(map[string]any{"type": "end"})
}
