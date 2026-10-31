package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
)

// CrawlOptions are the per-job/per-page crawl settings accepted by the API.
type CrawlOptions struct {
	MaxAgeDays          *int    `json:"max_age_days,omitempty"`
	MaxPosts            *int64  `json:"max_posts,omitempty"`
	MaxCommentsPerPost  *int64  `json:"max_comments_per_post,omitempty"`
	MaxDepth            *int32  `json:"max_depth,omitempty"`
	IncludeComments     *bool   `json:"include_comments,omitempty"`
	IncludeReplies      *bool   `json:"include_replies,omitempty"`
	IncludeMedia        *bool   `json:"include_media,omitempty"`
	IncludeCommentMedia *bool   `json:"include_comment_media,omitempty"`
	VideoPolicy         *string `json:"video_policy,omitempty"`
}

// limits applies options over config defaults, enforcing the age cap.
func (s *Server) limits(o CrawlOptions, backfill bool) (*taskv1.Limits, error) {
	l := task.DefaultLimits(s.Cfg.Limits, backfill)
	if o.MaxAgeDays != nil {
		if *o.MaxAgeDays <= 0 || *o.MaxAgeDays > s.Cfg.Limits.MaxAgeCapDays {
			return nil, errors.New("max_age_days must be between 1 and the configured cap")
		}
		l.MaxAgeSeconds = int64(*o.MaxAgeDays) * 86400
	}
	if o.MaxPosts != nil {
		l.MaxPosts = *o.MaxPosts
	}
	if o.MaxCommentsPerPost != nil {
		l.MaxCommentsPerPost = *o.MaxCommentsPerPost
	}
	if o.MaxDepth != nil {
		if *o.MaxDepth < 1 || *o.MaxDepth > 5 {
			return nil, errors.New("max_depth must be between 1 and 5")
		}
		l.MaxDepth = *o.MaxDepth
	}
	set := func(dst *bool, v *bool) {
		if v != nil {
			*dst = *v
		}
	}
	set(&l.IncludeComments, o.IncludeComments)
	set(&l.IncludeReplies, o.IncludeReplies)
	set(&l.IncludeMedia, o.IncludeMedia)
	set(&l.IncludeCommentMedia, o.IncludeCommentMedia)
	if o.VideoPolicy != nil {
		switch *o.VideoPolicy {
		case "none", "thumbnail", "sd", "hd":
			l.VideoPolicy = task.ParseVideoPolicy(*o.VideoPolicy)
		default:
			return nil, errors.New("video_policy must be none, thumbnail, sd or hd")
		}
	}
	return l, nil
}

type createJobReq struct {
	Page string `json:"page"`
	// auto (default): incremental when the page was crawled before, else full.
	Mode string `json:"mode,omitempty"`
	CrawlOptions
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Page)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid page", err.Error())
		return
	}
	switch req.Mode {
	case "", "auto", "full", "incremental":
	default:
		problem(w, http.StatusBadRequest, "invalid mode", "mode must be auto, full or incremental")
		return
	}
	lim, err := s.limits(req.CrawlOptions, false)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	c := clientFrom(r.Context())
	j, created, err := s.Dispatch.Create(r.Context(), dispatch.Request{
		ClientID: &c.ID, Kind: dispatch.KindOnDemand, Lane: taskv1.Lane_LANE_P0, Mode: req.Mode, PageRef: ref,
		Limits: lim, IdempotencyKey: r.Header.Get("Idempotency-Key"), Params: map[string]any{"page": req.Page},
		MaxRunningInLane: s.Cfg.API.MaxRunningP0Jobs,
	}, s.Cfg.API.MaxRunningP0JobsPerClient)
	switch {
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

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	j, err := s.Dispatch.Get(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	before := time.Now().Add(time.Second)
	if c, ok := decodeCursor(r.URL.Query().Get("cursor")); ok {
		before = c.T
	}
	var ref string
	if p := r.URL.Query().Get("page"); p != "" {
		var err error
		if ref, err = task.NormalizeRef(p); err != nil {
			problem(w, http.StatusBadRequest, "invalid page", err.Error())
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
	out := map[string]any{"jobs": js}
	if len(js) == limit {
		out["next_cursor"] = encodeCursor(js[len(js)-1].CreatedAt, 0)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	if err := s.Dispatch.Cancel(r.Context(), id); err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": id.String(), "status": "cancelling"})
}

// jobResults streams the job's page data as NDJSON: one {"type":"page"},
// then posts seen during the job, each followed by its comments when
// include=comments.
func (s *Server) jobResults(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	j, err := s.Dispatch.Get(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	if j.PageID == nil {
		problem(w, http.StatusConflict, "no data yet", "the job has not resolved its page yet")
		return
	}
	withComments := r.URL.Query().Get("include") == "comments"
	pii := clientFrom(r.Context()).Has(ScopePIIRead)
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	flush := func() {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	if p, err := s.pageByID(r.Context(), *j.PageID); err == nil {
		_ = enc.Encode(map[string]any{"type": "page", "page": p})
	}
	posts, err := s.queryPosts(r.Context(), *j.PageID, j.CreatedAt.Add(-time.Minute), time.Time{}, nil, 100000, true)
	if err != nil {
		serverError(w, err)
		return
	}
	for _, p := range posts {
		_ = enc.Encode(map[string]any{"type": "post", "post": p})
		if withComments {
			cs, err := s.queryComments(r.Context(), p.PostID, p.CreatedAt, nil, nil, "", nil, 1_000_000, pii)
			if err != nil {
				return
			}
			for _, c := range cs {
				_ = enc.Encode(map[string]any{"type": "comment", "comment": c})
			}
		}
		flush()
	}
}
