package api

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
)

// Page is the API view of a page.
type Page struct {
	PageID         int64           `json:"page_id"`
	Vanity         *string         `json:"vanity,omitempty"`
	URL            *string         `json:"url,omitempty"`
	Name           *string         `json:"name,omitempty"`
	Categories     []string        `json:"categories,omitempty"`
	Intro          *string         `json:"intro,omitempty"`
	FollowersCount *int64          `json:"followers_count,omitempty"`
	LikesCount     *int64          `json:"likes_count,omitempty"`
	Verified       *bool           `json:"verified,omitempty"`
	ProfilePicURL  *string         `json:"profile_pic_url,omitempty"`
	CoverURL       *string         `json:"cover_url,omitempty"`
	Contact        json.RawMessage `json:"contact,omitempty"`
	Status         string          `json:"status"`
	FirstSeenAt    time.Time       `json:"first_seen_at"`
	LastScrapedAt  *time.Time      `json:"last_scraped_at,omitempty"`
}

func (s *Server) pageByID(ctx context.Context, id int64) (*Page, error) {
	var p Page
	err := s.ReadDB.QueryRow(ctx, `SELECT page_id, vanity, url, name, categories, intro, followers_count, likes_count,
		verified, profile_pic_url, cover_url, contact, status, first_seen_at, last_scraped_at FROM pages WHERE page_id = $1`, id).
		Scan(&p.PageID, &p.Vanity, &p.URL, &p.Name, &p.Categories, &p.Intro, &p.FollowersCount, &p.LikesCount,
			&p.Verified, &p.ProfilePicURL, &p.CoverURL, &p.Contact, &p.Status, &p.FirstSeenAt, &p.LastScrapedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// resolvePage maps the {page} path parameter (id, vanity or URL-escaped
// reference) to a page id and normalised reference.
func (s *Server) resolvePage(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	ref, err := task.NormalizeRef(chi.URLParam(r, "page"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid page", err.Error())
		return 0, "", false
	}
	id, ok := s.Dispatch.ResolvePageID(r.Context(), ref)
	if !ok {
		problem(w, http.StatusNotFound, "unknown page", "the page has not been scraped yet")
		return 0, ref, false
	}
	return id, ref, true
}

type registerReq struct {
	Page     string `json:"page"`
	Schedule *struct {
		Enabled       *bool `json:"enabled,omitempty"`
		IntervalHours *int  `json:"interval_hours,omitempty"`
	} `json:"schedule,omitempty"`
	// Backfill runs a full crawl now. By default only for a page that has
	// had no job yet, so registering a list again starts nothing twice,
	// while a registration whose backfill failed to start gets it on retry.
	Backfill *bool `json:"backfill,omitempty"`
	CrawlOptions
}

// registerPage adds a page to the schedule and starts its backfill.
func (s *Server) registerPage(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Page)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid page", err.Error())
		return
	}
	backfillLim, err := s.limits(req.CrawlOptions, true)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	interval := s.Cfg.Scheduler.DefaultInterval
	enabled, adaptive := true, true
	if req.Schedule != nil {
		if req.Schedule.IntervalHours != nil {
			adaptive = false // an explicit interval is kept; tiers no longer move it
			if *req.Schedule.IntervalHours < 1 || *req.Schedule.IntervalHours > 24*30 {
				problem(w, http.StatusBadRequest, "invalid schedule", "interval_hours must be between 1 and 720")
				return
			}
			interval = time.Duration(*req.Schedule.IntervalHours) * time.Hour
		}
		if req.Schedule.Enabled != nil {
			enabled = *req.Schedule.Enabled
		}
	}
	opts, _ := json.Marshal(req.CrawlOptions)
	c := clientFrom(r.Context())
	next := time.Now().Add(interval + time.Duration(rand.Int64N(int64(interval)/10+1)))
	var maxAge *int
	if req.MaxAgeDays != nil {
		maxAge = req.MaxAgeDays
	}
	var created, neverRun bool
	var intervalSeconds int
	err = pgx.BeginFunc(r.Context(), s.DB, func(tx pgx.Tx) error {
		err := tx.QueryRow(r.Context(), `INSERT INTO page_schedule (page_ref, enabled, interval_seconds, next_run_at, options, max_age_days, max_posts, adaptive)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (page_ref) DO UPDATE SET enabled = page_schedule.enabled OR EXCLUDED.enabled,
				interval_seconds = CASE WHEN EXCLUDED.adaptive THEN page_schedule.interval_seconds
					ELSE LEAST(page_schedule.interval_seconds, EXCLUDED.interval_seconds) END,
				adaptive = page_schedule.adaptive AND EXCLUDED.adaptive
			RETURNING old.page_ref IS NULL, old.last_job_id IS NULL,
				new.enabled, new.interval_seconds, new.next_run_at, new.adaptive`,
			ref, enabled, int(interval.Seconds()), next, opts, maxAge, req.MaxPosts, adaptive).
			Scan(&created, &neverRun, &enabled, &intervalSeconds, &next, &adaptive)
		if err != nil {
			return err
		}
		_, err = tx.Exec(r.Context(), `INSERT INTO page_registrations (api_client_id, page_ref) VALUES ($1,$2) ON CONFLICT DO NOTHING`, c.ID, ref)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	resp := map[string]any{"page_ref": ref, "created": created, "schedule": map[string]any{"enabled": enabled,
		"interval_seconds": intervalSeconds, "next_run_at": next, "adaptive": adaptive}}
	if (req.Backfill == nil && neverRun) || (req.Backfill != nil && *req.Backfill) {
		j, _, err := s.Dispatch.Create(r.Context(), dispatch.Request{ClientID: &c.ID, Kind: dispatch.KindBackfill,
			Lane: taskv1.Lane_LANE_P2, Mode: "full", PageRef: ref, Limits: backfillLim,
			Params: map[string]any{"page": req.Page}}, 0)
		if err != nil {
			serverError(w, err)
			return
		}
		resp["backfill_job"] = j
		_, _ = s.DB.Exec(r.Context(), `UPDATE page_schedule SET last_job_id = $2 WHERE page_ref = $1`, ref, j.ID)
	}
	writeJSON(w, http.StatusCreated, resp)
}

type scheduleView struct {
	PageRef         string          `json:"page_ref"`
	PageID          *int64          `json:"page_id,omitempty"`
	Enabled         bool            `json:"enabled"`
	Tier            string          `json:"tier"`
	IntervalSeconds int             `json:"interval_seconds"`
	NextRunAt       time.Time       `json:"next_run_at"`
	LastRunAt       *time.Time      `json:"last_run_at,omitempty"`
	Options         json.RawMessage `json:"options,omitempty"`
	Failures        int             `json:"consecutive_failures"`
	Adaptive        bool            `json:"adaptive"`
	PostsPerDay     *float64        `json:"posts_per_day,omitempty"`
	LastPostAt      *time.Time      `json:"last_post_at,omitempty"`
}

const scheduleColumns = `s.page_ref, s.page_id, s.enabled, s.tier, s.interval_seconds, s.next_run_at, s.last_run_at, s.options,
	s.consecutive_failures, s.adaptive, s.posts_per_day, s.last_post_at`

func scanSchedule(row pgx.Row) (scheduleView, error) {
	var v scheduleView
	err := row.Scan(&v.PageRef, &v.PageID, &v.Enabled, &v.Tier, &v.IntervalSeconds, &v.NextRunAt, &v.LastRunAt, &v.Options,
		&v.Failures, &v.Adaptive, &v.PostsPerDay, &v.LastPostAt)
	return v, err
}

func (s *Server) listPages(w http.ResponseWriter, r *http.Request) {
	c := clientFrom(r.Context())
	q := `SELECT ` + scheduleColumns + ` FROM page_schedule s`
	args := []any{}
	if !c.Has(ScopeAdmin) {
		q += ` JOIN page_registrations r ON r.page_ref = s.page_ref AND r.api_client_id = $1`
		args = append(args, c.ID)
	}
	args = append(args, limitParam(r))
	q += ` ORDER BY s.page_ref LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.ReadDB.Query(r.Context(), q, args...)
	if err != nil {
		serverError(w, err)
		return
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (scheduleView, error) { return scanSchedule(row) })
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pages": out})
}

func (s *Server) getPage(w http.ResponseWriter, r *http.Request) {
	id, ref, ok := s.resolvePage(w, r)
	if !ok {
		return
	}
	p, err := s.pageByID(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	out := map[string]any{"page": p, "page_ref": ref}
	v, err := scanSchedule(s.ReadDB.QueryRow(r.Context(), `SELECT `+scheduleColumns+`
		FROM page_schedule s WHERE s.page_id = $1 OR s.page_ref = $2 LIMIT 1`, id, ref))
	if err == nil {
		out["schedule"] = v
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) updatePage(w http.ResponseWriter, r *http.Request) {
	ref, err := task.NormalizeRef(chi.URLParam(r, "page"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid page", err.Error())
		return
	}
	var req struct {
		Enabled       *bool `json:"enabled,omitempty"`
		IntervalHours *int  `json:"interval_hours,omitempty"`
		// Adaptive true hands the interval back to the adaptive tiers.
		Adaptive *bool `json:"adaptive,omitempty"`
	}
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	var interval *int
	if req.IntervalHours != nil {
		if *req.IntervalHours < 1 || *req.IntervalHours > 720 {
			problem(w, http.StatusBadRequest, "invalid schedule", "interval_hours must be between 1 and 720")
			return
		}
		v := *req.IntervalHours * 3600
		interval = &v
	}
	adaptive := req.Adaptive
	if interval != nil && adaptive == nil {
		f := false
		adaptive = &f // an explicit interval is kept
	}
	tag, err := s.DB.Exec(r.Context(), `UPDATE page_schedule SET enabled = COALESCE($2, enabled),
		interval_seconds = COALESCE($3, interval_seconds), adaptive = COALESCE($5, adaptive)
		WHERE page_ref = $1 OR page_id::text = $4`,
		ref, req.Enabled, interval, strings.TrimPrefix(ref, "id:"), adaptive)
	if err != nil {
		serverError(w, err)
		return
	}
	if tag.RowsAffected() == 0 {
		problem(w, http.StatusNotFound, "not registered", "page is not registered")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deletePage unregisters the caller; ?purge=true (admin) erases the page's
// data and suppresses it.
func (s *Server) deletePage(w http.ResponseWriter, r *http.Request) {
	ref, err := task.NormalizeRef(chi.URLParam(r, "page"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid page", err.Error())
		return
	}
	c := clientFrom(r.Context())
	if _, err := s.DB.Exec(r.Context(), `DELETE FROM page_registrations WHERE api_client_id = $1 AND page_ref = $2`, c.ID, ref); err != nil {
		serverError(w, err)
		return
	}
	// Disable the schedule once nobody is registered.
	if _, err := s.DB.Exec(r.Context(), `UPDATE page_schedule SET enabled = false WHERE page_ref = $1
		AND NOT EXISTS (SELECT 1 FROM page_registrations WHERE page_ref = $1)`, ref); err != nil {
		serverError(w, err)
		return
	}
	if boolParam(r, "purge") {
		if !c.Has(ScopeAdmin) {
			problem(w, http.StatusForbidden, "forbidden", "purge requires the admin scope")
			return
		}
		id, ok := s.Dispatch.ResolvePageID(r.Context(), ref)
		if !ok {
			problem(w, http.StatusNotFound, "unknown page", "page id unknown")
			return
		}
		er, err := s.insertErasure(r.Context(), c, "page", nil, id, map[string]any{"page": ref})
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, er)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) pageSnapshots(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.resolvePage(w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, name, followers_count, likes_count, verified
		FROM page_snapshots WHERE page_id = $1 ORDER BY captured_at DESC LIMIT $2`, id, limitParam(r))
	if err != nil {
		serverError(w, err)
		return
	}
	type snap struct {
		CapturedAt time.Time `json:"captured_at"`
		Name       *string   `json:"name,omitempty"`
		Followers  *int64    `json:"followers_count,omitempty"`
		Likes      *int64    `json:"likes_count,omitempty"`
		Verified   *bool     `json:"verified,omitempty"`
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (snap, error) {
		var s snap
		return s, row.Scan(&s.CapturedAt, &s.Name, &s.Followers, &s.Likes, &s.Verified)
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}
