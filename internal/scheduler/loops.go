package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

func taskLane(l int16) taskv1.Lane { return taskv1.Lane(l) }

// Every runs fn every interval until ctx is done, logging errors.
func Every(ctx context.Context, name string, interval time.Duration, fn func(context.Context) error) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("scheduler: loop failed", "loop", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Planner enqueues scheduled re-scrapes of registered pages, with the
// page's due post comment refreshes riding on the same job.
type Planner struct {
	DB       *pgxpool.Pool
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
	// Adaptive supplies post refreshes; nil disables them.
	Adaptive *Adaptive
	// Budget, when set, returns the traffic budget state: its stretch
	// lengthens the slow tiers' intervals.
	Budget func(context.Context) ratelimit.BudgetState
}

type due struct {
	ref      string
	pageID   *int64
	tier     string
	options  []byte
	maxAge   *int
	maxPosts *int64
	lastJob  *uuid.UUID
}

func (p *Planner) Tick(ctx context.Context) error {
	jitter := p.Cfg.Scheduler.Jitter
	stretch, stretchTiers := 1.0, []string{}
	if p.Budget != nil {
		if st := p.Budget(ctx); st.Stretch > 1 {
			stretch, stretchTiers = st.Stretch, st.StretchTiers
		}
	}
	rows, err := p.DB.Query(ctx, `
		WITH d AS (
			SELECT page_ref FROM page_schedule
			WHERE enabled AND next_run_at <= now() AND (paused_until IS NULL OR paused_until < now())
			ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE page_schedule s SET
			next_run_at = now() + make_interval(secs => s.interval_seconds
				* least(power(2, s.consecutive_failures), 2592000.0 / s.interval_seconds)
				* CASE WHEN s.tier = ANY($3::text[]) THEN $4::float8 ELSE 1.0 END
				* (1 + (random() * 2 - 1) * $2)),
			last_run_at = now()
		FROM d WHERE s.page_ref = d.page_ref
		RETURNING s.page_ref, s.page_id, s.tier, s.options, s.max_age_days, s.max_posts, s.last_job_id`,
		p.Cfg.Scheduler.PlannerBatch, jitter, stretchTiers, stretch)
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
		var d due
		return d, r.Scan(&d.ref, &d.pageID, &d.tier, &d.options, &d.maxAge, &d.maxPosts, &d.lastJob)
	})
	if err != nil {
		return err
	}
	if len(list) == 0 {
		metrics.ScheduleLag.WithLabelValues("all").Set(0)
		return nil
	}
	for _, d := range list {
		if d.lastJob != nil {
			var st string
			if err := p.DB.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, *d.lastJob).Scan(&st); err == nil &&
				(st == "running" || st == "finalizing" || st == "cancelling") {
				continue // previous run still going; skip this slot
			}
		}
		lim := task.DefaultLimits(p.Cfg.Limits, false)
		applyOptions(lim, d.options)
		if d.maxAge != nil && *d.maxAge > 0 {
			lim.MaxAgeSeconds = int64(*d.maxAge) * 86400
		}
		if d.maxPosts != nil {
			lim.MaxPosts = *d.maxPosts
		}
		var posts []dispatch.PostRoot
		commit := func(context.Context) error { return nil }
		if p.Adaptive != nil && d.pageID != nil && lim.IncludeComments {
			if posts, commit, err = p.Adaptive.DueRefreshes(ctx, *d.pageID, d.tier); err != nil {
				slog.Warn("planner: post refreshes", "page", d.ref, "err", err)
				posts, commit = nil, func(context.Context) error { return nil }
			}
		}
		j, _, err := p.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindScheduled, Lane: taskv1.Lane_LANE_P1,
			Mode: "incremental", PageRef: d.ref, Limits: lim, Posts: posts}, 0)
		if err != nil {
			slog.Warn("planner: dispatch failed", "page", d.ref, "err", err)
			continue // the refreshes stay due
		}
		if err := commit(ctx); err != nil {
			slog.Warn("planner: post refreshes not recorded", "page", d.ref, "err", err)
		}
		if _, err := p.DB.Exec(ctx, `UPDATE page_schedule SET last_job_id = $2 WHERE page_ref = $1`, d.ref, j.ID); err != nil {
			return err
		}
	}
	var lag float64
	_ = p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM now() - min(next_run_at)), 0) FROM page_schedule
		WHERE enabled AND next_run_at <= now()`).Scan(&lag)
	metrics.ScheduleLag.WithLabelValues("all").Set(math.Max(lag, 0))
	return nil
}

// TierGauges publishes the number of scheduled pages and the schedule lag
// per tier.
func (p *Planner) TierGauges(ctx context.Context) error {
	rows, err := p.DB.Query(ctx, `SELECT tier, count(*),
			coalesce(extract(epoch FROM now() - min(next_run_at) FILTER (WHERE next_run_at <= now())), 0)
		FROM page_schedule WHERE enabled GROUP BY tier`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tier string
		var n int64
		var lag float64
		if err := rows.Scan(&tier, &n, &lag); err != nil {
			return err
		}
		metrics.PageTier.WithLabelValues(tier).Set(float64(n))
		metrics.ScheduleLag.WithLabelValues(tier).Set(math.Max(lag, 0))
	}
	return rows.Err()
}

// applyOptions overlays stored per-page options (API CrawlOptions JSON).
func applyOptions(l *taskv1.Limits, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var o struct {
		MaxAgeDays          *int    `json:"max_age_days"`
		MaxPosts            *int64  `json:"max_posts"`
		MaxDepth            *int32  `json:"max_depth"`
		IncludeComments     *bool   `json:"include_comments"`
		IncludeReplies      *bool   `json:"include_replies"`
		IncludeMedia        *bool   `json:"include_media"`
		IncludeCommentMedia *bool   `json:"include_comment_media"`
		VideoPolicy         *string `json:"video_policy"`
	}
	if json.Unmarshal(raw, &o) != nil {
		return
	}
	if o.MaxAgeDays != nil {
		l.MaxAgeSeconds = int64(*o.MaxAgeDays) * 86400
	}
	if o.MaxPosts != nil {
		l.MaxPosts = *o.MaxPosts
	}
	if o.MaxDepth != nil {
		l.MaxDepth = *o.MaxDepth
	}
	if o.IncludeComments != nil {
		l.IncludeComments = *o.IncludeComments
	}
	if o.IncludeReplies != nil {
		l.IncludeReplies = *o.IncludeReplies
	}
	if o.IncludeMedia != nil {
		l.IncludeMedia = *o.IncludeMedia
	}
	if o.IncludeCommentMedia != nil {
		l.IncludeCommentMedia = *o.IncludeCommentMedia
	}
	if o.VideoPolicy != nil {
		l.VideoPolicy = task.ParseVideoPolicy(*o.VideoPolicy)
	}
}

// Reaper marks jobs that stopped making progress as partial (or failed when
// their accounting state is gone).
type Reaper struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	After    time.Duration
}

func (r *Reaper) Tick(ctx context.Context) error {
	rows, err := r.DB.Query(ctx, `SELECT job_id, kind, page_ref, created_at FROM jobs
		WHERE status IN ('running','cancelling') AND created_at < now() - $1::interval LIMIT 500`, r.After.String())
	if err != nil {
		return err
	}
	type row struct {
		id      uuid.UUID
		kind    string
		ref     string
		created time.Time
	}
	list, err := pgx.CollectRows(rows, func(c pgx.CollectableRow) (row, error) {
		var x row
		return x, c.Scan(&x.id, &x.kind, &x.ref, &x.created)
	})
	if err != nil {
		return err
	}
	for _, j := range list {
		st, err := r.Jobs.Status(ctx, j.id.String())
		if err != nil {
			continue
		}
		var final, reason string
		switch {
		case st.Spawned == 0:
			final, reason = "failed", "job state lost"
		case st.Pending > 0 && time.Since(st.LastActivity) > r.After:
			final, reason = "partial", "stalled with pending tasks"
		default:
			continue
		}
		eb, _ := json.Marshal(map[string]any{"message": reason, "pending_tasks": st.Pending})
		cb, _ := json.Marshal(st.Counters)
		tag, err := r.DB.Exec(ctx, `UPDATE jobs SET status = $2, error = $3, counters = $4, finished_at = now()
			WHERE job_id = $1 AND status IN ('running','cancelling')`, j.id, final, eb, cb)
		if err != nil || tag.RowsAffected() == 0 {
			continue
		}
		b := r.Producer.NewBatch()
		_ = b.AddProto(ctx, topics.Job, j.id.String(), &fbv1.JobEvent{EventId: j.id.String() + ":" + final, JobId: j.id.String(),
			Status: jobStatus(final), Kind: j.kind, PageRef: j.ref, CreatedAt: timestamppb.New(j.created),
			FinishedAt: timestamppb.Now(), Counters: st.Counters, Errors: []string{reason}})
		_ = b.Wait(ctx)
		metrics.Jobs.WithLabelValues(final, "unknown").Inc()
		slog.Warn("reaper: job closed", "job", j.id, "status", final, "reason", reason)
	}
	return nil
}

// Maintenance keeps partitions ahead of time and applies retention.
type Maintenance struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
}

func (m *Maintenance) Tick(ctx context.Context) error {
	now := time.Now().UTC()
	monthsBack := m.Cfg.Limits.MaxAgeCapDays/30 + 1
	n, err := store.EnsurePartitions(ctx, m.DB, now, store.PartitionWindow{
		MonthsBack: monthsBack, MonthsAhead: m.Cfg.Scheduler.PartitionMonthsAhead, DaysBack: 1, DaysAhead: 14})
	if err != nil {
		return err
	}
	if n > 0 {
		slog.Info("partitions created", "count", n)
	}
	ret := m.Cfg.Privacy.Retention
	drops := []struct {
		table string
		days  int
	}{{"comments", ret.CommentsDays}, {"post_metrics", ret.MetricsDays}}
	for _, d := range drops {
		if d.days <= 0 {
			continue
		}
		dropped, err := store.DropPartitionsBefore(ctx, m.DB, d.table, now.AddDate(0, 0, -d.days))
		if err != nil {
			return err
		}
		if len(dropped) > 0 {
			slog.Info("retention: partitions dropped", "table", d.table, "partitions", dropped)
		}
	}
	stmts := []struct {
		q    string
		days int
	}{
		{`DELETE FROM jobs WHERE job_id IN (SELECT job_id FROM jobs WHERE finished_at < now() - make_interval(days => $1) LIMIT 5000)`, ret.JobsDays},
		{`DELETE FROM media WHERE media_id IN (SELECT media_id FROM media WHERE status IN ('deleted','expired','failed','skipped')
			AND discovered_at < now() - make_interval(days => $1) LIMIT 5000)`, ret.MediaRowsDays},
		{`DELETE FROM comment_authors WHERE author_key IN (SELECT author_key FROM comment_authors
			WHERE last_seen_at < now() - make_interval(days => $1) LIMIT 5000)`, ret.AuthorsInactiveDays},
		{`DELETE FROM registry_sync_runs WHERE started_at < now() - make_interval(days => $1)`, ret.JobsDays},
	}
	for _, s := range stmts {
		if s.days <= 0 {
			continue
		}
		if _, err := m.DB.Exec(ctx, s.q, s.days); err != nil {
			return err
		}
	}
	// Refresh rows of posts past every refresh window and recrawl age, and
	// of pages no longer scheduled.
	if _, err := m.DB.Exec(ctx, `DELETE FROM post_refresh WHERE post_id IN (SELECT r.post_id FROM post_refresh r
		LEFT JOIN page_schedule s ON s.page_id = r.page_id AND s.enabled
		WHERE r.post_created_at < $1 OR s.page_id IS NULL LIMIT 20000)`,
		now.Add(-maxRefreshAge(m.Cfg.Scheduler.PostRefresh)-24*time.Hour)); err != nil {
		return err
	}
	return nil
}
