package scheduler

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
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
	type gauge struct{ pages, lag float64 }
	seen := map[string]gauge{}
	for rows.Next() {
		var tier string
		var n int64
		var lag float64
		if err := rows.Scan(&tier, &n, &lag); err != nil {
			return err
		}
		seen[tier] = gauge{float64(n), math.Max(lag, 0)}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A configured tier left with no pages reads 0, not its last count and
	// lag.
	names := []string{DormantTier}
	for _, t := range p.Cfg.Scheduler.Tiers {
		names = append(names, t.Name)
	}
	for _, n := range names {
		if _, ok := seen[n]; !ok {
			seen[n] = gauge{}
		}
	}
	for tier, g := range seen {
		metrics.PageTier.WithLabelValues(tier).Set(g.pages)
		metrics.ScheduleLag.WithLabelValues(tier).Set(g.lag)
	}
	return nil
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
	// After is how long a job may go without progress. While tasks wait in
	// its lane's topics (Backlog), its own are taken to be queued behind
	// them, not lost, and it is left alone until MaxAge.
	After  time.Duration
	MaxAge time.Duration
	// Backlog returns the tasks waiting per lane (TaskBacklog); nil takes
	// every lane to be empty.
	Backlog func(context.Context) (map[int16]int64, error)
	// PageSize bounds the jobs one tick looks at (default 500).
	PageSize int

	// The tick after a full page goes on from its last job (by creation),
	// so more than a page of long-running jobs can't keep a stuck one from
	// ever being looked at.
	afterCreated time.Time
	afterID      uuid.UUID
}

// stalled decides what becomes of a job without progress: failed when its
// accounting state is gone, partial when its tasks look lost (no progress
// for after while nothing waits in its lane, or older than maxAge).
func stalled(st jobs.Status, queued, overdue bool, after time.Duration, now time.Time) (final, reason string) {
	switch {
	case st.Spawned == 0:
		return "failed", "job state lost"
	case st.Pending > 0 && now.Sub(st.LastActivity) > after && (!queued || overdue):
		return "partial", "stalled with pending tasks"
	}
	return "", ""
}

func (r *Reaper) Tick(ctx context.Context) error {
	maxAge := max(r.MaxAge, r.After)
	page := cmp.Or(r.PageSize, 500)
	rows, err := r.DB.Query(ctx, `SELECT job_id, kind, page_ref, created_at, lane, created_at < now() - $2::interval FROM jobs
		WHERE status IN ('running','cancelling') AND created_at < now() - $1::interval AND (created_at, job_id) > ($3, $4)
		ORDER BY created_at, job_id LIMIT $5`, r.After.String(), maxAge.String(), r.afterCreated, r.afterID, page)
	if err != nil {
		return err
	}
	type row struct {
		id      uuid.UUID
		kind    string
		ref     string
		created time.Time
		lane    int16
		overdue bool
	}
	list, err := pgx.CollectRows(rows, func(c pgx.CollectableRow) (row, error) {
		var x row
		return x, c.Scan(&x.id, &x.kind, &x.ref, &x.created, &x.lane, &x.overdue)
	})
	if err != nil {
		return err
	}
	if len(list) < page {
		r.afterCreated, r.afterID = time.Time{}, uuid.UUID{} // all seen: start over next tick
	} else {
		r.afterCreated, r.afterID = list[len(list)-1].created, list[len(list)-1].id
	}
	if len(list) == 0 {
		return nil
	}
	queued := func(int16) bool { return false }
	if r.Backlog != nil {
		backlog, err := r.Backlog(ctx)
		switch {
		case err != nil:
			// Unknown: take every lane to be busy, so only jobs past MaxAge
			// (or without state) are closed until the backlog reads again.
			slog.Warn("reaper: task backlog", "err", err)
			queued = func(int16) bool { return true }
		default:
			queued = func(l int16) bool { return backlog[l] > 0 }
		}
	}
	now := time.Now()
	for _, j := range list {
		st, err := r.Jobs.Status(ctx, j.id.String())
		if err != nil {
			continue
		}
		final, reason := stalled(st, queued(j.lane), j.overdue, r.After, now)
		if final == "" {
			continue
		}
		eb, _ := json.Marshal(map[string]any{"message": reason, "pending_tasks": st.Pending})
		cb, _ := json.Marshal(st.Counters)
		var prev string
		if err := r.DB.QueryRow(ctx, `UPDATE jobs SET status = $2, error = $3, counters = $4, finished_at = now()
			WHERE job_id = $1 AND status IN ('running','cancelling') RETURNING old.status`, j.id, final, eb, cb).Scan(&prev); err != nil {
			continue // finalized meanwhile, or the database failed: next tick
		}
		b := r.Producer.NewBatch()
		err = b.AddProto(ctx, topics.Job, j.id.String(), &fbv1.JobEvent{EventId: j.id.String() + ":" + final, JobId: j.id.String(),
			Status: jobStatus(final), Kind: j.kind, PageRef: j.ref, CreatedAt: timestamppb.New(j.created),
			FinishedAt: timestamppb.Now(), Counters: st.Counters, Errors: []string{reason}})
		if err == nil {
			err = b.Wait(ctx)
		}
		if err != nil {
			// Not announced: reopen the job, so the next tick closes it
			// again and sends the event.
			if _, rerr := r.DB.Exec(ctx, `UPDATE jobs SET status = $2, error = NULL, finished_at = NULL WHERE job_id = $1 AND status = $3`,
				j.id, prev, final); rerr != nil {
				slog.Error("reaper: job closed but its event was not sent", "job", j.id, "err", errors.Join(err, rerr))
			}
			continue
		}
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
	// Every step runs whatever the others did: a partition that can't be
	// created now (its table locked by a long reader) must not hold up
	// retention, which is a privacy obligation.
	var errs []error
	now := time.Now().UTC()
	monthsBack := m.Cfg.Limits.MaxAgeCapDays/30 + 1
	ret := m.Cfg.Privacy.Retention
	// Partitions retention would drop again are not created.
	notBefore := map[string]time.Time{}
	if ret.CommentsDays > 0 {
		notBefore["comments"] = now.AddDate(0, 0, -ret.CommentsDays)
	}
	if ret.MetricsDays > 0 {
		notBefore["post_metrics"] = now.AddDate(0, 0, -ret.MetricsDays)
	}
	n, err := store.EnsurePartitions(ctx, m.DB, now, store.PartitionWindow{
		MonthsBack: monthsBack, MonthsAhead: m.Cfg.Scheduler.PartitionMonthsAhead, DaysBack: 1, DaysAhead: 14, NotBefore: notBefore})
	if err != nil {
		errs = append(errs, err)
	}
	if n > 0 {
		slog.Info("partitions created", "count", n)
	}
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
			errs = append(errs, err)
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
		// Rows never handed over (variants the policy leaves out, media of
		// jobs without media, failed downloads) are purged: their signed
		// URLs expired long ago, and they keep comment ids after retention.
		{`DELETE FROM media WHERE media_id IN (SELECT media_id FROM media WHERE status IN ('discovered','failed','skipped','source_expired')
			AND discovered_at < now() - make_interval(days => $1) LIMIT 5000)`, ret.MediaRowsDays},
		// Rows of media handed over stay, as the record that it was (a later
		// scrape would download and announce it again under the same event
		// id), but without what ties them to a person or its content. An
		// empty fb_media_id marks a row done (media_unscrubbed_idx); the
		// persister leaves these rows alone.
		{`UPDATE media SET fb_media_id = '', source_url = NULL, alt_text = NULL, comment_id = NULL, owner_id = NULL,
			author_key = NULL, sha256 = NULL, s3_key = NULL, last_error = NULL
			WHERE media_id IN (SELECT media_id FROM media WHERE status IN ('deleted','expired') AND fb_media_id <> ''
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
			errs = append(errs, err)
		}
	}
	// Refresh rows of posts past every refresh window and recrawl age, and
	// of pages no longer scheduled.
	if _, err := m.DB.Exec(ctx, `DELETE FROM post_refresh WHERE post_id IN (SELECT r.post_id FROM post_refresh r
		LEFT JOIN page_schedule s ON s.page_id = r.page_id AND s.enabled
		WHERE r.post_created_at < $1 OR s.page_id IS NULL LIMIT 20000)`,
		now.Add(-maxRefreshAge(m.Cfg.Scheduler.PostRefresh)-24*time.Hour)); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
