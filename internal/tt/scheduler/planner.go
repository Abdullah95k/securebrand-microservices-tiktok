// Package scheduler runs tks's leader-only control loops (docs/tiktok ADR
// 0007 §12): the planner (each account's check every 2 h 45 min, and the
// deeper metrics walks), the finalizer (job events, comment coverage,
// deleted comments), the reaper and maintenance (partitions, retention).
// It is fbs's internal/scheduler on tks's envelope and schema tt; the
// budget and AIMD controllers and the lag gauges are the shared ones.
package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Planner starts the scheduled checks of registered accounts: an
// incremental check of the post list every tt.freshness.list_interval, a
// backfill for an account never read, and, when one is due, a metrics walk
// (tt.freshness.metrics_walks) in the check's place, since a walk reads the
// newest posts too.
type Planner struct {
	DB       *pgxpool.Pool
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
	// Budget, when set, returns the traffic budget state: its stretch
	// lengthens the intervals of the tiers it names.
	Budget func(context.Context) ratelimit.BudgetState
	Now    func() time.Time
}

func (p *Planner) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

type due struct {
	ref      string
	tier     string
	options  []byte
	maxAge   *int
	maxPosts *int64
	lastJob  *uuid.UUID
	walks    []byte
}

// Tick claims the accounts that are due, moves their next check on by
// their interval (doubled per consecutive failure, at most 30 days; with
// jitter) and dispatches a job for each. Suppressed accounts are skipped:
// their erasure disables the schedule, and the crawler and the persister
// drop them too.
func (p *Planner) Tick(ctx context.Context) error {
	stretch, stretchTiers := 1.0, []string{}
	if p.Budget != nil {
		if st := p.Budget(ctx); st.Stretch > 1 {
			stretch, stretchTiers = st.Stretch, st.StretchTiers
		}
	}
	rows, err := p.DB.Query(ctx, `
		WITH d AS (
			SELECT account_ref FROM account_schedule s
			WHERE enabled AND next_run_at <= $5 AND (paused_until IS NULL OR paused_until < $5)
			  AND NOT EXISTS (SELECT 1 FROM suppression x WHERE x.subject_type = 'account' AND x.subject_key = s.user_id::text)
			ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE account_schedule s SET
			next_run_at = $5::timestamptz + make_interval(secs => s.interval_seconds
				* least(power(2, s.consecutive_failures), 2592000.0 / s.interval_seconds)
				* CASE WHEN s.tier = ANY($3::text[]) THEN $4::float8 ELSE 1.0 END
				* (1 + (random() * 2 - 1) * $2)),
			last_run_at = $5
		FROM d WHERE s.account_ref = d.account_ref
		RETURNING s.account_ref, s.tier, s.options, s.max_age_days, s.max_posts, s.last_job_id, s.walks`,
		p.Cfg.Scheduler.PlannerBatch, p.Cfg.Scheduler.Jitter, stretchTiers, stretch, p.now())
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
		var d due
		return d, r.Scan(&d.ref, &d.tier, &d.options, &d.maxAge, &d.maxPosts, &d.lastJob, &d.walks)
	})
	if err != nil {
		return err
	}
	for _, d := range list {
		if d.lastJob != nil {
			var st string
			if err := p.DB.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, *d.lastJob).Scan(&st); err == nil &&
				(st == "running" || st == "finalizing" || st == "cancelling") {
				continue // the previous check is still going; skip this slot
			}
		}
		req, err := p.request(ctx, d)
		if err != nil {
			slog.Warn("planner: account", "account", d.ref, "err", err)
			continue
		}
		j, _, err := p.Dispatch.Create(ctx, req, 0)
		if err != nil {
			slog.Warn("planner: dispatch failed", "account", d.ref, "err", err)
			continue
		}
		if _, err := p.DB.Exec(ctx, `UPDATE account_schedule SET last_job_id = $2 WHERE account_ref = $1`, d.ref, j.ID); err != nil {
			return err
		}
	}
	var lag float64
	_ = p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM $1::timestamptz - min(next_run_at)), 0) FROM account_schedule
		WHERE enabled AND next_run_at <= $1`, p.now()).Scan(&lag)
	metrics.ScheduleLag.WithLabelValues("all").Set(math.Max(lag, 0))
	return nil
}

// request builds a due account's job: a backfill when the account was never
// read, the deepest metrics walk that is due, or an incremental check.
func (p *Planner) request(ctx context.Context, d due) (dispatch.Request, error) {
	acc, _, err := dispatch.Resolve(ctx, p.DB, d.ref)
	if err != nil {
		return dispatch.Request{}, err
	}
	read := acc != nil && acc.ListCheckedAt != nil
	lim := task.DefaultLimits(p.Cfg.Limits, p.Cfg.TT, !read)
	applyOptions(lim, d.options)
	if d.maxAge != nil && *d.maxAge > 0 {
		lim.MaxAgeSeconds = int64(*d.maxAge) * 86400
	}
	if d.maxPosts != nil {
		lim.MaxPosts = *d.maxPosts
	}
	r := dispatch.Request{Kind: dispatch.KindScheduled, Lane: tkstaskv1.Lane_LANE_P1, Mode: dispatch.ModeIncremental,
		AccountRef: d.ref, Limits: lim}
	switch {
	case !read:
		// The account's first read: its year back (or to the first post), in
		// the backfill lane, so it cannot hold up the 2 h 45 min checks.
		r.Kind, r.Lane, r.Mode = dispatch.KindBackfill, tkstaskv1.Lane_LANE_P2, dispatch.ModeFull
	default:
		if w, ok := DueWalk(p.Cfg.TT.Freshness.MetricsWalks, Walks(d.walks), p.now()); ok {
			// A walk reads the list from the top, so it stands in for the
			// check. It goes in the backfill lane too: it is long, and the
			// delay target is met by the checks.
			r.Lane, r.Mode = tkstaskv1.Lane_LANE_P2, dispatch.ModeRefresh
			depth := int64(w.Back / time.Second)
			if lim.MaxAgeSeconds > 0 {
				depth = min(depth, lim.MaxAgeSeconds) // an account's own window stays its limit
			}
			r.Limits.MaxAgeSeconds = depth
			r.Params = map[string]any{"walk": WalkKey(w)}
		}
	}
	return r, nil
}

// WalkKey is a metrics walk's key in account_schedule.walks: its depth in
// seconds.
func WalkKey(w config.MetricsWalk) string { return strconv.FormatInt(int64(w.Back/time.Second), 10) }

// Walks reads account_schedule.walks: when each walk last ended well.
func Walks(raw []byte) map[string]time.Time {
	out := map[string]time.Time{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}

// DueWalk returns the deepest metrics walk whose interval has passed since
// it last ended well; a deeper walk covers the shallower ones. A walk never
// recorded is due: the backfill records every walk, so only an account read
// before the walks were configured has none.
func DueWalk(walks []config.MetricsWalk, last map[string]time.Time, now time.Time) (config.MetricsWalk, bool) {
	var best config.MetricsWalk
	found := false
	for _, w := range walks {
		at, ok := last[WalkKey(w)]
		if ok && now.Sub(at) < w.Every {
			continue
		}
		if !found || w.Back > best.Back {
			best, found = w, true
		}
	}
	return best, found
}

// WalksDone returns the walks a job that ended well covers, all stamped at
// its start: a backfill covers every walk, a walk those no deeper than
// itself, a check none.
func WalksDone(walks []config.MetricsWalk, mode, walk string, at time.Time) map[string]time.Time {
	out := map[string]time.Time{}
	var depth time.Duration
	switch mode {
	case dispatch.ModeFull:
		depth = math.MaxInt64
	case dispatch.ModeRefresh:
		s, err := strconv.ParseInt(walk, 10, 64)
		if err != nil {
			return out
		}
		depth = time.Duration(s) * time.Second
	default:
		return out
	}
	for _, w := range walks {
		if w.Back <= depth {
			out[WalkKey(w)] = at.UTC()
		}
	}
	return out
}

// TierGauges publishes the scheduled accounts by status, the schedule lag
// and the oldest successful check among active accounts (build brief §8
// A1: no account more than 6 h without one).
func (p *Planner) TierGauges(ctx context.Context) error {
	rows, err := p.DB.Query(ctx, `SELECT coalesce(a.status, 'unknown'), count(*) FROM account_schedule s
		LEFT JOIN accounts a ON a.user_id = s.user_id WHERE s.enabled GROUP BY 1`)
	if err != nil {
		return err
	}
	seen := map[string]float64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return err
		}
		seen[st] = float64(n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, st := range []string{"active", "private", "unavailable", "banned", "suppressed", "unknown"} {
		metrics.ScheduledAccounts.WithLabelValues(st).Set(seen[st])
	}
	var oldest float64
	if err := p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM $1::timestamptz - min(coalesce(s.last_success_at, s.created_at))), 0)
		FROM account_schedule s LEFT JOIN accounts a ON a.user_id = s.user_id
		WHERE s.enabled AND coalesce(a.status, 'active') = 'active'`, p.now()).Scan(&oldest); err != nil {
		return err
	}
	metrics.CheckOldest.Set(math.Max(oldest, 0))
	return nil
}

// applyOptions overlays an account's stored options (the API's crawl
// options, JSON) on the job's limits.
func applyOptions(l *tkstaskv1.Limits, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var o struct {
		MaxAgeDays          *int    `json:"max_age_days"`
		MaxPosts            *int64  `json:"max_posts"`
		MaxCommentsPerPost  *int64  `json:"max_comments_per_post"`
		IncludeComments     *bool   `json:"include_comments"`
		IncludeReplies      *bool   `json:"include_replies"`
		IncludeMedia        *bool   `json:"include_media"`
		IncludeCommentMedia *bool   `json:"include_comment_media"`
		VideoPolicy         *string `json:"video_policy"`
	}
	if json.Unmarshal(raw, &o) != nil {
		return
	}
	set := func(dst *int64, v *int64) {
		if v != nil {
			*dst = *v
		}
	}
	if o.MaxAgeDays != nil {
		l.MaxAgeSeconds = int64(*o.MaxAgeDays) * 86400
	}
	set(&l.MaxPosts, o.MaxPosts)
	set(&l.MaxCommentsPerPost, o.MaxCommentsPerPost)
	for dst, v := range map[*bool]*bool{&l.IncludeComments: o.IncludeComments, &l.IncludeReplies: o.IncludeReplies,
		&l.IncludeMedia: o.IncludeMedia, &l.IncludeCommentMedia: o.IncludeCommentMedia} {
		if v != nil {
			*dst = *v
		}
	}
	if o.VideoPolicy != nil {
		l.VideoPolicy = task.ParseVideoPolicy(*o.VideoPolicy)
	}
}
