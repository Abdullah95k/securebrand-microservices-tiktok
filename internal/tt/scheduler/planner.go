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
	"errors"
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
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// SlotRetry is how soon a check whose slot found the account's previous
// job still going is tried again.
const SlotRetry = 15 * time.Minute

type slot int

const (
	slotGo         slot = iota // no job in the way: dispatch
	slotWait                   // the previous job is still going: retry in SlotRetry
	slotCancelWalk             // a walk or backfill the budget holds in the paused p2 lane: cancel it, then check
)

// slotFor decides a due slot from the account's previous job. A walk held
// in the paused backfill lane would leave the account unchecked until the
// budget resumes p2 (hours, the 3-hour target missed), so it gives way to
// the check, as a due walk does (request); so does a held backfill when
// light first reads are on (firstRead: the first read runs in its place,
// the year back comes with the walk). A running check, a backfill without
// light first reads, or a walk or backfill p2 is running wait.
func slotFor(status, mode string, lane int16, p2Paused, firstRead bool) slot {
	switch status {
	case "running", "finalizing":
		held := p2Paused && status == "running" && lane == int16(tkstaskv1.Lane_LANE_P2)
		if held && (mode == dispatch.ModeRefresh || (mode == dispatch.ModeFull && firstRead)) {
			return slotCancelWalk
		}
		return slotWait
	case "cancelling":
		return slotWait
	}
	return slotGo
}

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

// Tick admits the new accounts paced admission lets start (tt.admission:
// the owner's first, then discovered ones by score), then claims the
// admitted accounts that are due, moves their next check on by their
// interval (doubled per consecutive failure, at most 30 days; with jitter)
// and dispatches a job for each. A new account waiting for admission is
// never claimed; an admitted one's checks never wait.
func (p *Planner) Tick(ctx context.Context) error {
	stretch, stretchTiers := 1.0, []string{}
	var budget *ratelimit.BudgetState
	if p.Budget != nil {
		st := p.Budget(ctx)
		budget = &st
		if st.Stretch > 1 {
			stretch, stretchTiers = st.Stretch, st.StretchTiers
		}
	}
	admitted, err := accounts.Admit(ctx, p.DB, p.Cfg, budget, p.now(), p.Cfg.Scheduler.PlannerBatch)
	if err != nil {
		return err
	}
	if admitted > 0 {
		metrics.AdmittedAccounts.Add(float64(admitted))
	}
	rows, err := p.DB.Query(ctx, `
		WITH d AS (
			SELECT account_ref FROM account_schedule s
			WHERE enabled AND admitted_at IS NOT NULL AND next_run_at <= $5 AND (paused_until IS NULL OR paused_until < $5)
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
	paused := budget != nil && budget.PauseP2
	for _, d := range list {
		// A backfill or walk still reading the account's history in p2
		// (hours or days behind a busy backfill lane) must not hold up its
		// list checks: once the list has been read, a check runs beside it.
		history, err := p.openHistory(ctx, d.ref)
		if err != nil {
			return err
		}
		besideComments := false
		if d.lastJob != nil {
			var st, mode string
			var lane int16
			if err := p.DB.QueryRow(ctx, `SELECT status, mode, lane FROM jobs WHERE job_id = $1`, *d.lastJob).Scan(&st, &mode, &lane); err == nil {
				s := slotFor(st, mode, lane, paused, p.Cfg.TT.Freshness.FirstReadDays > 0)
				if s == slotWait && history && lane == int16(tkstaskv1.Lane_LANE_P2) && st != "cancelling" {
					s = slotGo // the check beside the history, if the list was read (request)
				}
				if s == slotWait && st == "running" && lane == int16(tkstaskv1.Lane_LANE_P1) && mode == dispatch.ModeIncremental {
					// A check whose list walk has ended waits only for its
					// comment and reply reads, which can take hours: the next
					// check runs beside it (at most two at once).
					ok, err := p.listDoneBeside(ctx, *d.lastJob, d.ref)
					if err != nil {
						return err
					}
					if ok {
						s, besideComments = slotGo, true
					}
				}
				switch s {
				case slotWait:
					// The previous job is still going: try again soon,
					// not a whole interval later.
					metrics.PlannerSlots.WithLabelValues("waited").Inc()
					if _, err := p.DB.Exec(ctx, `UPDATE account_schedule SET next_run_at = least(next_run_at, $2::timestamptz + $3::interval)
						WHERE account_ref = $1`, d.ref, p.now(), SlotRetry); err != nil {
						return err
					}
					continue
				case slotCancelWalk:
					// A walk (or backfill) the budget holds in the paused
					// p2 lane: it would keep the account unchecked until
					// p2 resumes. Cancelled, it stays due (a cancelled job
					// records no walk, and an account never read gets its
					// light first read now).
					outcome := "held_walk_cancelled"
					if mode == dispatch.ModeFull {
						outcome = "held_backfill_cancelled"
					}
					metrics.PlannerSlots.WithLabelValues(outcome).Inc()
					if err := p.Dispatch.Cancel(ctx, *d.lastJob); err != nil && !errors.Is(err, pgx.ErrNoRows) {
						slog.Warn("planner: cancel held walk", "account", d.ref, "err", err)
						continue
					}
					// The cancelled job no longer counts as running history.
					if history, err = p.openHistory(ctx, d.ref); err != nil {
						return err
					}
				}
			}
		}
		req, err := p.request(ctx, d, paused, history || besideComments)
		if errors.Is(err, errNotRead) {
			// History is being read for an account whose list was never
			// read: its first read is that job, so wait for it.
			metrics.PlannerSlots.WithLabelValues("waited").Inc()
			if _, err := p.DB.Exec(ctx, `UPDATE account_schedule SET next_run_at = least(next_run_at, $2::timestamptz + $3::interval)
				WHERE account_ref = $1`, d.ref, p.now(), SlotRetry); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			slog.Warn("planner: account", "account", d.ref, "err", err)
			metrics.PlannerSlots.WithLabelValues("request_failed").Inc()
			continue
		}
		j, _, err := p.Dispatch.Create(ctx, req, 0)
		if err != nil {
			slog.Warn("planner: dispatch failed", "account", d.ref, "err", err)
			metrics.PlannerSlots.WithLabelValues("dispatch_failed").Inc()
			continue
		}
		switch {
		case besideComments:
			metrics.PlannerSlots.WithLabelValues("check_beside_comments").Inc()
		case history:
			metrics.PlannerSlots.WithLabelValues("check_beside_history").Inc()
		}
		if _, err := p.DB.Exec(ctx, `UPDATE account_schedule SET last_job_id = $2 WHERE account_ref = $1`, d.ref, j.ID); err != nil {
			return err
		}
	}
	var lag float64
	_ = p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM $1::timestamptz - min(next_run_at)), 0) FROM account_schedule
		WHERE enabled AND admitted_at IS NOT NULL AND next_run_at <= $1`, p.now()).Scan(&lag)
	metrics.ScheduleLag.WithLabelValues("all").Set(math.Max(lag, 0))
	return nil
}

// errNotRead is request's answer for a check beside running history when
// the account's list was never read: the history job is its first read.
var errNotRead = errors.New("planner: the account's list was never read")

// openHistory reports whether a backfill or metrics walk is still running
// (or finalizing) for the account in the backfill lane.
func (p *Planner) openHistory(ctx context.Context, ref string) (bool, error) {
	var open bool
	err := p.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE account_ref = $1 AND lane = $2
		AND status IN ('running', 'finalizing'))`, ref, int16(tkstaskv1.Lane_LANE_P2)).Scan(&open)
	return open, err
}

// maxOpenChecks is how many scheduled checks of one account may run at
// once: a check whose comment work is still going, and the next.
const maxOpenChecks = 2

// listDoneBeside reports whether a new check may start beside the running
// check job: its list walk has ended (the account's list cursor was saved
// after the job began) and fewer than maxOpenChecks checks are open.
func (p *Planner) listDoneBeside(ctx context.Context, job uuid.UUID, ref string) (bool, error) {
	var ok bool
	err := p.DB.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM jobs j JOIN account_schedule s ON s.account_ref = j.account_ref
			JOIN crawl_cursors c ON c.scope_type = 'account_list' AND c.scope_id = s.user_id
			WHERE j.job_id = $1 AND c.updated_at >= j.created_at)
		AND (SELECT count(*) FROM jobs WHERE account_ref = $2 AND lane = $3 AND status IN ('running', 'finalizing')) < $4`,
		job, ref, int16(tkstaskv1.Lane_LANE_P1), maxOpenChecks).Scan(&ok)
	return ok, err
}

// request builds a due account's job: a backfill when the account was never
// read, the deepest metrics walk that is due, or an incremental check.
// While the budget pauses the backfill lane (p2), a due walk waits and the
// check runs in its place: a walk stands in for the check, so a paused walk
// would leave the account unchecked (the 3-hour target holds; owner,
// 2026-11-05). beside (a backfill or walk still running in p2) asks for the
// incremental check alone, never a second walk or backfill; errNotRead when
// the list was never read.
func (p *Planner) request(ctx context.Context, d due, backfillPaused, beside bool) (dispatch.Request, error) {
	acc, _, err := dispatch.Resolve(ctx, p.DB, d.ref)
	if err != nil {
		return dispatch.Request{}, err
	}
	read := acc != nil && acc.ListCheckedAt != nil
	if beside && !read {
		return dispatch.Request{}, errNotRead
	}
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
	case !read && backfillPaused && p.Cfg.TT.Freshness.FirstReadDays > 0:
		// The budget holds the backfill lane: a light first read of the
		// last first_read_days days in the check lane, so the account's
		// checks start now. It records no metrics walk, so the deepest
		// walk (a year back) is due at once and runs in p2 when the budget
		// allows: the backfill, later.
		depth := int64(p.Cfg.TT.Freshness.FirstReadDays) * 86400
		if lim.MaxAgeSeconds > 0 {
			depth = min(depth, lim.MaxAgeSeconds)
		}
		r.Limits.MaxAgeSeconds = depth
		r.Params = map[string]any{"first_read": true}
	case !read:
		// The account's first read: its year back (or to the first post), in
		// the backfill lane, so it cannot hold up the 2 h 45 min checks.
		r.Kind, r.Lane, r.Mode = dispatch.KindBackfill, tkstaskv1.Lane_LANE_P2, dispatch.ModeFull
	case beside:
		// The history job reads the older posts; this check, the new ones.
	default:
		if w, ok := DueWalk(p.Cfg.TT.Freshness.MetricsWalks, Walks(d.walks), p.now()); ok && !backfillPaused {
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

// TierGauges publishes the scheduled accounts by status, the schedule lag,
// the oldest successful check among active accounts (build brief §8 A1: no
// account more than 6 h without one) and the active accounts whose daily
// refresh is overdue.
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
	for _, st := range []string{"active", "private", "unavailable", "banned", "unknown"} {
		metrics.ScheduledAccounts.WithLabelValues(st).Set(seen[st])
	}
	var oldest float64
	if err := p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM $1::timestamptz - min(coalesce(s.last_success_at, s.created_at))), 0)
		FROM account_schedule s LEFT JOIN accounts a ON a.user_id = s.user_id
		WHERE s.enabled AND coalesce(a.status, 'active') = 'active'`, p.now()).Scan(&oldest); err != nil {
		return err
	}
	metrics.CheckOldest.Set(math.Max(oldest, 0))
	// The daily account refresh (build brief §8 A1: 99.5 % within 24 h): an
	// active account's full read is due tt.freshness.account_refresh after
	// the last, at its next check; past a further day it is overdue.
	var overdue int64
	if err := p.DB.QueryRow(ctx, `SELECT count(*) FROM account_schedule s JOIN accounts a ON a.user_id = s.user_id
		WHERE s.enabled AND a.status = 'active'
		  AND coalesce(a.profile_refreshed_at, a.first_seen_at) < $1::timestamptz - make_interval(secs => $2)`,
		p.now(), (p.Cfg.TT.Freshness.AccountRefresh + 24*time.Hour).Seconds()).Scan(&overdue); err != nil {
		return err
	}
	metrics.RefreshOverdue.Set(float64(overdue))
	var queued int64
	if err := p.DB.QueryRow(ctx, `SELECT count(*) FROM account_schedule WHERE enabled AND admitted_at IS NULL`).Scan(&queued); err != nil {
		return err
	}
	metrics.AdmissionQueue.Set(float64(queued))
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
