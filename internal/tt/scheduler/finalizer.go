package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Finalizer turns jobs whose tasks are all done into terminal job rows and
// tt.job.v1 events, once the persister has written everything produced so
// far (so "completed" means "queryable"). On the way it records each read
// post's comment coverage, marks the comments a whole read no longer
// returned as deleted, and moves the account's schedule on.
type Finalizer struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	Cfg      *config.Config
	// Timeout: how long a job waits for the persister to catch up before it
	// is finalised anyway.
	Timeout time.Duration
	// PersisterGroup defaults to tks's persister group (persist.Group).
	PersisterGroup string
	// Entry decides the found account an entry check read, once its records
	// are stored (docs/tiktok/discovery-plan.md §4); nil leaves entry jobs
	// undecided (the discovery leader's Recover queues them again).
	Entry func(ctx context.Context, job uuid.UUID, final string, counters map[string]int64) error
	// Content, when set, publishes the shared contract's coverage (M15b):
	// each read post's Post.coverage, through its gate, and the job's
	// JobCoverage on collection.coverage.v1 beside tt.job.v1.
	Content *content.Emitter
	Now     func() time.Time

	pending []finalBatch
	held    map[string]bool // jobs in pending
}

type finalBatch struct {
	jobs  []string
	ends  kadm.ListedOffsets
	since time.Time
}

func (f *Finalizer) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// Tick takes ready jobs and finalises those the persister has caught up
// on. A job leaves the finalize set only once it is finalised, so jobs held
// here while the persister catches up are not lost when the leader changes.
func (f *Finalizer) Tick(ctx context.Context) error {
	phase := time.Now()
	lap := func(name string) {
		now := time.Now()
		metrics.FinalizerTick.WithLabelValues(name).Observe(now.Sub(phase).Seconds())
		phase = now
	}
	ids, err := f.Jobs.ReadyToFinalize(ctx, 500)
	if err != nil {
		return err
	}
	if f.held == nil {
		f.held = map[string]bool{}
	}
	var fresh, waited []string
	for _, id := range ids {
		if f.held[id] {
			continue
		}
		// A job that has been ready longer than Timeout (its tasks' last
		// activity) has given the persister its time already: a finalise
		// that failed (a leader change cancels the whole batch in hand) or
		// a new leader would otherwise make it wait Timeout again, every
		// time (2026-11-14: 7,435 jobs in the set, some ready for 54 h).
		if f.waited(ctx, id) {
			waited = append(waited, id)
			continue
		}
		fresh = append(fresh, id)
	}
	if len(fresh) > 0 {
		ts := make([]string, len(topics.Entity))
		for i, t := range topics.Entity {
			ts[i] = f.Producer.Topic(t)
		}
		ends, err := kafka.EndOffsets(ctx, f.Producer.Client(), ts...)
		if err != nil {
			return err
		}
		for _, id := range fresh {
			f.held[id] = true
		}
		f.pending = append(f.pending, finalBatch{jobs: fresh, ends: ends, since: time.Now()})
	}
	group := f.PersisterGroup
	if group == "" {
		group = f.Producer.Topic(ttpersist.Group)
	}
	lap("sample")
	keep := f.pending[:0]
	ready := waited
	// One fetch of the persister's offsets answers for every batch held.
	var committed kadm.OffsetResponses
	if len(f.pending) > 0 {
		if committed, err = kafka.CommittedOffsets(ctx, f.Producer.Client(), group); err != nil {
			slog.Warn("finalizer: offset check failed", "err", err)
		}
	}
	for _, b := range f.pending {
		caught := err == nil && kafka.CaughtUp(committed, b.ends)
		timedOut := time.Since(b.since) > f.Timeout
		if !caught && !timedOut {
			keep = append(keep, b)
			continue
		}
		if timedOut && !caught {
			slog.Warn("finalizer: persister lag; finalising anyway", "jobs", len(b.jobs))
		}
		for _, id := range b.jobs {
			delete(f.held, id)
		}
		ready = append(ready, b.jobs...)
	}
	f.pending = keep
	lap("batches")
	ready = f.byLane(ctx, ready)
	// The rest stay in the set; having waited, they are taken at once on a
	// later tick.
	if n := f.batch(); len(ready) > n {
		ready = ready[:n]
	}
	f.finalizeAll(ctx, ready)
	lap("finalize")
	return nil
}

// stepClock times a job's finalisation by step.
type stepClock struct {
	start, last time.Time
	steps       []string
	took        map[string]time.Duration
}

func newStepClock() *stepClock {
	now := time.Now()
	return &stepClock{start: now, last: now, took: map[string]time.Duration{}}
}

// mark ends step.
func (c *stepClock) mark(step string) {
	now := time.Now()
	if _, ok := c.took[step]; !ok {
		c.steps = append(c.steps, step)
	}
	c.took[step] += now.Sub(c.last)
	c.last = now
}

// observe records the steps and the total in FinalizeStep and returns them
// as log attributes (milliseconds).
func (c *stepClock) observe(lane string) []any {
	out := make([]any, 0, 2*len(c.steps)+2)
	for _, s := range c.steps {
		metrics.FinalizeStep.WithLabelValues(s, lane).Observe(c.took[s].Seconds())
		out = append(out, s+"_ms", c.took[s].Milliseconds())
	}
	total := time.Since(c.start)
	metrics.FinalizeStep.WithLabelValues("total", lane).Observe(total.Seconds())
	return append(out, "total_ms", total.Milliseconds())
}

// waited reports whether job has been ready (its tasks' last activity)
// longer than Timeout.
func (f *Finalizer) waited(ctx context.Context, job string) bool {
	st, err := f.Jobs.Status(ctx, job)
	return err == nil && !st.LastActivity.IsZero() && f.now().Sub(st.LastActivity) > f.Timeout
}

// workers is scheduler.finalize_workers (at least 1). Eight at once over a
// backlog of 7,435 jobs overloaded the shared PostgreSQL (2026-11-14).
func (f *Finalizer) workers() int {
	if f.Cfg != nil && f.Cfg.Scheduler.FinalizeWorkers > 0 {
		return f.Cfg.Scheduler.FinalizeWorkers
	}
	return 1
}

// batch is scheduler.finalize_batch, the most jobs finalised a tick (50
// when unset), checks first.
func (f *Finalizer) batch() int {
	if f.Cfg != nil && f.Cfg.Scheduler.FinalizeBatch > 0 {
		return f.Cfg.Scheduler.FinalizeBatch
	}
	return 50
}

// recountAfter is scheduler.coverage_recount_after (6 h when unset).
func (f *Finalizer) recountAfter() time.Duration {
	if f.Cfg != nil && f.Cfg.Scheduler.CoverageRecountAfter > 0 {
		return f.Cfg.Scheduler.CoverageRecountAfter
	}
	return 6 * time.Hour
}

// coverageChunk bounds the posts one coverage statement counts comments
// for: a backfill reads hundreds or thousands of posts.
const coverageChunk = 200

// byLane orders jobs on-demand first, then checks, then the backfill lane,
// so a check is never finalised behind a batch of backfills. Jobs it
// cannot place keep their order, last.
func (f *Finalizer) byLane(ctx context.Context, ids []string) []string {
	if len(ids) < 2 {
		return ids
	}
	lane := map[string]int16{}
	rows, err := f.DB.Query(ctx, `SELECT job_id::text, lane FROM jobs WHERE job_id = ANY($1::uuid[])`, ids)
	if err != nil {
		slog.Warn("finalizer: job lanes", "err", err)
		return ids
	}
	for rows.Next() {
		var id string
		var l int16
		if rows.Scan(&id, &l) == nil {
			lane[id] = l
		}
	}
	rows.Close()
	out := append([]string(nil), ids...)
	sort.SliceStable(out, func(i, j int) bool {
		li, ok := lane[out[i]]
		if !ok {
			li = math.MaxInt16
		}
		lj, ok := lane[out[j]]
		if !ok {
			lj = math.MaxInt16
		}
		return li < lj
	})
	return out
}

// finalizeAll finalises ids, workers() at a time, in their order.
// A job that fails stays in the set and is taken again on a later tick.
func (f *Finalizer) finalizeAll(ctx context.Context, ids []string) {
	work := make(chan string)
	var wg sync.WaitGroup
	for range min(f.workers(), len(ids)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range work {
				if err := f.finalize(ctx, id); err != nil {
					if ctx.Err() == nil {
						slog.Error("finalizer: job failed to finalise", "job", id, "err", err)
					}
					continue
				}
				if err := f.Jobs.Finalized(ctx, id); err != nil {
					slog.Warn("finalizer: could not clear a finalised job", "job", id, "err", err)
				}
			}
		}()
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			break // the rest stay in the set
		}
		work <- id
	}
	close(work)
	wg.Wait()
}

// Completeness summary stored on the job.
type Completeness struct {
	Posts             int64   `json:"posts"`
	PostsComplete     int64   `json:"posts_complete"`
	PostsCapped       int64   `json:"posts_capped"`
	PostsTruncated    int64   `json:"posts_truncated"`
	PostsFailed       int64   `json:"posts_failed"`
	MedianRatio       float64 `json:"median_ratio"`
	MinRatio          float64 `json:"min_ratio"`
	CommentsReported  int64   `json:"comments_reported"`
	CommentsCollected int64   `json:"comments_collected"`
}

// Status decides a job's terminal status from its counters and comment
// coverage. A private or banned account is an answer, not a failure (its
// status is stored); an account TikTok does not serve at all, with no post
// read, fails the job. A comment read stopped at the cap per post is the
// configured outcome (build brief §8 A4), not a partial job.
func Status(cancelling bool, counters map[string]int64, comp Completeness) string {
	switch {
	case cancelling:
		return "cancelled"
	case comp.PostsTruncated > 0 || comp.PostsFailed > 0 || counters["tasks_failed"] > 0 || counters["lists_truncated"] > 0:
		return "partial"
	case counters["accounts_unavailable"] > 0 && counters["posts"] == 0:
		return "failed"
	}
	return "completed"
}

func (f *Finalizer) finalize(ctx context.Context, id string) error {
	clock := newStepClock()
	jid, err := uuid.Parse(id)
	if err != nil {
		return nil // not ours
	}
	var status, kind, mode, ref string
	var params []byte
	var lane int16
	var userID *int64
	var created time.Time
	err = f.DB.QueryRow(ctx, `SELECT status, kind, mode, account_ref, user_id, params, lane, created_at FROM jobs WHERE job_id = $1`, jid).
		Scan(&status, &kind, &mode, &ref, &userID, &params, &lane, &created)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}
	if status != "running" && status != "cancelling" && status != "finalizing" {
		return nil // already terminal
	}
	st, err := f.Jobs.Status(ctx, id)
	if err != nil {
		return err
	}
	if st.Pending > 0 && status != "cancelling" {
		// Reopened since it was queued (a dead letter replayed): its last
		// task's Done queues it again.
		return nil
	}
	posts, err := f.Jobs.Posts(ctx, id)
	if err != nil {
		return err
	}
	clock.mark("load")
	deleted, err := f.deletions(ctx, id, status == "cancelling")
	if err != nil {
		return fmt.Errorf("deletions: %w", err)
	}
	clock.mark("deletions")
	if deleted > 0 {
		st.Counters["comments_deleted"] += deleted
	}
	laneName := topics.LaneName(tkstaskv1.Lane(lane))
	comp, covered, err := f.coverage(ctx, posts, laneName)
	if err != nil {
		return err
	}
	clock.mark("coverage")
	if f.Content != nil {
		evs := make([]*content.Event, 0, len(covered))
		for _, c := range covered {
			c.JobID = id
			if e := content.PostCoverage(c); e != nil {
				evs = append(evs, e)
			}
		}
		if err := f.Content.Send(ctx, evs); err != nil {
			return fmt.Errorf("post coverage: %w", err)
		}
	}
	clock.mark("content")
	final := Status(status == "cancelling", st.Counters, comp)
	// The account as stored now that its records are written: its user id
	// for a job that started from a username, and its status.
	acc, _, err := dispatch.Resolve(ctx, f.DB, ref)
	if err != nil {
		return err
	}
	if userID == nil && acc != nil {
		userID = &acc.UserID
	}
	now := f.now().UTC()
	ev := &commonv1.JobEvent{
		EventId: id + ":" + final, JobId: id, Status: jobStatus(final), Kind: kind, SubjectRef: ref,
		CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(now), Counters: st.Counters,
		Platform: commonv1.Platform_PLATFORM_TIKTOK,
		Completeness: &commonv1.Completeness{Posts: comp.Posts, PostsComplete: comp.PostsComplete, PostsTruncated: comp.PostsTruncated,
			MedianRatio: comp.MedianRatio, MinRatio: comp.MinRatio, CommentsReported: comp.CommentsReported,
			CommentsCollected: comp.CommentsCollected},
	}
	if userID != nil {
		ev.SubjectId = *userID
	}
	// The event goes out before the job turns terminal: a failure after it
	// leaves the job to finalize again, which sends the same event (same
	// id) once more. Turning terminal first would lose it, since a
	// terminal job is never finalized again.
	b := f.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Job, id, ev); err != nil {
		return err
	}
	if f.Content != nil {
		if err := addCoverage(ctx, b, f.Content, ev, mode); err != nil {
			return err
		}
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	clock.mark("event")
	switch kind {
	case dispatch.KindEntry:
		// An entry check moves no schedule: discovery decides the account.
		if f.Entry != nil {
			if err := f.Entry(ctx, jid, final, st.Counters); err != nil {
				return fmt.Errorf("entry: %w", err)
			}
		}
	case dispatch.KindFollows, dispatch.KindDiscover:
		// A list or discovery page read moves no schedule either: its stage
		// wrote the next read (tt.follow_lists, tt.discovery_targets).
	default:
		if err := f.schedule(ctx, jid, final, mode, params, created, acc); err != nil {
			return err
		}
	}
	clock.mark("schedule")
	cb, _ := json.Marshal(st.Counters)
	compb, _ := json.Marshal(comp)
	// Guarded: a job something else made terminal meanwhile keeps that.
	if _, err := f.DB.Exec(ctx, `UPDATE jobs SET status = $2, counters = $3, completeness = $4, finished_at = $5,
		user_id = coalesce(user_id, $6) WHERE job_id = $1 AND status IN ('running','cancelling','finalizing')`,
		jid, final, cb, compb, now, userID); err != nil {
		return err
	}
	metrics.Jobs.WithLabelValues(final, laneName).Inc()
	if kind != dispatch.KindOnDemand && kind != dispatch.KindEntry && kind != dispatch.KindFollows && kind != dispatch.KindDiscover {
		metrics.Checks.WithLabelValues(checkKind(kind, mode), final, f.window(ctx, jid, kind, mode, created, now)).Inc()
		if final != "cancelled" {
			metrics.CheckDuration.WithLabelValues(checkKind(kind, mode)).Observe(now.Sub(created).Seconds())
		}
	}
	_ = f.Jobs.Expire(ctx, id, 24*time.Hour)
	clock.mark("update")
	attrs := append([]any{"job", id, "status", final, "mode", mode, "lane", laneName, "posts", st.Counters["posts"],
		"comments", st.Counters["comments"], "replies", st.Counters["replies"], "median_completeness", comp.MedianRatio,
		"posts_covered", comp.Posts}, clock.observe(laneName)...)
	slog.Info("job finished", attrs...)
	return nil
}

// BackfillWindow is how long a backfill may take and still count as in its
// window: build brief §8 A3's 48 h from registration.
const BackfillWindow = 48 * time.Hour

// window says whether a scheduled job ended within its window (build brief
// §8 A1): a check or a walk standing in for one within its account's
// interval of being started (the next check would be due), a backfill
// within BackfillWindow.
func (f *Finalizer) window(ctx context.Context, jid uuid.UUID, kind, mode string, created, ended time.Time) string {
	limit := BackfillWindow
	if checkKind(kind, mode) != "backfill" {
		limit = f.Cfg.TT.Freshness.ListInterval
		var secs int
		if err := f.DB.QueryRow(ctx, `SELECT interval_seconds FROM account_schedule WHERE last_job_id = $1`, jid).
			Scan(&secs); err == nil && secs > 0 {
			limit = time.Duration(secs) * time.Second
		}
	}
	if ended.Sub(created) <= limit {
		return "in"
	}
	return "late"
}

func checkKind(kind, mode string) string {
	switch {
	case kind == dispatch.KindBackfill || mode == dispatch.ModeFull:
		return "backfill"
	case mode == dispatch.ModeRefresh:
		return "walk"
	}
	return "check"
}

// schedule moves the account's schedule on after its job (the row whose
// last job this was): its user id once known, the failure count that
// stretches its interval, the last successful check, the metrics walks
// the job covered, and its posting rate. An account that is not active
// (private, banned, gone) gets the lighter checks of
// tt.freshness.inactive, spaced by how long it has been so
// (inactive_since), until a check finds it active (docs/tiktok/
// discovery-plan.md §5.4). Walks count from when they ended; the last
// success from when the check started.
func (f *Finalizer) schedule(ctx context.Context, jid uuid.UUID, final, mode string, params []byte, created time.Time, acc *dispatch.Account) error {
	ok := final == "completed" || final == "partial"
	var p struct {
		Walk string `json:"walk"`
	}
	_ = json.Unmarshal(params, &p)
	walks := map[string]time.Time{}
	if ok {
		walks = WalksDone(f.Cfg.TT.Freshness.MetricsWalks, mode, p.Walk, f.now())
	}
	wb, _ := json.Marshal(walks)
	var uid *int64
	inactive, status := false, ""
	if acc != nil {
		uid = &acc.UserID
		inactive, status = acc.Status != "active", acc.Status
	}
	if final == "cancelled" {
		return nil
	}
	step := f.Cfg.TT.Freshness.Inactive.Gone
	if status == "private" {
		step = f.Cfg.TT.Freshness.Inactive.Private
	}
	window := f.Cfg.Limits.MaxAgeCapDays
	_, err := f.DB.Exec(ctx, `UPDATE account_schedule s SET user_id = coalesce(s.user_id, $2),
			consecutive_failures = CASE WHEN $3 THEN 0 ELSE s.consecutive_failures + 1 END,
			last_success_at = CASE WHEN $3 THEN greatest(s.last_success_at, $4) ELSE s.last_success_at END,
			walks = s.walks || $5::jsonb,
			inactive_since = CASE WHEN $6 THEN coalesce(s.inactive_since, $7) ELSE NULL END,
			next_run_at = CASE WHEN $6 THEN greatest(s.next_run_at, $7::timestamptz + make_interval(secs =>
				CASE WHEN $7::timestamptz - coalesce(s.inactive_since, $7) < make_interval(secs => $10::float8) THEN $8::float8 ELSE $11::float8 END))
				ELSE s.next_run_at END,
			posts_per_day = r.per_day, last_post_at = coalesce(r.last_post, s.last_post_at)
		FROM (SELECT count(*) FILTER (WHERE created_at > $7::timestamptz - interval '30 days') / 30.0 AS per_day,
				max(created_at) AS last_post
			FROM posts WHERE user_id = $2 AND created_at > $7::timestamptz - make_interval(days => $9)) r
		WHERE s.last_job_id = $1`,
		jid, uid, ok, created, wb, inactive, f.now(), step.First.Seconds(), max(window, 30), step.For.Seconds(), step.Then.Seconds())
	return err
}

// deletions compares the stored top-level comments of each post read whole
// during the job with what that read returned. A comment stored before the
// read began and not returned by it is published as deleted on
// tt.comment.v1, with its replies, and marked deleted in the store. It
// returns how many were marked; a cancelled job only drops its pass
// records.
func (f *Finalizer) deletions(ctx context.Context, job string, cancelled bool) (int64, error) {
	passes, posts, err := f.Jobs.Passes(ctx, job)
	if err != nil || len(posts) == 0 {
		return 0, err
	}
	var marked int64
	if f.Cfg.Scheduler.PostRefresh.DetectDeletions && !cancelled {
		var dels []*ttv1.Comment
		for _, p := range passes {
			ds, err := f.missing(ctx, job, p)
			if err != nil {
				return 0, err
			}
			dels = append(dels, ds...)
		}
		if len(dels) > 0 {
			// One time for the whole check: a comment and the replies deleted
			// with it share deleted_at, so a later sighting of the comment
			// restores them together.
			at := f.now().UTC()
			em := emit.Emitter{Producer: f.Producer, Now: func() time.Time { return at }}
			src := emit.Source{JobID: job, Query: "comments", Mode: commonv1.CrawlMode_CRAWL_MODE_RECRAWL}
			b := f.Producer.NewBatch()
			for _, d := range dels {
				if err := em.CommentDeleted(ctx, b, src, d); err != nil {
					return 0, err
				}
			}
			if err := b.Wait(ctx); err != nil {
				return 0, err
			}
			// The persister may apply the records first; either way they are
			// marked once, all at the same time.
			for _, d := range dels {
				d.Obs = &commonv1.Observation{ObservedAt: timestamppb.New(at)}
			}
			if _, err := ttpersist.MarkDeleted(ctx, f.DB, dels); err != nil {
				return 0, err
			}
			marked = int64(len(dels))
			// Before the pass records go: a retry finds these already
			// marked and does not count them again.
			if err := f.Jobs.AddCounters(ctx, job, map[string]int64{"comments_deleted": marked}); err != nil {
				return 0, err
			}
		}
	}
	return marked, f.Jobs.ClearPasses(ctx, job, posts)
}

// missing returns the top-level comments of p's post stored before the pass
// began and not returned by it, with their replies. The post stage records
// a pass only for a read that reached TikTok's end of the list; on top of
// that, what the read returned (each comment with the replies TikTok counts
// for it) must be within tt.comments.deletion_slack of the post's comment
// count, or TikTok may have answered short and the post is skipped.
func (f *Finalizer) missing(ctx context.Context, job string, p jobs.Pass) ([]*ttv1.Comment, error) {
	seen, err := f.Jobs.Seen(ctx, job, p.PostID)
	if err != nil {
		return nil, err
	}
	var covered int64
	for _, replies := range seen {
		covered += 1 + replies
	}
	slack := int64(math.Round(f.Cfg.TT.Comments.DeletionSlack * float64(p.Reported)))
	if p.Reported <= 0 || covered < p.Reported-slack || covered > p.Reported+slack {
		metrics.DeletionChecks.WithLabelValues("skipped_partial").Inc()
		slog.Debug("finalizer: comment read short of the count; not checked for deletions", "job", job, "post", p.PostID,
			"returned", covered, "reported", p.Reported)
		return nil, nil
	}
	metrics.DeletionChecks.WithLabelValues("checked").Inc()
	rows, err := f.DB.Query(ctx, `SELECT comment_id, user_id FROM comments
		WHERE post_created_at = $1 AND post_id = $2 AND depth = 0 AND status = 'visible' AND NOT redacted
		  AND first_seen_at < $3`, p.PostCreated, p.PostID, p.Start)
	if err != nil {
		return nil, err
	}
	var out []*ttv1.Comment
	for rows.Next() {
		var id, user int64
		if err := rows.Scan(&id, &user); err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := seen[id]; !ok {
			out = append(out, &ttv1.Comment{CommentId: id, PostId: p.PostID, UserId: user,
				PostCreatedAt: timestamppb.New(p.PostCreated)})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return nil, err
	}
	// Deleting a comment deletes its replies.
	roots := make([]int64, len(out))
	for i, c := range out {
		roots[i] = c.CommentId
	}
	rows, err = f.DB.Query(ctx, `SELECT comment_id, user_id, parent_comment_id FROM comments
		WHERE post_created_at = $1 AND post_id = $2 AND parent_comment_id = ANY($3) AND depth > 0
		  AND status = 'visible' AND NOT redacted`, p.PostCreated, p.PostID, roots)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, user, parent int64
		if err := rows.Scan(&id, &user, &parent); err != nil {
			return nil, err
		}
		out = append(out, &ttv1.Comment{CommentId: id, PostId: p.PostID, UserId: user,
			PostCreatedAt: timestamppb.New(p.PostCreated), Depth: 1, ParentCommentId: &parent})
	}
	return out, rows.Err()
}

// coverage writes each read post's collected counts, completeness and
// comments state, and returns the job's summary and each post's coverage
// as written (for the contract's Post.coverage). Posts whose comments the
// job did not read (none, or not due) are left as they are.
func (f *Finalizer) coverage(ctx context.Context, posts []jobs.PostState, lane string) (Completeness, []content.PostComments, error) {
	var c Completeness
	var out []content.PostComments
	failed := map[int64]bool{}
	var ids []int64
	var created []time.Time
	var states []string
	// One order for every job: several finalise at once, and posts updated
	// in different orders deadlock.
	posts = append([]jobs.PostState(nil), posts...)
	sort.Slice(posts, func(i, j int) bool { return posts[i].PostID < posts[j].PostID })
	for _, p := range posts {
		state := p.State
		switch p.State {
		case jobs.PostComplete:
			c.PostsComplete++
		case jobs.PostPartial:
			c.PostsCapped++
		case jobs.PostTruncated:
			c.PostsTruncated++
		case jobs.PostPending, jobs.PostFailed:
			// Not read to its end (a task dead-lettered): the next read is
			// whole (the post's incomplete mark).
			c.PostsFailed++
			state = "partial"
			failed[p.PostID] = true
		default:
			continue
		}
		ids = append(ids, p.PostID)
		created = append(created, p.CreatedAt)
		states = append(states, state)
	}
	c.Posts = int64(len(ids))
	if len(ids) == 0 {
		c.MedianRatio, c.MinRatio = 1, 1
		return c, nil, nil
	}
	synced := f.now().UTC()
	var ratios []float64
	for lo := 0; lo < len(ids); lo += coverageChunk {
		hi := min(lo+coverageChunk, len(ids))
		got, err := f.coverageChunk(ctx, ids[lo:hi], created[lo:hi], states[lo:hi], synced, failed, lane, &c)
		if err != nil {
			return c, nil, err
		}
		out = append(out, got...)
		for _, pc := range got {
			ratios = append(ratios, pc.Completeness)
		}
	}
	if len(ratios) > 0 {
		sort.Float64s(ratios)
		c.MinRatio = math.Round(ratios[0]*1000) / 1000
		c.MedianRatio = math.Round(ratios[len(ratios)/2]*1000) / 1000
	}
	return c, out, nil
}

// coverageChunk writes the coverage of some of a job's posts in one
// statement and adds their counts to c.
func (f *Finalizer) coverageChunk(ctx context.Context, ids []int64, created []time.Time, states []string, synced time.Time,
	failed map[int64]bool, lane string, c *Completeness) ([]content.PostComments, error) {
	var out []content.PostComments
	// A post counted within coverage_recount_after keeps its counts (its
	// state and read time are still written): counting every stored
	// comment of every post read, on each check, took 40-65 s a statement
	// (2026-11-14). One scan a post counts both depths.
	recountBefore := synced.Add(-f.recountAfter())
	rows, err := f.DB.Query(ctx, `
		WITH t AS (
			SELECT unnest($1::bigint[]) AS post_id, unnest($2::timestamptz[]) AS created, unnest($3::text[]) AS state
		), cur AS (
			SELECT t.post_id, t.created, t.state,
				coalesce(p.comments_counted_at, p.comments_synced_at) IS NULL
					OR coalesce(p.comments_counted_at, p.comments_synced_at) < $5 AS recount
			FROM t JOIN posts p ON p.created_at = t.created AND p.post_id = t.post_id
		), counts AS (
			SELECT cur.post_id, cur.created, cur.state, cur.recount, x.top, x.replies
			FROM cur LEFT JOIN LATERAL (
				SELECT count(*) FILTER (WHERE c.depth = 0) AS top, count(*) FILTER (WHERE c.depth > 0) AS replies
				FROM comments c WHERE cur.recount AND c.post_created_at = cur.created AND c.post_id = cur.post_id
					AND c.status = 'visible'
			) x ON cur.recount
		)
		UPDATE posts p SET
			comments_collected = CASE WHEN counts.recount THEN counts.top ELSE p.comments_collected END,
			replies_collected = CASE WHEN counts.recount THEN counts.replies ELSE p.replies_collected END,
			completeness = CASE WHEN coalesce(p.comment_count, 0) = 0 THEN 1
				ELSE least(1.0, (CASE WHEN counts.recount THEN counts.top + counts.replies
					ELSE coalesce(p.comments_collected, 0) + coalesce(p.replies_collected, 0) END)::real / p.comment_count) END,
			comments_state = counts.state, comments_synced_at = $4,
			comments_counted_at = CASE WHEN counts.recount THEN $4 ELSE coalesce(p.comments_counted_at, p.comments_synced_at) END
		FROM counts WHERE p.created_at = counts.created AND p.post_id = counts.post_id
		RETURNING coalesce(p.comment_count, 0), coalesce(p.comments_collected, 0) + coalesce(p.replies_collected, 0), p.completeness,
			p.post_id, p.user_id, counts.state, coalesce(p.comments_disabled, false), coalesce(p.comments_collected, 0),
			coalesce(p.replies_collected, 0)`, ids, created, states, synced, recountBefore)
	if err != nil {
		return nil, fmt.Errorf("coverage: %w", err)
	}
	for rows.Next() {
		var reported, collected, top, replies, postID, userID int64
		var ratio float32
		var state string
		var disabled bool
		if err := rows.Scan(&reported, &collected, &ratio, &postID, &userID, &state, &disabled, &top, &replies); err != nil {
			rows.Close()
			return nil, err
		}
		pc := content.PostComments{PostID: postID, UserID: userID, State: state, CommentsDisabled: disabled, Reported: reported,
			Collected: top, RepliesCollected: replies, Completeness: float64(ratio), SyncedAt: synced}
		if failed[postID] {
			// Not read to its end: stopped by an error, not at the cap.
			pc.State, pc.CommentsReadError = "truncated", true
		}
		out = append(out, pc)
		c.CommentsReported += reported
		c.CommentsCollected += collected
		metrics.CommentCompleteness.WithLabelValues(lane).Observe(float64(ratio))
	}
	rows.Close()
	return out, rows.Err()
}

// addCoverage puts the job's JobCoverage on collection.coverage.v1 in b,
// beside its tt.job.v1 event; one that fails tks's check is counted and
// left out.
func addCoverage(ctx context.Context, b *kafka.Batch, e *content.Emitter, ev *commonv1.JobEvent, mode string) error {
	m := content.JobCoverage(ev, mode)
	if err := content.Check(m, e.Options); err != nil {
		metrics.ContentEvents.WithLabelValues(content.TopicCoverage, "invalid").Inc()
		slog.Warn("coverage: event breaks the contract, not sent", "job", ev.JobId, "err", err)
		return nil
	}
	rec, err := content.Record(e.Producer, content.TopicCoverage, ev.JobId, m)
	if err != nil {
		return err
	}
	b.Add(ctx, rec)
	metrics.ContentEvents.WithLabelValues(content.TopicCoverage, "sent").Inc()
	return nil
}

func jobStatus(s string) commonv1.JobStatus {
	switch s {
	case "completed":
		return commonv1.JobStatus_JOB_STATUS_COMPLETED
	case "partial":
		return commonv1.JobStatus_JOB_STATUS_PARTIAL
	case "cancelled":
		return commonv1.JobStatus_JOB_STATUS_CANCELLED
	case "failed":
		return commonv1.JobStatus_JOB_STATUS_FAILED
	default:
		return commonv1.JobStatus_JOB_STATUS_RUNNING
	}
}
