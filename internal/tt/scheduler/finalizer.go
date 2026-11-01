package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
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
	Now            func() time.Time

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
	ids, err := f.Jobs.ReadyToFinalize(ctx, 500)
	if err != nil {
		return err
	}
	if f.held == nil {
		f.held = map[string]bool{}
	}
	var fresh []string
	for _, id := range ids {
		if !f.held[id] {
			fresh = append(fresh, id)
		}
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
	keep := f.pending[:0]
	for _, b := range f.pending {
		caught, err := kafka.GroupCaughtUp(ctx, f.Producer.Client(), group, b.ends)
		if err != nil {
			slog.Warn("finalizer: offset check failed", "err", err)
		}
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
			if err := f.finalize(ctx, id); err != nil {
				// Still in the set: taken again on the next tick.
				slog.Error("finalizer: job failed to finalise", "job", id, "err", err)
				continue
			}
			if err := f.Jobs.Finalized(ctx, id); err != nil {
				slog.Warn("finalizer: could not clear a finalised job", "job", id, "err", err)
			}
		}
	}
	f.pending = keep
	return nil
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
	deleted, err := f.deletions(ctx, id, status == "cancelling")
	if err != nil {
		return fmt.Errorf("deletions: %w", err)
	}
	if deleted > 0 {
		st.Counters["comments_deleted"] += deleted
	}
	laneName := topics.LaneName(tkstaskv1.Lane(lane))
	comp, err := f.coverage(ctx, posts, laneName)
	if err != nil {
		return err
	}
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
	if err := b.Wait(ctx); err != nil {
		return err
	}
	if err := f.schedule(ctx, jid, final, mode, params, created, acc); err != nil {
		return err
	}
	cb, _ := json.Marshal(st.Counters)
	compb, _ := json.Marshal(comp)
	// Guarded: a job something else made terminal meanwhile keeps that.
	if _, err := f.DB.Exec(ctx, `UPDATE jobs SET status = $2, counters = $3, completeness = $4, finished_at = $5,
		user_id = coalesce(user_id, $6) WHERE job_id = $1 AND status IN ('running','cancelling','finalizing')`,
		jid, final, cb, compb, now, userID); err != nil {
		return err
	}
	metrics.Jobs.WithLabelValues(final, laneName).Inc()
	if kind != dispatch.KindOnDemand {
		metrics.Checks.WithLabelValues(checkKind(kind, mode), final).Inc()
	}
	_ = f.Jobs.Expire(ctx, id, 24*time.Hour)
	slog.Info("job finished", "job", id, "status", final, "mode", mode, "posts", st.Counters["posts"],
		"comments", st.Counters["comments"], "replies", st.Counters["replies"], "median_completeness", comp.MedianRatio)
	return nil
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
// (private, banned, gone) is checked again only after
// tt.freshness.account_refresh, until a check finds it active. Walks count
// from when they ended; the last success from when the check started.
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
	inactive := false
	if acc != nil {
		uid = &acc.UserID
		inactive = acc.Status != "active"
	}
	if final == "cancelled" {
		return nil
	}
	window := f.Cfg.Limits.MaxAgeCapDays
	_, err := f.DB.Exec(ctx, `UPDATE account_schedule s SET user_id = coalesce(s.user_id, $2),
			consecutive_failures = CASE WHEN $3 THEN 0 ELSE s.consecutive_failures + 1 END,
			last_success_at = CASE WHEN $3 THEN greatest(s.last_success_at, $4) ELSE s.last_success_at END,
			walks = s.walks || $5::jsonb,
			next_run_at = CASE WHEN $6 THEN greatest(s.next_run_at, $7::timestamptz + make_interval(secs => $8)) ELSE s.next_run_at END,
			posts_per_day = r.per_day, last_post_at = coalesce(r.last_post, s.last_post_at)
		FROM (SELECT count(*) FILTER (WHERE created_at > $7::timestamptz - interval '30 days') / 30.0 AS per_day,
				max(created_at) AS last_post
			FROM posts WHERE user_id = $2 AND created_at > $7::timestamptz - make_interval(days => $9)) r
		WHERE s.last_job_id = $1`,
		jid, uid, ok, created, wb, inactive, f.now(), f.Cfg.TT.Freshness.AccountRefresh.Seconds(), max(window, 30))
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
// comments state, and returns the job's summary. Posts whose comments the
// job did not read (none, or not due) are left as they are.
func (f *Finalizer) coverage(ctx context.Context, posts []jobs.PostState, lane string) (Completeness, error) {
	var c Completeness
	var ids []int64
	var created []time.Time
	var states []string
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
		return c, nil
	}
	rows, err := f.DB.Query(ctx, `
		WITH t AS (
			SELECT unnest($1::bigint[]) AS post_id, unnest($2::timestamptz[]) AS created, unnest($3::text[]) AS state
		), counts AS (
			SELECT t.post_id, t.created, t.state,
				(SELECT count(*) FROM comments c WHERE c.post_created_at = t.created AND c.post_id = t.post_id AND c.depth = 0
					AND c.status = 'visible') AS top,
				(SELECT count(*) FROM comments c WHERE c.post_created_at = t.created AND c.post_id = t.post_id AND c.depth > 0
					AND c.status = 'visible') AS replies
			FROM t
		)
		UPDATE posts p SET comments_collected = counts.top, replies_collected = counts.replies,
			completeness = CASE WHEN coalesce(p.comment_count, 0) = 0 THEN 1
				ELSE least(1.0, (counts.top + counts.replies)::real / p.comment_count) END,
			comments_state = counts.state, comments_synced_at = $4
		FROM counts WHERE p.created_at = counts.created AND p.post_id = counts.post_id
		RETURNING coalesce(p.comment_count, 0), counts.top + counts.replies, p.completeness`, ids, created, states, f.now())
	if err != nil {
		return c, fmt.Errorf("coverage: %w", err)
	}
	var ratios []float64
	for rows.Next() {
		var reported, collected int64
		var ratio float32
		if err := rows.Scan(&reported, &collected, &ratio); err != nil {
			rows.Close()
			return c, err
		}
		c.CommentsReported += reported
		c.CommentsCollected += collected
		ratios = append(ratios, float64(ratio))
		metrics.CommentCompleteness.WithLabelValues(lane).Observe(float64(ratio))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}
	if len(ratios) > 0 {
		sort.Float64s(ratios)
		c.MinRatio = math.Round(ratios[0]*1000) / 1000
		c.MedianRatio = math.Round(ratios[len(ratios)/2]*1000) / 1000
	}
	return c, nil
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
