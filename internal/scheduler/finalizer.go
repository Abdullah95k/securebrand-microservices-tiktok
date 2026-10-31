// Package scheduler runs the leader-only control loops: scheduled
// re-scrapes, job finalisation, stalled-job reaping, partition maintenance,
// retention and GDPR erasure.
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

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Finalizer turns jobs whose tasks are all done into terminal job rows and
// fb.job.v1 events, once the persister has written everything produced so
// far (so "completed" means "queryable").
type Finalizer struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	Timeout  time.Duration
	// PersisterGroup defaults to persist.Group.
	PersisterGroup string
	// Adaptive, when set, re-tiers the page and registers post refreshes
	// once a job's data is stored.
	Adaptive *Adaptive
	// Deletions, when enabled, marks known top-level comments deleted when a
	// full read of their post during the job no longer returned them.
	Deletions Deletions

	pending []finalBatch
	held    map[string]bool // jobs in pending
}

type finalBatch struct {
	jobs  []string
	ends  kadm.ListedOffsets
	since time.Time
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
		group = f.Producer.Topic(persist.Group)
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
	PostsTruncated    int64   `json:"posts_truncated"`
	PostsFailed       int64   `json:"posts_failed"`
	MedianRatio       float64 `json:"median_ratio"`
	MinRatio          float64 `json:"min_ratio"`
	CommentsReported  int64   `json:"comments_reported"`
	CommentsCollected int64   `json:"comments_collected"`
}

func (f *Finalizer) finalize(ctx context.Context, id string) error {
	jid, err := uuid.Parse(id)
	if err != nil {
		return nil // not ours
	}
	var status, kind, pageRef string
	var lane int16
	var pageID *int64
	var created time.Time
	err = f.DB.QueryRow(ctx, `SELECT status, kind, lane, page_ref, page_id, created_at FROM jobs WHERE job_id = $1`, jid).
		Scan(&status, &kind, &lane, &pageRef, &pageID, &created)
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
	comp, err := f.coverage(ctx, posts, topics.LaneName(taskLane(lane)))
	if err != nil {
		return err
	}
	final := "completed"
	switch {
	case status == "cancelling":
		final = "cancelled"
	case comp.PostsTruncated > 0 || comp.PostsFailed > 0 || st.Counters["tasks_failed"] > 0 || st.Counters["timelines_truncated"] > 0:
		final = "partial"
	case st.Counters["pages_unavailable"] > 0 && st.Counters["posts"] == 0:
		final = "failed"
	}
	// Re-tier the page and register its posts for refresh before the job
	// turns terminal, so a finished job means an updated schedule.
	if f.Adaptive != nil && pageID != nil && final != "cancelled" {
		if err := f.Adaptive.AfterJob(ctx, *pageID); err != nil {
			slog.Warn("finalizer: adaptive schedule", "job", id, "page", *pageID, "err", err)
		}
	}
	cb, _ := json.Marshal(st.Counters)
	compb, _ := json.Marshal(comp)
	now := time.Now().UTC()
	if _, err := f.DB.Exec(ctx, `UPDATE jobs SET status = $2, counters = $3, completeness = $4, finished_at = $5 WHERE job_id = $1`,
		jid, final, cb, compb, now); err != nil {
		return err
	}
	if _, err := f.DB.Exec(ctx, `UPDATE page_schedule SET page_id = COALESCE(page_id, $2),
		consecutive_failures = CASE WHEN $3 IN ('completed','partial') THEN 0 ELSE consecutive_failures + 1 END
		WHERE last_job_id = $1`, jid, pageID, final); err != nil {
		return err
	}
	ev := &fbv1.JobEvent{
		EventId: id + ":" + final, JobId: id, Status: jobStatus(final), Kind: kind, PageRef: pageRef,
		CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(now), Counters: st.Counters,
		Completeness: &fbv1.Completeness{Posts: comp.Posts, PostsComplete: comp.PostsComplete, PostsTruncated: comp.PostsTruncated,
			MedianRatio: comp.MedianRatio, MinRatio: comp.MinRatio, CommentsReported: comp.CommentsReported,
			CommentsCollected: comp.CommentsCollected},
	}
	if pageID != nil {
		ev.PageId = *pageID
	}
	b := f.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Job, id, ev); err != nil {
		return err
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	metrics.Jobs.WithLabelValues(final, topics.LaneName(taskLane(lane))).Inc()
	_ = f.Jobs.Expire(ctx, id, 24*time.Hour)
	slog.Info("job finished", "job", id, "status", final, "posts", st.Counters["posts"], "comments", st.Counters["comments"],
		"replies", st.Counters["replies"], "median_completeness", comp.MedianRatio)
	return nil
}

// Deletions configures deletion detection.
type Deletions struct {
	Enabled bool
	// CountSlack: how far, as a share of Facebook's comment count, what a
	// full read returned may be from that count and the read still count as
	// complete.
	CountSlack float64
}

// DeletionsFrom reads the deletion settings from the configuration.
func DeletionsFrom(cfg *config.Config) Deletions {
	pr := cfg.Scheduler.PostRefresh
	return Deletions{Enabled: pr.DetectDeletions, CountSlack: pr.DeletionCountSlack}
}

// deletions compares the stored top-level comments of each post read in full
// during the job with what that read returned. A comment stored before the
// read began and not returned by it is published as deleted on
// fb.comment.v1, with its replies, and marked deleted in the store. It
// returns how many were marked; a cancelled job only drops its pass records.
func (f *Finalizer) deletions(ctx context.Context, job string, cancelled bool) (int64, error) {
	passes, posts, err := f.Jobs.Passes(ctx, job)
	if err != nil || len(posts) == 0 {
		return 0, err
	}
	var marked int64
	if f.Deletions.Enabled && !cancelled {
		var dels []*fbv1.Comment
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
			at := time.Now().UTC()
			em := emit.Emitter{Producer: f.Producer, Now: func() time.Time { return at }}
			src := emit.Source{JobID: job, Query: "comments", Mode: fbv1.CrawlMode_CRAWL_MODE_RECRAWL}
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
			// marked once.
			if _, err := persist.MarkDeleted(ctx, f.DB, dels); err != nil {
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
// began and not returned by it, with their replies. Only a complete read
// counts: the comments it returned, with the reply counts Facebook gave for
// them, must add up to the post's comment count from the same read (within
// Deletions.CountSlack). Otherwise Facebook may have answered partially and
// the post is skipped.
func (f *Finalizer) missing(ctx context.Context, job string, p jobs.Pass) ([]*fbv1.Comment, error) {
	seen, err := f.Jobs.Seen(ctx, job, p.PostID)
	if err != nil {
		return nil, err
	}
	var covered int64
	for _, replies := range seen {
		covered += 1 + replies
	}
	slack := int64(math.Round(f.Deletions.CountSlack * float64(p.Reported)))
	if p.Reported <= 0 || covered < p.Reported-slack || covered > p.Reported+slack {
		metrics.DeletionChecks.WithLabelValues("skipped_partial").Inc()
		slog.Debug("finalizer: partial comment read; not checked for deletions", "job", job, "post", p.PostID,
			"returned", covered, "reported", p.Reported)
		return nil, nil
	}
	metrics.DeletionChecks.WithLabelValues("checked").Inc()
	rows, err := f.DB.Query(ctx, `SELECT comment_id, page_id FROM comments
		WHERE post_created_at = $1 AND post_id = $2 AND depth = 0 AND status = 'visible' AND NOT redacted
		  AND first_seen_at < $3`, p.PostCreated, p.PostID, p.Start)
	if err != nil {
		return nil, err
	}
	var out []*fbv1.Comment
	for rows.Next() {
		var id, page int64
		if err := rows.Scan(&id, &page); err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := seen[id]; !ok {
			out = append(out, &fbv1.Comment{CommentId: id, PostId: p.PostID, PageId: page,
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
	rows, err = f.DB.Query(ctx, `SELECT comment_id, page_id, depth FROM comments
		WHERE post_created_at = $1 AND post_id = $2 AND root_comment_id = ANY($3) AND depth > 0
		  AND status = 'visible' AND NOT redacted`, p.PostCreated, p.PostID, roots)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, page int64
		var depth int16
		if err := rows.Scan(&id, &page, &depth); err != nil {
			return nil, err
		}
		out = append(out, &fbv1.Comment{CommentId: id, PostId: p.PostID, PageId: page,
			PostCreatedAt: timestamppb.New(p.PostCreated), Depth: int32(depth)})
	}
	return out, rows.Err()
}

// coverage writes per-post collected counts and completeness, and returns
// the job summary.
func (f *Finalizer) coverage(ctx context.Context, posts []jobs.PostState, lane string) (Completeness, error) {
	var c Completeness
	var ids []int64
	var created []time.Time
	var states []string
	for _, p := range posts {
		switch p.State {
		case jobs.PostComplete, jobs.PostTruncated, jobs.PostFailed, jobs.PostPending:
			ids = append(ids, p.PostID)
			created = append(created, p.CreatedAt)
			state := p.State
			if state == jobs.PostPending || state == jobs.PostFailed {
				state = "partial"
			}
			states = append(states, state)
			switch p.State {
			case jobs.PostComplete:
				c.PostsComplete++
			case jobs.PostTruncated:
				c.PostsTruncated++
			default:
				c.PostsFailed++
			}
		}
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
			completeness = CASE WHEN coalesce(p.comment_count_total, 0) = 0 THEN 1
				ELSE least(1.0, (counts.top + counts.replies)::real / p.comment_count_total) END,
			comments_state = counts.state, comments_synced_at = now()
		FROM counts WHERE p.post_created_at = counts.created AND p.post_id = counts.post_id
		RETURNING coalesce(p.comment_count_total, 0), counts.top + counts.replies, p.completeness`, ids, created, states)
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
		c.MinRatio = ratios[0]
		c.MedianRatio = ratios[len(ratios)/2]
		c.MedianRatio = math.Round(c.MedianRatio*1000) / 1000
		c.MinRatio = math.Round(c.MinRatio*1000) / 1000
	}
	return c, nil
}

func jobStatus(s string) fbv1.JobStatus {
	switch s {
	case "completed":
		return fbv1.JobStatus_JOB_STATUS_COMPLETED
	case "partial":
		return fbv1.JobStatus_JOB_STATUS_PARTIAL
	case "cancelled":
		return fbv1.JobStatus_JOB_STATUS_CANCELLED
	case "failed":
		return fbv1.JobStatus_JOB_STATUS_FAILED
	default:
		return fbv1.JobStatus_JOB_STATUS_RUNNING
	}
}
