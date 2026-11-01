// Package stage runs tks's crawl stages from Kafka: the account stage (an
// account's post list, and the account itself when due), and the worker that
// owns every task's lifecycle (docs/tiktok build brief §4).
//
// The worker and the helpers below are internal/crawl/stage's on tks's
// envelope (securebrand.tks.task.v1) and TikTok's records (tt.v1): the same
// duplicate suppression, stream leases, checkpoints, retries, dead letters and
// job accounting, with the same Redis stores (internal/crawl/checkpoint and
// jobs). The copied Facebook stages stay in internal/crawl/stage until M9
// removes the Facebook code; a fix to the lifecycle belongs in both until
// then (UPSTREAM.md).
package stage

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Deps are the collaborators every stage uses.
type Deps struct {
	Cfg      *config.Config
	Pool     *session.Pool
	Proxies  *proxy.Pool
	Loader   *session.Loader
	Client   *graphql.Client
	Emit     *emit.Emitter
	Producer *kafka.Producer
	Jobs     *jobs.Tracker
	Ckpt     *checkpoint.Store
	// DB is schema tt (tt.crawl_cursors: the list watermarks); nil reads
	// every account as never checked.
	DB *pgxpool.Pool
	// Suppression holds erased authors and accounts (ForAccounts).
	Suppression *gdpr.Suppression
	Now         func() time.Time
}

func (d *Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now().UTC()
}

// Result of one stage run.
type Result int

const (
	// Done: the task finished.
	Done Result = iota
	// Continue: the task yielded; Next continues the same stream.
	Continue
	// Retry: transient failure; Next is the same task to retry later.
	Retry
	// Fail: permanent failure; the task goes to the DLQ.
	Fail
)

// Outcome is what a stage returns to the worker.
type Outcome struct {
	Result   Result
	Next     *tkstaskv1.Task
	Delay    string // retry topic: "30s" | "5m" | "1h"
	Reason   string
	Counters map[string]int64
	// FollowUps are new streams to start once this task released its lease.
	FollowUps []*tkstaskv1.Task
	// Waiting marks a retry the task did not cause (every proxy exit is
	// cooling down): it does not count toward the task's stalls.
	Waiting bool
}

// Stage runs one task kind.
type Stage interface {
	Run(ctx context.Context, t *tkstaskv1.Task) Outcome
}

func done(c map[string]int64) Outcome { return Outcome{Result: Done, Counters: c} }

func retry(t *tkstaskv1.Task, cursor, delay, reason string, c map[string]int64) Outcome {
	return Outcome{Result: Retry, Next: task.Retry(t, cursor), Delay: delay, Reason: reason, Counters: c}
}

// noIdentity retries a task that got no identity, as internal/crawl/stage
// does: waits for exits are not held against the task.
func noIdentity(t *tkstaskv1.Task, err error, c map[string]int64) Outcome {
	var o Outcome
	switch {
	case errors.Is(err, proxy.ErrTierOpen):
		o = retry(t, "", "5m", "no identity: "+err.Error(), c)
	case errors.Is(err, proxy.ErrExitsPaced):
		o = retry(t, "", "30s", "no identity: "+err.Error(), c)
	default:
		return retry(t, "", "30s", "no identity: "+err.Error(), c)
	}
	o.Waiting = true
	return o
}

// pacedRetry retries a task whose identity had to load a page while its
// exit paces page loads (30s), or backs off after a walled one (5m).
func pacedRetry(t *tkstaskv1.Task, cursor string, err error, c map[string]int64) Outcome {
	delay := "30s"
	if errors.Is(err, session.ErrExitPaused) {
		delay = "5m"
	}
	o := retry(t, cursor, delay, err.Error(), c)
	o.Waiting = true
	return o
}

// identity acquires an identity for a query class, on the tier tierFor
// picks.
func (d *Deps) identity(ctx context.Context, class string, stalls uint32) (*session.Identity, error) {
	return d.Pool.Acquire(ctx, d.tierFor(class, stalls))
}

// tierFor picks the tier for a task's next attempt: cheapest first,
// escalating one tier per stall and skipping tiers whose exits are all
// cooling down (internal/crawl/stage's rule).
func (d *Deps) tierFor(class string, stalls uint32) string {
	tiers := d.Proxies.TiersFor(class)
	if len(tiers) == 0 {
		return "direct"
	}
	from := min(int(stalls), len(tiers)-1)
	for _, t := range tiers[from:] {
		if d.Proxies.Available(t) {
			return t
		}
	}
	return tiers[from]
}

// observeLoad reports a page load to the traffic budget and AIMD.
func (d *Deps) observeLoad(pl *session.PageLoad, err error, lane tkstaskv1.Lane) {
	if d.Client == nil || d.Client.Outcomes == nil {
		return
	}
	switch {
	case pl != nil:
		d.Client.Outcomes.Observe(pl.Class, topics.LaneName(lane), pl.Bytes)
	case err != nil:
		d.Client.Outcomes.Observe(detect.Transport, topics.LaneName(lane), 0)
	}
}

// classOutcome maps a non-OK response class to an outcome for task t;
// cursor is the resume point to retry from.
func classOutcome(t *tkstaskv1.Task, cls detect.Class, reason, cursor string, c map[string]int64) Outcome {
	if c == nil {
		c = map[string]int64{}
	}
	switch cls {
	case detect.RateLimited, detect.Checkpoint, detect.LoginWall:
		c["blocked"]++
	case detect.Transport:
		c["transport_errors"]++
	case detect.SoftError:
		c["soft_errors"]++
	case detect.RegistryStale, detect.SchemaDrift:
		c["drift"]++
	}
	switch cls {
	case detect.SoftError:
		return retry(t, cursor, "30s", "soft: "+reason, c)
	case detect.RegistryStale, detect.SchemaDrift:
		// A refused call (an empty answer: a variant that now needs a
		// signature) or a changed shape needs a registry or parser update.
		return retry(t, cursor, "5m", cls.String()+": "+reason, c)
	default:
		return retry(t, cursor, "30s", cls.String()+": "+reason, c)
	}
}

// spawn registers child tasks with the job and produces them.
func (d *Deps) spawn(ctx context.Context, children ...*tkstaskv1.Task) error {
	if len(children) == 0 {
		return nil
	}
	ids := make([]string, len(children))
	recs := make([]*kgo.Record, 0, len(children))
	for i, c := range children {
		ids[i] = c.TaskId
		rec, err := d.Producer.Record(topics.Task(c.Kind, c.Lane), task.ScopeKey(c), c)
		if err != nil {
			return err
		}
		recs = append(recs, rec)
	}
	if err := d.Jobs.Spawn(ctx, children[0].JobId, ids...); err != nil {
		return fmt.Errorf("stage: spawn: %w", err)
	}
	return d.Producer.ProduceSync(ctx, recs...)
}

func (d *Deps) saveCkpt(ctx context.Context, t *tkstaskv1.Task, st checkpoint.State, c map[string]int64) error {
	st.Counters = maps.Clone(c)
	return d.Ckpt.Save(ctx, t.LineageId, t.Seq, st)
}

// media publishes media metadata, and download tasks when the job's limits
// ask for media (tt.media.include, off by default: build brief D5).
func (d *Deps) media(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, src emit.Source, ms []*ttv1.Media, comment bool) (int64, error) {
	lim := t.Limits
	include := lim.GetIncludeMedia()
	if comment {
		include = lim.GetIncludeCommentMedia()
	}
	var queued int64
	hasHD := map[int64]bool{}
	for _, m := range ms {
		if m.Kind == ttv1.MediaKind_MEDIA_KIND_VIDEO && m.Variant == "hd" {
			hasHD[m.GetPostId()] = true
		}
	}
	for _, m := range ms {
		if err := d.Emit.Media(ctx, b, src, m); err != nil {
			return queued, err
		}
		if !include || !wanted(m, lim.GetVideoPolicy(), hasHD[m.GetPostId()]) {
			continue
		}
		rec, err := d.Producer.Record(topics.TaskMedia, m.MediaId, task.NewMedia(t.JobId, m))
		if err != nil {
			return queued, err
		}
		b.Add(ctx, rec)
		queued++
	}
	return queued, nil
}

// wanted applies the video policy: one rendition per video (sd, or hd when
// asked and served), covers unless the policy is none, photos and the rest
// always. TikTok's links expire (T18), so a download runs from the read that
// produced its link.
func wanted(m *ttv1.Media, vp tkstaskv1.VideoPolicy, hasHD bool) bool {
	switch m.Kind {
	case ttv1.MediaKind_MEDIA_KIND_VIDEO:
		switch vp {
		case tkstaskv1.VideoPolicy_VIDEO_POLICY_HD:
			return m.Variant == "hd" || (m.Variant == "sd" && !hasHD)
		case tkstaskv1.VideoPolicy_VIDEO_POLICY_SD, tkstaskv1.VideoPolicy_VIDEO_POLICY_UNSPECIFIED:
			return m.Variant == "sd"
		default:
			return false
		}
	case ttv1.MediaKind_MEDIA_KIND_COVER, ttv1.MediaKind_MEDIA_KIND_ORIGIN_COVER, ttv1.MediaKind_MEDIA_KIND_DYNAMIC_COVER:
		return vp != tkstaskv1.VideoPolicy_VIDEO_POLICY_NONE && m.Kind == ttv1.MediaKind_MEDIA_KIND_COVER
	default:
		return true
	}
}

// listScope is tt.crawl_cursors' scope type for an account's post list.
const listScope = "account_list"

// listWatermark returns the newest post time stored for an account's list.
func (d *Deps) listWatermark(ctx context.Context, userID int64) (time.Time, bool, error) {
	if d.DB == nil || userID == 0 {
		return time.Time{}, false, nil
	}
	var ts *time.Time
	err := d.DB.QueryRow(ctx, `SELECT newest_seen_at FROM crawl_cursors
		WHERE scope_type = $1 AND scope_id = $2 AND ordering = 'default'`, listScope, userID).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) || ts == nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return *ts, true, nil
}

// saveListWatermark records the end of a list walk: the newest and oldest
// post times seen, its state and why it stopped (ListStop*).
func (d *Deps) saveListWatermark(ctx context.Context, userID int64, newest, oldest time.Time, state, reason string, full bool) error {
	if d.DB == nil || userID == 0 {
		return nil
	}
	var nw, ol *time.Time
	if !newest.IsZero() {
		nw = &newest
	}
	if !oldest.IsZero() {
		ol = &oldest
	}
	_, err := d.DB.Exec(ctx, `
		INSERT INTO crawl_cursors (scope_type, scope_id, ordering, newest_seen_at, oldest_seen_at, state,
			truncated_reason, last_full_at, last_incremental_at, updated_at)
		VALUES ($1, $2, 'default', $3, $4, $5, NULLIF($6, ''),
			CASE WHEN $7 THEN now() END, CASE WHEN NOT $7 THEN now() END, now())
		ON CONFLICT (scope_type, scope_id, ordering) DO UPDATE SET
			newest_seen_at = GREATEST(crawl_cursors.newest_seen_at, EXCLUDED.newest_seen_at),
			oldest_seen_at = LEAST(crawl_cursors.oldest_seen_at, EXCLUDED.oldest_seen_at),
			state = EXCLUDED.state, truncated_reason = EXCLUDED.truncated_reason,
			last_full_at = COALESCE(EXCLUDED.last_full_at, crawl_cursors.last_full_at),
			last_incremental_at = COALESCE(EXCLUDED.last_incremental_at, crawl_cursors.last_incremental_at),
			updated_at = now()`, listScope, userID, nw, ol, state, reason, full)
	return err
}
