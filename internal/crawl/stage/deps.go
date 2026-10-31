// Package stage implements the crawl stages (page timeline, post comments,
// reply threads) and the worker that runs them from Kafka.
package stage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/shadow"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Deps are the collaborators every stage uses.
type Deps struct {
	Cfg         *config.Config
	Pool        *session.Pool
	Proxies     *proxy.Pool
	Loader      *session.Loader
	GQL         *graphql.Client
	Emit        *emit.Emitter
	Producer    *kafka.Producer
	Jobs        *jobs.Tracker
	Ckpt        *checkpoint.Store
	DB          *pgxpool.Pool // crawl_cursors; nil disables watermarks
	Suppression *gdpr.Suppression
	// Shadow runs candidate parsers on a sample of responses; nil = none.
	Shadow *shadow.Set
	Now    func() time.Time
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
	Next     *taskv1.Task
	Delay    string // retry topic: "30s" | "5m" | "1h"
	Reason   string
	Counters map[string]int64
	// FollowUps are new streams to start once this task released its lease
	// (for example a full comment pass after an incremental one).
	FollowUps []*taskv1.Task
}

func done(c map[string]int64) Outcome { return Outcome{Result: Done, Counters: c} }

func retry(t *taskv1.Task, cursor, delay, reason string, c map[string]int64) Outcome {
	return Outcome{Result: Retry, Next: task.Retry(t, cursor), Delay: delay, Reason: reason, Counters: c}
}

// Stage runs one task kind.
type Stage interface {
	Run(ctx context.Context, t *taskv1.Task) Outcome
}

// identity acquires an identity for a query class, escalating tiers on
// retries (cheapest first) and skipping tiers whose exits are all cooling
// down behind their breakers.
func (d *Deps) identity(ctx context.Context, class string, attempt uint32) (*session.Identity, error) {
	tiers := d.Proxies.TiersFor(class)
	from := min(int(attempt), len(tiers)-1)
	tier := tiers[from]
	for _, t := range tiers[from:] {
		if d.Proxies.Available(t) {
			tier = t
			break
		}
	}
	return d.Pool.Acquire(ctx, tier)
}

// ensureBootstrapped loads a page on a fresh identity to obtain tokens.
func (d *Deps) ensureBootstrapped(ctx context.Context, id *session.Identity, pageID int64, lane taskv1.Lane) (*session.PageLoad, error) {
	if id.Bootstrapped() {
		return nil, nil
	}
	u, _ := id.Base.Parse("/" + strconv.FormatInt(pageID, 10))
	pl, err := d.Loader.Bootstrap(ctx, id, u.String())
	d.observeLoad(pl, err, lane)
	return pl, err
}

// observeLoad reports an HTML page load to the traffic budget and AIMD;
// page loads go through the proxies like GraphQL requests.
func (d *Deps) observeLoad(pl *session.PageLoad, err error, lane taskv1.Lane) {
	if d.GQL == nil || d.GQL.Outcomes == nil {
		return
	}
	switch {
	case pl != nil:
		d.GQL.Outcomes.Observe(pl.Class, topics.LaneName(lane), pl.Bytes)
	case err != nil:
		d.GQL.Outcomes.Observe(detect.Transport, topics.LaneName(lane), 0)
	}
}

// classOutcome maps a non-OK response class to an outcome for task t.
// cursor is the resume point to retry from.
func classOutcome(t *taskv1.Task, cls detect.Class, reason, cursor string, c map[string]int64) Outcome {
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
	case detect.RateLimited, detect.Transport, detect.Checkpoint, detect.LoginWall:
		return retry(t, cursor, "30s", cls.String()+": "+reason, c)
	case detect.SoftError:
		return retry(t, cursor, "30s", "soft: "+reason, c)
	case detect.RegistryStale, detect.SchemaDrift:
		// Needs a registry or parser update; wait longer.
		return retry(t, cursor, "5m", cls.String()+": "+reason, c)
	default:
		return retry(t, cursor, "30s", cls.String()+": "+reason, c)
	}
}

// spawn registers child tasks with the job and produces them.
func (d *Deps) spawn(ctx context.Context, children ...*taskv1.Task) error {
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

// media publishes media metadata and download tasks per the job's policy.
func (d *Deps) media(ctx context.Context, b *kafka.Batch, t *taskv1.Task, src emit.Source, ms []*fbv1.Media, comment bool) (int64, error) {
	lim := t.Limits
	include := lim.GetIncludeMedia()
	if comment {
		include = lim.GetIncludeCommentMedia()
	}
	var queued int64
	// Group video variants so the policy can pick one.
	hasHD := map[string]bool{}
	for _, m := range ms {
		if m.Variant == "hd" {
			hasHD[m.FbMediaId] = true
		}
	}
	for _, m := range ms {
		if err := d.Emit.Media(ctx, b, src, m); err != nil {
			return queued, err
		}
		if !include || !wanted(m, lim.GetVideoPolicy(), hasHD[m.FbMediaId]) {
			continue
		}
		mt := task.NewMedia(t.JobId, m)
		rec, err := d.Producer.Record(topics.TaskMedia, m.MediaId, mt)
		if err != nil {
			return queued, err
		}
		b.Add(ctx, rec)
		queued++
	}
	return queued, nil
}

func wanted(m *fbv1.Media, vp taskv1.VideoPolicy, hasHD bool) bool {
	switch m.Kind {
	case fbv1.MediaKind_MEDIA_KIND_VIDEO, fbv1.MediaKind_MEDIA_KIND_REEL, fbv1.MediaKind_MEDIA_KIND_COMMENT_VIDEO:
		switch vp {
		case taskv1.VideoPolicy_VIDEO_POLICY_HD:
			return m.Variant == "hd" || (m.Variant == "sd" && !hasHD)
		case taskv1.VideoPolicy_VIDEO_POLICY_SD, taskv1.VideoPolicy_VIDEO_POLICY_UNSPECIFIED:
			return m.Variant == "sd"
		default:
			return false
		}
	case fbv1.MediaKind_MEDIA_KIND_THUMBNAIL:
		return vp != taskv1.VideoPolicy_VIDEO_POLICY_NONE
	default:
		return true
	}
}

// pageWatermark returns the newest post time stored for a page.
func (d *Deps) pageWatermark(ctx context.Context, pageID int64) (time.Time, bool, error) {
	if d.DB == nil || pageID == 0 {
		return time.Time{}, false, nil
	}
	var ts *time.Time
	err := d.DB.QueryRow(ctx, `SELECT newest_seen_at FROM crawl_cursors
		WHERE scope_type = 'page_timeline' AND scope_id = $1 AND ordering = 'default'`, pageID).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) || ts == nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return *ts, true, nil
}

// savePageWatermark records the end of a timeline pass.
func (d *Deps) savePageWatermark(ctx context.Context, pageID int64, newest, oldest time.Time, state, reason string, full bool) error {
	if d.DB == nil || pageID == 0 {
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
		VALUES ('page_timeline', $1, 'default', $2, $3, $4, NULLIF($5, ''),
			CASE WHEN $6 THEN now() END, CASE WHEN NOT $6 THEN now() END, now())
		ON CONFLICT (scope_type, scope_id, ordering) DO UPDATE SET
			newest_seen_at = GREATEST(crawl_cursors.newest_seen_at, EXCLUDED.newest_seen_at),
			oldest_seen_at = LEAST(crawl_cursors.oldest_seen_at, EXCLUDED.oldest_seen_at),
			state = EXCLUDED.state, truncated_reason = EXCLUDED.truncated_reason,
			last_full_at = COALESCE(EXCLUDED.last_full_at, crawl_cursors.last_full_at),
			last_incremental_at = COALESCE(EXCLUDED.last_incremental_at, crawl_cursors.last_incremental_at),
			updated_at = now()`, pageID, nw, ol, state, reason, full)
	return err
}
