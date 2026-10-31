// Package dispatch creates jobs: it records the job row, registers the root
// task with job accounting and produces it. The API (on-demand jobs) and the
// scheduler (scheduled re-scrapes, backfills) both use it.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Job kinds.
const (
	KindOnDemand  = "on_demand"
	KindScheduled = "scheduled"
	KindBackfill  = "backfill"
)

// Request describes a job to create.
type Request struct {
	ClientID       *uuid.UUID
	Kind           string
	Lane           taskv1.Lane
	Mode           string // auto | full | incremental
	PageRef        string // normalised
	Limits         *taskv1.Limits
	IdempotencyKey string
	Params         map[string]any
	// Posts are extra root tasks: comment refreshes of older posts that ride
	// on the page's job.
	Posts []PostRoot
	// MaxRunningInLane caps jobs running in Lane fleet-wide (0 = no cap).
	MaxRunningInLane int
}

// PostRoot is a post task to start with a job.
type PostRoot struct {
	Mode  fbv1.CrawlMode
	Scope *taskv1.PostScope
}

// Job is a job row.
type Job struct {
	ID           uuid.UUID        `json:"job_id"`
	Kind         string           `json:"kind"`
	Lane         string           `json:"lane"`
	Mode         string           `json:"mode"`
	PageRef      string           `json:"page_ref"`
	PageID       *int64           `json:"page_id,omitempty"`
	Status       string           `json:"status"`
	Params       json.RawMessage  `json:"params,omitempty"`
	Counters     map[string]int64 `json:"counters,omitempty"`
	Completeness json.RawMessage  `json:"completeness,omitempty"`
	Error        json.RawMessage  `json:"error,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	FinishedAt   *time.Time       `json:"finished_at,omitempty"`
	Pending      *int64           `json:"pending_tasks,omitempty"`
}

// Dispatcher creates jobs.
type Dispatcher struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	BaseURL  string
}

// ErrTooManyJobs is returned when a client exceeds its concurrent job cap.
var ErrTooManyJobs = errors.New("dispatch: too many running jobs")

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ErrLaneFull is returned when the lane's fleet-wide job cap is reached.
var ErrLaneFull = errors.New("dispatch: the fleet is running its maximum of jobs in this lane")

// ResolvePageID returns the page id for a normalised ref if known.
func (d *Dispatcher) ResolvePageID(ctx context.Context, ref string) (int64, bool) {
	if strings.HasPrefix(ref, "id:") {
		id := task.RefID(ref)
		return id, id != 0
	}
	var id int64
	err := d.DB.QueryRow(ctx, `SELECT page_id FROM pages WHERE lower(vanity) = $1`, strings.TrimPrefix(ref, "v:")).Scan(&id)
	if err != nil {
		err = d.DB.QueryRow(ctx, `SELECT page_id FROM page_schedule WHERE page_ref = $1 AND page_id IS NOT NULL`, ref).Scan(&id)
	}
	return id, err == nil
}

// Create records and dispatches a job. With an idempotency key it returns
// the existing job instead of creating a duplicate (created = false).
func (d *Dispatcher) Create(ctx context.Context, r Request, maxRunning int) (*Job, bool, error) {
	if r.IdempotencyKey != "" && r.ClientID != nil {
		if j, err := d.byIdempotency(ctx, *r.ClientID, r.IdempotencyKey); err == nil {
			return j, false, nil
		}
	}
	clientCap := maxRunning > 0 && r.ClientID != nil
	pageID, known := d.ResolvePageID(ctx, r.PageRef)
	mode := r.Mode
	if mode == "" || mode == "auto" {
		mode = "full"
		if known {
			var exists bool
			_ = d.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM crawl_cursors WHERE scope_type = 'page_timeline' AND scope_id = $1)`,
				pageID).Scan(&exists)
			if exists {
				mode = "incremental"
			}
		}
	}
	cm := fbv1.CrawlMode_CRAWL_MODE_FULL
	if mode == "incremental" {
		cm = fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, false, err
	}
	params := r.Params
	if params == nil {
		params = map[string]any{}
	}
	params["limits"] = r.Limits
	pb, _ := json.Marshal(params)
	var pid *int64
	if known {
		pid = &pageID
	}
	var idem *string
	if r.IdempotencyKey != "" {
		idem = &r.IdempotencyKey
	}
	j := &Job{ID: id, Kind: r.Kind, Lane: topics.LaneName(r.Lane), Mode: mode, PageRef: r.PageRef, PageID: pid,
		Status: "running", Params: pb, CreatedAt: time.Now().UTC()}
	insert := func(db execer) error {
		_, err := db.Exec(ctx, `INSERT INTO jobs (job_id, api_client_id, kind, lane, mode, page_ref, page_id, params, status,
			idempotency_key, created_at, dispatched_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'running',$9,$10,$10)`,
			id, r.ClientID, r.Kind, int16(r.Lane), mode, r.PageRef, pid, pb, idem, j.CreatedAt)
		return err
	}
	if clientCap || r.MaxRunningInLane > 0 {
		// Count and insert under advisory locks (lane, then client), so
		// concurrent requests on any API replica cannot all pass the caps.
		err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
			if r.MaxRunningInLane > 0 {
				var running int
				if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
					"igs:lane:"+topics.LaneName(r.Lane)); err != nil {
					return err
				}
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE lane = $1 AND status IN ('running','finalizing')`,
					int16(r.Lane)).Scan(&running); err != nil {
					return err
				}
				if running >= r.MaxRunningInLane {
					return ErrLaneFull
				}
			}
			if clientCap {
				var running int
				if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
					"igs:client:"+r.ClientID.String()); err != nil {
					return err
				}
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE api_client_id = $1 AND status IN ('running','finalizing')`,
					*r.ClientID).Scan(&running); err != nil {
					return err
				}
				if running >= maxRunning {
					return ErrTooManyJobs
				}
			}
			return insert(tx)
		})
		if errors.Is(err, ErrLaneFull) || errors.Is(err, ErrTooManyJobs) {
			return nil, false, err
		}
	} else {
		err = insert(d.DB)
	}
	if err != nil {
		if r.IdempotencyKey != "" && r.ClientID != nil {
			if existing, e2 := d.byIdempotency(ctx, *r.ClientID, r.IdempotencyKey); e2 == nil {
				return existing, false, nil
			}
		}
		return nil, false, fmt.Errorf("dispatch: insert job: %w", err)
	}
	root := task.NewPage(id.String(), r.Lane, cm, r.PageRef, pageID, "", r.Limits)
	roots := []*taskv1.Task{root}
	for _, p := range r.Posts {
		pt := task.NewPost(root, p.Mode, p.Scope)
		roots = append(roots, pt)
		// Coverage of refreshed posts is reported with the job's.
		if _, err := d.Jobs.SetPost(ctx, id.String(), p.Scope.PostId, p.Scope.PostCreatedAt.AsTime().Unix(),
			p.Scope.ReportedCommentCount, jobs.PostPending); err != nil {
			d.fail(ctx, id, err)
			return nil, false, err
		}
	}
	ids := make([]string, len(roots))
	recs := make([]*kgo.Record, len(roots))
	for i, t := range roots {
		ids[i] = t.TaskId
		rec, err := d.Producer.Record(topics.Task(t.Kind, t.Lane), task.ScopeKey(t), t)
		if err != nil {
			d.fail(ctx, id, err)
			return nil, false, err
		}
		recs[i] = rec
	}
	if err := d.Jobs.Spawn(ctx, id.String(), ids...); err != nil {
		d.fail(ctx, id, err)
		return nil, false, err
	}
	if err := d.Producer.ProduceSync(ctx, recs...); err != nil {
		d.fail(ctx, id, err)
		return nil, false, fmt.Errorf("dispatch: produce: %w", err)
	}
	return j, true, nil
}

func (d *Dispatcher) fail(ctx context.Context, id uuid.UUID, cause error) {
	b, _ := json.Marshal(map[string]string{"message": cause.Error()})
	_, _ = d.DB.Exec(context.WithoutCancel(ctx), `UPDATE jobs SET status = 'failed', error = $2, finished_at = now() WHERE job_id = $1`, id, b)
}

func (d *Dispatcher) byIdempotency(ctx context.Context, client uuid.UUID, key string) (*Job, error) {
	rows, err := d.DB.Query(ctx, selectJob+` WHERE api_client_id = $1 AND idempotency_key = $2`, client, key)
	if err != nil {
		return nil, err
	}
	return pgx.CollectExactlyOneRow(rows, scanJob)
}

const selectJob = `SELECT job_id, kind, lane, mode, page_ref, page_id, status, params, counters, completeness, error,
	created_at, finished_at FROM jobs`

func scanJob(row pgx.CollectableRow) (*Job, error) {
	var j Job
	var lane int16
	var counters []byte
	err := row.Scan(&j.ID, &j.Kind, &lane, &j.Mode, &j.PageRef, &j.PageID, &j.Status, &j.Params, &counters,
		&j.Completeness, &j.Error, &j.CreatedAt, &j.FinishedAt)
	if err != nil {
		return nil, err
	}
	j.Lane = topics.LaneName(taskv1.Lane(lane))
	if len(counters) > 0 {
		_ = json.Unmarshal(counters, &j.Counters)
	}
	return &j, nil
}

// Get loads a job, merging live counters from Redis while it runs.
func (d *Dispatcher) Get(ctx context.Context, id uuid.UUID) (*Job, error) {
	rows, err := d.DB.Query(ctx, selectJob+` WHERE job_id = $1`, id)
	if err != nil {
		return nil, err
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanJob)
	if err != nil {
		return nil, err
	}
	if j.Status == "running" || j.Status == "finalizing" || j.Status == "cancelling" {
		if st, err := d.Jobs.Status(ctx, id.String()); err == nil {
			j.Counters = st.Counters
			p := st.Pending
			j.Pending = &p
		}
	}
	return j, nil
}

// List returns recent jobs, newest first.
func (d *Dispatcher) List(ctx context.Context, client *uuid.UUID, pageRef, status string, before time.Time, limit int) ([]*Job, error) {
	q := selectJob + ` WHERE created_at < $1`
	args := []any{before}
	if client != nil {
		args = append(args, *client)
		q += fmt.Sprintf(" AND api_client_id = $%d", len(args))
	}
	if pageRef != "" {
		args = append(args, pageRef)
		q += fmt.Sprintf(" AND page_ref = $%d", len(args))
	}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	rows, err := d.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanJob)
}

// Cancel flags a running job.
func (d *Dispatcher) Cancel(ctx context.Context, id uuid.UUID) error {
	tag, err := d.DB.Exec(ctx, `UPDATE jobs SET status = 'cancelling' WHERE job_id = $1 AND status IN ('running','queued')`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return d.Jobs.Cancel(ctx, id.String())
}
