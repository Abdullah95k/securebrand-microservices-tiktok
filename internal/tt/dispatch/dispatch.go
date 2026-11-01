// Package dispatch creates tks's jobs: it records the job row in schema tt,
// registers the root account task with job accounting and produces it on
// tks.task.account.<lane>. The scheduler (scheduled checks, metrics walks)
// and the API (on-demand jobs, M6b) both use it. It is fbs's
// internal/dispatch on tks's envelope and tables (docs/tiktok ADR 0007 §12).
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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Job kinds.
const (
	KindOnDemand  = "on_demand"
	KindScheduled = "scheduled"
	KindBackfill  = "backfill"
)

// Modes a job may ask for. auto reads a never-checked account in full (the
// backfill) and a checked one incrementally; refresh is a metrics walk to
// Limits.MaxAgeSeconds.
const (
	ModeAuto        = "auto"
	ModeFull        = "full"
	ModeIncremental = "incremental"
	ModeRefresh     = "refresh"
)

// Request describes a job to create.
type Request struct {
	ClientID       *uuid.UUID
	Kind           string
	Lane           tkstaskv1.Lane
	Mode           string
	AccountRef     string // normalised (task.NormalizeRef)
	Limits         *tkstaskv1.Limits
	IdempotencyKey string
	Params         map[string]any
	// MaxRunningInLane caps jobs running in Lane fleet-wide (0 = no cap).
	MaxRunningInLane int
}

// Job is a job row.
type Job struct {
	ID           uuid.UUID        `json:"job_id"`
	Kind         string           `json:"kind"`
	Lane         string           `json:"lane"`
	Mode         string           `json:"mode"`
	AccountRef   string           `json:"account_ref"`
	UserID       *int64           `json:"user_id,omitempty"`
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
}

// Errors.
var (
	// ErrTooManyJobs: the client runs its maximum of concurrent jobs.
	ErrTooManyJobs = errors.New("dispatch: too many running jobs")
	// ErrLaneFull: the fleet runs its maximum of jobs in the lane.
	ErrLaneFull = errors.New("dispatch: the fleet is running its maximum of jobs in this lane")
	// ErrUnknownAccount: an id: reference names no stored account. TikTok's
	// calls take the secUid and its pages the username, so a user id alone
	// cannot be read (ADR 0007 §9).
	ErrUnknownAccount = errors.New("dispatch: unknown account (an id: reference needs a stored account)")
)

// Account is what the store knows of an account, for a task's scope.
type Account struct {
	UserID        int64
	SecUID        string
	Username      string
	Status        string
	RefreshedAt   *time.Time // profile_refreshed_at
	PageReadAt    *time.Time // page_read_at
	ListCheckedAt *time.Time // the post list's cursor, when it was ever read
}

// Resolve returns the stored account behind a normalised reference: by user
// id, secUid, or the username (current or former: account_usernames, the
// latest holder first), and last the schedule's user id for the reference.
func Resolve(ctx context.Context, db *pgxpool.Pool, ref string) (*Account, bool, error) {
	sc := task.ScopeFromRef(ref)
	const cols = `SELECT a.user_id, a.sec_uid, coalesce(a.username, ''), a.status, a.profile_refreshed_at, a.page_read_at,
		(SELECT max(updated_at) FROM crawl_cursors c WHERE c.scope_type = 'account_list' AND c.scope_id = a.user_id)
		FROM accounts a`
	var row pgx.Row
	switch {
	case sc.UserId != 0:
		row = db.QueryRow(ctx, cols+` WHERE a.user_id = $1`, sc.UserId)
	case sc.SecUid != "":
		row = db.QueryRow(ctx, cols+` WHERE a.sec_uid = $1`, sc.SecUid)
	default:
		row = db.QueryRow(ctx, cols+` JOIN account_usernames u USING (user_id) WHERE lower(u.username) = lower($1)
			ORDER BY u.last_seen_at DESC LIMIT 1`, sc.Username)
	}
	var a Account
	err := row.Scan(&a.UserID, &a.SecUID, &a.Username, &a.Status, &a.RefreshedAt, &a.PageReadAt, &a.ListCheckedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = db.QueryRow(ctx, cols+` JOIN account_schedule s ON s.user_id = a.user_id WHERE s.account_ref = $1`, ref).
			Scan(&a.UserID, &a.SecUID, &a.Username, &a.Status, &a.RefreshedAt, &a.PageReadAt, &a.ListCheckedAt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("dispatch: resolve %s: %w", ref, err)
	}
	return &a, true, nil
}

// Scope builds the root task's scope: the reference, and what the store
// knows (the user id and secUid, so a renamed account is still found; the
// current username; when the account and its page were last read, so the
// stage reads the page only when it is due). An account that is not active
// gets no read times, so its check reads the page again.
func Scope(ref string, a *Account) *tkstaskv1.AccountScope {
	sc := task.ScopeFromRef(ref)
	if a == nil {
		return sc
	}
	sc.UserId, sc.SecUid = a.UserID, a.SecUID
	if a.Username != "" {
		sc.Username = a.Username
	}
	if a.Status == "active" {
		sc.AccountSeenAt, sc.PageSeenAt = ts(a.RefreshedAt), ts(a.PageReadAt)
	}
	return sc
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// CrawlMode maps a job mode to the root task's: auto reads a never-checked
// account in full (the account stage also turns a check of a never-checked
// account into a backfill).
func CrawlMode(mode string, a *Account) (string, commonv1.CrawlMode) {
	switch mode {
	case ModeFull:
		return ModeFull, commonv1.CrawlMode_CRAWL_MODE_FULL
	case ModeIncremental:
		return ModeIncremental, commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	case ModeRefresh:
		return ModeRefresh, commonv1.CrawlMode_CRAWL_MODE_REFRESH
	}
	if a != nil && a.ListCheckedAt != nil {
		return ModeIncremental, commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	}
	return ModeFull, commonv1.CrawlMode_CRAWL_MODE_FULL
}

// Create records and dispatches a job. With an idempotency key it returns
// the existing job instead of creating a duplicate (created = false).
func (d *Dispatcher) Create(ctx context.Context, r Request, maxRunning int) (*Job, bool, error) {
	if r.IdempotencyKey != "" && r.ClientID != nil {
		if j, err := d.byIdempotency(ctx, *r.ClientID, r.IdempotencyKey); err == nil {
			return j, false, nil
		}
	}
	acc, known, err := Resolve(ctx, d.DB, r.AccountRef)
	if err != nil {
		return nil, false, err
	}
	if !known && strings.HasPrefix(r.AccountRef, "id:") {
		return nil, false, ErrUnknownAccount
	}
	mode, cm := CrawlMode(r.Mode, acc)
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
	var uid *int64
	if known {
		uid = &acc.UserID
	}
	var idem *string
	if r.IdempotencyKey != "" {
		idem = &r.IdempotencyKey
	}
	j := &Job{ID: id, Kind: r.Kind, Lane: topics.LaneName(r.Lane), Mode: mode, AccountRef: r.AccountRef, UserID: uid,
		Status: "running", Params: pb, CreatedAt: time.Now().UTC()}
	insert := func(db execer) error {
		_, err := db.Exec(ctx, `INSERT INTO jobs (job_id, api_client_id, kind, lane, mode, account_ref, user_id, params, status,
			idempotency_key, created_at, dispatched_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'running',$9,$10,$10)`,
			id, r.ClientID, r.Kind, int16(r.Lane), mode, r.AccountRef, uid, pb, idem, j.CreatedAt)
		return err
	}
	clientCap := maxRunning > 0 && r.ClientID != nil
	if clientCap || r.MaxRunningInLane > 0 {
		// Count and insert under advisory locks (lane, then client), so
		// concurrent requests on any API replica cannot all pass the caps.
		err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
			if r.MaxRunningInLane > 0 {
				if err := capped(ctx, tx, "tks:lane:"+topics.LaneName(r.Lane),
					`SELECT count(*) FROM jobs WHERE lane = $1 AND status IN ('running','finalizing')`, int16(r.Lane),
					r.MaxRunningInLane, ErrLaneFull); err != nil {
					return err
				}
			}
			if clientCap {
				if err := capped(ctx, tx, "tks:client:"+r.ClientID.String(),
					`SELECT count(*) FROM jobs WHERE api_client_id = $1 AND status IN ('running','finalizing')`, *r.ClientID,
					maxRunning, ErrTooManyJobs); err != nil {
					return err
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
	root := task.NewAccount(id.String(), r.Lane, cm, Scope(r.AccountRef, acc), r.Limits)
	rec, err := d.Producer.Record(topics.Task(root.Kind, root.Lane), task.ScopeKey(root), root)
	if err != nil {
		d.fail(ctx, id, err)
		return nil, false, err
	}
	if err := d.Jobs.Spawn(ctx, id.String(), root.TaskId); err != nil {
		d.fail(ctx, id, err)
		return nil, false, err
	}
	if err := d.Producer.ProduceSync(ctx, rec); err != nil {
		d.fail(ctx, id, err)
		return nil, false, fmt.Errorf("dispatch: produce: %w", err)
	}
	return j, true, nil
}

// capped takes the advisory lock key and fails with full when count's
// result reaches limit.
func capped(ctx context.Context, tx pgx.Tx, key, count string, arg any, limit int, full error) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return err
	}
	var running int
	if err := tx.QueryRow(ctx, count, arg).Scan(&running); err != nil {
		return err
	}
	if running >= limit {
		return full
	}
	return nil
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

const selectJob = `SELECT job_id, kind, lane, mode, account_ref, user_id, status, params, counters, completeness, error,
	created_at, finished_at FROM jobs`

func scanJob(row pgx.CollectableRow) (*Job, error) {
	var j Job
	var lane int16
	var counters []byte
	err := row.Scan(&j.ID, &j.Kind, &lane, &j.Mode, &j.AccountRef, &j.UserID, &j.Status, &j.Params, &counters,
		&j.Completeness, &j.Error, &j.CreatedAt, &j.FinishedAt)
	if err != nil {
		return nil, err
	}
	j.Lane = topics.LaneName(tkstaskv1.Lane(lane))
	if len(counters) > 0 {
		_ = json.Unmarshal(counters, &j.Counters)
	}
	return &j, nil
}

// Get loads a job, merging live counters from Redis while it runs.
func (d *Dispatcher) Get(ctx context.Context, id uuid.UUID) (*Job, error) {
	return d.get(ctx, selectJob+` WHERE job_id = $1`, id)
}

// Access is what a client may do with a job.
type Access int

const (
	// Read: the client made the job, or it is a scheduled job of an account
	// the client registered.
	Read Access = iota
	// Write: the client made the job. A scheduled job is shared by every
	// client registered to its account, so none of them alone may cancel it.
	Write
)

// GetFor returns job id if client may access it as asked (pgx.ErrNoRows
// otherwise, as for a missing job, so a client cannot tell another's job
// from none); a nil client is an admin.
func (d *Dispatcher) GetFor(ctx context.Context, id uuid.UUID, client *uuid.UUID, a Access) (*Job, error) {
	switch {
	case client == nil:
		return d.Get(ctx, id)
	case a == Write:
		return d.get(ctx, selectJob+` WHERE job_id = $1 AND api_client_id = $2`, id, *client)
	default:
		return d.get(ctx, selectJob+` WHERE job_id = $1 AND `+visibleTo(2), id, *client)
	}
}

func (d *Dispatcher) get(ctx context.Context, q string, args ...any) (*Job, error) {
	rows, err := d.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanJob)
	if err != nil {
		return nil, err
	}
	if j.Status == "running" || j.Status == "finalizing" || j.Status == "cancelling" {
		if st, err := d.Jobs.Status(ctx, j.ID.String()); err == nil {
			j.Counters = st.Counters
			p := st.Pending
			j.Pending = &p
		}
	}
	return j, nil
}

// List returns recent jobs, newest first.
func (d *Dispatcher) List(ctx context.Context, client *uuid.UUID, accountRef, status string, before time.Time, limit int) ([]*Job, error) {
	q := selectJob + ` WHERE created_at < $1`
	args := []any{before}
	if client != nil {
		args = append(args, *client)
		q += " AND " + visibleTo(len(args))
	}
	if accountRef != "" {
		args = append(args, accountRef)
		q += fmt.Sprintf(" AND account_ref = $%d", len(args))
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

// visibleTo is the condition on a jobs row that the client in parameter n
// may see it: the client made the job, or it is a scheduled job of an
// account the client registered.
func visibleTo(n int) string {
	return fmt.Sprintf(`(api_client_id = $%[1]d OR (api_client_id IS NULL AND EXISTS (
		SELECT 1 FROM account_registrations r JOIN account_schedule s ON s.account_ref = r.account_ref
		WHERE r.api_client_id = $%[1]d AND (s.account_ref = jobs.account_ref OR s.user_id = jobs.user_id))))`, n)
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
