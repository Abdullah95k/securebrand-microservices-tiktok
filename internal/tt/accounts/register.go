package accounts

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Ways an account is registered (account_sources.how): the owner's own,
// through the API or an import; config.DiscoverySources are discovery's.
const (
	HowAPI    = "api"
	HowImport = "import"
)

// Origins of a schedule (account_schedule.origin).
const (
	OriginOwner     = "owner"
	OriginDiscovery = "discovery"
)

// Admission priorities: the owner's accounts go first; discovered ones
// queue behind them by Iraq score (M10c: PriorityDiscovered plus the score
// in hundredths, below PriorityOwner).
const (
	PriorityOwner      = 1_000_000
	PriorityDiscovered = 0
)

// Outcomes of a registration.
const (
	OutcomeAdded   = "added"   // a new schedule
	OutcomeJoined  = "joined"  // the schedule existed (another client's): this client's registration added
	OutcomeAlready = "already" // this client was registered already
)

// ErrUnknownID: an id: reference names no stored account. TikTok's calls
// take the secUid and its pages the username, so a user id alone cannot be
// read (ADR 0007 §9).
var ErrUnknownID = errors.New("an id: reference needs an account tks has read; register it by username or secUid")

// Registration is one account to put on the schedule for a client.
type Registration struct {
	ClientID uuid.UUID
	Ref      string // normalised (task.NormalizeRef)
	// UserID is the account's TikTok user id when the caller knows it and
	// tks has not stored the account yet (discovery: the source named it),
	// so that the account joins a schedule registered under another
	// reference of it.
	UserID int64
	// How is HowAPI, HowImport or a discovery source; "" records no source
	// (discovery recorded its sources when it found the account).
	How      string
	Via      string // what led to it: an import batch, a keyword, an account ('' for the API)
	Origin   string // OriginOwner or OriginDiscovery
	Priority int
	// Interval is the time between checks; DefaultInterval
	// (tt.freshness.list_interval) unless set, and an explicit interval
	// only shortens an existing schedule's.
	Interval        *time.Duration
	DefaultInterval time.Duration
	// Enabled false registers the account without checking it (a schedule
	// that exists stays enabled if it was).
	Enabled bool
	// Options are the API's crawl options (JSON) for a new schedule; the
	// first registration's stay.
	Options    []byte
	MaxAgeDays *int
	MaxPosts   *int64
	Labels     []string
	Evidence   json.RawMessage
	Score      *float64
	Now        time.Time
	// DryRun works the outcome out and writes nothing.
	DryRun bool
}

// Result is what a registration did (or, dry, would do).
type Result struct {
	ScheduleRef string
	UserID      *int64
	Outcome     string
	// Queued: the schedule waits for paced admission before its first read.
	Queued bool
}

// DB is what ScheduleRef needs of a pool or a transaction.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ScheduleRef returns the schedule row an account is registered under: the
// reference itself, or, for an account tks has read, any row of its user
// id (an account registered by username and asked for by secUid or id is
// one schedule).
func ScheduleRef(ctx context.Context, db DB, ref string, userID int64) (string, bool, error) {
	var got string
	err := db.QueryRow(ctx, `SELECT account_ref FROM account_schedule WHERE account_ref = $1
		OR ($2::bigint IS NOT NULL AND user_id = $2) ORDER BY account_ref = $1 DESC, created_at LIMIT 1`, ref, nilIfZero(userID)).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return ref, false, nil
	}
	return got, err == nil, err
}

func nilIfZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

// Register puts an account on the schedule for a client, in one
// transaction (RegisterTx), committed unless DryRun.
func Register(ctx context.Context, db *pgxpool.Pool, r Registration) (Result, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := RegisterTx(ctx, tx, r)
	if err != nil || r.DryRun {
		return res, err // rolled back
	}
	return res, tx.Commit(ctx)
}

// RegisterTx puts an account on the schedule for a client within tx: the
// schedule (shared by every client registered to the account; a new one
// waits for admission unless tks has read the account's list already), the
// client's registration and labels, the source, and the end of the
// client's own block. Registering again is safe: the schedule is kept
// (enabled if it was not, an explicit interval only shortens it, an
// owner's registration makes a discovered account the owner's) and no
// second backfill starts.
func RegisterTx(ctx context.Context, tx pgx.Tx, r Registration) (Result, error) {
	if r.Now.IsZero() {
		r.Now = time.Now()
	}
	acc, known, err := dispatch.Resolve(ctx, tx, r.Ref)
	if err != nil {
		return Result{}, err
	}
	uid := r.UserID
	if known {
		uid = acc.UserID
	}
	if !known && task.ScopeFromRef(r.Ref).UserId != 0 {
		return Result{}, ErrUnknownID
	}
	interval, explicit := int(r.DefaultInterval.Seconds()), false
	if r.Interval != nil {
		interval, explicit = int(r.Interval.Seconds()), true
	}
	if len(r.Options) == 0 {
		r.Options = []byte("{}")
	}
	// An account whose list tks has read needs no backfill: its first job is
	// a check, so it is admitted at once (not paced: admission's hourly
	// count leaves it out).
	var admitted *time.Time
	if known && acc.ListCheckedAt != nil {
		admitted = &r.Now
	}
	res := Result{UserID: nilIfZero(uid)}
	ref, existed, err := ScheduleRef(ctx, tx, r.Ref, uid)
	if err != nil {
		return res, err
	}
	res.ScheduleRef = ref
	var mine bool
	if existed {
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM account_registrations WHERE api_client_id = $1 AND account_ref = $2)`,
			r.ClientID, ref).Scan(&mine); err != nil {
			return res, err
		}
	}
	switch {
	case !existed:
		res.Outcome = OutcomeAdded
	case mine:
		res.Outcome = OutcomeAlready
	default:
		res.Outcome = OutcomeJoined
	}
	if err := tx.QueryRow(ctx, `INSERT INTO account_schedule AS s (account_ref, user_id, enabled, interval_seconds, next_run_at,
			options, max_age_days, max_posts, origin, admit_priority, admitted_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$5)
		ON CONFLICT (account_ref) DO UPDATE SET enabled = s.enabled OR EXCLUDED.enabled,
			interval_seconds = CASE WHEN $12 THEN least(s.interval_seconds, EXCLUDED.interval_seconds) ELSE s.interval_seconds END,
			user_id = coalesce(s.user_id, EXCLUDED.user_id),
			origin = CASE WHEN EXCLUDED.origin = 'owner' THEN 'owner' ELSE s.origin END,
			admit_priority = greatest(s.admit_priority, EXCLUDED.admit_priority)
		RETURNING s.admitted_at IS NULL`,
		ref, nilIfZero(uid), r.Enabled, interval, r.Now, r.Options, r.MaxAgeDays, r.MaxPosts, r.Origin, r.Priority, admitted,
		explicit).Scan(&res.Queued); err != nil {
		return res, err
	}
	b := &pgx.Batch{}
	b.Queue(`INSERT INTO account_registrations (api_client_id, account_ref) VALUES ($1,$2) ON CONFLICT DO NOTHING`, r.ClientID, ref)
	for _, l := range r.Labels {
		b.Queue(`INSERT INTO account_labels (api_client_id, account_ref, label) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, r.ClientID, ref, l)
	}
	if r.How != "" {
		queueSource(b, Source{AccountRef: ref, How: r.How, Via: r.Via, UserID: nilIfZero(uid), ClientID: &r.ClientID,
			At: r.Now, Evidence: r.Evidence, Score: r.Score})
	}
	b.Queue(`DELETE FROM account_blocks WHERE api_client_id = $1 AND (account_ref = $2 OR account_ref = $3
		OR ($4::bigint IS NOT NULL AND user_id = $4))`, r.ClientID, ref, r.Ref, nilIfZero(uid))
	return res, tx.SendBatch(ctx, b).Close()
}

// Source is one way an account was found or registered.
type Source struct {
	AccountRef string
	How        string
	Via        string
	UserID     *int64
	ClientID   *uuid.UUID
	At         time.Time
	Evidence   json.RawMessage
	Score      *float64
	// Seen is how many times this finding saw it (0 counts as 1): part B
	// counts a batch of sightings of one person at once.
	Seen int64
}

// AddSource records a source, or counts it again: the first time stays,
// the last time and the evidence move on.
func AddSource(ctx context.Context, db DB, s Source) error {
	sql, args := sourceSQL(s)
	_, err := db.Exec(ctx, sql, args...)
	return err
}

func queueSource(b *pgx.Batch, s Source) {
	sql, args := sourceSQL(s)
	b.Queue(sql, args...)
}

func sourceSQL(s Source) (string, []any) {
	var ev any
	if len(s.Evidence) > 0 {
		ev = s.Evidence
	}
	seen := max(s.Seen, 1)
	return `INSERT INTO account_sources AS x (account_ref, how, via, user_id, api_client_id, first_at, last_at, evidence, score, seen)
		VALUES ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9)
		ON CONFLICT (account_ref, how, via) DO UPDATE SET last_at = greatest(x.last_at, EXCLUDED.last_at), seen = x.seen + EXCLUDED.seen,
			user_id = coalesce(x.user_id, EXCLUDED.user_id), evidence = coalesce(EXCLUDED.evidence, x.evidence),
			score = coalesce(EXCLUDED.score, x.score)`,
		[]any{s.AccountRef, s.How, s.Via, s.UserID, s.ClientID, s.At, ev, s.Score, seen}
}

// Block records that a client removed an account, so discovery never adds
// it back for anyone (discovery-plan §5.5). The client's next registration
// of it lifts the block.
func Block(ctx context.Context, db DB, client uuid.UUID, ref string, userID int64, at time.Time) error {
	_, err := db.Exec(ctx, `INSERT INTO account_blocks (api_client_id, account_ref, user_id, created_at) VALUES ($1,$2,$3,$4)
		ON CONFLICT (api_client_id, account_ref) DO UPDATE SET user_id = coalesce(EXCLUDED.user_id, account_blocks.user_id)`,
		client, ref, nilIfZero(userID), at)
	return err
}

// Blocked says whether any client removed the account (by its reference
// or its user id).
func Blocked(ctx context.Context, db DB, ref string, userID int64) (bool, error) {
	var b bool
	err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM account_blocks WHERE account_ref = $1
		OR ($2::bigint IS NOT NULL AND user_id = $2))`, ref, nilIfZero(userID)).Scan(&b)
	return b, err
}
