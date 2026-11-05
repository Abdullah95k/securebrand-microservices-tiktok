package accounts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
)

// ImportOptions configure an import.
type ImportOptions struct {
	ClientID uuid.UUID
	// Labels go on every account of the import, with each CSV line's own.
	Labels []string
	DryRun bool
	// DisableMissing removes the client's registrations of accounts not in
	// the file (their data stays); it refuses to remove more than
	// MaxDisableShare of the client's list without Force.
	DisableMissing bool
	Force          bool
	// Budget is the budget controller's state, for the estimated finish
	// (nil: not known).
	Budget *ratelimit.BudgetState
	Now    time.Time
}

// MaxDisableShare is the share of a client's list --disable-missing removes
// without --force.
const MaxDisableShare = 0.10

// Line outcomes beyond a registration's.
const (
	OutcomeDuplicate = "duplicate"
	OutcomeRejected  = "rejected"
)

// Report is what an import did (or, dry, would do).
type Report struct {
	Batch   uuid.UUID `json:"batch"`
	DryRun  bool      `json:"dry_run"`
	Lines   int       `json:"lines"`
	Added   int       `json:"added"`
	Joined  int       `json:"joined"`
	Already int       `json:"already_monitored"`
	// Duplicates are lines naming an account an earlier line named.
	Duplicates int        `json:"duplicates"`
	Rejected   []Rejected `json:"not_accepted"`
	// Queued: accounts of this import waiting for paced admission; Queue
	// is the whole queue with its estimated finish.
	Queued int   `json:"queued"`
	Queue  Queue `json:"queue"`
	// Disabled: the client's registrations --disable-missing removed (or,
	// dry, would remove).
	Disabled *Disabled `json:"disable_missing,omitempty"`
}

// Rejected is a line not accepted.
type Rejected struct {
	Line   int    `json:"line"`
	Input  string `json:"input"`
	Reason string `json:"reason"`
}

// Disabled reports --disable-missing.
type Disabled struct {
	Registered int      `json:"registered"`
	Missing    int      `json:"missing"`
	Removed    int      `json:"removed"`
	Refs       []string `json:"accounts,omitempty"`
	Refused    string   `json:"refused,omitempty"`
}

// ErrTooManyToDisable: --disable-missing would remove more than
// MaxDisableShare of the client's list without --force.
var ErrTooManyToDisable = errors.New("--disable-missing would remove more than 10 % of your list: check the dry run, then add --force")

// ImportChunk is how many lines one transaction of an import registers.
const ImportChunk = 500

// Import registers an import's accounts for a client, ImportChunk lines a
// transaction, so an import stopped halfway can simply run again: lines
// already done come back "already monitored". It records the batch and
// every line (a dry run's too) for its report.
func Import(ctx context.Context, db *pgxpool.Pool, cfg *config.Config, lines []Line, o ImportOptions) (*Report, error) {
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	for _, l := range o.Labels {
		if err := CheckLabel(l); err != nil {
			return nil, err
		}
	}
	rep := &Report{Batch: uuid.New(), DryRun: o.DryRun, Lines: len(lines), Rejected: []Rejected{}}
	if _, err := db.Exec(ctx, `INSERT INTO import_batches (id, api_client_id, labels, dry_run, created_at) VALUES ($1,$2,$3,$4,$5)`,
		rep.Batch, o.ClientID, append([]string{}, o.Labels...), o.DryRun, o.Now); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	inFile := map[string]bool{}
	for i := 0; i < len(lines); i += ImportChunk {
		if err := importChunk(ctx, db, cfg, lines[i:min(i+ImportChunk, len(lines))], o, rep, seen, inFile); err != nil {
			return rep, err
		}
	}
	if o.DisableMissing {
		d, err := disableMissing(ctx, db, o, inFile)
		rep.Disabled = d
		if err != nil {
			return rep, err
		}
	}
	q, err := QueueStatus(ctx, db, cfg, o.Budget, o.Now)
	if err != nil {
		return rep, err
	}
	rep.Queue = q
	counts, _ := json.Marshal(map[string]int{"lines": rep.Lines, "added": rep.Added, "joined": rep.Joined, "already": rep.Already,
		"duplicates": rep.Duplicates, "rejected": len(rep.Rejected), "queued": rep.Queued})
	_, err = db.Exec(ctx, `UPDATE import_batches SET counts = $2, finished_at = $3 WHERE id = $1`, rep.Batch, counts, time.Now())
	return rep, err
}

// importChunk registers lines in one transaction (rolled back when dry,
// except the lines' records).
func importChunk(ctx context.Context, db *pgxpool.Pool, cfg *config.Config, lines []Line, o ImportOptions, rep *Report,
	seen, inFile map[string]bool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	type rec struct {
		n              int
		input, outcome string
		ref, reason    *string
	}
	var recs []rec
	for _, ln := range lines {
		outcome, reason, ref := "", ln.Reason, ln.Ref
		switch {
		case ln.Ref == "":
			outcome = OutcomeRejected
		case seen[ln.Ref]:
			outcome = OutcomeDuplicate
		default:
			seen[ln.Ref] = true
			var iv *time.Duration
			if ln.IntervalMins != nil {
				d := time.Duration(*ln.IntervalMins) * time.Minute
				iv = &d
			}
			opts := map[string]any{}
			if ln.MaxAgeDays != nil {
				opts["max_age_days"] = *ln.MaxAgeDays
			}
			if ln.IncludeMedia != nil {
				opts["include_media"] = *ln.IncludeMedia
			}
			ob, _ := json.Marshal(opts)
			res, err := RegisterTx(ctx, tx, Registration{ClientID: o.ClientID, Ref: ln.Ref, How: HowImport, Via: rep.Batch.String(),
				Origin: OriginOwner, Priority: PriorityOwner, Interval: iv, DefaultInterval: cfg.TT.Freshness.ListInterval,
				Enabled: true, Options: ob, MaxAgeDays: ln.MaxAgeDays, Labels: append(append([]string{}, o.Labels...), ln.Labels...),
				Now: o.Now})
			switch {
			case errors.Is(err, ErrUnknownID):
				outcome, reason = OutcomeRejected, err.Error()
			case err != nil:
				return fmt.Errorf("line %d: %w", ln.N, err)
			default:
				outcome, ref = res.Outcome, res.ScheduleRef
				inFile[ref] = true
				if res.Queued {
					rep.Queued++
				}
			}
		}
		switch outcome {
		case OutcomeAdded:
			rep.Added++
		case OutcomeJoined:
			rep.Joined++
		case OutcomeAlready:
			rep.Already++
		case OutcomeDuplicate:
			rep.Duplicates++
		case OutcomeRejected:
			rep.Rejected = append(rep.Rejected, Rejected{Line: ln.N, Input: ln.Input, Reason: reason})
		}
		r := rec{n: ln.N, input: ln.Input, outcome: outcome}
		if ref != "" {
			r.ref = &ref
		}
		if reason != "" {
			r.reason = &reason
		}
		recs = append(recs, r)
	}
	if o.DryRun {
		// Nothing registered; the lines are still recorded for the report.
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if tx, err = db.Begin(ctx); err != nil {
			return err
		}
	}
	b := &pgx.Batch{}
	for _, r := range recs {
		b.Queue(`INSERT INTO import_lines (batch_id, line, input, account_ref, outcome, reason) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT DO NOTHING`, rep.Batch, r.n, r.input, r.ref, r.outcome, r.reason)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// disableMissing removes the client's registrations of accounts the file
// did not name; a schedule no client is registered to any more is
// disabled. The accounts' data stays (ADR 0007 §14).
func disableMissing(ctx context.Context, db *pgxpool.Pool, o ImportOptions, inFile map[string]bool) (*Disabled, error) {
	rows, err := db.Query(ctx, `SELECT account_ref FROM account_registrations WHERE api_client_id = $1 ORDER BY account_ref`, o.ClientID)
	if err != nil {
		return nil, err
	}
	all, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	d := &Disabled{Registered: len(all), Refs: []string{}}
	for _, r := range all {
		if !inFile[r] {
			d.Refs = append(d.Refs, r)
		}
	}
	d.Missing = len(d.Refs)
	if d.Missing > 0 && float64(d.Missing) > MaxDisableShare*float64(d.Registered) && !o.Force {
		d.Refused = ErrTooManyToDisable.Error()
		if o.DryRun {
			return d, nil
		}
		return d, ErrTooManyToDisable
	}
	if o.DryRun || d.Missing == 0 {
		return d, nil
	}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM account_registrations WHERE api_client_id = $1 AND account_ref = ANY($2)`, o.ClientID, d.Refs)
		if err != nil {
			return err
		}
		d.Removed = int(tag.RowsAffected())
		if _, err := tx.Exec(ctx, `DELETE FROM account_labels WHERE api_client_id = $1 AND account_ref = ANY($2)`, o.ClientID, d.Refs); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE account_schedule s SET enabled = false WHERE s.account_ref = ANY($1)
			AND NOT EXISTS (SELECT 1 FROM account_registrations g WHERE g.account_ref = s.account_ref)`, d.Refs)
		return err
	})
	return d, err
}

// BatchAccount is an account of an earlier import and what tks has found
// since.
type BatchAccount struct {
	Line       int        `json:"line"`
	Input      string     `json:"input"`
	AccountRef string     `json:"account_ref"`
	Outcome    string     `json:"outcome"`
	Status     string     `json:"status"` // queued | unread | active | private | unavailable | banned | inactive
	LastPostAt *time.Time `json:"last_post_at,omitempty"`
}

// InactiveAfter: an active account with no post for this long is reported
// inactive (the discovery plan's 3 months).
const InactiveAfter = 90 * 24 * time.Hour

// BatchReport lists an import's accounts, with what checks found since:
// waiting for admission, not read yet, active, private, gone, or with no
// post for 90 days. onlyProblems keeps the accounts that turned out
// private, inactive or missing.
func BatchReport(ctx context.Context, db *pgxpool.Pool, client uuid.UUID, admin bool, batch uuid.UUID, onlyProblems bool, now time.Time) ([]BatchAccount, error) {
	var owner uuid.UUID
	if err := db.QueryRow(ctx, `SELECT api_client_id FROM import_batches WHERE id = $1`, batch).Scan(&owner); err != nil {
		return nil, err
	}
	if owner != client && !admin {
		return nil, pgx.ErrNoRows
	}
	rows, err := db.Query(ctx, `SELECT l.line, l.input, l.account_ref, l.outcome, s.admitted_at IS NULL AND s.enabled,
			a.status, (SELECT max(updated_at) FROM crawl_cursors c WHERE c.scope_type = 'account_list' AND c.scope_id = a.user_id),
			coalesce(s.last_post_at, (SELECT max(created_at) FROM posts p WHERE p.user_id = a.user_id))
		FROM import_lines l
		LEFT JOIN account_schedule s ON s.account_ref = l.account_ref
		LEFT JOIN accounts a ON a.user_id = s.user_id
		WHERE l.batch_id = $1 AND l.outcome IN ('added', 'joined', 'already') ORDER BY l.line`, batch)
	if err != nil {
		return nil, err
	}
	out := []BatchAccount{}
	for rows.Next() {
		var b BatchAccount
		var queued *bool
		var status *string
		var read *time.Time
		if err := rows.Scan(&b.Line, &b.Input, &b.AccountRef, &b.Outcome, &queued, &status, &read, &b.LastPostAt); err != nil {
			rows.Close()
			return nil, err
		}
		switch {
		case queued != nil && *queued:
			b.Status = "queued"
		case status == nil:
			b.Status = "unread"
		case *status == "active" && read != nil && (b.LastPostAt == nil || now.Sub(*b.LastPostAt) > InactiveAfter):
			b.Status = "inactive"
		default:
			b.Status = *status
		}
		if onlyProblems && (b.Status == "queued" || b.Status == "unread" || b.Status == "active") {
			continue
		}
		out = append(out, b)
	}
	rows.Close()
	return out, rows.Err()
}
