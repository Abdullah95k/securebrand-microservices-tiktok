package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Candidate states (discovery_candidates.state; docs/tiktok/discovery-plan.md
// §4, ADR 0007 §19).
const (
	StatePending   = "pending"   // waits for its entry check
	StateChecking  = "checking"  // its entry check runs
	StateQualified = "qualified" // passed; waits to be registered (the day's cap, if any)
	StateAdded     = "added"     // registered for monitoring
	StateNotIraqi  = "not_iraqi" // public and active, the score does not pass (yet)
	StateDormant   = "dormant"   // the dormant list
	StateBlocked   = "blocked"   // a client removed it: never added again
)

// Why an account is dormant.
const (
	ReasonPrivate     = "private"
	ReasonInactive    = "inactive"
	ReasonNeverPosted = "never_posted"
	ReasonBanned      = "banned"
	ReasonGone        = "gone"
	ReasonUnreadable  = "unreadable"
)

// Finding is an account a source found (M11 to M13 call Observe with it).
type Finding struct {
	UserID   int64
	SecUID   string
	Username string
	// How is the source (config.DiscoverySources); Via what led to it: the
	// monitored account's user id, the keyword, the hashtag.
	How string
	Via string
	At  time.Time
	// Evidence is the source's own (JSON object): a search's region, the
	// post it was found on. "region": "IQ" makes a search, hashtag, sound,
	// feed or related find count as shown to viewers in Iraq.
	Evidence json.RawMessage
	// What the source already says, so that no read is needed (the cheap
	// pre-checks of discovery-plan §4): a private account, or one with no
	// post, goes on the dormant list at once.
	Private *bool
	Posts   *int64
	// Seen is how many times the source saw it in this finding (0 counts
	// as 1): part B counts a batch of sightings at once.
	Seen int64
}

// Observed is what Observe did with a finding.
type Observed struct {
	Ref   string `json:"account_ref"`
	State string `json:"state"`
	// Monitored: tks already monitors the account; the source is recorded
	// as evidence and nothing is read.
	Monitored bool `json:"monitored,omitempty"`
}

// Discoverer runs the entry check's decisions and the dormant list over
// schema tt.
type Discoverer struct {
	DB       *pgxpool.Pool
	Cfg      *config.Config
	Keywords *Keywords
	Now      func() time.Time
}

func (d *Discoverer) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func (d *Discoverer) matcher(ctx context.Context) (*Matcher, error) {
	if d.Keywords == nil {
		d.Keywords = &Keywords{DB: d.DB}
	}
	return d.Keywords.Matcher(ctx)
}

// ErrNoReference: a finding names neither a secUid nor a username.
var ErrNoReference = errors.New("discovery: a finding needs a secUid or a username")

// FindingRef is the reference a found account is registered by: its secUid,
// which a rename does not change, else its username (discovery-plan §5.5).
func FindingRef(f Finding) (string, error) {
	switch {
	case f.SecUID != "":
		return task.NormalizeRef("sec:" + f.SecUID)
	case f.Username != "":
		return task.NormalizeRef(f.Username)
	}
	return "", ErrNoReference
}

// Observe records an account a source found and moves it on: an account tks
// monitors gains the source as evidence and nothing is read; a new one
// waits for its entry check, or goes on the dormant list when the source
// already says it is private or has no post; a dormant one is read again
// only when found after its next check date; one not Iraqi (yet) is scored
// again from what is stored, with no read, and qualifies if the new signal
// lifts it over the threshold; one a client removed stays out.
func (d *Discoverer) Observe(ctx context.Context, f Finding) (Observed, error) {
	if f.Username != "" && !task.ValidUsername(f.Username) {
		f.Username = "" // never a page path
	}
	ref, err := FindingRef(f)
	if err != nil {
		return Observed{}, err
	}
	if f.At.IsZero() {
		f.At = d.now()
	}
	out := Observed{Ref: ref}
	err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		sref, existed, err := accounts.ScheduleRef(ctx, tx, ref, f.UserID)
		if err != nil {
			return err
		}
		if existed {
			var enabled bool
			if err := tx.QueryRow(ctx, `SELECT enabled FROM account_schedule WHERE account_ref = $1`, sref).Scan(&enabled); err != nil {
				return err
			}
			if enabled {
				out.Ref, out.State, out.Monitored = sref, StateAdded, true
				return accounts.AddSource(ctx, tx, d.source(sref, f))
			}
		}
		c, found, err := lockCandidate(ctx, tx, ref, f.UserID)
		if err != nil {
			return err
		}
		blocked, err := accounts.Blocked(ctx, tx, ref, f.UserID)
		if err != nil {
			return err
		}
		if !found {
			c = candidate{ref: ref, state: StatePending}
			switch {
			case blocked:
				c.state = StateBlocked
			case f.Private != nil && *f.Private:
				c.state, c.reason = StateDormant, ReasonPrivate
			case f.Posts != nil && *f.Posts == 0:
				c.state, c.reason = StateDormant, ReasonNeverPosted
			}
			var next *time.Time
			if c.state == StateDormant {
				t := f.At.Add(d.Cfg.TT.Discovery.Entry.RecheckAfter)
				next = &t
			}
			if _, err := tx.Exec(ctx, `INSERT INTO discovery_candidates (account_ref, user_id, username, state, reason, first_seen_at,
				last_seen_at, checked_at, next_check_at, seen) VALUES ($1,$2,$3,$4,$5,$6,$6,CASE WHEN $4 = 'dormant' THEN $6::timestamptz END,$7,$8)`,
				ref, nilIfZero(f.UserID), nilIfEmpty(f.Username), c.state, nilIfEmpty(c.reason), f.At, next, max(f.Seen, 1)); err != nil {
				return err
			}
		} else {
			ref = c.ref
			state := c.state
			switch {
			case blocked:
				state = StateBlocked
			case c.state == StateBlocked:
				state = StatePending // the client registered it since, lifting its block
			case c.state == StateDormant && c.nextCheck != nil && !f.At.Before(*c.nextCheck):
				state = StatePending // past its date: one entry check
			case c.state == StateAdded:
				state = StatePending // registered once, its schedule since removed or disabled by its owner
			}
			if _, err := tx.Exec(ctx, `UPDATE discovery_candidates SET last_seen_at = greatest(last_seen_at, $2), seen = seen + $6,
				user_id = coalesce(user_id, $3), state = $4, attempts = CASE WHEN $4 <> state THEN 0 ELSE attempts END,
				username = coalesce($5, username), updated_at = now() WHERE account_ref = $1`,
				ref, f.At, nilIfZero(f.UserID), state, nilIfEmpty(f.Username), max(f.Seen, 1)); err != nil {
				return err
			}
			c.state = state
		}
		out.Ref, out.State = ref, c.state
		if err := accounts.AddSource(ctx, tx, d.source(ref, f)); err != nil {
			return err
		}
		if c.state == StateNotIraqi {
			// The new signal may lift the score: no read needed.
			st, err := d.rescore(ctx, tx, ref)
			if err != nil {
				return err
			}
			out.State = st
		}
		return nil
	})
	if err == nil {
		metrics.DiscoveryFindings.WithLabelValues(f.How, out.State).Inc()
	}
	return out, err
}

func (d *Discoverer) source(ref string, f Finding) accounts.Source {
	return accounts.Source{AccountRef: ref, How: f.How, Via: f.Via, UserID: nilIfZero(f.UserID), At: f.At, Evidence: f.Evidence,
		Seen: f.Seen}
}

type candidate struct {
	ref       string
	userID    *int64
	state     string
	reason    string
	nextCheck *time.Time
	attempts  int
}

// lockCandidate reads a candidate by reference or user id, locked.
func lockCandidate(ctx context.Context, tx pgx.Tx, ref string, userID int64) (candidate, bool, error) {
	var c candidate
	var reason *string
	err := tx.QueryRow(ctx, `SELECT account_ref, user_id, state, reason, next_check_at, attempts FROM discovery_candidates
		WHERE account_ref = $1 OR ($2::bigint IS NOT NULL AND user_id = $2) ORDER BY account_ref = $1 DESC LIMIT 1 FOR UPDATE`,
		ref, nilIfZero(userID)).Scan(&c.ref, &c.userID, &c.state, &reason, &c.nextCheck, &c.attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	if reason != nil {
		c.reason = *reason
	}
	return c, err == nil, err
}

// Entry is an entry check's outcome, as the finalizer saw its job end.
type Entry struct {
	JobID uuid.UUID
	// Final is the job's terminal status.
	Final    string
	Counters map[string]int64
}

// Decide settles an account after its entry check, from what the check
// stored (the finalizer calls it once the persister has written the job's
// records): private, banned or gone, or with no post within
// tt.discovery.entry.active_for, it goes on the dormant list until
// recheck_after; otherwise its Iraq score qualifies it (registered by the
// leader's next Admit) or keeps it as not Iraqi (yet). A check that read
// nothing is tried again, max_attempts times. It returns the new state, or
// "" when the job is no candidate's current check.
func (d *Discoverer) Decide(ctx context.Context, e Entry) (string, error) {
	var state string
	err := pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		var c candidate
		var reason *string
		err := tx.QueryRow(ctx, `SELECT account_ref, user_id, state, reason, next_check_at, attempts FROM discovery_candidates
			WHERE job_id = $1 FOR UPDATE`, e.JobID).Scan(&c.ref, &c.userID, &c.state, &reason, &c.nextCheck, &c.attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if c.state != StateChecking {
			return nil
		}
		st, err := d.decide(ctx, tx, c, e)
		state = st
		return err
	})
	if err == nil && state != "" {
		metrics.EntryChecks.WithLabelValues(state).Inc()
	}
	return state, err
}

func (d *Discoverer) decide(ctx context.Context, tx pgx.Tx, c candidate, e Entry) (string, error) {
	now, ec := d.now(), d.Cfg.TT.Discovery.Entry
	if e.Final == "cancelled" {
		_, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'pending', job_id = NULL, updated_at = now()
			WHERE account_ref = $1`, c.ref)
		return StatePending, err
	}
	acc, err := storedAccount(ctx, tx, c)
	if err != nil {
		return "", err
	}
	read := e.Counters["entry_read"] > 0
	var reason string
	switch {
	case acc != nil && acc.status == "private", e.Counters["accounts_private"] > 0:
		reason = ReasonPrivate
	case acc != nil && acc.status == "banned", e.Counters["accounts_banned"] > 0:
		reason = ReasonBanned
	case acc != nil && acc.status == "unavailable", e.Counters["accounts_unavailable"] > 0:
		reason = ReasonGone
	case read && acc == nil && e.Counters["posts"] == 0:
		// The list answered with no post and no author to store.
		return StateDormant, d.dormant(ctx, tx, c.ref, c.userID, ReasonNeverPosted, intp(0), nil, now)
	case !read || acc == nil:
		// Nothing read and no status: try again, then give up.
		attempts := c.attempts + 1
		if attempts < ec.MaxAttempts {
			_, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'pending', job_id = NULL, attempts = $2,
				updated_at = now() WHERE account_ref = $1`, c.ref, attempts)
			return StatePending, err
		}
		reason = ReasonUnreadable
	}
	var uid *int64
	if acc != nil {
		uid = &acc.userID
	}
	if reason != "" {
		return StateDormant, d.dormant(ctx, tx, c.ref, uid, reason, nil, nil, now)
	}
	posts, newest, err := recentPosts(ctx, tx, acc.userID, ec.Posts)
	if err != nil {
		return "", err
	}
	switch {
	case newest == nil:
		return StateDormant, d.dormant(ctx, tx, c.ref, uid, ReasonNeverPosted, intp(0), nil, now)
	case newest.Before(now.Add(-ec.ActiveFor)):
		return StateDormant, d.dormant(ctx, tx, c.ref, uid, ReasonInactive, intp(len(posts)), newest, now)
	}
	r, err := d.score(ctx, tx, c.ref, acc, posts)
	if err != nil {
		return "", err
	}
	state := StateNotIraqi
	if r.Pass {
		state = StateQualified
	}
	ev, _ := json.Marshal(r)
	_, err = tx.Exec(ctx, `UPDATE discovery_candidates SET state = $2, reason = NULL, score = $3, evidence = $4, posts_read = $5,
		newest_post_at = $6, checked_at = $7, next_check_at = NULL, job_id = NULL, attempts = 0,
		user_id = coalesce(user_id, $8), updated_at = now() WHERE account_ref = $1`,
		c.ref, state, r.Score, ev, len(posts), newest, now, uid)
	return state, err
}

func intp(v int) *int { return &v }

// dormant puts a candidate on the dormant list until recheck_after.
func (d *Discoverer) dormant(ctx context.Context, tx pgx.Tx, ref string, uid *int64, reason string, postsRead *int, newest *time.Time, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'dormant', reason = $2, posts_read = coalesce($3, posts_read),
		newest_post_at = coalesce($4, newest_post_at), checked_at = $5, next_check_at = $6, job_id = NULL, attempts = 0,
		user_id = coalesce(user_id, $7), updated_at = now() WHERE account_ref = $1`,
		ref, reason, postsRead, newest, now, now.Add(d.Cfg.TT.Discovery.Entry.RecheckAfter), uid)
	return err
}

type storedAcc struct {
	userID                 int64
	status                 string
	nickname, bio, bioLink string
}

// storedAccount reads the candidate's account as stored: by its user id,
// else by its reference.
func storedAccount(ctx context.Context, q accounts.DB, c candidate) (*storedAcc, error) {
	sc := task.ScopeFromRef(c.ref)
	var a storedAcc
	const cols = `SELECT user_id, status, coalesce(nickname, ''), coalesce(bio, ''), coalesce(bio_link, '') FROM accounts`
	var row pgx.Row
	switch {
	case c.userID != nil:
		row = q.QueryRow(ctx, cols+` WHERE user_id = $1`, *c.userID)
	case sc.SecUid != "":
		row = q.QueryRow(ctx, cols+` WHERE sec_uid = $1`, sc.SecUid)
	default:
		row = q.QueryRow(ctx, cols+` WHERE lower(username) = lower($1) ORDER BY last_seen_at DESC LIMIT 1`, sc.Username)
	}
	err := row.Scan(&a.userID, &a.status, &a.nickname, &a.bio, &a.bioLink)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// recentPosts reads an account's newest stored public posts (pinned ones
// included: a post is a post), and the newest one's time.
func recentPosts(ctx context.Context, q DB, userID int64, n int) ([]PostFacts, *time.Time, error) {
	rows, err := q.Query(ctx, `SELECT created_at, coalesce(region, ''), coalesce(caption, ''), coalesce(hashtags, '{}'), coalesce(text_language, '')
		FROM posts WHERE user_id = $1 AND status = 'visible' ORDER BY created_at DESC LIMIT $2`, userID, n)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []PostFacts
	var newest *time.Time
	for rows.Next() {
		var p PostFacts
		var at time.Time
		if err := rows.Scan(&at, &p.Region, &p.Caption, &p.Hashtags, &p.TextLanguage); err != nil {
			return nil, nil, err
		}
		if newest == nil {
			newest = &at
		}
		out = append(out, p)
	}
	return out, newest, rows.Err()
}

// score computes a stored account's Iraq score: its text, its newest posts
// and its sources (how many different monitored accounts or searches led
// to it, by source).
func (d *Discoverer) score(ctx context.Context, tx pgx.Tx, ref string, acc *storedAcc, posts []PostFacts) (Result, error) {
	m, err := d.matcher(ctx)
	if err != nil {
		return Result{}, err
	}
	in := Inputs{Account: AccountText{Nickname: acc.nickname, Bio: acc.bio, BioLink: acc.bioLink}, Posts: posts,
		Audience: map[string]int{}}
	rows, err := tx.Query(ctx, `SELECT how, count(DISTINCT via),
			count(DISTINCT via) FILTER (WHERE how = ANY($3) AND upper(evidence->>'region') = ANY($4))
		FROM account_sources WHERE account_ref = $1 OR ($2::bigint IS NOT NULL AND user_id = $2) GROUP BY how`,
		ref, acc.userID, ShownInIraqSources, d.Cfg.TT.Discovery.Score.Regions)
	if err != nil {
		return Result{}, err
	}
	for rows.Next() {
		var how string
		var n, shown int
		if err := rows.Scan(&how, &n, &shown); err != nil {
			rows.Close()
			return Result{}, err
		}
		in.Audience[how], in.ShownInIraq = n, in.ShownInIraq+shown
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	return Score(d.Cfg.TT.Discovery.Score, m, in), nil
}

// rescore scores a not-Iraqi-(yet) candidate again from what is stored,
// after a new signal, and qualifies it when the score now passes.
func (d *Discoverer) rescore(ctx context.Context, tx pgx.Tx, ref string) (string, error) {
	c, found, err := lockCandidate(ctx, tx, ref, 0)
	if err != nil || !found {
		return StateNotIraqi, err
	}
	acc, err := storedAccount(ctx, tx, c)
	if err != nil || acc == nil {
		return StateNotIraqi, err
	}
	posts, _, err := recentPosts(ctx, tx, acc.userID, d.Cfg.TT.Discovery.Entry.Posts)
	if err != nil {
		return StateNotIraqi, err
	}
	r, err := d.score(ctx, tx, ref, acc, posts)
	if err != nil {
		return StateNotIraqi, err
	}
	state := StateNotIraqi
	if r.Pass {
		state = StateQualified
	}
	ev, _ := json.Marshal(r)
	_, err = tx.Exec(ctx, `UPDATE discovery_candidates SET state = $2, score = $3, evidence = $4, updated_at = now()
		WHERE account_ref = $1`, ref, state, r.Score, ev)
	return state, err
}

// Claim takes up to n pending candidates for their entry checks, the most
// promising first (the highest score, then the most often found, then the
// oldest), and marks them checking. Unclaim puts one back when its job
// could not be made.
func (d *Discoverer) Claim(ctx context.Context, n int) ([]Claimed, error) {
	if n <= 0 {
		return nil, nil
	}
	rows, err := d.DB.Query(ctx, `UPDATE discovery_candidates c SET state = 'checking', queued_at = $2, updated_at = now()
		FROM (SELECT account_ref FROM discovery_candidates WHERE state = 'pending'
			ORDER BY score DESC NULLS LAST, seen DESC, first_seen_at, account_ref LIMIT $1 FOR UPDATE SKIP LOCKED) p
		WHERE c.account_ref = p.account_ref RETURNING c.account_ref, coalesce(c.user_id, 0), coalesce(c.username, '')`, n, d.now())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Claimed, error) {
		var c Claimed
		return c, r.Scan(&c.Ref, &c.UserID, &c.Username)
	})
}

// Claimed is a candidate taken for its entry check: its reference, and the
// user id and username its sources gave.
type Claimed struct {
	Ref      string
	UserID   int64
	Username string
}

// Started records the entry check's job of a claimed candidate.
func (d *Discoverer) Started(ctx context.Context, ref string, job uuid.UUID) error {
	_, err := d.DB.Exec(ctx, `UPDATE discovery_candidates SET job_id = $2, updated_at = now() WHERE account_ref = $1`, ref, job)
	return err
}

// Unclaim puts a claimed candidate back in the queue.
func (d *Discoverer) Unclaim(ctx context.Context, ref string) error {
	_, err := d.DB.Exec(ctx, `UPDATE discovery_candidates SET state = 'pending', job_id = NULL, updated_at = now()
		WHERE account_ref = $1 AND state = 'checking'`, ref)
	return err
}

// Recover puts back in the queue the checks whose job ended without a
// decision (a job the reaper failed, a check whose job was never made),
// counting an attempt; after max_attempts the account is dormant as
// unreadable. It returns how many it moved.
func (d *Discoverer) Recover(ctx context.Context, after time.Duration) (int64, error) {
	now := d.now()
	tag, err := d.DB.Exec(ctx, `UPDATE discovery_candidates c SET
			state = CASE WHEN c.attempts + 1 >= $3 THEN 'dormant' ELSE 'pending' END,
			reason = CASE WHEN c.attempts + 1 >= $3 THEN 'unreadable' END,
			next_check_at = CASE WHEN c.attempts + 1 >= $3 THEN $1::timestamptz + make_interval(secs => $4::float8) END,
			checked_at = CASE WHEN c.attempts + 1 >= $3 THEN $1::timestamptz ELSE c.checked_at END,
			attempts = CASE WHEN c.attempts + 1 >= $3 THEN 0 ELSE c.attempts + 1 END,
			job_id = NULL, updated_at = now()
		WHERE c.state = 'checking' AND c.queued_at < $1::timestamptz - make_interval(secs => $2::float8)
			AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id = c.job_id AND j.status IN ('running','finalizing','cancelling'))`,
		now, after.Seconds(), d.Cfg.TT.Discovery.Entry.MaxAttempts, d.Cfg.TT.Discovery.Entry.RecheckAfter.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Excess returns the jobs of the entry checks running beyond limit, the most
// recently queued first, for the caller to cancel: a lower
// tt.discovery.entry.max_running then holds at once instead of when the
// checks already running end (2026-11-13: 500 entry checks waiting on the
// ISP exits' page-load gate ended 4 an hour). A cancelled check's account
// goes back to pending with no attempt counted (Decide); checks already
// cancelling are not counted again.
func (d *Discoverer) Excess(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := d.DB.Query(ctx, `SELECT c.job_id FROM discovery_candidates c JOIN jobs j ON j.job_id = c.job_id
		WHERE c.state = 'checking' AND j.status IN ('queued', 'running')
		ORDER BY c.queued_at, c.account_ref OFFSET $1`, max(limit, 0))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
}

// ReleaseCancelled puts back in the queue, with no attempt counted, the
// accounts whose entry check's job is being cancelled. A cancelled job ends
// only when each of its tasks is next taken from its queue, which in a
// busy backfill lane can take hours (2026-11-14: 441 of 444 still
// cancelling after 70 minutes), and until then the account would count as
// checking and hold a place under max_running. The job's later decision
// finds no candidate and changes nothing. It returns how many it released.
func (d *Discoverer) ReleaseCancelled(ctx context.Context) (int64, error) {
	tag, err := d.DB.Exec(ctx, `UPDATE discovery_candidates c SET state = 'pending', job_id = NULL, updated_at = now()
		FROM jobs j WHERE j.job_id = c.job_id AND c.state = 'checking' AND j.status = 'cancelling'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Running counts the entry checks running now.
func (d *Discoverer) Running(ctx context.Context) (int, error) {
	var n int
	err := d.DB.QueryRow(ctx, `SELECT count(*) FROM discovery_candidates WHERE state = 'checking'`).Scan(&n)
	return n, err
}

// Admit registers qualified candidates for monitoring, the highest score
// first, under the discovery client (paced admission then spaces their
// first reads, by score: discovery-plan §5.3): all of them, or what is
// left of tt.discovery.max_new_per_day (UTC days) when it is set. One a
// client removed meanwhile is blocked instead. It returns how many it
// registered.
func (d *Discoverer) Admit(ctx context.Context, client uuid.UUID, n int) (int, error) {
	now := d.now()
	if limit := d.Cfg.TT.Discovery.MaxNewPerDay; limit > 0 {
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		var today int
		if err := d.DB.QueryRow(ctx, `SELECT count(*) FROM discovery_candidates WHERE added_at >= $1`, day).Scan(&today); err != nil {
			return 0, err
		}
		n = min(n, limit-today)
	}
	if n <= 0 {
		return 0, nil
	}
	added := 0
	for added < n {
		more, ok, err := d.admitOne(ctx, client, now)
		if err != nil {
			return added, err
		}
		if !more {
			break
		}
		if ok {
			added++
		}
	}
	return added, nil
}

// admitOne settles the best qualified candidate, in one transaction:
// registered (added), or blocked. more is false when none is left.
func (d *Discoverer) admitOne(ctx context.Context, client uuid.UUID, now time.Time) (more, added bool, err error) {
	err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		var ref string
		var uid *int64
		var score *float64
		err := tx.QueryRow(ctx, `SELECT account_ref, user_id, score FROM discovery_candidates WHERE state = 'qualified'
			ORDER BY score DESC NULLS LAST, first_seen_at, account_ref LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&ref, &uid, &score)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		more = true
		var id int64
		if uid != nil {
			id = *uid
		}
		blocked, err := accounts.Blocked(ctx, tx, ref, id)
		if err != nil {
			return err
		}
		if blocked {
			_, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'blocked', updated_at = now() WHERE account_ref = $1`, ref)
			return err
		}
		added = true
		res, err := accounts.RegisterTx(ctx, tx, accounts.Registration{
			ClientID: client, Ref: ref, UserID: id, Origin: accounts.OriginDiscovery, Priority: Priority(score),
			DefaultInterval: d.Cfg.TT.Freshness.ListInterval, Enabled: true, Score: score, Now: now,
		})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'added', schedule_ref = $2, added_at = $3, updated_at = now()
			WHERE account_ref = $1`, ref, res.ScheduleRef, now)
		if err == nil {
			metrics.DiscoveredAccounts.Inc()
		}
		return err
	})
	return more, added, err
}

// Priority is a discovered account's admission priority: below every
// owner's account, by its Iraq score in hundredths.
func Priority(score *float64) int {
	if score == nil {
		return accounts.PriorityDiscovered
	}
	return min(accounts.PriorityDiscovered+int(*score*100), accounts.PriorityOwner-1)
}

// Quiet moves the discovered accounts tks monitors that went quiet to the
// dormant list (discovery-plan §4): private, banned or gone (the schedule's
// inactive_since), or no post within active_for after their first read.
// Their checks stop (the schedule is disabled) and their data stays.
// Accounts an owner registered are never moved: they get the lighter
// checks of tt.freshness.inactive. It returns how many it moved.
func (d *Discoverer) Quiet(ctx context.Context, n int) (int, error) {
	now := d.now()
	ec := d.Cfg.TT.Discovery.Entry
	rows, err := d.DB.Query(ctx, `
		SELECT s.account_ref, s.user_id, coalesce(a.status, 'active'), cc.newest_seen_at
		FROM account_schedule s
		LEFT JOIN accounts a ON a.user_id = s.user_id
		LEFT JOIN crawl_cursors cc ON cc.scope_type = 'account_list' AND cc.scope_id = s.user_id AND cc.ordering = 'default'
		WHERE s.enabled AND s.origin = 'discovery' AND s.admitted_at IS NOT NULL AND s.user_id IS NOT NULL
			AND (s.inactive_since IS NOT NULL OR (s.last_success_at IS NOT NULL
				AND coalesce(cc.newest_seen_at, '-infinity') < $1::timestamptz - make_interval(secs => $2::float8)))
		ORDER BY s.account_ref LIMIT $3`, now, ec.ActiveFor.Seconds(), n)
	if err != nil {
		return 0, err
	}
	type quiet struct {
		ref    string
		uid    int64
		status string
		newest *time.Time
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (quiet, error) {
		var q quiet
		return q, r.Scan(&q.ref, &q.uid, &q.status, &q.newest)
	})
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, q := range list {
		reason := ReasonInactive
		switch q.status {
		case "private":
			reason = ReasonPrivate
		case "banned":
			reason = ReasonBanned
		case "unavailable":
			reason = ReasonGone
		case "active":
			if q.newest == nil {
				reason = ReasonNeverPosted
			}
		}
		err := pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
			// Guarded: an owner's registration meanwhile keeps it.
			tag, err := tx.Exec(ctx, `UPDATE account_schedule SET enabled = false WHERE account_ref = $1 AND origin = 'discovery' AND enabled`, q.ref)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO discovery_candidates (account_ref, user_id, state, first_seen_at, last_seen_at)
				VALUES ($1, $2, 'dormant', $3, $3)
				ON CONFLICT DO NOTHING`, q.ref, q.uid, now); err != nil {
				return err
			}
			// The candidate row is the schedule's, or the one of its user id.
			_, err = tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'dormant', reason = $3, checked_at = $4,
				next_check_at = $5, newest_post_at = coalesce($6, newest_post_at), schedule_ref = $1, job_id = NULL,
				user_id = coalesce(user_id, $2), updated_at = now()
				WHERE account_ref = (SELECT account_ref FROM discovery_candidates WHERE user_id = $2 OR account_ref = $1
					ORDER BY user_id = $2 DESC LIMIT 1)`,
				q.ref, q.uid, reason, now, now.Add(ec.RecheckAfter), q.newest)
			return err
		})
		if err != nil {
			return moved, fmt.Errorf("discovery: quiet %s: %w", q.ref, err)
		}
		moved++
		metrics.DormantMoves.WithLabelValues(reason).Inc()
	}
	return moved, nil
}

// EnsureClient returns the API client discovered accounts are registered
// to (tt.discovery.client), creating it with no usable key when missing.
func EnsureClient(ctx context.Context, db DB, name string) (uuid.UUID, error) {
	var id uuid.UUID
	err := db.QueryRow(ctx, `SELECT id FROM api_clients WHERE name = $1 AND key_prefix = $2 AND revoked_at IS NULL`,
		name, clientPrefix(name)).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return id, err
	}
	id = uuid.New()
	// A one-byte hash no key's SHA-256 can equal: nobody can sign in as it.
	_, err = db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, $2, $3, '\x00', '{}')
		ON CONFLICT (key_prefix) DO NOTHING`, id, name, clientPrefix(name))
	if err != nil {
		return id, err
	}
	err = db.QueryRow(ctx, `SELECT id FROM api_clients WHERE key_prefix = $1`, clientPrefix(name)).Scan(&id)
	return id, err
}

func clientPrefix(name string) string { return "tks_discovery:" + name }

// Counts are the candidates by state, for the gauge and the API.
func Counts(ctx context.Context, db DB) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT state, count(*) FROM discovery_candidates GROUP BY state`)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			rows.Close()
			return nil, err
		}
		out[s] = n
	}
	rows.Close()
	return out, rows.Err()
}

func nilIfZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// View is a found account as the API shows it. The username is personal
// data (pii:read); the ids, the state and the evidence are not.
type View struct {
	// ID is the found account's own number (not TikTok's): the review
	// page removes it by it, with no name in the path.
	ID           int64           `json:"id"`
	UserID       *int64          `json:"user_id,omitempty,string"`
	SecUID       string          `json:"sec_uid,omitempty"`
	Username     *string         `json:"username,omitempty"`
	State        string          `json:"state"`
	Reason       *string         `json:"reason,omitempty"`
	Score        *float64        `json:"score,omitempty"`
	Evidence     json.RawMessage `json:"evidence,omitempty"`
	PostsRead    *int            `json:"posts_read,omitempty"`
	NewestPostAt *time.Time      `json:"newest_post_at,omitempty"`
	FirstSeenAt  time.Time       `json:"first_seen_at"`
	LastSeenAt   time.Time       `json:"last_seen_at"`
	Seen         int64           `json:"seen"`
	Attempts     int             `json:"attempts,omitempty"`
	CheckedAt    *time.Time      `json:"checked_at,omitempty"`
	NextCheckAt  *time.Time      `json:"next_check_at,omitempty"`
	AddedAt      *time.Time      `json:"added_at,omitempty"`
	// Sources are the ways it was found (account_sources.how).
	Sources []string `json:"sources"`
	// Cursor is the row's position for ?after=.
	Cursor int64 `json:"-"`
}

// List lists found accounts, in one state or all, the latest found first,
// after a cursor (a View's Cursor, opaque: it carries no name).
func List(ctx context.Context, db DB, state string, after int64, limit int, pii bool) ([]View, error) {
	f := Filter{State: state, Limit: limit}
	if after > 0 {
		f.After = strconv.FormatInt(after, 10)
	}
	out, _, err := Search(ctx, db, f, pii)
	return out, err
}

// States are the candidate states, for validating a filter.
var States = []string{StatePending, StateChecking, StateQualified, StateAdded, StateNotIraqi, StateDormant, StateBlocked}
