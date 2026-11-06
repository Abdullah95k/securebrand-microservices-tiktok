package discovery

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Part D (docs/tiktok/discovery-plan.md §9; ADR 0007 §23): the leader
// keeps one tt.follow_lists row per monitored account and list, claims the
// due ones and sends each to be read (stage.FollowsStage); the persister
// keeps the links and records the people new to a list as sightings, which
// Count turns into findings (sources following and follower) like part B's.

// ListDirections are the follow_lists directions of the part D sources
// switched on.
func (d *Discoverer) ListDirections() []string {
	var out []string
	for _, s := range PartD {
		if d.Cfg.TT.Discovery.SourceOn(s) {
			out = append(out, s)
		}
	}
	return out
}

// monitoredList is the condition that list l's account is monitored: an
// enabled schedule, admitted (paced admission starts its reads), whose
// account tks has stored with its secUid.
const monitoredList = `EXISTS (SELECT 1 FROM account_schedule s WHERE s.user_id = l.user_id AND s.enabled AND s.admitted_at IS NOT NULL)`

// SeedLists creates, due now, the lists of monitored accounts that have no
// row yet, up to n; it returns how many it created.
func (d *Discoverer) SeedLists(ctx context.Context, dirs []string, n int) (int, error) {
	if len(dirs) == 0 || n <= 0 {
		return 0, nil
	}
	tag, err := d.DB.Exec(ctx, `INSERT INTO follow_lists (user_id, direction, next_read_at)
		SELECT l.user_id, l.direction, $3 FROM (
			SELECT DISTINCT s.user_id, dir AS direction FROM account_schedule s CROSS JOIN unnest($1::text[]) AS dir
			WHERE s.enabled AND s.admitted_at IS NOT NULL AND s.user_id IS NOT NULL
			  AND NOT EXISTS (SELECT 1 FROM follow_lists f WHERE f.user_id = s.user_id AND f.direction = dir)
			LIMIT $2) l
		WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.user_id = l.user_id AND a.sec_uid IS NOT NULL)
		ON CONFLICT DO NOTHING`, dirs, n, d.now())
	return int(tag.RowsAffected()), err
}

// SkipLists moves on, without a call, the due lists TikTok would not serve:
// a following list the profile hides (followingVisibility 2), and the lists
// of an account the checks found private, banned or gone. Each is due
// again after its interval, when the profile may say otherwise. It returns
// how many it moved by state.
func (d *Discoverer) SkipLists(ctx context.Context, dirs []string) (map[string]int, error) {
	out := map[string]int{}
	if len(dirs) == 0 {
		return out, nil
	}
	f := d.Cfg.TT.Discovery.Follows
	now := d.now()
	rows, err := d.DB.Query(ctx, `UPDATE follow_lists l SET
			state = CASE WHEN a.status <> 'active' THEN 'gone' ELSE 'hidden' END,
			last_stop = CASE WHEN a.status <> 'active' THEN 'gone' ELSE 'hidden' END,
			next_read_at = $2 + CASE WHEN l.direction = 'following' THEN $3::interval ELSE $4::interval END
		FROM accounts a
		WHERE a.user_id = l.user_id AND l.direction = ANY($1) AND l.next_read_at <= $2
		  AND (a.status <> 'active' OR (l.direction = 'following' AND a.following_visibility = 2))
		RETURNING l.state`, dirs, now, f.FollowingEvery, f.FollowersEvery)
	if err != nil {
		return nil, err
	}
	states, err := pgx.CollectRows(rows, pgx.RowTo[string])
	for _, s := range states {
		out[s]++
	}
	return out, err
}

// DueList is a list the leader sends to be read.
type DueList struct {
	UserID     int64
	SecUID     string
	Username   string
	Direction  string
	DeepCursor string
	DeepDone   bool
	ReadBefore bool
}

// ClaimLists takes up to n due lists of monitored accounts whose last read
// is not still running, never read first, then the longest due; a claimed
// list is due again after retry_after unless its read records sooner.
func (d *Discoverer) ClaimLists(ctx context.Context, dirs []string, n int) ([]DueList, error) {
	if len(dirs) == 0 || n <= 0 {
		return nil, nil
	}
	now := d.now()
	rows, err := d.DB.Query(ctx, `WITH due AS (
			SELECT l.user_id, l.direction FROM follow_lists l
			WHERE l.direction = ANY($1) AND l.next_read_at <= $2 AND `+monitoredList+`
			  AND (l.last_job_id IS NULL OR NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id = l.last_job_id
				AND j.status IN ('running', 'finalizing', 'cancelling')))
			ORDER BY l.first_read_at NULLS FIRST, l.next_read_at, l.user_id
			LIMIT $3 FOR UPDATE OF l SKIP LOCKED)
		UPDATE follow_lists l SET next_read_at = $2 + $4::interval, claimed_at = $2
		FROM due, accounts a
		WHERE l.user_id = due.user_id AND l.direction = due.direction AND a.user_id = l.user_id AND a.sec_uid IS NOT NULL
		RETURNING l.user_id, a.sec_uid, coalesce(a.username, ''), l.direction, coalesce(l.deep_cursor, ''), l.deep_done,
			l.first_read_at IS NOT NULL AND l.people_seen > 0`, dirs, now, n, d.Cfg.TT.Discovery.Follows.RetryAfter)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (DueList, error) {
		var l DueList
		return l, r.Scan(&l.UserID, &l.SecUID, &l.Username, &l.Direction, &l.DeepCursor, &l.DeepDone, &l.ReadBefore)
	})
}

// ListStarted records the read's job of a claimed list, before the job is
// produced (it may end before Create returns).
func (d *Discoverer) ListStarted(ctx context.Context, userID int64, dir string, job uuid.UUID) error {
	_, err := d.DB.Exec(ctx, `UPDATE follow_lists SET last_job_id = $3 WHERE user_id = $1 AND direction = $2`, userID, dir, job)
	return err
}

// ListUnclaim puts back a claimed list whose read could not start.
func (d *Discoverer) ListUnclaim(ctx context.Context, userID int64, dir string) error {
	_, err := d.DB.Exec(ctx, `UPDATE follow_lists SET next_read_at = $3, claimed_at = NULL WHERE user_id = $1 AND direction = $2`,
		userID, dir, d.now())
	return err
}

// ListsRunning is how many list reads are running fleet-wide.
func (d *Discoverer) ListsRunning(ctx context.Context) (int, error) {
	var n int
	err := d.DB.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE kind = 'follows' AND status IN ('running', 'finalizing', 'cancelling')`).Scan(&n)
	return n, err
}

// ListPagesToday is the pages every list read fetched this UTC day.
func (d *Discoverer) ListPagesToday(ctx context.Context) (int, error) {
	now := d.now()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var n int
	err := d.DB.QueryRow(ctx, `SELECT coalesce(sum(day_pages), 0) FROM follow_lists WHERE day = $1::date`, day).Scan(&n)
	return n, err
}

// ListCounts counts lists by direction and state, and the due ones.
func ListCounts(ctx context.Context, db DB, now time.Time) (states map[[2]string]int64, due map[string]int64, err error) {
	states, due = map[[2]string]int64{}, map[string]int64{}
	rows, err := db.Query(ctx, `SELECT direction, state, count(*), count(*) FILTER (WHERE next_read_at <= $1)
		FROM follow_lists GROUP BY 1, 2`, now)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var dir, st string
		var n, d int64
		if err := rows.Scan(&dir, &st, &n, &d); err != nil {
			return nil, nil, err
		}
		states[[2]string{dir, st}] = n
		due[dir] += d
	}
	return states, due, rows.Err()
}
