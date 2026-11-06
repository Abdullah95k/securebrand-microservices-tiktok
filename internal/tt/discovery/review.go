package discovery

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// What the review page reads and does (docs/tiktok/discovery-plan.md §11,
// ADR 0007 §22): found accounts by source, keyword, label, score and date,
// with their evidence; removing one; hits per keyword; and discovery's
// accounts and traffic by source and day.

// Filter selects found accounts. The zero value is every account, the
// latest found first.
type Filter struct {
	State string
	// How is a source (config.DiscoverySources); Via what led to it within
	// the source (a keyword's id for search, a monitored account's user id
	// for part B), only with How.
	How string
	Via string
	// Label is a label on the account's schedule, set by LabelClient (any
	// client's when nil).
	Label       string
	LabelClient *uuid.UUID
	// MinScore and MaxScore bound the Iraq score (inclusive); an account
	// never scored has none and is left out by either.
	MinScore, MaxScore *float64
	// Since and Until bound when the account was first found (Until
	// exclusive).
	Since, Until time.Time
	// ByScore orders by score, highest first (the accounts closest to the
	// threshold, for not_iraqi), instead of the latest found first.
	ByScore bool
	// After is a previous page's Next.
	After string
	Limit int
}

// ErrBadCursor is an After that no page gave.
var ErrBadCursor = errors.New("discovery: invalid cursor")

// sourcesOf matches the account_sources rows of candidate c: under its own
// reference, the schedule it was registered under, or its user id.
const sourcesOf = `(s.account_ref = c.account_ref OR s.account_ref = c.schedule_ref OR (c.user_id IS NOT NULL AND s.user_id = c.user_id))`

// Search lists found accounts by f, with the ways each was found, and the
// cursor of the next page ("" on the last).
func Search(ctx context.Context, db DB, f Filter, pii bool) ([]View, string, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	where := []string{"true"}
	if f.State != "" {
		where = append(where, "c.state = "+arg(f.State))
	}
	if f.How != "" {
		w := "s.how = " + arg(f.How)
		if f.Via != "" {
			w += " AND s.via = " + arg(f.Via)
		}
		where = append(where, "EXISTS (SELECT 1 FROM account_sources s WHERE "+w+" AND "+sourcesOf+")")
	}
	if f.Label != "" {
		w := "l.account_ref = c.schedule_ref AND l.label = " + arg(f.Label)
		if f.LabelClient != nil {
			w += " AND l.api_client_id = " + arg(*f.LabelClient)
		}
		where = append(where, "EXISTS (SELECT 1 FROM account_labels l WHERE "+w+")")
	}
	if f.MinScore != nil {
		where = append(where, "c.score >= "+arg(*f.MinScore))
	}
	if f.MaxScore != nil {
		where = append(where, "c.score <= "+arg(*f.MaxScore))
	}
	if !f.Since.IsZero() {
		where = append(where, "c.first_seen_at >= "+arg(f.Since))
	}
	if !f.Until.IsZero() {
		where = append(where, "c.first_seen_at < "+arg(f.Until))
	}
	order := "c.id DESC"
	if f.ByScore {
		order = "coalesce(c.score, -1) DESC, c.id DESC"
	}
	if f.After != "" {
		score, id, err := parseCursor(f.After, f.ByScore)
		if err != nil {
			return nil, "", err
		}
		if f.ByScore {
			where = append(where, "(coalesce(c.score, -1), c.id) < ("+arg(score)+"::float8, "+arg(id)+"::bigint)")
		} else {
			where = append(where, "c.id < "+arg(id))
		}
	}
	q := `SELECT c.id, c.account_ref, c.user_id, c.username, c.state, c.reason, c.score, c.evidence, c.posts_read, c.newest_post_at,
			c.first_seen_at, c.last_seen_at, c.seen, c.attempts, c.checked_at, c.next_check_at, c.added_at,
			coalesce((SELECT array_agg(DISTINCT s.how ORDER BY s.how) FROM account_sources s WHERE ` + sourcesOf + `), '{}')
		FROM discovery_candidates c WHERE ` + strings.Join(where, " AND ") + ` ORDER BY ` + order + ` LIMIT ` + arg(f.Limit)
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (View, error) {
		var v View
		var cref string
		err := r.Scan(&v.ID, &cref, &v.UserID, &v.Username, &v.State, &v.Reason, &v.Score, &v.Evidence, &v.PostsRead, &v.NewestPostAt,
			&v.FirstSeenAt, &v.LastSeenAt, &v.Seen, &v.Attempts, &v.CheckedAt, &v.NextCheckAt, &v.AddedAt, &v.Sources)
		v.Cursor = v.ID
		if sc := task.ScopeFromRef(cref); sc.SecUid != "" {
			v.SecUID = sc.SecUid
		} else if v.Username == nil && sc.Username != "" {
			v.Username = &sc.Username
		}
		if !pii {
			v.Username = nil
		}
		return v, err
	})
	if err != nil {
		return nil, "", err
	}
	if out == nil {
		out = []View{}
	}
	next := ""
	if len(out) == f.Limit {
		last := out[len(out)-1]
		if f.ByScore {
			s := -1.0
			if last.Score != nil {
				s = *last.Score
			}
			next = strconv.FormatFloat(s, 'g', -1, 64) + ":" + strconv.FormatInt(last.ID, 10)
		} else {
			next = strconv.FormatInt(last.ID, 10)
		}
	}
	return out, next, nil
}

// parseCursor reads a Next: "<id>", or "<score>:<id>" when ordered by
// score.
func parseCursor(s string, byScore bool) (float64, int64, error) {
	score := 0.0
	if byScore {
		a, b, ok := strings.Cut(s, ":")
		if !ok {
			return 0, 0, ErrBadCursor
		}
		v, err := strconv.ParseFloat(a, 64)
		if err != nil {
			return 0, 0, ErrBadCursor
		}
		score, s = v, b
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, 0, ErrBadCursor
	}
	return score, id, nil
}

// Removal is what removing a found account did.
type Removal struct {
	State string `json:"state"`
	// MonitoringStopped: discovery had added the account and its schedule
	// is now off (its data stays: ADR 0007 §14).
	MonitoringStopped bool `json:"monitoring_stopped"`
	// StillMonitored: another client registered the account itself, so it
	// stays monitored for them; discovery's and the caller's registrations
	// are gone and discovery never adds it back.
	StillMonitored bool `json:"still_monitored"`
}

// Remove takes a found account out of discovery for good (the review
// page's remove button, discovery-plan §11): the caller's block keeps every
// source from adding it back (§5.5), discovery's registration and the
// caller's go, and the schedule stops when no client is left on it. The
// candidate becomes blocked. discoveryClient holds discovered accounts'
// registrations (tt.discovery.client). A registration by the caller lifts
// the block, as for any removal. It is idempotent.
func Remove(ctx context.Context, db *pgxpool.Pool, discoveryClient, caller uuid.UUID, id int64, now time.Time) (Removal, error) {
	out := Removal{State: StateBlocked}
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var ref string
		var uid *int64
		var sref *string
		err := tx.QueryRow(ctx, `SELECT account_ref, user_id, schedule_ref FROM discovery_candidates WHERE id = $1 FOR UPDATE`, id).
			Scan(&ref, &uid, &sref)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: no found account %d", ErrNotFound, id)
		}
		if err != nil {
			return err
		}
		var u int64
		if uid != nil {
			u = *uid
		}
		if err := accounts.Block(ctx, tx, caller, ref, u, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE discovery_candidates SET state = 'blocked', updated_at = now() WHERE account_ref = $1`, ref); err != nil {
			return err
		}
		if sref == nil {
			return nil
		}
		if *sref != ref {
			if err := accounts.Block(ctx, tx, caller, *sref, u, now); err != nil {
				return err
			}
		}
		clients := []uuid.UUID{discoveryClient, caller}
		if _, err := tx.Exec(ctx, `DELETE FROM account_registrations WHERE account_ref = $1 AND api_client_id = ANY($2)`, *sref, clients); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM account_labels WHERE account_ref = $1 AND api_client_id = ANY($2)`, *sref, clients); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE account_schedule SET enabled = false WHERE account_ref = $1 AND enabled
			AND NOT EXISTS (SELECT 1 FROM account_registrations WHERE account_ref = $1)`, *sref)
		if err != nil {
			return err
		}
		out.MonitoringStopped = tag.RowsAffected() > 0
		return tx.QueryRow(ctx, `SELECT coalesce((SELECT enabled FROM account_schedule WHERE account_ref = $1), false)`, *sref).
			Scan(&out.StillMonitored)
	})
	return out, err
}

// SearchSource is how part C records a keyword search's finds in
// account_sources (M12): via is the keyword's id, which a correction of
// its spelling does not change.
const SearchSource = "search"

// KeywordHits counts, per keyword id, the accounts its searches found.
func KeywordHits(ctx context.Context, db DB, ids []int64) (map[int64]int64, error) {
	out := map[int64]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	vias := make([]string, len(ids))
	for i, id := range ids {
		vias[i] = strconv.FormatInt(id, 10)
	}
	rows, err := db.Query(ctx, `SELECT via, count(DISTINCT account_ref) FROM account_sources WHERE how = $1 AND via = ANY($2)
		GROUP BY via`, SearchSource, vias)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var via string
		var n int64
		if err := rows.Scan(&via, &n); err != nil {
			return nil, err
		}
		id, _ := strconv.ParseInt(via, 10, 64)
		out[id] = n
	}
	return out, rows.Err()
}

// JobKinds are the job kinds discovery's own reads run as (dispatch.Kind*):
// their requests and data are discovery's cost: entry checks, and part D's
// list reads. Part C's sources add theirs (M12).
var JobKinds = []string{dispatch.KindEntry, dispatch.KindFollows}

// Reads are the jobs of one kind on one day and what they cost.
type Reads struct {
	Jobs      int64 `json:"jobs"`
	Requests  int64 `json:"requests"`
	WireBytes int64 `json:"wire_bytes"`
}

// Day is discovery on one UTC day: accounts found and added, by source (an
// account found by two sources counts under each), and its reads by job
// kind.
type Day struct {
	Day   string           `json:"day"`
	Found map[string]int64 `json:"found"`
	Added map[string]int64 `json:"added"`
	Reads map[string]Reads `json:"reads"`
}

// Daily is discovery's days from since (UTC days) to now, oldest first,
// every day listed.
func Daily(ctx context.Context, db DB, since, now time.Time) ([]Day, error) {
	since = time.Date(since.Year(), since.Month(), since.Day(), 0, 0, 0, 0, time.UTC)
	var days []Day
	idx := map[string]int{}
	for d := since; !d.After(now); d = d.AddDate(0, 0, 1) {
		k := d.Format(time.DateOnly)
		idx[k] = len(days)
		days = append(days, Day{Day: k, Found: map[string]int64{}, Added: map[string]int64{}, Reads: map[string]Reads{}})
	}
	add := func(q string, args []any, f func(d *Day, key string, n []int64)) error {
		rows, err := db.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var day time.Time
			var key string
			n := make([]int64, 3)
			dest := []any{&day, &key, &n[0]}
			if len(rows.FieldDescriptions()) == 5 {
				dest = append(dest, &n[1], &n[2])
			}
			if err := rows.Scan(dest...); err != nil {
				return err
			}
			if i, ok := idx[day.Format(time.DateOnly)]; ok {
				f(&days[i], key, n)
			}
		}
		return rows.Err()
	}
	sources := config.DiscoverySources
	err := add(`SELECT date_trunc('day', first_at AT TIME ZONE 'UTC'), how, count(DISTINCT account_ref) FROM account_sources
		WHERE how = ANY($1) AND first_at >= $2 GROUP BY 1, 2`, []any{sources, since},
		func(d *Day, how string, n []int64) { d.Found[how] = n[0] })
	if err != nil {
		return nil, err
	}
	err = add(`SELECT date_trunc('day', c.added_at AT TIME ZONE 'UTC'), s.how, count(DISTINCT c.account_ref)
		FROM discovery_candidates c JOIN account_sources s ON `+sourcesOf+`
		WHERE c.added_at >= $2 AND s.how = ANY($1) GROUP BY 1, 2`, []any{sources, since},
		func(d *Day, how string, n []int64) { d.Added[how] = n[0] })
	if err != nil {
		return nil, err
	}
	err = add(`SELECT date_trunc('day', created_at AT TIME ZONE 'UTC'), kind, count(*),
			coalesce(sum((counters->>'requests')::bigint), 0), coalesce(sum((counters->>'wire_bytes')::bigint), 0)
		FROM jobs WHERE kind = ANY($1) AND created_at >= $2 GROUP BY 1, 2`, []any{JobKinds, since},
		func(d *Day, kind string, n []int64) {
			d.Reads[kind] = Reads{Jobs: n[0], Requests: n[1], WireBytes: n[2]}
		})
	if err != nil {
		return nil, err
	}
	return days, nil
}
