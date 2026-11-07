package discovery

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Part C (docs/tiktok/discovery-plan.md §8; ADR 0007 §27): the leader keeps
// one tt.discovery_targets row per thing read (a keyword, a hashtag, a
// sound, a monitored post's related posts, the For You feed), claims the
// due ones and sends each to be read (stage.DiscoverStage); the persister
// stores the posts found and records their authors as sightings, which
// Count turns into findings like parts B and D's.

// Origins of a target (discovery_targets.origin).
const (
	OriginKeyword   = "keyword"   // the Iraq list: a search, a one-word term as a hashtag
	OriginMonitored = "monitored" // used by monitored accounts' recent posts
	OriginPost      = "post"      // a monitored post's related posts
	OriginFeed      = "feed"
)

// FeedKey is the For You feed's one target.
const FeedKey = "feed"

// PageSources are part C's sources switched on. Search needs a tier TikTok
// places in or near Iraq (tt.discovery.search.tier): from US exits it asks
// to log in (T37), so without one it is off whatever its switch says.
func (d *Discoverer) PageSources() []string {
	dc := d.Cfg.TT.Discovery
	var out []string
	for _, s := range PartC {
		if dc.SourceOn(s) && (s != SourceSearch || dc.Search.Tier != "") {
			out = append(out, s)
		}
	}
	return out
}

// HashtagOf makes a hashtag of a keyword as TikTok spells hashtags: one
// word, lower-cased; "" when the keyword is several words.
func HashtagOf(term string) string {
	term = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(term), "#"))
	if term == "" || strings.IndexFunc(term, unicode.IsSpace) >= 0 {
		return ""
	}
	return strings.ToLower(term)
}

// SeedTargets brings the targets of the sources on up to date: the
// keywords to search (following the keyword list's switches), the Iraq
// list's one-word terms as hashtags, the feed and, up to related.per_tick
// a call, monitored accounts' new posts. With refresh it also picks again
// the hashtags and sounds monitored accounts used most in the window
// (tt.discovery.pages.refresh): those no longer picked stop being read.
// New targets are due at once.
func (d *Discoverer) SeedTargets(ctx context.Context, sources []string, refresh bool) error {
	on := map[string]bool{}
	for _, s := range sources {
		on[s] = true
	}
	dc := d.Cfg.TT.Discovery
	now := d.now()
	if on[SourceSearch] {
		if _, err := d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, name, origin, next_read_at)
			SELECT 'search', k.id::text, k.term, 'keyword', $1 FROM discovery_keywords k WHERE k.enabled
			ON CONFLICT (source, key) DO NOTHING`, now); err != nil {
			return err
		}
		if _, err := d.DB.Exec(ctx, `UPDATE discovery_targets t SET enabled = k.enabled, name = k.term
			FROM discovery_keywords k WHERE t.source = 'search' AND t.key = k.id::text
			  AND (t.enabled IS DISTINCT FROM k.enabled OR t.name IS DISTINCT FROM k.term)`); err != nil {
			return err
		}
	}
	if on[SourceHashtag] {
		if err := d.seedKeywordTags(ctx, now); err != nil {
			return err
		}
		if refresh {
			if err := d.seedUsed(ctx, SourceHashtag, now); err != nil {
				return err
			}
		}
	}
	if on[SourceSound] && refresh {
		if err := d.seedUsed(ctx, SourceSound, now); err != nil {
			return err
		}
	}
	if on[SourceRelated] && dc.Related.PerTick > 0 {
		if _, err := d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, origin, item_id, post_user_id, next_read_at)
			SELECT 'related', p.post_id::text, 'post', p.post_id, p.user_id, $1 FROM posts p
			WHERE p.created_at >= $2 AND p.status = 'visible'
			  AND EXISTS (SELECT 1 FROM account_schedule s WHERE s.user_id = p.user_id AND s.enabled AND s.admitted_at IS NOT NULL)
			  AND NOT EXISTS (SELECT 1 FROM discovery_targets t WHERE t.source = 'related' AND t.key = p.post_id::text)
			ORDER BY p.play_count DESC NULLS LAST, p.post_id DESC LIMIT $3
			ON CONFLICT (source, key) DO NOTHING`, now, now.Add(-dc.Related.Window), dc.Related.PerTick); err != nil {
			return err
		}
	}
	if on[SourceFeed] {
		if _, err := d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, origin, next_read_at) VALUES ('feed', $1, 'feed', $2)
			ON CONFLICT (source, key) DO NOTHING`, FeedKey, now); err != nil {
			return err
		}
	}
	return nil
}

// seedKeywordTags makes the enabled one-word keywords hashtags to read, and
// stops reading those whose keyword was turned off.
func (d *Discoverer) seedKeywordTags(ctx context.Context, now time.Time) error {
	tags := []string{}
	if d.Cfg.TT.Discovery.Hashtags.Keywords {
		rows, err := d.DB.Query(ctx, `SELECT term FROM discovery_keywords WHERE enabled`)
		if err != nil {
			return err
		}
		terms, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		skip := skipSet(d.Cfg.TT.Discovery.Hashtags.Skip)
		seen := map[string]bool{}
		for _, t := range terms {
			if h := HashtagOf(t); h != "" && !seen[h] && !skip[h] {
				seen[h] = true
				tags = append(tags, h)
			}
		}
	}
	if _, err := d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, name, origin, next_read_at)
		SELECT 'hashtag', h, h, 'keyword', $2 FROM unnest($1::text[]) AS h
		ON CONFLICT (source, key) DO UPDATE SET origin = 'keyword', enabled = true
		WHERE discovery_targets.origin <> 'keyword' OR NOT discovery_targets.enabled`, tags, now); err != nil {
		return err
	}
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets SET enabled = false
		WHERE source = 'hashtag' AND origin = 'keyword' AND enabled AND NOT (key = ANY($1))`, tags)
	return err
}

// seedUsed picks the hashtags or sounds at least min_accounts monitored
// accounts used in posts of the window, the top most used, as targets; the
// ones picked before and not now stop being read (a keyword's hashtag
// stays).
func (d *Discoverer) seedUsed(ctx context.Context, source string, now time.Time) error {
	u := d.Cfg.TT.Discovery.Hashtags
	q := `SELECT h.name, max(nullif(h.id, 0)), count(DISTINCT p.user_id) AS uses
		FROM posts p CROSS JOIN LATERAL unnest(p.hashtags, p.hashtag_ids) AS h(name, id)
		WHERE p.created_at >= $1 AND h.name <> '' AND NOT (h.name = ANY($2))
		  AND EXISTS (SELECT 1 FROM account_schedule s WHERE s.user_id = p.user_id AND s.enabled AND s.admitted_at IS NOT NULL)
		GROUP BY h.name HAVING count(DISTINCT p.user_id) >= $3 ORDER BY uses DESC, h.name LIMIT $4`
	if source == SourceSound {
		u = d.Cfg.TT.Discovery.Sounds
		q = `SELECT p.music_id::text, p.music_id, count(DISTINCT p.user_id) AS uses
		FROM posts p
		WHERE p.created_at >= $1 AND p.music_id IS NOT NULL AND p.music_id <> 0 AND NOT (p.music_id::text = ANY($2))
		  AND EXISTS (SELECT 1 FROM account_schedule s WHERE s.user_id = p.user_id AND s.enabled AND s.admitted_at IS NOT NULL)
		GROUP BY p.music_id HAVING count(DISTINCT p.user_id) >= $3 ORDER BY uses DESC, p.music_id LIMIT $4`
	}
	keys := []string{}
	var ids []*int64
	var uses []int32
	if u.Top > 0 {
		rows, err := d.DB.Query(ctx, q, now.Add(-u.Window), lower(u.Skip), u.MinAccounts, u.Top)
		if err != nil {
			return err
		}
		for rows.Next() {
			var k string
			var id *int64
			var n int32
			if err := rows.Scan(&k, &id, &n); err != nil {
				rows.Close()
				return err
			}
			keys, ids, uses = append(keys, k), append(ids, id), append(uses, n)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	name := "k"
	if source == SourceSound {
		name = "NULL"
	}
	if _, err := d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, name, item_id, uses, origin, next_read_at)
		SELECT $1, k, `+name+`, id, n, 'monitored', $5 FROM unnest($2::text[], $3::bigint[], $4::int[]) AS d(k, id, n)
		ON CONFLICT (source, key) DO UPDATE SET uses = EXCLUDED.uses, enabled = true,
			item_id = coalesce(discovery_targets.item_id, EXCLUDED.item_id)`, source, keys, ids, uses, now); err != nil {
		return err
	}
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets SET enabled = false
		WHERE source = $1 AND origin = 'monitored' AND enabled AND NOT (key = ANY($2))`, source, keys)
	return err
}

func skipSet(xs []string) map[string]bool {
	out := map[string]bool{}
	for _, x := range lower(xs) {
		out[x] = true
	}
	return out
}

func lower(xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, strings.ToLower(strings.TrimPrefix(strings.TrimSpace(x), "#")))
	}
	return out
}

// DueTarget is a target the leader sends to be read.
type DueTarget struct {
	Source, Key, Name string
	ItemID            int64
	// PostUserID and PostUsername are a related target's post's author.
	PostUserID   int64
	PostUsername string
}

// ClaimTargets takes up to n due targets of sources whose last read is not
// still running, never read first, then the longest due; a claimed target
// is due again after retry_after unless its read records sooner.
func (d *Discoverer) ClaimTargets(ctx context.Context, sources []string, n int) ([]DueTarget, error) {
	if len(sources) == 0 || n <= 0 {
		return nil, nil
	}
	now := d.now()
	rows, err := d.DB.Query(ctx, `WITH due AS (
			SELECT t.source, t.key FROM discovery_targets t
			WHERE t.source = ANY($1) AND t.enabled AND t.next_read_at <= $2
			  AND (t.last_job_id IS NULL OR NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id = t.last_job_id
				AND j.status IN ('running', 'finalizing', 'cancelling')))
			ORDER BY t.first_read_at NULLS FIRST, t.next_read_at, t.source, t.key
			LIMIT $3 FOR UPDATE OF t SKIP LOCKED)
		UPDATE discovery_targets t SET next_read_at = $2 + $4::interval, claimed_at = $2
		FROM due WHERE t.source = due.source AND t.key = due.key
		RETURNING t.source, t.key, coalesce(t.name, ''), coalesce(t.item_id, 0), coalesce(t.post_user_id, 0),
			coalesce((SELECT a.username FROM accounts a WHERE a.user_id = t.post_user_id), '')`,
		sources, now, n, d.Cfg.TT.Discovery.Pages.RetryAfter)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (DueTarget, error) {
		var t DueTarget
		return t, r.Scan(&t.Source, &t.Key, &t.Name, &t.ItemID, &t.PostUserID, &t.PostUsername)
	})
	if err != nil {
		return nil, err
	}
	var kw []int64
	for _, t := range out {
		if t.Source == SourceSearch {
			if id, err := strconv.ParseInt(t.Key, 10, 64); err == nil {
				kw = append(kw, id)
			}
		}
	}
	if len(kw) > 0 {
		if _, err := d.DB.Exec(ctx, `UPDATE discovery_keywords SET last_queued_at = $2 WHERE id = ANY($1)`, kw, now); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// TargetStarted records the read's job of a claimed target, before the job
// is produced (it may end before Create returns).
func (d *Discoverer) TargetStarted(ctx context.Context, source, key string, job uuid.UUID) error {
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets SET last_job_id = $3 WHERE source = $1 AND key = $2`, source, key, job)
	return err
}

// TargetUnclaim puts back a claimed target whose read could not start.
func (d *Discoverer) TargetUnclaim(ctx context.Context, source, key string) error {
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets SET next_read_at = $3, claimed_at = NULL WHERE source = $1 AND key = $2`,
		source, key, d.now())
	return err
}

// TargetsRunning is how many discovery page reads are running fleet-wide.
func (d *Discoverer) TargetsRunning(ctx context.Context) (int, error) {
	var n int
	err := d.DB.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE kind = 'discover' AND status IN ('running', 'finalizing', 'cancelling')`).Scan(&n)
	return n, err
}

// TargetPagesToday is the pages every discovery page read fetched this UTC
// day.
func (d *Discoverer) TargetPagesToday(ctx context.Context) (int, error) {
	now := d.now()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var n int
	err := d.DB.QueryRow(ctx, `SELECT coalesce(sum(day_pages), 0) FROM discovery_targets WHERE day = $1::date`, day).Scan(&n)
	return n, err
}

// BootUsernames returns up to n usernames of monitored, active accounts:
// a discovery page read bootstraps its identity on one's profile page, the
// page TikTok's script signs these calls on (C6 run 2).
func (d *Discoverer) BootUsernames(ctx context.Context, n int) ([]string, error) {
	rows, err := d.DB.Query(ctx, `SELECT a.username FROM account_schedule s JOIN accounts a ON a.user_id = s.user_id
		WHERE s.enabled AND s.admitted_at IS NOT NULL AND a.status = 'active' AND a.username IS NOT NULL
		LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// TargetCounts counts enabled targets by source and state, and the due
// ones.
func TargetCounts(ctx context.Context, db DB, now time.Time) (states map[[2]string]int64, due map[string]int64, err error) {
	states, due = map[[2]string]int64{}, map[string]int64{}
	rows, err := db.Query(ctx, `SELECT source, state, count(*), count(*) FILTER (WHERE next_read_at <= $1)
		FROM discovery_targets WHERE enabled GROUP BY 1, 2`, now)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var src, st string
		var n, dn int64
		if err := rows.Scan(&src, &st, &n, &dn); err != nil {
			return nil, nil, err
		}
		states[[2]string{src, st}] = n
		due[src] += dn
	}
	return states, due, rows.Err()
}
