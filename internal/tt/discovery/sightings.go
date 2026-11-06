package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Part B's sources (docs/tiktok/discovery-plan.md §7): accounts found in
// what tks already collects from the accounts it monitors.
const (
	SourceMention   = "mention"   // named in a caption, a bio or a comment
	SourceReply     = "reply"     // a monitored account answered their comment
	SourceCommenter = "commenter" // commented on a monitored account's post
)

// Part D's sources (docs/tiktok/discovery-plan.md §9): the following and
// followers lists of the accounts tks monitors.
const (
	SourceFollowing = "following" // a monitored account follows them
	SourceFollower  = "follower"  // they follow a monitored account
)

// Where a sighting was seen (discovery_sightings.found_in).
const (
	InCaption   = "caption"
	InBio       = "bio"
	InComment   = "comment"
	InFollowing = "following" // the monitored account's following list
	InFollowers = "followers" // its followers list
)

// PartB are part B's sources, the ones the persister records from posts,
// bios and comments.
var PartB = []string{SourceMention, SourceReply, SourceCommenter}

// PartD are part D's sources, the ones the persister records from list
// pages.
var PartD = []string{SourceFollowing, SourceFollower}

// bioMention is an @username in a bio: TikTok usernames are letters,
// digits, underscores and periods, 2 to 24 of them. The character before
// the @ must not continue a word or an address (name@example.org).
var bioMention = regexp.MustCompile(`(?:^|[^\p{L}\p{N}_.@])@([A-Za-z0-9_.]{2,24})`)

// maxBioMentions bounds the usernames one bio yields.
const maxBioMentions = 20

// BioMentions returns the usernames a bio names with @, lower-cased, each
// once, without the account's own: a bio carries no user id, so the entry
// check reads the profile page of each (discovery-plan §7).
func BioMentions(bio, own string) []string {
	var out []string
	seen := map[string]bool{strings.ToLower(own): true}
	for _, m := range bioMention.FindAllStringSubmatch(bio, -1) {
		u := strings.ToLower(strings.TrimRight(m[1], "."))
		if len(u) < 2 || seen[u] || !task.ValidUsername(u) {
			continue
		}
		seen[u] = true
		out = append(out, u)
		if len(out) == maxBioMentions {
			break
		}
	}
	return out
}

// sighting is one row of discovery_sightings.
type sighting struct {
	id        int64
	how, in   string
	via       int64
	userID    *int64
	secUID    *string
	username  *string
	postID    *int64
	commentID *int64
	at        time.Time
	// What a list page showed (part D): the cheap pre-checks.
	private *bool
	videos  *int64
}

// person keys a sighting by whom it saw: the user id, else the secUid,
// else the username.
func (s sighting) person() string {
	switch {
	case s.userID != nil:
		return "id:" + strconv.FormatInt(*s.userID, 10)
	case s.secUID != nil:
		return "sec:" + *s.secUID
	case s.username != nil:
		return "u:" + strings.ToLower(*s.username)
	}
	return ""
}

// Count counts up to n sightings the persister recorded and the leader has
// not counted yet (part B, discovery-plan §7), oldest first, into the
// sources and candidates: one Observe per person, source and monitored
// account in the batch, with how often the batch saw it. An account tks
// monitors gains the source and nothing is read; a new one waits for its
// entry check; a dormant one waits for its date. Sightings of a source
// switched off since, or naming nobody, are counted and dropped. It
// returns how many sightings it counted.
//
// Counting is at least once: a leader that stops between an Observe and
// stamping its rows counts them again, which can only add to the evidence
// (seen); no account is read twice for it.
func (d *Discoverer) Count(ctx context.Context, n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	rows, err := d.DB.Query(ctx, `SELECT id, how, found_in, via_user_id, user_id, sec_uid, username, post_id, comment_id, seen_at,
			private, videos
		FROM discovery_sightings WHERE counted_at IS NULL ORDER BY id LIMIT $1`, n)
	if err != nil {
		return 0, err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (sighting, error) {
		var s sighting
		return s, r.Scan(&s.id, &s.how, &s.in, &s.via, &s.userID, &s.secUID, &s.username, &s.postID, &s.commentID, &s.at,
			&s.private, &s.videos)
	})
	if err != nil || len(list) == 0 {
		return 0, err
	}
	var order []string
	groups := map[string]*sightingGroup{}
	ids := make([]int64, 0, len(list))
	for _, s := range list {
		ids = append(ids, s.id)
		p := s.person()
		if p == "" || (s.userID != nil && *s.userID == s.via) {
			continue // nobody, or the monitored account itself
		}
		k := s.how + "|" + strconv.FormatInt(s.via, 10) + "|" + p
		g, ok := groups[k]
		if !ok {
			g = &sightingGroup{first: s, f: Finding{How: s.how, Via: strconv.FormatInt(s.via, 10), At: s.at}}
			groups[k] = g
			order = append(order, k)
		}
		g.seen++
		if s.at.After(g.f.At) {
			g.f.At = s.at
		}
		if s.userID != nil {
			g.f.UserID = *s.userID
		}
		if s.secUID != nil && *s.secUID != "" {
			g.f.SecUID = *s.secUID
		}
		if s.username != nil && *s.username != "" {
			g.f.Username = *s.username
		}
		// The newest list page's view of the person decides the pre-check.
		if s.private != nil && !s.at.Before(g.f.At) {
			g.f.Private = s.private
		}
		if s.videos != nil && !s.at.Before(g.f.At) {
			g.f.Posts = s.videos
		}
	}
	if err := d.fillUsernames(ctx, groups); err != nil {
		return 0, err
	}
	cfg := d.Cfg.TT.Discovery
	for _, k := range order {
		g := groups[k]
		if !cfg.SourceOn(g.f.How) {
			continue
		}
		g.f.Seen = g.seen
		g.f.Evidence = sightingEvidence(g.first, g.seen)
		if _, err := d.Observe(ctx, g.f); err != nil && !errors.Is(err, ErrNoReference) {
			return 0, err
		}
	}
	if _, err := d.DB.Exec(ctx, `UPDATE discovery_sightings SET counted_at = $2 WHERE id = ANY($1)`, ids, d.now()); err != nil {
		return 0, err
	}
	metrics.DiscoverySightingsCounted.Add(float64(len(ids)))
	return len(ids), nil
}

// sightingGroup is the sightings of one person by one source via one
// monitored account in a batch.
type sightingGroup struct {
	f     Finding
	first sighting
	seen  int64
}

// fillUsernames gives the findings that carry a user id and no username
// (a mention in a comment carries only ids) the newest username tks stored
// for the person: the entry check needs one to load a profile page when
// its identity has not loaded one yet.
func (d *Discoverer) fillUsernames(ctx context.Context, groups map[string]*sightingGroup) error {
	var ids []int64
	for _, g := range groups {
		if g.f.Username == "" && g.f.UserID != 0 {
			ids = append(ids, g.f.UserID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := d.DB.Query(ctx, `SELECT DISTINCT ON (user_id) user_id, username FROM (
			SELECT user_id, username, last_seen_at FROM people WHERE user_id = ANY($1) AND username IS NOT NULL
			UNION ALL SELECT user_id, username, last_seen_at FROM accounts WHERE user_id = ANY($1) AND username IS NOT NULL) n
		ORDER BY user_id, last_seen_at DESC`, ids)
	if err != nil {
		return err
	}
	names := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			rows.Close()
			return err
		}
		names[id] = name
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, g := range groups {
		if g.f.Username == "" {
			g.f.Username = names[g.f.UserID]
		}
	}
	return nil
}

// sightingEvidence is a part B source's evidence: where the account was
// seen first in the batch (the post and comment, by id), and how often.
// Never any text.
func sightingEvidence(s sighting, seen int64) json.RawMessage {
	ev := map[string]any{"found_in": s.in, "sightings": seen}
	if s.postID != nil {
		ev["post_id"] = strconv.FormatInt(*s.postID, 10)
	}
	if s.commentID != nil {
		ev["comment_id"] = strconv.FormatInt(*s.commentID, 10)
	}
	b, _ := json.Marshal(ev)
	return b
}

// Uncounted is how many sightings wait to be counted (the backlog gauge).
func Uncounted(ctx context.Context, db DB) (int64, error) {
	var n int64
	err := db.QueryRow(ctx, `SELECT count(*) FROM discovery_sightings WHERE counted_at IS NULL`).Scan(&n)
	return n, err
}
