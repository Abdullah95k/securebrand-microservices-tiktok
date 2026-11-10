package persist

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// Discovery part B (docs/tiktok/discovery-plan.md §7; ADR 0007 §21),
// ported from fbs's mentions (fbs ADR 0004): the persister records the
// people a monitored account's posts, bio and comments bring in, only for
// rows it writes for the first time, in the same transaction, so a
// re-read, an edit or a redelivered batch records nothing again. The
// leader counts them (discovery.Discoverer.Count).

// sighting is one person seen, before it is recorded.
type sighting struct {
	how, in   string
	via       int64 // the monitored account
	userID    *int64
	secUID    string
	username  string
	postID    *int64
	commentID *int64
	at        time.Time
	// What a list page showed of the person (part D).
	private *bool
	videos  *int64
}

// on reports whether the persister records a part B source.
func (w *Writer) on(how string) bool { return w.Sightings != nil && w.Sightings(how) }

// partB reports whether any part B source is on.
func (w *Writer) partB() bool {
	for _, s := range discovery.PartB {
		if w.on(s) {
			return true
		}
	}
	return false
}

// personSighting is a sighting of p, or false when p names nobody tks can
// find again (no user id and no secUid: pseudonymized) or is the
// monitored account itself.
func personSighting(how, in string, via int64, p *ttv1.Person, at time.Time) (sighting, bool) {
	if p == nil || (p.UserId == nil && p.SecUid == "") || (p.UserId != nil && *p.UserId == via) {
		return sighting{}, false
	}
	return sighting{how: how, in: in, via: via, userID: p.UserId, secUID: p.SecUid, username: p.Username, at: at}, true
}

// postSightings are the people new posts' captions mention.
func (w *Writer) postSightings(ps []*ttv1.Post, fresh []int64) []sighting {
	if len(fresh) == 0 || !w.on(discovery.SourceMention) {
		return nil
	}
	isNew := make(map[int64]bool, len(fresh))
	for _, id := range fresh {
		isNew[id] = true
	}
	var out []sighting
	for _, p := range ps {
		if !isNew[p.PostId] {
			continue
		}
		seen := map[string]bool{}
		for _, r := range p.TextRanges {
			if r.Type != "mention" || seen[string(r.GetPerson().GetAuthorKey())] {
				continue
			}
			seen[string(r.GetPerson().GetAuthorKey())] = true
			if s, ok := personSighting(discovery.SourceMention, discovery.InCaption, p.UserId, r.Person, observed(p.Obs)); ok {
				s.postID = &p.PostId
				out = append(out, s)
			}
		}
	}
	return out
}

// commentSightings are the people new comments bring in: their authors
// (commenter), the people they mention (mention), and, for the monitored
// account's own replies, whom it answered (reply: looked up in the stored
// comment it answers, see recordReplies).
func (w *Writer) commentSightings(cs []*ttv1.Comment, fresh map[int64]bool) (direct []sighting, replies []*ttv1.Comment) {
	for _, c := range cs {
		if !fresh[c.CommentId] || c.Redacted || c.UserId == 0 {
			continue
		}
		at := observed(c.Obs)
		owner := c.Author.GetUserId() == c.UserId
		if !owner && w.on(discovery.SourceCommenter) {
			if s, ok := personSighting(discovery.SourceCommenter, discovery.InComment, c.UserId, c.Author, at); ok {
				s.postID, s.commentID = &c.PostId, &c.CommentId
				direct = append(direct, s)
			}
		}
		if w.on(discovery.SourceMention) {
			seen := map[string]bool{}
			for _, r := range c.TextRanges {
				p := r.GetPerson()
				if r.Type != "mention" || seen[string(p.GetAuthorKey())] || (p.UserId != nil && *p.UserId == c.Author.GetUserId()) {
					continue
				}
				seen[string(p.GetAuthorKey())] = true
				if s, ok := personSighting(discovery.SourceMention, discovery.InComment, c.UserId, p, at); ok {
					s.postID, s.commentID = &c.PostId, &c.CommentId
					direct = append(direct, s)
				}
			}
		}
		if owner && c.Depth > 0 && w.on(discovery.SourceReply) {
			replies = append(replies, c)
		}
	}
	return direct, replies
}

// monitored is the condition that a sighting's account (d.via) is one tks
// monitors: an enabled schedule by its user id, or, before its first job
// has ended and the finalizer has stamped the id, by the reference it was
// registered under (its id, secUid or any username it had; the batch
// writes accounts before posts and comments). An entry check's posts,
// stored like a monitored account's (discovery-plan §10), bring no one in.
const monitored = `EXISTS (SELECT 1 FROM account_schedule s WHERE s.enabled AND (s.user_id = d.via
	OR s.account_ref = 'id:' || d.via
	OR s.account_ref IN (SELECT 'sec:' || a.sec_uid FROM accounts a WHERE a.user_id = d.via
		UNION ALL SELECT 'u:' || lower(u.username) FROM account_usernames u WHERE u.user_id = d.via)))`

// recordSightings appends sightings of monitored accounts.
func recordSightings(ctx context.Context, tx pgx.Tx, ss []sighting, got sightingCounts) error {
	if len(ss) == 0 {
		return nil
	}
	n := len(ss)
	how, in := make([]string, 0, n), make([]string, 0, n)
	via := make([]int64, 0, n)
	uid := make([]*int64, 0, n)
	sec, user := make([]*string, 0, n), make([]*string, 0, n)
	post, comment := make([]*int64, 0, n), make([]*int64, 0, n)
	at := make([]time.Time, 0, n)
	private, videos := make([]*bool, 0, n), make([]*int64, 0, n)
	for _, s := range ss {
		how, in, via, uid = append(how, s.how), append(in, s.in), append(via, s.via), append(uid, s.userID)
		sec, user = append(sec, nilIfEmpty(s.secUID)), append(user, nilIfEmpty(s.username))
		post, comment, at = append(post, s.postID), append(comment, s.commentID), append(at, s.at)
		private, videos = append(private, s.private), append(videos, s.videos)
	}
	rows, err := tx.Query(ctx, `INSERT INTO discovery_sightings (how, found_in, via_user_id, user_id, sec_uid, username, post_id,
			comment_id, seen_at, private, videos)
		SELECT d.how, d.found_in, d.via, d.user_id, d.sec_uid, d.username, d.post_id, d.comment_id, d.at, d.private, d.videos
		FROM unnest($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::text[], $6::text[], $7::bigint[], $8::bigint[],
			$9::timestamptz[], $10::boolean[], $11::bigint[])
			AS d(how, found_in, via, user_id, sec_uid, username, post_id, comment_id, at, private, videos)
		WHERE `+monitored+` RETURNING how`, how, in, via, uid, sec, user, post, comment, at, private, videos)
	if err != nil {
		return err
	}
	return got.add(rows)
}

// recordReplies appends, for the monitored account's own new replies, a
// sighting of the author of the comment each answers (reply_to_comment_id,
// else its top-level comment), read from the stored comments: the thread's
// top-level comment is always written before its replies are read.
func recordReplies(ctx context.Context, tx pgx.Tx, cs []*ttv1.Comment, got sightingCounts) error {
	if len(cs) == 0 {
		return nil
	}
	via := make([]int64, 0, len(cs))
	created := make([]time.Time, 0, len(cs))
	posts, targets, ids := make([]int64, 0, len(cs)), make([]int64, 0, len(cs)), make([]int64, 0, len(cs))
	at := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		target := c.GetReplyToCommentId()
		if target == 0 {
			target = c.GetParentCommentId()
		}
		if target == 0 {
			continue
		}
		via, created, posts = append(via, c.UserId), append(created, c.PostCreatedAt.AsTime()), append(posts, c.PostId)
		targets, ids, at = append(targets, target), append(ids, c.CommentId), append(at, observed(c.Obs))
	}
	rows, err := tx.Query(ctx, `INSERT INTO discovery_sightings (how, found_in, via_user_id, user_id, sec_uid, username, post_id,
			comment_id, seen_at)
		SELECT 'reply', 'comment', d.via, t.author_user_id, p.sec_uid, p.username, d.post_id, d.comment_id, d.at
		FROM unnest($1::bigint[], $2::timestamptz[], $3::bigint[], $4::bigint[], $5::bigint[], $6::timestamptz[])
			AS d(via, created, post_id, target, comment_id, at)
		JOIN comments t ON t.post_created_at = d.created AND t.post_id = d.post_id AND t.comment_id = d.target
		LEFT JOIN people p ON p.author_key = t.author_key
		WHERE t.author_user_id IS NOT NULL AND t.author_user_id <> d.via AND `+monitored+` RETURNING how`,
		via, created, posts, targets, ids, at)
	if err != nil {
		return err
	}
	return got.add(rows)
}

// recordBio appends the usernames a monitored account's new or changed bio
// names with @.
func recordBio(ctx context.Context, tx pgx.Tx, a *ttv1.Account, got sightingCounts) error {
	names := discovery.BioMentions(a.Bio, a.Username)
	if len(names) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `INSERT INTO discovery_sightings (how, found_in, via_user_id, username, seen_at)
		SELECT 'mention', 'bio', d.via, u, $3 FROM unnest($2::text[]) AS u, (SELECT $1::bigint AS via) d
		WHERE `+monitored+` RETURNING how`, a.UserId, names, observed(a.Obs))
	if err != nil {
		return err
	}
	return got.add(rows)
}

// find is the author of a post discovery part C found (Post.find).
type find struct {
	how, via, region string
	userID, postID   int64
	at               time.Time
	job              string // the read's job (a repost's first and last)
}

// postFinds are the finds of the part C sources switched on in ps.
func (w *Writer) postFinds(ps []*ttv1.Post) []find {
	var out []find
	for _, p := range ps {
		f := p.GetFind()
		if f == nil || p.UserId == 0 || p.PostId == 0 || f.Via == "" || !w.on(f.Source) {
			continue
		}
		var job string
		if ids := p.GetObs().GetJobIds(); len(ids) > 0 {
			job = ids[0]
		}
		out = append(out, find{how: f.Source, via: f.Via, region: f.Region, userID: p.UserId, postID: p.PostId, at: observed(p.Obs), job: job})
	}
	return out
}

// recordFinds appends part C's finds (docs/tiktok/discovery-plan.md §8):
// each post's author, once per source, target and post, so a re-read or a
// redelivery records nothing again; the author's secUid and username are
// the stored account's (the stage sends the author with the post). Every
// author is recorded, monitored or not: a monitored one gains the source
// with no read.
func recordFinds(ctx context.Context, tx pgx.Tx, fs []find, got sightingCounts) error {
	if len(fs) == 0 {
		return nil
	}
	n := len(fs)
	how, via, region := make([]string, 0, n), make([]string, 0, n), make([]*string, 0, n)
	uid, post, at := make([]int64, 0, n), make([]int64, 0, n), make([]time.Time, 0, n)
	for _, f := range fs {
		how, via, region = append(how, f.how), append(via, f.via), append(region, nilIfEmpty(f.region))
		uid, post, at = append(uid, f.userID), append(post, f.postID), append(at, f.at)
	}
	rows, err := tx.Query(ctx, `INSERT INTO discovery_sightings (how, found_in, via, region, user_id, sec_uid, username, post_id, seen_at)
		SELECT d.how, d.how, d.via, d.region, d.user_id, a.sec_uid, a.username, d.post_id, d.at
		FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::timestamptz[])
			AS d(how, via, region, user_id, post_id, at)
		LEFT JOIN accounts a ON a.user_id = d.user_id
		ON CONFLICT (how, via, post_id, user_id) WHERE via IS NOT NULL DO NOTHING
		RETURNING how`, how, via, region, uid, post, at)
	if err != nil {
		return err
	}
	return got.add(rows)
}

// sightingCounts counts a transaction's sightings by source, published
// once it commits.
type sightingCounts map[string]int

func (c sightingCounts) add(rows pgx.Rows) error {
	hows, err := pgx.CollectRows(rows, pgx.RowTo[string])
	for _, h := range hows {
		c[h]++
	}
	return err
}

func (c sightingCounts) publish() {
	for h, n := range c {
		metrics.DiscoverySightings.WithLabelValues(h).Add(float64(n))
	}
}

// recordReposts keeps the reposts among fs (source repost: via is the
// reposting account's user id) in tt.reposts, every account's full repost
// history (owner D-047): first seen once, last seen on every read.
func recordReposts(ctx context.Context, tx pgx.Tx, fs []find) error {
	var uid, post, author []int64
	var at []time.Time
	var job []*string
	for _, f := range fs {
		if f.how != discovery.SourceRepost {
			continue
		}
		by, err := strconv.ParseInt(f.via, 10, 64)
		if err != nil || by == 0 || by == f.userID {
			continue // no reposting account, or its own post
		}
		uid, post, author, at = append(uid, by), append(post, f.postID), append(author, f.userID), append(at, f.at)
		job = append(job, uuidOrNil(f.job))
	}
	if len(uid) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO reposts AS r (user_id, post_id, author_user_id, first_seen_at, last_seen_at, first_job_id, last_job_id)
		SELECT user_id, post_id, max(author_user_id), min(at), max(at), (array_agg(job ORDER BY at))[1], (array_agg(job ORDER BY at DESC))[1]
		FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::timestamptz[], $5::uuid[]) AS d(user_id, post_id, author_user_id, at, job)
		GROUP BY user_id, post_id
		ON CONFLICT (user_id, post_id) DO UPDATE SET
			first_seen_at = least(r.first_seen_at, EXCLUDED.first_seen_at),
			first_job_id = CASE WHEN EXCLUDED.first_seen_at < r.first_seen_at THEN EXCLUDED.first_job_id ELSE r.first_job_id END,
			last_seen_at = greatest(r.last_seen_at, EXCLUDED.last_seen_at),
			last_job_id = CASE WHEN EXCLUDED.last_seen_at >= r.last_seen_at THEN EXCLUDED.last_job_id ELSE r.last_job_id END,
			author_user_id = EXCLUDED.author_user_id`, uid, post, author, at, job)
	return err
}

// uuidOrNil is s when it is a UUID's text form, else nil.
func uuidOrNil(s string) *string {
	if len(s) != 36 {
		return nil
	}
	return &s
}
