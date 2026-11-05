package stage

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"time"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

// Posts that are no longer public (docs/tiktok/discovery-plan.md §17).
//
// A full page of the creator list holds every post between its oldest one
// and the cursor it was asked with (the newest posts older than the cursor,
// newest first: C2, C3). A stored visible post of the account dated strictly
// inside that span that the page did not return is a candidate. One read of
// its page by plain HTTP decides: the page shows the post (it is refreshed
// from the page), or answers a statusCode that means the post is not public
// (it is published as unavailable, with the code and the exit's region), or
// one about the account, or one not known (nothing is marked). A logged-out
// reader cannot tell a deletion from a post made private or removed, so a
// post is never "deleted", and nothing is (D8).
//
// Guards: a short or empty page marks nothing (tt.posts.unavailable.
// min_page_posts); more candidates on one page than max_per_page is a
// reading problem, and none is checked; an account that is private, banned
// or gone ends the check before any page is compared, so its posts keep
// their status; a check reads at most max_checks post pages. Only visible
// posts are candidates, so an unavailable post is not read again, until a
// list returns it and it is visible again.

// Results of a candidate (the post_vanish_total metric, and the task's
// counters as "vanish_<result>").
const (
	VanishUnavailable    = "unavailable"
	VanishVisible        = "visible"
	VanishAccount        = "account"
	VanishUnknownCode    = "unknown_code"
	VanishReadingProblem = "reading_problem"
	VanishDeferred       = "deferred"
	VanishError          = "error"
)

// vanish is one segment's state: every post id the walk's pages returned
// (a stored post is not a candidate once any page returned it), and the
// post pages read so far.
type vanish struct {
	seen   map[int64]bool
	checks int
	// stopped: a post page was refused or paced; the segment checks no more.
	stopped bool
}

// candidate is a stored post a page did not return.
type candidate struct {
	postID  int64
	created time.Time
	photo   bool
}

func vanishResult(c map[string]int64, result string, n int) {
	if n <= 0 {
		return
	}
	metrics.PostVanish.WithLabelValues(result).Add(float64(n))
	c["vanish_"+result] += int64(n)
}

// vanished compares a list page asked with cursor (ms) with the account's
// stored posts in the span it covers, and checks the candidates. Only a
// failure to read the store is returned; a post page that fails marks
// nothing.
func (s *AccountStage) vanished(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, id *session.Identity,
	v *vanish, res parse.PostListResult, cursor int64, edgeIDs []int64, c map[string]int64) error {
	d := s.D
	cfg := d.Cfg.TT.Posts.Unavailable
	for _, pb := range res.Posts {
		v.seen[pb.Post.PostId] = true
	}
	for _, pid := range edgeIDs {
		v.seen[pid] = true
	}
	if !cfg.Enabled || d.DB == nil || sc.UserId == 0 || len(res.Posts) < cfg.MinPagePosts {
		return nil
	}
	oldest := int64(0)
	for _, pb := range res.Posts {
		if u := pb.Post.CreatedAt.AsTime().Unix(); oldest == 0 || u < oldest {
			oldest = u
		}
	}
	// Strictly inside the span: posts at the oldest post's second may be on
	// the next page, and those at the cursor's second on the one before.
	lo, hi := time.Unix(oldest, 0), time.Unix(cursor/1000, 0)
	if !lo.Before(hi) {
		return nil
	}
	rows, err := d.DB.Query(ctx, `SELECT post_id, created_at, kind = 'photo' FROM posts
		WHERE user_id = $1 AND created_at > $2 AND created_at < $3 AND status = 'visible'
		ORDER BY created_at DESC`, sc.UserId, lo, hi)
	if err != nil {
		return err
	}
	var cands []candidate
	for rows.Next() {
		var k candidate
		if err := rows.Scan(&k.postID, &k.created, &k.photo); err != nil {
			rows.Close()
			return err
		}
		if !v.seen[k.postID] {
			cands = append(cands, k)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(cands) == 0 {
		return nil
	}
	if len(cands) > cfg.MaxPerPage {
		slog.Warn("account: many stored posts missing from one list page; treated as a reading problem",
			"user_id", sc.UserId, "missing", len(cands), "page_posts", len(res.Posts))
		vanishResult(c, VanishReadingProblem, len(cands))
		return nil
	}
	for i, k := range cands {
		if v.stopped || v.checks >= cfg.MaxChecks || sc.Username == "" {
			vanishResult(c, VanishDeferred, len(cands)-i)
			return nil
		}
		v.checks++
		vanishResult(c, s.confirm(ctx, b, t, sc, id, v, k, c), 1)
	}
	return nil
}

// confirm reads a candidate's page and publishes what it shows.
func (s *AccountStage) confirm(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, id *session.Identity,
	v *vanish, k candidate, c map[string]int64) string {
	d := s.D
	cfg := d.Cfg.TT.Posts.Unavailable
	pl, err := d.Loader.Load(ctx, id, postURL(id, sc.Username, k.postID, k.photo))
	if errors.Is(err, session.ErrPaced) || errors.Is(err, session.ErrExitPaused) {
		v.stopped = true
		return VanishDeferred
	}
	d.observeLoad(pl, err, t.Lane)
	c["requests"]++
	if pl != nil {
		c["wire_bytes"] += pl.Bytes
	}
	if err != nil {
		if pl == nil || pl.Class != detect.ContentUnavailable {
			v.stopped = pl != nil && pl.Class != detect.OK
			return VanishError
		}
	}
	switch pl.Class {
	case detect.OK, detect.ContentUnavailable:
	default:
		// A wall or a challenge: the identity is in trouble, not the post.
		v.stopped = true
		return VanishError
	}
	res, _ := parse.PostPage(pl.HTML, parse.Options{AuthorKey: d.Emit.Key, Query: "post_page"})
	src := emit.Source{JobID: t.JobId, Query: "post_page", ParserVersion: parse.Version, Mode: t.Mode}
	switch code := res.StatusCode; {
	case res.Post != nil && res.Post.Post.PostId == k.postID && res.Post.Post.UserId == sc.UserId:
		// Still served: the list missed it. Its page refreshes it (a post
		// made private this way is stored as private).
		if err := d.Emit.Post(ctx, b, src, res.Post.Post); err != nil {
			return VanishError
		}
		if err := d.Emit.Music(ctx, b, src, res.Post.Music); err != nil {
			return VanishError
		}
		return VanishVisible
	case res.Post != nil:
		return VanishError // another post or account: never stored under this one
	case code != 0 && slices.Contains(cfg.Codes, code):
		region := id.Tokens()["region"]
		if err := d.Emit.PostUnavailable(ctx, b, src, k.postID, sc.UserId, k.created, code, region); err != nil {
			return VanishError
		}
		return VanishUnavailable
	case code != 0 && slices.Contains(cfg.AccountCodes, code):
		return VanishAccount
	case code != 0:
		// Not known to mean a post that is gone (10204 is unconfirmed:
		// T34): nothing is marked, and the code goes on the owner's next
		// probe list.
		slog.Info("account: post page answered a statusCode not known to mean a post that is not public",
			"code", strconv.FormatInt(code, 10))
		return VanishUnknownCode
	default:
		return VanishError // a page without the post or a code (a shell: T1)
	}
}
