package stage

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Discovery part C's sources (docs/tiktok/discovery-plan.md §8): what a
// discover task reads (DiscoverScope.source).
const (
	DiscoverSearch  = "search"
	DiscoverHashtag = "hashtag"
	DiscoverSound   = "sound"
	DiscoverRelated = "related"
	DiscoverFeed    = "feed"
)

// Operations of part C's sources (configs/tt/operations.seed.yaml), all
// signed (C6 run 2).
const (
	searchQuery          = "search_general"
	challengeDetailQuery = "challenge_detail"
	challengeListQuery   = "challenge_list"
	musicListQuery       = "music_list"
	relatedQuery         = "related_list"
	feedQuery            = "recommend_list"
)

// Why a discovery page read stopped (discovery_targets.last_stop;
// tks_discover_reads_total).
const (
	// DiscoverStopEnd: the list said no more posts remain (a related list
	// is one page).
	DiscoverStopEnd = "end"
	// DiscoverStopCap: the read's page cap (its source's max_pages, or the
	// feed's reads).
	DiscoverStopCap = "cap"
	// DiscoverStopStuck: TikTok's cursor did not move.
	DiscoverStopStuck = "stuck"
	// DiscoverStopEmpty: a page with no post.
	DiscoverStopEmpty = "empty"
	// DiscoverStopGone: TikTok does not know the hashtag or sound.
	DiscoverStopGone = "gone"
	// DiscoverStopRefused: a status code twice: a page TikTok will not
	// serve this visitor (search from US exits answers 2483, T37).
	DiscoverStopRefused = "refused"
)

// discoverOp is how one source is read.
type discoverOp struct {
	query string
	via   ttv1.DiscoveredVia
	// paged sources walk TikTok's cursor; the feed brings new posts on
	// each read, and a related list is one page.
	paged bool
}

var discoverOps = map[string]discoverOp{
	DiscoverSearch:  {searchQuery, ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH, true},
	DiscoverHashtag: {challengeListQuery, ttv1.DiscoveredVia_DISCOVERED_VIA_HASHTAG, true},
	DiscoverSound:   {musicListQuery, ttv1.DiscoveredVia_DISCOVERED_VIA_SOUND, true},
	DiscoverRelated: {relatedQuery, ttv1.DiscoveredVia_DISCOVERED_VIA_RELATED, false},
	DiscoverFeed:    {feedQuery, ttv1.DiscoveredVia_DISCOVERED_VIA_FEED, false},
}

// DiscoverStage reads one discovery page target (discovery part C,
// docs/tiktok/discovery-plan.md §8): a keyword's search results, a
// hashtag's or a sound's posts, a monitored post's related posts or the
// For You feed, page after page up to the scope's max_pages. Every call is
// signed by TikTok's own script through the signer (C6 run 2), on an
// identity bootstrapped on a monitored account's profile page, as C6
// signed them. Every post found goes out on tt.post.v1 with where it was
// found (Post.find) and its author on tt.account.v1; the persister stores
// them and records each author as a find, which the leader sends to the
// entry check. The read's outcome is written to tt.discovery_targets.
type DiscoverStage struct {
	D *Deps
	// SoftPause overrides softPause (tests).
	SoftPause time.Duration
}

// pageRead is a read's state across its pages.
type pageRead struct {
	stop  string
	code  int64
	posts int64
}

func (s *DiscoverStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.D
	started := time.Now()
	sc := t.GetDiscover()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "discover task without discover scope"}
	}
	op, ok := discoverOps[sc.Source]
	if !ok {
		return Outcome{Result: Fail, Reason: "discover task with unknown source " + sc.Source}
	}
	if !d.Cfg.TT.Discovery.SourceOn(sc.Source) {
		// Switched off since the task was planned (a queued task or a
		// retry): it is dropped, not run (the 2026-11-07 STOP: search and
		// feed on res_iq).
		metrics.DiscoverPages.WithLabelValues(sc.Source, "source_off").Inc()
		return done(map[string]int64{"source_off": 1})
	}
	if sc.Source == DiscoverSearch && sc.Tier == "" {
		// From US exits TikTok asks a logged-out search to log in (T37).
		return Outcome{Result: Fail, Reason: "search without tt.discovery.search.tier"}
	}
	if sc.BootUsername == "" {
		return Outcome{Result: Fail, Reason: "discover task without an account to bootstrap on"}
	}
	var id *session.Identity
	var err error
	if sc.Tier != "" {
		id, err = d.Pool.Acquire(ctx, sc.Tier)
	} else {
		id, err = d.identity(ctx, "list", t.Stalls)
	}
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)

	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	if !id.Bootstrapped() {
		a := &AccountStage{D: d}
		if out, stop := a.bootstrap(ctx, t, &tkstaskv1.AccountScope{Username: sc.BootUsername}, id, st.Cursor, c); stop {
			return out
		}
	}
	referer := profileURL(id, sc.BootUsername)
	lane := topics.LaneName(t.Lane)

	var r pageRead
	if sc.Source == DiscoverHashtag && sc.ItemId == 0 {
		// The tag page's HTML holds no hashtag id (C6 run 2): its detail
		// call names it.
		out, stop := s.challenge(ctx, t, sc, id, referer, &r, c)
		if stop {
			return out
		}
	}

	q, _ := d.Client.Registry.Get(op.query)
	region := regionOf(id)
	src := emit.Source{JobID: t.JobId, Query: op.query, Variant: q.DocID, ParserVersion: parse.Version, Mode: t.Mode, Region: region}
	popt := parse.Options{AuthorKey: d.Emit.Key, Query: op.query, Variant: q.DocID}
	via := sc.Key
	if sc.Source == DiscoverFeed {
		// The feed's finds count by day: an account the feed shows on many
		// days is shown to many viewers.
		via = d.now().Format(time.DateOnly)
	}
	f := fetcherOf(id)
	softRetried := false
	for r.stop == "" {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		page := sc.PagesSeen + int32(st.Pages)
		if int(page) >= int(sc.MaxPages) {
			r.stop = DiscoverStopCap
			break
		}
		offset := st.Cursor
		if offset == "" {
			offset = "0"
		}
		vars := map[string]any{}
		switch sc.Source {
		case DiscoverSearch:
			vars["keyword"], vars["offset"] = sc.Name, offset
			if sc.SearchId != "" {
				vars["search_id"] = sc.SearchId
			}
		case DiscoverHashtag, DiscoverSound:
			vars["id"], vars["offset"] = strconv.FormatInt(sc.ItemId, 10), offset
		case DiscoverRelated:
			vars["item_id"] = sc.Key
		case DiscoverFeed:
			vars["pull"] = int(page) + 1
		}
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: op.query, Vars: vars, PageID: sc.ItemId, Lane: lane, Referer: referer})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			metrics.DiscoverPages.WithLabelValues(sc.Source, "error").Inc()
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.ContentUnavailable {
			r.code, r.stop = codeOf(resp.Errors), DiscoverStopGone
			metrics.DiscoverPages.WithLabelValues(sc.Source, r.stop).Inc()
			break
		}
		if resp.Class == detect.SoftError && len(resp.Errors) > 0 {
			// A status code the rules do not know (search's 2483 from US
			// exits, T37): once more after a pause, then the page is one
			// TikTok will not serve: the read ends, nothing fails.
			if !softRetried {
				softRetried = true
				c["soft_errors"]++
				if err := sleep(ctx, s.pause()); err != nil {
					return retry(t, st.Cursor, "30s", err.Error(), c)
				}
				continue
			}
			r.code, r.stop = codeOf(resp.Errors), DiscoverStopRefused
			metrics.DiscoverPages.WithLabelValues(sc.Source, r.stop).Inc()
			break
		}
		if resp.Class != detect.OK {
			metrics.DiscoverPages.WithLabelValues(sc.Source, "error").Inc()
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		softRetried = false
		res, rep := parse.Found(resp.Body, op.via, popt)
		d.Client.Parsed(resp, rep.OK())
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(op.query, strconv.Itoa(parse.Version), "drift").Inc()
			metrics.DiscoverPages.WithLabelValues(sc.Source, "error").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues(op.query, strconv.Itoa(parse.Version), "ok").Inc()
		if res.StatusCode != 0 {
			// A status the client classed OK (a list TikTok answers with a
			// code it puts in no error): the read ends.
			r.code, r.stop = res.StatusCode, DiscoverStopRefused
			metrics.DiscoverPages.WithLabelValues(sc.Source, r.stop).Inc()
			break
		}
		metrics.DiscoverPages.WithLabelValues(sc.Source, "ok").Inc()
		for k, n := range res.Dropped {
			c["dropped_"+k] += int64(n)
		}

		b := d.Producer.NewBatch()
		for _, a := range res.Authors {
			if err := d.Emit.Account(ctx, b, src, a); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			c["authors"]++
		}
		if _, err := d.media(ctx, b, t, src, res.AuthorMedia, false, f); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		n := 0
		for i, pb := range res.Posts {
			p := pb.Post
			if p.UserId == 0 {
				continue // no author: nobody to find
			}
			p.Find = &ttv1.PostFind{Source: sc.Source, Via: via, Region: region, Page: page, Position: int32(i)}
			if err := d.Emit.Post(ctx, b, src, p); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			if err := d.Emit.Music(ctx, b, src, pb.Music); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			// Metadata only: a discover task's limits download nothing.
			if _, err := d.media(ctx, b, t, src, pb.Media, false, f); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			n++
		}
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		st.Pages++
		c["discover_pages"]++
		c["posts"] += int64(n)
		r.posts += int64(n)
		metrics.DiscoverPosts.WithLabelValues(sc.Source).Add(float64(n))

		switch {
		case n == 0 && len(res.Cards) == 0:
			r.stop = DiscoverStopEmpty
		case !op.paged && sc.Source == DiscoverRelated:
			r.stop = DiscoverStopEnd
		case !op.paged:
			// The feed: the next read brings new posts.
		case !res.HasMore:
			r.stop = DiscoverStopEnd
		case res.Cursor == "" || res.Cursor == offset:
			r.stop = DiscoverStopStuck
		default:
			st.Cursor = res.Cursor
			if res.SearchID != "" {
				sc.SearchId = res.SearchID
			}
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("discover: checkpoint save failed", "err", err)
		}
	}
	if r.stop != "" {
		if err := d.discoverRead(ctx, t, sc, r, int64(st.Pages)); err != nil {
			return retry(t, st.Cursor, "30s", "discovery target: "+err.Error(), c)
		}
		metrics.DiscoverReads.WithLabelValues(sc.Source, r.stop).Inc()
		c["discover_stop_"+r.stop] = 1
		return done(c)
	}
	// Yield: continue the read in a new segment.
	if err := d.discoverProgress(ctx, sc, int64(st.Pages), r.posts); err != nil {
		slog.Warn("discover: target progress not saved", "err", err)
	}
	next := task.Continuation(t, st.Cursor)
	ns := next.GetDiscover()
	ns.PagesSeen = sc.PagesSeen + int32(st.Pages)
	ns.SearchId, ns.ItemId = sc.SearchId, sc.ItemId
	metrics.TaskYields.WithLabelValues("discover").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

// challenge resolves a hashtag's id by its detail call and saves it on the
// target. A hashtag TikTok does not know ends the read (gone).
func (s *DiscoverStage) challenge(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.DiscoverScope, id *session.Identity,
	referer string, r *pageRead, c map[string]int64) (Outcome, bool) {
	d := s.D
	for attempt := 0; ; attempt++ {
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: challengeDetailQuery, Vars: map[string]any{"name": sc.Name},
			Lane: topics.LaneName(t.Lane), Referer: referer})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, "", "30s", err.Error(), c), true
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		switch {
		case resp.Class == detect.SoftError && len(resp.Errors) > 0 && attempt == 0:
			c["soft_errors"]++
			if err := sleep(ctx, s.pause()); err != nil {
				return retry(t, "", "30s", err.Error(), c), true
			}
			continue
		case resp.Class == detect.ContentUnavailable || (resp.Class == detect.SoftError && len(resp.Errors) > 0):
			r.code = codeOf(resp.Errors)
			return s.ended(ctx, t, sc, r, DiscoverStopGone, c)
		case resp.Class != detect.OK:
			return classOutcome(t, resp.Class, resp.Reason, "", c), true
		}
		res, rep := parse.ChallengeDetail(resp.Body)
		d.Client.Parsed(resp, rep.OK())
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(challengeDetailQuery, strconv.Itoa(parse.Version), "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), "", c), true
		}
		metrics.ParseTotal.WithLabelValues(challengeDetailQuery, strconv.Itoa(parse.Version), "ok").Inc()
		if res.ID == 0 {
			r.code = res.StatusCode
			return s.ended(ctx, t, sc, r, DiscoverStopGone, c)
		}
		sc.ItemId = res.ID
		if d.DB != nil {
			if _, err := d.DB.Exec(ctx, `UPDATE discovery_targets SET item_id = $3 WHERE source = $1 AND key = $2`,
				sc.Source, sc.Key, res.ID); err != nil {
				return retry(t, "", "30s", "discovery target: "+err.Error(), c), true
			}
		}
		return Outcome{}, false
	}
}

// ended records a read that stopped before its first page.
func (s *DiscoverStage) ended(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.DiscoverScope, r *pageRead, stop string,
	c map[string]int64) (Outcome, bool) {
	r.stop = stop
	metrics.DiscoverPages.WithLabelValues(sc.Source, stop).Inc()
	if err := s.D.discoverRead(ctx, t, sc, *r, 0); err != nil {
		return retry(t, "", "30s", "discovery target: "+err.Error(), c), true
	}
	metrics.DiscoverReads.WithLabelValues(sc.Source, stop).Inc()
	c["discover_stop_"+stop] = 1
	return done(c), true
}

func (s *DiscoverStage) pause() time.Duration {
	if s.SoftPause > 0 {
		return s.SoftPause
	}
	return softPause
}

// discoverEvery is how often a source's targets are read again; 0 for a
// target read once (a post's related posts).
func (d *Deps) discoverEvery(source string) time.Duration {
	dc := d.Cfg.TT.Discovery
	switch source {
	case DiscoverSearch:
		return dc.Search.Every
	case DiscoverHashtag:
		return dc.Hashtags.Every
	case DiscoverSound:
		return dc.Sounds.Every
	case DiscoverFeed:
		return dc.Feed.Every
	}
	return 0
}

// discoverRead records a read's outcome in tt.discovery_targets: its state,
// what it read and when the target is read next (never again for a related
// list).
func (d *Deps) discoverRead(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.DiscoverScope, r pageRead, pages int64) error {
	if d.DB == nil {
		return nil
	}
	state := "ok"
	switch r.stop {
	case DiscoverStopGone:
		state = "gone"
	case DiscoverStopRefused:
		state = "refused"
	}
	now := d.now()
	var next *time.Time
	if every := d.discoverEvery(sc.Source); every > 0 {
		n := now.Add(every)
		next = &n
	}
	var code *int64
	if r.code != 0 {
		code = &r.code
	}
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets t SET state = $3, status_code = $4, next_read_at = $5, last_job_id = $6::uuid,
			first_read_at = coalesce(t.first_read_at, $7), last_read_at = $7, reads = t.reads + 1,
			pages_read = t.pages_read + $8, posts_seen = t.posts_seen + $9,
			day_pages = CASE WHEN t.day = ($7 AT TIME ZONE 'UTC')::date THEN t.day_pages + $8::int ELSE $8::int END,
			day = ($7 AT TIME ZONE 'UTC')::date, last_stop = $10, claimed_at = NULL
		WHERE source = $1 AND key = $2`,
		sc.Source, sc.Key, state, code, next, nilUUID(t.JobId), now, pages, r.posts, r.stop)
	if err != nil || sc.Source != DiscoverSearch {
		return err
	}
	_, err = d.DB.Exec(ctx, `UPDATE discovery_keywords SET last_searched_at = $2 WHERE id::text = $1`, sc.Key, now)
	return err
}

// discoverProgress saves a yielding segment's pages and posts, so a read
// that dies in a later segment still counts them (and the day's page count
// stays true).
func (d *Deps) discoverProgress(ctx context.Context, sc *tkstaskv1.DiscoverScope, pages, posts int64) error {
	if d.DB == nil {
		return nil
	}
	now := d.now()
	_, err := d.DB.Exec(ctx, `UPDATE discovery_targets t SET pages_read = t.pages_read + $3, posts_seen = t.posts_seen + $4,
			day_pages = CASE WHEN t.day = ($5 AT TIME ZONE 'UTC')::date THEN t.day_pages + $3::int ELSE $3::int END,
			day = ($5 AT TIME ZONE 'UTC')::date
		WHERE source = $1 AND key = $2`, sc.Source, sc.Key, pages, posts, now)
	return err
}
