package stage

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// followQuery is the following and followers lists' operation: unsigned
// (C6 run 1).
const followQuery = "user_list"

// Scenes of /api/user/list/ (public sources; T36): the following list and
// the followers list.
const (
	SceneFollowing = 21
	SceneFollowers = 67
)

// Why a list read stopped (follow_lists.last_stop; tks_follow_reads_total).
const (
	// FollowStopEnd: the list said no more people remain.
	FollowStopEnd = "end"
	// FollowStopKnown: a re-read from the top reached people already
	// stored, and the deep walk had reached the end before.
	FollowStopKnown = "known"
	// FollowStopCap: the read's page cap (tt.discovery.follows.max_pages).
	FollowStopCap = "cap"
	// FollowStopStuck: TikTok's cursor did not move.
	FollowStopStuck = "stuck"
	// FollowStopHidden: the account hides its following list (10222).
	FollowStopHidden = "hidden"
	// FollowStopGone: the account is private, banned or missing.
	FollowStopGone = "gone"
	// FollowStopRefused: another status code twice: a list TikTok will
	// not serve a logged-out visitor (a login wall, T36).
	FollowStopRefused = "refused"
)

// The phases of a list read, in the task's cursor: from the top down to
// the people already stored (head), then from where the last read
// stopped (deep).
const (
	phaseHead = "h"
	phaseDeep = "d"
)

// followCursor is a read's position: its phase and TikTok's cursor.
type followCursor struct{ phase, cursor string }

func (c followCursor) String() string { return c.phase + ":" + c.cursor }

func parseFollowCursor(s string) (followCursor, bool) {
	p, c, ok := strings.Cut(s, ":")
	if !ok || (p != phaseHead && p != phaseDeep) {
		return followCursor{}, false
	}
	return followCursor{p, c}, true
}

// FollowsStage reads one monitored account's following or followers list
// (discovery part D, docs/tiktok/discovery-plan.md §9). TikTok serves the
// newest people first (T41), so a re-read walks from the top until
// tt.discovery.follows.known_pages pages in a row hold nobody already
// stored; a list not yet read to its end then goes deeper, from where the
// last read stopped. A read fetches at most its scope's max_pages. Every
// person read goes out on tt.follow.v1; the persister keeps the links and
// sends the people new to the list to discovery's entry check. The read's
// outcome (state, deep cursor, next read) is written to tt.follow_lists.
type FollowsStage struct {
	D *Deps
	// SoftPause overrides softPause (tests).
	SoftPause time.Duration
}

// listRead is a read's state across its pages.
type listRead struct {
	pos      followCursor
	deep     string // where the deep walk is
	deepDone bool
	stop     string
	code     int64
	total    *int64
}

func (s *FollowsStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.D
	started := time.Now()
	sc := t.GetFollows()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "follows task without follow scope"}
	}
	scene, dir := SceneFollowers, "follower"
	switch sc.Direction {
	case ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING:
		scene, dir = SceneFollowing, "following"
	case ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER:
	default:
		return Outcome{Result: Fail, Reason: "follows task without direction"}
	}
	if sc.SecUid == "" || sc.UserId == 0 {
		return Outcome{Result: Fail, Reason: "follows task without the account's ids"}
	}
	id, err := d.identity(ctx, "list", t.Stalls)
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
	r := listRead{deep: sc.DeepCursor, deepDone: sc.DeepDone}
	if pos, ok := parseFollowCursor(st.Cursor); ok {
		r.pos = pos
	} else if sc.ReadBefore {
		r.pos = followCursor{phaseHead, ""}
	} else {
		r.pos = followCursor{phaseDeep, sc.DeepCursor}
	}
	if r.pos.phase == phaseDeep {
		r.deep = r.pos.cursor
	}
	if !id.Bootstrapped() {
		a := &AccountStage{D: d}
		if out, stop := a.bootstrap(ctx, t, &tkstaskv1.AccountScope{UserId: sc.UserId, SecUid: sc.SecUid, Username: sc.Username},
			id, st.Cursor, c); stop {
			return out
		}
	}

	fl := d.Cfg.TT.Discovery.Follows
	q, _ := d.Client.Registry.Get(followQuery)
	src := emit.Source{JobID: t.JobId, Query: followQuery, Variant: q.DocID, ParserVersion: parse.Version, Mode: t.Mode, Region: regionOf(id)}
	popt := parse.Options{AuthorKey: d.Emit.Key, Query: followQuery, Variant: q.DocID}
	softRetried := false
	for r.stop == "" {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		if int(sc.PagesSeen)+st.Pages >= int(sc.MaxPages) {
			r.stop = FollowStopCap
			break
		}
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: followQuery,
			Vars:   map[string]any{"sec_uid": sc.SecUid, "scene": scene, "count": fl.PageSize, "cursor": parse.FollowCursor(r.pos.cursor)},
			PageID: sc.UserId, Lane: topics.LaneName(t.Lane), Referer: profileURL(id, sc.Username)})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			metrics.FollowPages.WithLabelValues(dir, "error").Inc()
			return retry(t, r.pos.String(), "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.ContentUnavailable {
			r.code = codeOf(resp.Errors)
			r.stop = FollowStopGone
			if r.code == ttdetect.StatusPrivateAccount && scene == SceneFollowing {
				r.stop = FollowStopHidden
			}
			metrics.FollowPages.WithLabelValues(dir, r.stop).Inc()
			break
		}
		if resp.Class == detect.SoftError && len(resp.Errors) > 0 {
			// A status code the rules do not know (fakett's login answer;
			// TikTok's own is unknown, T36): once more after a pause, then
			// the list is one TikTok will not serve: the read ends, nothing
			// fails.
			if !softRetried {
				softRetried = true
				c["soft_errors"]++
				if err := sleep(ctx, s.pause()); err != nil {
					return retry(t, r.pos.String(), "30s", err.Error(), c)
				}
				continue
			}
			r.code, r.stop = codeOf(resp.Errors), FollowStopRefused
			metrics.FollowPages.WithLabelValues(dir, r.stop).Inc()
			break
		}
		if resp.Class != detect.OK {
			metrics.FollowPages.WithLabelValues(dir, "error").Inc()
			return classOutcome(t, resp.Class, resp.Reason, r.pos.String(), c)
		}
		softRetried = false
		res, rep := parse.FollowList(resp.Body, popt)
		d.Client.Parsed(resp, rep.OK())
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues(followQuery, f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(followQuery, strconv.Itoa(parse.Version), "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), r.pos.String(), c)
		}
		metrics.ParseTotal.WithLabelValues(followQuery, strconv.Itoa(parse.Version), "ok").Inc()
		metrics.FollowPages.WithLabelValues(dir, "ok").Inc()
		for k, n := range res.Dropped {
			c["dropped_"+k] += int64(n)
		}
		if res.Total != nil {
			r.total = res.Total
		}

		page := sc.PagesSeen + int32(st.Pages)
		b := d.Producer.NewBatch()
		ids := make([]int64, 0, len(res.People))
		for i, e := range res.People {
			if e.Person.GetUserId() == sc.UserId {
				continue // never the account itself
			}
			f := &ttv1.Follow{UserId: sc.UserId, Direction: sc.Direction, Person: e.Person, Private: e.Private,
				VideoCount: e.Videos, FollowerCount: e.Followers, FollowingCount: e.Follows, Page: page, Position: int32(i)}
			if err := d.Emit.Follow(ctx, b, src, f); err != nil {
				return retry(t, r.pos.String(), "30s", err.Error(), c)
			}
			ids = append(ids, e.Person.GetUserId())
		}
		if err := b.Wait(ctx); err != nil {
			return retry(t, r.pos.String(), "30s", "produce: "+err.Error(), c)
		}
		st.Pages++
		c["follow_pages"]++
		c["follows"] += int64(len(ids))

		// The next position.
		next := res.Cursor
		moved := next != "" && next != r.pos.cursor
		switch r.pos.phase {
		case phaseHead:
			known, err := d.knownFollows(ctx, sc.UserId, dir, ids)
			if err != nil {
				return retry(t, r.pos.String(), "30s", "follows: "+err.Error(), c)
			}
			if len(ids) > 0 && known == len(ids) {
				st.Stuck++
			} else {
				st.Stuck = 0
			}
			switch {
			case !res.HasMore:
				// Read from the top to the end: the whole list.
				r.deepDone, r.stop = true, FollowStopEnd
			case st.Stuck >= fl.KnownPages && r.deepDone:
				r.stop = FollowStopKnown
			case st.Stuck >= fl.KnownPages:
				// Down to known people: on with the deep walk.
				r.pos, st.Stuck = followCursor{phaseDeep, r.deep}, 0
			case !moved:
				r.stop = FollowStopStuck
			default:
				r.pos.cursor = next
			}
		case phaseDeep:
			switch {
			case !res.HasMore:
				r.deepDone, r.stop = true, FollowStopEnd
				r.deep = ""
			case !moved:
				r.stop = FollowStopStuck
			default:
				r.pos.cursor, r.deep = next, next
			}
		}
		st.Cursor = r.pos.String()
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("follows: checkpoint save failed", "err", err)
		}
	}
	if r.stop != "" {
		if err := d.followListRead(ctx, t, sc, dir, r, int64(st.Pages), c); err != nil {
			return retry(t, st.Cursor, "30s", "follow list: "+err.Error(), c)
		}
		metrics.FollowReads.WithLabelValues(dir, r.stop).Inc()
		c["follow_stop_"+r.stop] = 1
		return done(c)
	}
	// Yield: continue the read in a new segment.
	if err := d.followListProgress(ctx, sc, dir, r, int64(st.Pages), c["follows"]); err != nil {
		slog.Warn("follows: list progress not saved", "err", err)
	}
	next := task.Continuation(t, st.Cursor)
	ns := next.GetFollows()
	ns.PagesSeen = sc.PagesSeen + int32(st.Pages)
	ns.DeepCursor, ns.DeepDone = r.deep, r.deepDone
	metrics.TaskYields.WithLabelValues("follows").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

func (s *FollowsStage) pause() time.Duration {
	if s.SoftPause > 0 {
		return s.SoftPause
	}
	return softPause
}

// codeOf is the first status code of an answer's errors.
func codeOf(errs []detect.GQLError) int64 {
	for _, e := range errs {
		if e.Code != 0 {
			return e.Code
		}
	}
	return 0
}

// knownFollows counts the people of ids already stored on the list.
func (d *Deps) knownFollows(ctx context.Context, userID int64, dir string, ids []int64) (int, error) {
	if d.DB == nil || len(ids) == 0 {
		return 0, nil
	}
	var n int
	err := d.DB.QueryRow(ctx, `SELECT count(*) FROM follows WHERE user_id = $1 AND direction = $2 AND person_user_id = ANY($3)`,
		userID, dir, ids).Scan(&n)
	return n, err
}

// followListRead records a read's outcome in tt.follow_lists: its state,
// where the deep walk is, and when the list is read next.
func (d *Deps) followListRead(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.FollowScope, dir string, r listRead, pages int64,
	c map[string]int64) error {
	if d.DB == nil {
		return nil
	}
	fl := d.Cfg.TT.Discovery.Follows
	every := fl.FollowersEvery
	if dir == "following" {
		every = fl.FollowingEvery
	}
	state := "ok"
	switch r.stop {
	case FollowStopHidden:
		state = "hidden"
	case FollowStopGone:
		state = "gone"
	case FollowStopRefused:
		state = "refused"
	}
	now := d.now()
	var code *int64
	if r.code != 0 {
		code = &r.code
	}
	_, err := d.DB.Exec(ctx, `INSERT INTO follow_lists AS l (user_id, direction, state, status_code, next_read_at, last_job_id,
			first_read_at, last_read_at, deep_cursor, deep_done, total, people_seen, pages_read, day, day_pages, last_stop)
		VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $7, $8, $9, $10, $11, $13::bigint, ($7 AT TIME ZONE 'UTC')::date, $13::int, $12)
		ON CONFLICT (user_id, direction) DO UPDATE SET
			state = EXCLUDED.state, status_code = EXCLUDED.status_code, next_read_at = EXCLUDED.next_read_at,
			first_read_at = COALESCE(l.first_read_at, EXCLUDED.first_read_at), last_read_at = EXCLUDED.last_read_at,
			deep_cursor = CASE WHEN EXCLUDED.state = 'ok' THEN EXCLUDED.deep_cursor ELSE l.deep_cursor END,
			deep_done = CASE WHEN EXCLUDED.state = 'ok' THEN EXCLUDED.deep_done ELSE l.deep_done END,
			total = COALESCE(EXCLUDED.total, l.total),
			people_seen = l.people_seen + EXCLUDED.people_seen,
			pages_read = l.pages_read + EXCLUDED.pages_read,
			day = EXCLUDED.day,
			day_pages = CASE WHEN l.day = EXCLUDED.day THEN l.day_pages + EXCLUDED.day_pages ELSE EXCLUDED.day_pages END,
			last_stop = EXCLUDED.last_stop, claimed_at = NULL`,
		sc.UserId, dir, state, code, now.Add(every), nilUUID(t.JobId), now, nilIfEmpty(r.deep), r.deepDone, r.total,
		c["follows"], r.stop, pages)
	return err
}

// followListProgress saves a yielding segment's pages and people, and
// where the deep walk is, so a read that dies in a later segment still
// keeps them (and the day's page count stays true).
func (d *Deps) followListProgress(ctx context.Context, sc *tkstaskv1.FollowScope, dir string, r listRead, pages, people int64) error {
	if d.DB == nil {
		return nil
	}
	now := d.now()
	_, err := d.DB.Exec(ctx, `INSERT INTO follow_lists AS l (user_id, direction, deep_cursor, pages_read, people_seen, day, day_pages)
		VALUES ($1, $2, $3, $4::bigint, $6, ($5 AT TIME ZONE 'UTC')::date, $4::int)
		ON CONFLICT (user_id, direction) DO UPDATE SET deep_cursor = COALESCE(EXCLUDED.deep_cursor, l.deep_cursor),
			pages_read = l.pages_read + EXCLUDED.pages_read, people_seen = l.people_seen + EXCLUDED.people_seen,
			day_pages = CASE WHEN l.day = EXCLUDED.day THEN l.day_pages + EXCLUDED.day_pages ELSE EXCLUDED.day_pages END,
			day = EXCLUDED.day`, sc.UserId, dir, nilIfEmpty(r.deep), pages, now, people)
	return err
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nilUUID(s string) *string {
	if len(s) != 36 {
		return nil
	}
	return &s
}
