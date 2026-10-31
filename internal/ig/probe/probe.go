// Package probe answers the open questions the build cannot answer without
// real Instagram traffic (brief §3, checkpoint C2). The owner runs it from
// their own machine, once direct and once per proxy tier:
//
//   - U1: does the grid keep paging past the 24 posts the browser showed,
//     and how deep before a wall?
//   - U3: do queries work without the browser's client-state fields? Every
//     probe query is sent without them.
//   - U5: is a grid cursor bound to the identity that received it?
//   - U8: which cookies does a page load set?
//   - U9: does the grid query answer by user pk alone, with no cursor?
//
// It also reports which page tokens were found, and every request's class,
// latency and wire bytes.
//
// Beside the answers, the report holds what the probe read (owner's
// decision, 2026-10-31): each profile's public details, its posts' links,
// captions and thumbnails, the newest post's first comments, and the
// profile picture saved as a file. That is personal data: the command
// writes it 0600 under phase0/, which git ignores, and it is never
// committed.
package probe

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Options configure a run.
type Options struct {
	BaseURL  string   // https://www.instagram.com
	Profiles []string // usernames, in the owner's file order
	Tier     string   // proxy tier ("direct" for none)
	// MaxGridPages caps the U1 walk after the first grid page (12 posts each).
	MaxGridPages int
	// MaxAge, when set, stops the walk once the posts read are older than
	// this (judged by the time their pk encodes): the backfill window.
	MaxAge time.Duration
	Pool   *session.Pool
	Loader *session.Loader
	GQL    *graphql.Client
	// Fingerprint names the profile the pool uses, for the report.
	Fingerprint string
	Version     string
	// ImageDir, when set, receives each profile picture as <username>.jpg.
	ImageDir string
	// RawDir, when set, receives Instagram's raw answers, to check their
	// shape: per profile the first response of each query and every
	// response that did not answer OK or held no items, and the page HTML of
	// the first RawPages profiles.
	RawDir   string
	RawPages int
	// Progress, when set, gets one line per finished profile.
	Progress func(string)
	Now      func() time.Time
}

// Step is one request's outcome.
type Step struct {
	Class     string `json:"class"`
	Status    int    `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Ms        int64  `json:"ms"`
	WireBytes int64  `json:"wire_bytes"`
	Items     int    `json:"items,omitempty"`
	HasNext   bool   `json:"has_next,omitempty"`
	// Errors are the GraphQL error messages the answer carried beside its
	// data (an answer classed OK can hold some), each once.
	Errors []string `json:"errors,omitempty"`
}

// Grid is the U1 walk of one profile.
type Grid struct {
	Pages int `json:"pages"` // after the first
	Posts int `json:"posts"` // including the first page
	// Stop is "end" (has_next_page false), "cap" (MaxGridPages), "age"
	// (MaxAge reached), "stopped" (the operator stopped the run), or
	// "wall:<class>" for the first page that did not answer.
	Stop     string `json:"stop"`
	PastPage bool   `json:"past_24"` // read more than 24 posts
	// Oldest is when the oldest post read was posted (from its pk).
	Oldest time.Time `json:"oldest,omitzero"`
	Failed *Step     `json:"failed,omitempty"`
}

// ProfileData is what the profile query returned.
type ProfileData struct {
	PK                 string      `json:"pk"`
	FullName           string      `json:"full_name"`
	Biography          string      `json:"biography"`
	BioLinks           []Link      `json:"bio_links,omitempty"`
	Verified           bool        `json:"is_verified"`
	Private            bool        `json:"is_private"`
	Memorialized       bool        `json:"is_memorialized"`
	Followers          int64       `json:"follower_count"`
	Following          int64       `json:"following_count"`
	ProfilePicURL      string      `json:"profile_pic_url"`
	ProfilePicFile     string      `json:"profile_pic_file,omitempty"`
	LinkedFacebookPage string      `json:"linked_facebook_page,omitempty"`
	HasClips           bool        `json:"has_clips"`
	Highlights         []Highlight `json:"highlights,omitempty"`
}

// Link is a bio link.
type Link struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url"`
}

// Highlight is one highlights reel; Items is known for the one the probe read.
type Highlight struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Items int    `json:"items,omitempty"`
}

// Post is one grid item. Grid items carry no time or counts: those need the
// post page (U2).
type Post struct {
	PK        string `json:"pk"`
	Shortcode string `json:"shortcode"`
	URL       string `json:"url"`
	MediaType string `json:"media_type"`
	// PostedAt is when the post was made, from the time its pk encodes
	// (within about a minute of the post page's taken_at).
	PostedAt     time.Time `json:"posted_at,omitzero"`
	ProductType  string    `json:"product_type,omitempty"`
	Caption      string    `json:"caption,omitempty"`
	AltText      string    `json:"alt_text,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
}

// PostPage is what a post page held.
type PostPage struct {
	Step
	URL              string    `json:"url"`
	TakenAt          time.Time `json:"taken_at,omitzero"`
	LikeCount        *int64    `json:"like_count,omitempty"`
	CommentCount     *int64    `json:"comment_count,omitempty"`
	CommentsEmbedded int       `json:"comments_embedded"`
}

// Comment is one top-level comment of the newest post.
type Comment struct {
	PostShortcode string    `json:"post_shortcode"`
	PK            string    `json:"pk"`
	Username      string    `json:"username"`
	Verified      bool      `json:"is_verified"`
	Text          string    `json:"text"`
	CreatedAt     time.Time `json:"created_at"`
	Likes         int64     `json:"likes"`
}

// Profile is one profile's results; Index is its 1-based line in the list.
type Profile struct {
	Index       int             `json:"index"`
	Username    string          `json:"username"`
	URL         string          `json:"url"`
	Data        *ProfileData    `json:"data,omitempty"`
	Posts       []Post          `json:"posts,omitempty"`
	Comments    []Comment       `json:"comments,omitempty"`
	Bootstrap   Step            `json:"bootstrap"`
	TokensFound map[string]bool `json:"tokens_found,omitempty"`
	Cookies     []string        `json:"cookies,omitempty"`
	CSRFCookie  bool            `json:"csrf_cookie,omitempty"`
	Profile     *Step           `json:"profile,omitempty"`
	GridFirst   *Step           `json:"grid_first,omitempty"`
	Grid        *Grid           `json:"grid,omitempty"`
	OtherCursor *Step           `json:"cursor_other_identity,omitempty"`
	// OtherBootstrap is the page load of the second identity, when this
	// profile was the one that tried to start it.
	OtherBootstrap *Step `json:"other_identity_bootstrap,omitempty"`
	// NewestPost is the newest post's own page: what it holds that the grid
	// does not (U2's baseline), and its embedded first comments.
	NewestPost   *PostPage `json:"newest_post_page,omitempty"`
	GridByPK     *Step     `json:"grid_by_pk_no_cursor,omitempty"`
	CommentsRead *Step     `json:"comments_read,omitempty"`
	Highlights   *Step     `json:"highlights,omitempty"`
	NoHighlights bool      `json:"no_highlights,omitempty"`
	// Stopped marks the profile being read when the operator stopped the
	// run (Ctrl-C): what it holds is partial, and its unanswered steps
	// count for no question.
	Stopped bool `json:"stopped,omitempty"`
}

// Report is the whole run.
type Report struct {
	Version     string    `json:"version"`
	Tier        string    `json:"tier"`
	Fingerprint string    `json:"fingerprint"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Profiles    []Profile `json:"profiles"`
	Summary     Summary   `json:"summary"`
}

// Summary answers the questions over all profiles.
type Summary struct {
	Profiles int `json:"profiles"`
	// StoppedEarly: the operator stopped the run (Ctrl-C) before the last
	// profile; the last profile in the report is partial.
	StoppedEarly bool           `json:"stopped_early,omitempty"`
	Bootstrapped int            `json:"bootstrapped"`
	TokensFound  map[string]int `json:"tokens_found"`
	// U1
	GridEnds       int            `json:"grid_ends"`
	GridCaps       int            `json:"grid_caps"`
	GridAgeStops   int            `json:"grid_age_stops"`
	GridWalls      map[string]int `json:"grid_walls"`
	PagedPast24    int            `json:"paged_past_24"`
	MaxPosts       int            `json:"max_posts"`
	WalledAtPosts  []int          `json:"walled_at_posts,omitempty"`
	GridFirstAlone int            `json:"grid_first_only"` // a first page with no next page
	// U3: queries sent without client-state fields, and how they answered.
	QueriesWithoutClientState map[string]int `json:"queries_without_client_state"`
	// U5
	CursorPortable    int `json:"cursor_portable"`
	CursorNotPortable int `json:"cursor_not_portable"`
	// U8
	Cookies    map[string]int `json:"cookies"`
	CSRFCookie int            `json:"csrf_cookie"`
	// U9
	GridByPKWorks int `json:"grid_by_pk_works"`
	GridByPKFails int `json:"grid_by_pk_fails"`
	// Cost: per request kind, count, mean latency and mean wire bytes.
	Requests map[string]Cost `json:"requests"`
}

// Cost aggregates one request kind.
type Cost struct {
	N         int   `json:"n"`
	MeanMs    int64 `json:"mean_ms"`
	MeanBytes int64 `json:"mean_wire_bytes"`
	sumMs     int64
	sumBytes  int64
}

const (
	// stopped is the class of a step cut short by the operator (Ctrl-C),
	// and the grid's stop when that happens mid-walk.
	stopped = "stopped"
	// progressEvery is how often, in grid pages, a long walk reports
	// progress (about every 30 seconds at the probe's pace).
	progressEvery = 10
)

// Run probes every profile and returns the report.
func Run(ctx context.Context, o Options) Report {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxGridPages <= 0 {
		o.MaxGridPages = 32
	}
	r := &runner{o: o, sum: Summary{TokensFound: map[string]int{}, GridWalls: map[string]int{},
		QueriesWithoutClientState: map[string]int{}, Cookies: map[string]int{}, Requests: map[string]Cost{}}}
	rep := Report{Version: o.Version, Tier: o.Tier, Fingerprint: o.Fingerprint, StartedAt: o.Now().UTC()}
	for i, username := range o.Profiles {
		if ctx.Err() != nil {
			break
		}
		p := r.profile(ctx, i+1, username)
		if ctx.Err() != nil {
			p.Stopped, r.sum.StoppedEarly = true, true
		}
		rep.Profiles = append(rep.Profiles, p)
		if o.Progress != nil {
			line := fmt.Sprintf("profile %d/%d %s: bootstrap %s, grid %s", i+1, len(o.Profiles), username, p.Bootstrap.Class, gridLine(p.Grid))
			if p.Stopped {
				line = fmt.Sprintf("profile %d/%d %s: stopped (Ctrl-C) after %d posts; the report keeps what was read", i+1, len(o.Profiles), username, len(p.Posts))
			}
			o.Progress(line)
		}
	}
	if r.second != nil {
		o.Pool.Release(r.second)
	}
	for k, c := range r.sum.Requests {
		c.MeanMs, c.MeanBytes = c.sumMs/int64(c.N), c.sumBytes/int64(c.N)
		r.sum.Requests[k] = c
	}
	r.sum.Profiles = len(rep.Profiles)
	slices.Sort(r.sum.WalledAtPosts)
	rep.Summary = r.sum
	rep.FinishedAt = o.Now().UTC()
	return rep
}

type runner struct {
	o      Options
	sum    Summary
	second *session.Identity // the U5 identity, bootstrapped once
	// The profile being read, for the raw answers.
	rawDir   string
	rawSeq   int
	rawSaved map[string]bool
	pages    int // page HTML files saved
}

func (r *runner) profile(ctx context.Context, index int, username string) Profile {
	page := strings.TrimRight(r.o.BaseURL, "/") + "/" + username + "/"
	out := Profile{Index: index, Username: username, URL: page}
	r.rawSeq, r.rawSaved = 0, map[string]bool{}
	if r.o.RawDir != "" {
		r.rawDir = filepath.Join(r.o.RawDir, fmt.Sprintf("%03d-%s", index, unsafeName.ReplaceAllString(username, "_")))
	}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		out.Bootstrap = Step{Class: "no_identity", Reason: err.Error()}
		return out
	}
	defer r.o.Pool.Release(id)
	start := time.Now()
	pl, err := r.o.Loader.Load(ctx, id, page)
	out.Bootstrap = Step{Ms: time.Since(start).Milliseconds()}
	if err != nil {
		out.Bootstrap.Class, out.Bootstrap.Reason = detect.Transport.String(), redact(err.Error())
		if ctx.Err() != nil {
			out.Bootstrap.Class, out.Bootstrap.Reason = stopped, "stopped by the operator"
		}
		return out
	}
	out.Bootstrap.Class, out.Bootstrap.Status, out.Bootstrap.WireBytes = pl.Class.String(), pl.Status, pl.Bytes
	out.Bootstrap.Reason = pl.Reason
	r.cost("page", out.Bootstrap)
	if r.rawDir != "" && pl.Class == detect.OK && r.pages < r.o.RawPages {
		r.pages++
		r.saveRaw("page.html", pl.HTML)
	}
	if pl.Class == detect.OK {
		out.TokensFound = htmlx.Found(htmlx.ExtractTokens(pl.HTML))
		for k, ok := range out.TokensFound {
			if ok {
				r.sum.TokensFound[k]++
			}
		}
		// The identity's cookies after a good page load (U8), names only.
		out.Cookies = slices.Sorted(maps.Keys(id.Doer.Cookies(id.Base)))
		out.CSRFCookie = id.Doer.Cookie(id.Base, "csrftoken") != ""
		for _, c := range out.Cookies {
			r.sum.Cookies[c]++
		}
		if out.CSRFCookie {
			r.sum.CSRFCookie++
		}
	}
	// A bootstrap is this page load giving tokens. An identity reused from
	// an earlier profile still holds its tokens and goes on querying, as
	// production identities do.
	if pl.Class == detect.OK && out.TokensFound["lsd"] {
		r.sum.Bootstrapped++
	}
	if !id.Bootstrapped() {
		return out
	}

	// Profile: the user pk and the highlights list.
	resp, s := r.do(ctx, id, "profile", map[string]any{"username": username}, page)
	out.Profile = ptr(s)
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	user := first(resp.Parts, "data.xig_user_by_username")
	if !user.Exists() {
		user = findAll(resp.Parts, "xig_user_by_username")
	}
	pk := user.Get("pk").String()
	if pk == "" {
		r.keepRaw("profile", resp, 0)
		out.Profile.Reason = "no user in the response"
		return out
	}
	r.keepRaw("profile", resp, 1)
	out.Data = profileData(user)
	// The grid's _connection query takes the user object's id (17841...),
	// not its pk: with the pk it answers a null node (second probe run).
	userID := user.Get("id").String()
	if userID == "" {
		userID = pk
	}
	if len(out.Data.Highlights) == 0 {
		// The first run found no highlights on any profile: the list may
		// come in another part of the answer.
		for _, e := range findAll(resp.Parts, "lox_highlights_connection").Get("edges").Array() {
			out.Data.Highlights = append(out.Data.Highlights, Highlight{ID: e.Get("node.id").String(), Title: e.Get("node.title").String()})
		}
	}
	if r.o.ImageDir != "" && out.Data.ProfilePicURL != "" {
		out.Data.ProfilePicFile = r.saveImage(ctx, id, out.Data.ProfilePicURL, username, page)
	}
	var highlight string
	if len(out.Data.Highlights) > 0 {
		highlight = out.Data.Highlights[0].ID
	}

	// First grid page, by username.
	resp, s = r.do(ctx, id, "grid_first", map[string]any{"username": username}, page)
	out.GridFirst = ptr(s)
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	edges, pi := connection(resp.Parts, gridConn)
	out.GridFirst.Items, out.GridFirst.HasNext = len(edges), pi.HasNext
	r.keepRaw("grid_first", resp, len(edges))
	cursor := pi.EndCursor
	out.Posts = append(out.Posts, r.posts(edges)...)
	var mediaPK, newest, newestURL string
	if len(out.Posts) > 0 {
		mediaPK, newest, newestURL = out.Posts[0].PK, out.Posts[0].Shortcode, out.Posts[0].URL
	}

	// U5: the first cursor, from another identity.
	if cursor != "" {
		if other := r.otherIdentity(ctx, page, &out); other != nil {
			resp, s := r.do(ctx, other, "grid", map[string]any{"user_id": userID, "cursor": cursor}, page)
			s.Items = gridItems(resp)
			r.keepRaw("grid_other_identity", resp, s.Items)
			out.OtherCursor = ptr(s)
			switch {
			case s.Class == stopped:
			case s.Class == detect.OK.String() && s.Items > 0:
				r.sum.CursorPortable++
			default:
				r.sum.CursorNotPortable++
			}
		}
	}

	// U1: page the grid as far as it goes.
	out.Grid = r.walk(ctx, id, userID, cursor, &out, page)
	if ctx.Err() != nil {
		return out // stopped: the remaining steps would only fail
	}

	// U9: the grid by the user's id with no cursor (an absent cursor
	// renders as null; "" fails).
	resp, s = r.do(ctx, id, "grid", map[string]any{"user_id": userID}, page)
	s.Items = gridItems(resp)
	r.keepRaw("grid_by_pk", resp, s.Items)
	out.GridByPK = ptr(s)
	switch {
	case s.Class == stopped:
		return out
	case s.Class == detect.OK.String() && s.Items > 0:
		r.sum.GridByPKWorks++
	default:
		r.sum.GridByPKFails++
	}

	// Comments of the newest post, and the first highlight.
	if mediaPK != "" {
		resp, s = r.do(ctx, id, "comments", map[string]any{"media_pk": mediaPK}, page)
		cedges, cpi := connection(respParts(resp), "comments_connection")
		s.Items, s.HasNext = len(cedges), cpi.HasNext
		r.keepRaw("comments", resp, s.Items)
		out.CommentsRead = ptr(s)
		for _, e := range cedges {
			n := e.Get("node")
			out.Comments = append(out.Comments, Comment{PostShortcode: newest, PK: n.Get("pk").String(),
				Username: n.Get("user.username").String(), Verified: n.Get("user.is_verified").Bool(), Text: n.Get("text").String(),
				CreatedAt: time.Unix(n.Get("created_at").Int(), 0).UTC(), Likes: n.Get("comment_like_count").Int()})
		}
	}
	// The newest post's page: time and counts the grid lacks, and the first
	// comments, which stand in when the comments query gave none.
	if newestURL != "" {
		out.NewestPost = r.postPage(ctx, id, newestURL, newest, &out)
	}
	if highlight != "" {
		resp, s = r.do(ctx, id, "highlights", map[string]any{"reel_ids": []string{highlight}, "initial_reel_id": highlight}, page)
		s.Items = len(findAll(respParts(resp), "items").Array())
		r.keepRaw("highlights", resp, s.Items)
		out.Highlights = ptr(s)
		out.Data.Highlights[0].Items = s.Items
	} else {
		out.NoHighlights = true
	}
	return out
}

func (r *runner) walk(ctx context.Context, id *session.Identity, userID, cursor string, out *Profile, page string) *Grid {
	firstPage := out.GridFirst
	g := &Grid{Posts: firstPage.Items, Stop: "end"}
	// A first page that names a cursor is followed even when it says no
	// next page, so the answer comes from what the next query returns.
	if !firstPage.HasNext && cursor == "" {
		r.sum.GridEnds++
		r.sum.GridFirstAlone++
		r.sum.MaxPosts = max(r.sum.MaxPosts, g.Posts)
		return g
	}
	for cursor != "" {
		if g.Pages >= r.o.MaxGridPages {
			g.Stop = "cap"
			break
		}
		resp, s := r.do(ctx, id, "grid", map[string]any{"user_id": userID, "cursor": cursor}, page)
		if s.Class == stopped {
			g.Stop = stopped
			break
		}
		if resp == nil || resp.Class != detect.OK {
			g.Stop, g.Failed = "wall:"+s.Class, &s
			break
		}
		edges, pi := connection(resp.Parts, gridConn)
		r.keepRaw("grid", resp, len(edges))
		if len(edges) == 0 {
			break // nothing more, whatever page_info says
		}
		g.Pages++
		g.Posts += len(edges)
		out.Posts = append(out.Posts, r.posts(edges)...)
		if r.o.Progress != nil && g.Pages%progressEvery == 0 {
			line := fmt.Sprintf("  %s: %d posts so far", out.Username, g.Posts)
			if t := out.Posts[len(out.Posts)-1].PostedAt; !t.IsZero() {
				line += ", back to " + t.Format("2006-01-02")
			}
			r.o.Progress(line)
		}
		if r.o.MaxAge > 0 {
			if last := out.Posts[len(out.Posts)-1].PostedAt; !last.IsZero() && last.Before(r.o.Now().Add(-r.o.MaxAge)) {
				g.Stop = "age"
				break
			}
		}
		next := ""
		if pi.HasNext || pi.EndCursor != cursor {
			next = pi.EndCursor
		}
		if next == cursor {
			next = ""
		}
		cursor = next
	}
	g.PastPage = g.Posts > 24
	if n := len(out.Posts); n > 0 {
		g.Oldest = out.Posts[n-1].PostedAt
	}
	switch g.Stop {
	case "end":
		r.sum.GridEnds++
	case "cap":
		r.sum.GridCaps++
	case "age":
		r.sum.GridAgeStops++
	case stopped:
	default:
		r.sum.GridWalls[strings.TrimPrefix(g.Stop, "wall:")]++
		r.sum.WalledAtPosts = append(r.sum.WalledAtPosts, g.Posts)
	}
	if g.PastPage {
		r.sum.PagedPast24++
	}
	r.sum.MaxPosts = max(r.sum.MaxPosts, g.Posts)
	return g
}

// otherIdentity is a second identity for U5, bootstrapped on the first
// profile page it is needed for and reused after.
func (r *runner) otherIdentity(ctx context.Context, page string, out *Profile) *session.Identity {
	if r.second != nil {
		return r.second
	}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		out.OtherBootstrap = &Step{Class: "no_identity", Reason: err.Error()}
		return nil
	}
	start := time.Now()
	pl, err := r.o.Loader.Bootstrap(ctx, id, page)
	st := &Step{Ms: time.Since(start).Milliseconds()}
	if pl != nil {
		st.Class, st.Status, st.WireBytes, st.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	}
	if err != nil {
		if st.Class == "" {
			st.Class = detect.Transport.String()
		}
		st.Reason = redact(err.Error())
		if ctx.Err() != nil {
			st.Class, st.Reason = stopped, "stopped by the operator"
		}
		out.OtherBootstrap = st
		r.o.Pool.Release(id)
		return nil
	}
	out.OtherBootstrap = st
	r.second = id
	return id
}

// postPage loads a post's page and reads what it holds: the post's time,
// like and comment counts, and the embedded first comments (used for
// Comments when the comments query gave none).
func (r *runner) postPage(ctx context.Context, id *session.Identity, url, shortcode string, out *Profile) *PostPage {
	pp := &PostPage{URL: url}
	start := time.Now()
	pl, err := r.o.Loader.Load(ctx, id, url)
	pp.Ms = time.Since(start).Milliseconds()
	if err != nil {
		pp.Class, pp.Reason = detect.Transport.String(), redact(err.Error())
		r.cost("post_page", pp.Step)
		return pp
	}
	pp.Class, pp.Status, pp.WireBytes, pp.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	r.cost("post_page", pp.Step)
	if pl.Class != detect.OK {
		return pp
	}
	if r.rawDir != "" && r.pages <= r.o.RawPages {
		r.saveRaw("post-page.html", pl.HTML)
	}
	blobs := scrapehtml.JSONBlobs(pl.HTML)
	for _, b := range blobs {
		media := walk.Find(b, "xig_polaris_media")
		if !media.Exists() {
			media = b
		}
		if pp.TakenAt.IsZero() {
			if t := walk.Find(media, "taken_at").Int(); t > 0 {
				pp.TakenAt = time.Unix(t, 0).UTC()
			}
		}
		if pp.LikeCount == nil {
			if v := walk.Find(media, "like_count"); v.Exists() {
				n := v.Int()
				pp.LikeCount = &n
			}
		}
		if pp.CommentCount == nil {
			if v := walk.Find(media, "comment_count"); v.Exists() {
				n := v.Int()
				pp.CommentCount = &n
			}
		}
	}
	edges, _ := connection(blobs, "comments_connection")
	pp.CommentsEmbedded = len(edges)
	pp.Items = len(edges)
	if len(out.Comments) == 0 {
		for _, e := range edges {
			n := e.Get("node")
			out.Comments = append(out.Comments, Comment{PostShortcode: shortcode, PK: n.Get("pk").String(),
				Username: n.Get("user.username").String(), Verified: n.Get("user.is_verified").Bool(), Text: n.Get("text").String(),
				CreatedAt: time.Unix(n.Get("created_at").Int(), 0).UTC(), Likes: n.Get("comment_like_count").Int()})
		}
	}
	return pp
}

func (r *runner) do(ctx context.Context, id *session.Identity, query string, vars map[string]any, referer string) (*graphql.Response, Step) {
	start := time.Now()
	resp, err := r.o.GQL.Do(ctx, id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	s := Step{Ms: time.Since(start).Milliseconds()}
	if err != nil {
		if ctx.Err() != nil {
			// The operator stopped the run: not Instagram's answer, so it
			// counts for no question and no cost.
			s.Class, s.Reason = stopped, "stopped by the operator"
			return nil, s
		}
		s.Class, s.Reason = detect.Transport.String(), "request failed"
		r.cost(query, s)
		return nil, s
	}
	s.Class, s.Status, s.WireBytes, s.Reason = resp.Class.String(), resp.Status, resp.BytesIn+resp.BytesOut, resp.Reason
	for _, e := range resp.Errors {
		if m := e.Message; m != "" && !slices.Contains(s.Errors, m) && len(s.Errors) < 5 {
			s.Errors = append(s.Errors, m)
		}
	}
	r.sum.QueriesWithoutClientState[s.Class]++
	if resp.Class != detect.OK && r.rawDir != "" {
		r.rawSeq++
		r.saveRaw(fmt.Sprintf("%02d-%s-%s.json", r.rawSeq, query, resp.Class), resp.Body)
	}
	r.cost(query, s)
	return resp, s
}

func (r *runner) cost(kind string, s Step) {
	c := r.sum.Requests[kind]
	c.N++
	c.sumMs += s.Ms
	c.sumBytes += s.WireBytes
	r.sum.Requests[kind] = c
}

// ptr copies a step: each report field keeps its own, though the step
// variable is reused for the next request.
func ptr(s Step) *Step { return &s }

func first(parts []gjson.Result, path string) gjson.Result {
	for _, p := range parts {
		if v := p.Get(path); v.Exists() {
			return v
		}
	}
	return gjson.Result{}
}

func respParts(resp *graphql.Response) []gjson.Result {
	if resp == nil || resp.Class != detect.OK {
		return nil
	}
	return resp.Parts
}

func gridItems(resp *graphql.Response) int {
	edges, _ := connection(respParts(resp), gridConn)
	return len(edges)
}

// gridConn names the profile grid's connection in both grid queries.
const gridConn = "polaris_ordered_timeline_connection"

// connection reads the connection named key from every part of a response:
// its edges wherever they stand (inside the connection object, or in parts
// Relay streams separately, whose path ends in the connection's edges), and
// its page_info, including a deferred page_info part (walk.FindPageInfo).
// Edges repeated across parts count once.
func connection(parts []gjson.Result, key string) ([]gjson.Result, walk.PageInfo) {
	var edges []gjson.Result
	seen := map[string]bool{}
	add := func(e gjson.Result) {
		if !e.Get("node").Exists() {
			return
		}
		k := e.Get("node.pk").String() + "|" + e.Get("node.id").String()
		if k != "|" && seen[k] {
			return
		}
		seen[k] = true
		edges = append(edges, e)
	}
	for _, p := range parts {
		for _, c := range walk.FindAll(p, key) {
			for _, e := range c.Get("edges").Array() {
				add(e)
			}
		}
		if path := p.Get("path"); path.IsArray() && strings.Contains(path.Raw, `"`+key+`"`) && strings.Contains(path.Raw, `"edges"`) {
			d := p.Get("data")
			if d.IsArray() {
				for _, e := range d.Array() {
					add(e)
				}
			} else {
				add(d)
			}
		}
	}
	return edges, walk.FindPageInfo(parts, []string{key})
}

// findAll returns the first value stored under key in any part.
func findAll(parts []gjson.Result, key string) gjson.Result {
	for _, p := range parts {
		if v := walk.Find(p, key); v.Exists() {
			return v
		}
	}
	return gjson.Result{}
}

// keepRaw saves an OK response's body when it is the first of its kind for
// this profile, or held no items. do saves every response that was not OK.
func (r *runner) keepRaw(kind string, resp *graphql.Response, items int) {
	if r.rawDir == "" || resp == nil || resp.Class != detect.OK {
		return
	}
	if r.rawSaved[kind] && items > 0 {
		return
	}
	r.rawSaved[kind] = true
	r.rawSeq++
	r.saveRaw(fmt.Sprintf("%02d-%s-%s.json", r.rawSeq, kind, resp.Class), resp.Body)
}

func (r *runner) saveRaw(name string, body []byte) {
	if err := os.MkdirAll(r.rawDir, 0o700); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(r.rawDir, name), body, 0o600)
}

func gridLine(g *Grid) string {
	if g == nil {
		return "not read"
	}
	return fmt.Sprintf("%d posts, stop %s", g.Posts, g.Stop)
}

// profileData reads the profile query's user object. Field names are the
// findings' (har-findings §3); the linked Facebook page's shape was not
// recorded, so two candidate paths are tried.
func profileData(u gjson.Result) *ProfileData {
	d := &ProfileData{PK: u.Get("pk").String(), FullName: u.Get("full_name").String(), Biography: u.Get("biography").String(),
		Verified: u.Get("is_verified").Bool(), Private: u.Get("is_private").Bool(), Memorialized: u.Get("is_memorialized").Bool(),
		Followers: u.Get("follower_count").Int(), Following: u.Get("following_count").Int(), HasClips: u.Get("has_any_clips").Bool()}
	for _, p := range []string{"hd_profile_pic_url_info.url", "profile_pic_url_hd", "profile_pic_url"} {
		if v := u.Get(p).String(); v != "" {
			d.ProfilePicURL = v
			break
		}
	}
	for _, p := range []string{"linked_fb_info.linked_fb_page.name", "linked_fb_info.name"} {
		if v := u.Get(p).String(); v != "" {
			d.LinkedFacebookPage = v
			break
		}
	}
	for _, l := range u.Get("bio_links").Array() {
		if url := l.Get("url").String(); url != "" {
			d.BioLinks = append(d.BioLinks, Link{Title: l.Get("title").String(), URL: url})
		}
	}
	for _, e := range u.Get("lox_highlights_connection.edges").Array() {
		d.Highlights = append(d.Highlights, Highlight{ID: e.Get("node.id").String(), Title: e.Get("node.title").String()})
	}
	return d
}

var mediaTypes = map[int64]string{1: "photo", 2: "video", 8: "carousel"}

// posts reads grid edges.
func (r *runner) posts(edges []gjson.Result) []Post {
	base := strings.TrimRight(r.o.BaseURL, "/")
	var out []Post
	for _, e := range edges {
		n := e.Get("node")
		p := Post{PK: n.Get("pk").String(), Shortcode: n.Get("code").String(), ProductType: n.Get("product_type").String(),
			Caption: n.Get("caption.text").String(), AltText: n.Get("accessibility_caption").String()}
		p.MediaType = mediaTypes[n.Get("media_type").Int()]
		p.PostedAt = PostedAt(p.PK)
		if p.MediaType == "" {
			p.MediaType = n.Get("media_type").String()
		}
		for _, k := range []string{"display_uri", "image_versions2.candidates.0.url", "thumbnail_src"} {
			if v := n.Get(k).String(); v != "" {
				p.ThumbnailURL = v
				break
			}
		}
		kind := "p"
		if p.ProductType == "clips" {
			kind = "reel"
		}
		p.URL = base + "/" + kind + "/" + p.Shortcode + "/"
		out = append(out, p)
	}
	return out
}

// idEpochMs is the epoch of Instagram's ids: a media pk holds the
// milliseconds since it above its 23rd bit. Checked in the owner's probe run
// (2026-10-31): within about a minute of each post page's taken_at.
const idEpochMs = 1314220021721

// PostedAt is when a media pk says its post was made; zero for a pk that
// does not parse or encodes no plausible time.
func PostedAt(pk string) time.Time {
	v, err := strconv.ParseInt(pk, 10, 64)
	if err != nil || v <= 0 {
		return time.Time{}
	}
	t := time.UnixMilli(v>>23 + idEpochMs).UTC()
	// Ids that encode a time began in late 2011; a small number decodes to
	// the epoch itself.
	if t.Year() < 2012 || t.After(time.Now().Add(48*time.Hour)) {
		return time.Time{}
	}
	return t
}

var credentials = regexp.MustCompile(`://[^/@\s]+@`)

// redact removes credentials from URLs in an error text (a proxy URL can
// carry them).
func redact(s string) string { return credentials.ReplaceAllString(s, "://***@") }

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// saveImage downloads a profile picture through the identity (its exit and
// fingerprint) and returns the file name under ImageDir, or "" on failure.
func (r *runner) saveImage(ctx context.Context, id *session.Identity, url, username, referer string) string {
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: url, Header: [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"user-agent", id.FP.UserAgent},
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform}, {"accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"},
		{"sec-fetch-site", "cross-site"}, {"sec-fetch-mode", "no-cors"}, {"sec-fetch-dest", "image"},
		{"referer", referer}, {"accept-encoding", "gzip, deflate, br, zstd"}, {"priority", "i"},
	}})
	if err != nil || resp.Status != 200 || len(resp.Body) == 0 {
		return ""
	}
	r.cost("profile_picture", Step{WireBytes: resp.BytesIn + resp.BytesOut, Ms: resp.Elapsed.Milliseconds()})
	name := unsafeName.ReplaceAllString(username, "_") + ".jpg"
	if err := os.MkdirAll(r.o.ImageDir, 0o700); err != nil {
		return ""
	}
	if err := os.WriteFile(filepath.Join(r.o.ImageDir, name), resp.Body, 0o600); err != nil {
		return ""
	}
	return filepath.Join(filepath.Base(r.o.ImageDir), name)
}
