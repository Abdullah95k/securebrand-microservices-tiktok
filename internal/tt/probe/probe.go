package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// Options configure a run.
type Options struct {
	BaseURL  string   // https://www.tiktok.com
	Accounts []string // usernames, in the owner's file order
	Tier     string   // proxy tier ("direct" for none)
	Pool     *session.Pool
	// AltPool, when set, loads the first account's page once more with
	// another fingerprint profile (T5).
	AltPool        *session.Pool
	AltFingerprint string
	Loader         *session.Loader
	Client         *graphql.Client
	AcceptLanguage string
	Fingerprint    string
	Version        string
	// MaxListPages caps the walk; MaxAge stops it at posts older than this
	// (judged by the id's time, T8).
	MaxListPages    int
	MaxAge          time.Duration
	MaxCommentPages int
	// CountChecks is how many accounts get the page-size check (T11).
	CountChecks int
	// DepthChecks is how many accounts get a creator-list page read at each
	// of DepthDays back (T11).
	DepthChecks int
	DepthDays   []int
	Covers      int // covers saved per account
	Videos      int // video files downloaded per account
	// Signer "browser" repeats the main calls from inside a headless
	// Chromium page (T7, T27); BrowserBinary is that Chromium.
	Signer           string
	BrowserBinary    string
	BrowserNoSandbox bool
	OEmbed           bool
	MaxCaptchas      int
	// ImageDir, VideoDir and RawDir receive files; empty skips them.
	ImageDir string
	VideoDir string
	RawDir   string
	RawPages int
	Progress func(string)
	Now      func() time.Time
}

const (
	stopped       = "stopped"
	progressEvery = 10
)

// Run probes every account and returns the report.
func Run(ctx context.Context, o Options) Report {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxCaptchas <= 0 {
		o.MaxCaptchas = 10
	}
	r := &runner{o: o, rep: Report{Version: o.Version, Tier: o.Tier, Fingerprint: o.Fingerprint, Signer: o.Signer, StartedAt: o.Now().UTC()},
		cost: map[string]*Cost{}, rawSeen: map[string]bool{}}
	r.noMs = *o.Client
	r.noMs.Builder = ttclient.Builder{AcceptLanguage: o.AcceptLanguage, NoMsToken: true}
	for i, name := range o.Accounts {
		if ctx.Err() != nil {
			r.rep.Summary.StoppedEarly = "operator"
			break
		}
		if r.captchas >= o.MaxCaptchas {
			r.rep.Summary.StoppedEarly = "captchas"
			break
		}
		a := r.account(ctx, i, name)
		r.rep.Accounts = append(r.rep.Accounts, a)
		if a.Stopped {
			r.rep.Summary.StoppedEarly = "operator"
			break
		}
		if o.Progress != nil {
			o.Progress(fmt.Sprintf("%s: %s, %d posts, %d comments", name, a.Page.Class, len(a.Posts), len(a.Comments)))
		}
	}
	r.summarise()
	r.rep.FinishedAt = o.Now().UTC()
	return r.rep
}

type runner struct {
	o        Options
	rep      Report
	noMs     graphql.Client
	cost     map[string]*Cost
	captchas int
	rawSeen  map[string]bool
	pages    int
}

// stepOf turns a response into a Step and books its cost.
func (r *runner) stepOf(kind string, resp *graphql.Response) Step {
	s := Step{Class: resp.Class.String(), Status: resp.Status, Reason: redact(resp.Reason), Ms: resp.Elapsed.Milliseconds(),
		WireBytes: resp.BytesIn + resp.BytesOut, Empty: resp.Status == 200 && len(strings.TrimSpace(string(resp.Body))) == 0}
	r.book(kind, s)
	return s
}

func (r *runner) book(kind string, s Step) {
	if s.Class == detect.Checkpoint.String() {
		r.captchas++
	}
	c := r.cost[kind]
	if c == nil {
		c = &Cost{}
		r.cost[kind] = c
	}
	c.N++
	c.sumMs += s.Ms
	c.sumBytes += s.WireBytes
	if r.rep.Summary.Calls == nil {
		r.rep.Summary.Calls = map[string]map[string]int{}
	}
	m := r.rep.Summary.Calls[kind]
	if m == nil {
		m = map[string]int{}
		r.rep.Summary.Calls[kind] = m
	}
	label := s.Class
	if s.Empty {
		label += "_empty"
	}
	m[label]++
}

// call runs one operation; a stop by the operator comes back as Class
// "stopped".
func (r *runner) call(ctx context.Context, c *graphql.Client, id *session.Identity, kind, query string, vars map[string]any, referer string) (*graphql.Response, Step) {
	resp, err := c.Do(ctx, id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	if err != nil {
		if ctx.Err() != nil {
			return nil, Step{Class: stopped}
		}
		s := Step{Class: detect.Transport.String(), Reason: redact(err.Error())}
		r.book(kind, s)
		return nil, s
	}
	s := r.stepOf(kind, resp)
	r.keepRaw(kind, resp.Body, s.Class != detect.OK.String())
	return resp, s
}

func (r *runner) account(ctx context.Context, index int, name string) Account {
	a := Account{Index: index + 1, Username: name, URL: r.o.BaseURL + "/@" + name, Needs: map[string]Step{}, Media: map[string]Step{}}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		a.Error = redact(err.Error())
		return a
	}
	defer r.o.Pool.Release(id)
	// 1. The profile page (T1, T2, T3, T5, T28).
	page, took, err := r.load(ctx, id, a.URL)
	if ctx.Err() != nil {
		a.Stopped = true
		return a
	}
	a.Page, a.PageForm = r.pageStep("profile_page", page, err, took)
	if page != nil && r.pages < r.o.RawPages {
		r.pages++
		r.saveRaw(name+"/profile_page.html", page.HTML)
	}
	if page != nil && a.PageForm != "ssr" {
		if ud, ok := htmlx.Scope(page.HTML, htmlx.ScopeUserDetail); ok {
			a.PageStatus = int(ud.Get("statusCode").Int())
		}
		r.saveRaw("failed/"+safe(name)+"-profile_page.html", page.HTML)
	}
	for k := range id.Doer.Cookies(id.Base) {
		a.Cookies = append(a.Cookies, k)
	}
	slices.Sort(a.Cookies)
	tok := id.Tokens()
	for k := range tok {
		a.Tokens = append(a.Tokens, k)
	}
	slices.Sort(a.Tokens)
	a.BotType = tok["botType"]
	if index == 0 && r.o.AltPool != nil {
		if alt, err := r.o.AltPool.Acquire(ctx, r.o.Tier); err == nil {
			p, took, err := r.load(ctx, alt, a.URL)
			s, form := r.pageStep("profile_page_alt", p, err, took)
			s.Reason = strings.TrimSpace(r.o.AltFingerprint + " " + form + " " + s.Reason)
			a.AltPage = &s
			r.o.AltPool.Release(alt)
		}
	}
	// 2. The account from the page.
	if page != nil {
		if ud, ok := htmlx.Scope(page.HTML, htmlx.ScopeUserDetail); ok && ud.Get("userInfo.user.id").Exists() {
			a.Data = accountData(ud.Get("userInfo"), "page")
		}
	}
	if !id.Bootstrapped() {
		a.Error = "no page values: the page was " + a.PageForm
		return a
	}
	// 3. The account call, unsigned (T24).
	resp, s := r.call(ctx, r.o.Client, id, "user_detail", "user_detail", map[string]any{"username": name}, a.URL)
	a.UserDetail = &s
	if s.Class == stopped {
		a.Stopped = true
		return a
	}
	if a.Data == nil && resp != nil && s.Class == detect.OK.String() {
		a.Data = accountData(resp.Parts[0].Get("userInfo"), "user_detail")
	}
	if a.Data == nil || a.Data.SecUID == "" {
		if e := r.embed(ctx, id, name, &a); e != "" {
			a.Data = &AccountData{Source: "creator_embed", SecUID: e, Username: name}
		}
	}
	if a.Data == nil || a.Data.SecUID == "" {
		a.Error = "no secUid: the account could not be read"
		return a
	}
	sec := a.Data.SecUID
	r.saveAvatar(ctx, id, &a)
	// 4-5. Both post lists, unsigned (T4, T9, T10, T11).
	start := nowMs(r.o.Now())
	creator, cs := r.call(ctx, r.o.Client, id, "creator_list", "creator_list", map[string]any{"sec_uid": sec, "count": 15, "cursor": start}, a.URL)
	a.CreatorList = &cs
	app, as := r.call(ctx, r.o.Client, id, "post_list", "post_list", map[string]any{"sec_uid": sec, "count": 16, "cursor": 0}, a.URL)
	a.PostList = &as
	if cs.Class == stopped || as.Class == stopped {
		a.Stopped = true
		return a
	}
	countStep(&cs, creator)
	countStep(&as, app)
	if app != nil && as.Class == detect.OK.String() {
		a.AppListPinned = pinnedIn(app.Parts[0])
	}
	if index < r.o.CountChecks {
		a.CountChecks = map[string]Step{}
		for _, n := range []int{5, 16, 20, 30} {
			resp, s := r.call(ctx, r.o.Client, id, "creator_list_count", "creator_list", map[string]any{"sec_uid": sec, "count": n, "cursor": nowMs(r.o.Now())}, a.URL)
			countStep(&s, resp)
			a.CountChecks["creator_count_"+strconv.Itoa(n)] = s
		}
	}
	// 6. Walk the list that answered.
	switch {
	case cs.Class == detect.OK.String() && cs.Items > 0:
		r.walk(ctx, id, &a, "creator_list", creator, start)
	case as.Class == detect.OK.String() && as.Items > 0:
		r.walk(ctx, id, &a, "post_list", app, 0)
	}
	if a.Walk != nil && a.Walk.Stop == stopped {
		a.Stopped = true
		return a
	}
	// 6b. How far back the list answers (T11).
	if index < r.o.DepthChecks && cs.Class == detect.OK.String() && r.depth(ctx, id, &a, sec) {
		a.Stopped = true
		return a
	}
	// 7. Cursor portability (T22).
	r.portability(ctx, &a, sec)
	// 8. What a call needs (T2).
	r.needs(ctx, id, &a, sec)
	// 9. A post page and a photo page (T1, T16, T19).
	r.postPages(ctx, id, &a)
	// 10-11. Comments and replies (T12-T15).
	if r.comments(ctx, id, &a) {
		a.Stopped = true
		return a
	}
	// 12. Media (T18).
	r.media(ctx, id, &a)
	// 13. The browser signer (T7, T27).
	if r.o.Signer == "browser" {
		a.Signed = r.signed(ctx, id, &a, sec)
	}
	// 14. Embeds (T26).
	if r.o.OEmbed {
		r.oembed(ctx, id, &a)
	}
	return a
}

// load loads a page with the identity and times it.
func (r *runner) load(ctx context.Context, id *session.Identity, raw string) (*session.PageLoad, time.Duration, error) {
	start := time.Now()
	p, err := r.o.Loader.Load(ctx, id, raw)
	return p, time.Since(start), err
}

// pageStep records a page load: its class and the page's form.
func (r *runner) pageStep(kind string, p *session.PageLoad, err error, took time.Duration) (Step, string) {
	if p == nil {
		s := Step{Class: detect.Transport.String(), Ms: took.Milliseconds()}
		if err != nil {
			s.Reason = redact(err.Error())
		}
		r.book(kind, s)
		return s, "none"
	}
	s := Step{Class: p.Class.String(), Status: p.Status, Reason: redact(p.Reason), WireBytes: p.Bytes, Ms: took.Milliseconds()}
	form := "none"
	switch {
	case htmlx.IsChallenge(p.HTML):
		form = "challenge"
		r.rep.Summary.Challenges++
	case func() bool {
		ud, ok := htmlx.Scope(p.HTML, htmlx.ScopeUserDetail)
		return ok && ud.Get("userInfo.user").Exists()
	}():
		form = "ssr"
	case func() bool { _, ok := htmlx.Scope(p.HTML, htmlx.ScopeAppContext); return ok }():
		form = "shell"
	}
	r.book(kind, s)
	return s, form
}

func countStep(s *Step, resp *graphql.Response) {
	if resp == nil || len(resp.Parts) == 0 {
		return
	}
	p := resp.Parts[0]
	s.Items = len(p.Get("itemList").Array())
	if c := p.Get("comments"); c.Exists() {
		s.Items = len(c.Array())
	}
	s.HasMore = p.Get("hasMore").Bool() || p.Get("hasMorePrevious").Bool() || p.Get("has_more").Int() == 1
}

func nowMs(t time.Time) int64 { return (t.Unix() + 1) * 1000 }

// pinnedIn counts the items flagged isPinnedItem in a list answer.
func pinnedIn(p gjson.Result) int {
	n := 0
	for _, it := range p.Get("itemList").Array() {
		if it.Get("isPinnedItem").Bool() {
			n++
		}
	}
	return n
}

// IDTime is the time a post id's top 32 bits encode (T8).
func IDTime(id string) time.Time {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	return time.Unix(n>>32, 0).UTC()
}

func (r *runner) walk(ctx context.Context, id *session.Identity, a *Account, variant string, first *graphql.Response, cursor int64) {
	w := &Walk{Variant: variant}
	a.Walk = w
	seen := map[string]bool{}
	resp := first
	for {
		p := resp.Parts[0]
		if p.Get("filter_reasons").Exists() {
			w.Filtered++
		}
		posts := r.posts(a.Username, p.Get("itemList").Array())
		fresh := 0
		for _, x := range posts {
			if seen[x.ID] {
				w.Repeats++
				continue
			}
			seen[x.ID] = true
			fresh++
			a.Posts = append(a.Posts, x)
			if t := time.Unix(x.PostedAt, 0).UTC(); !x.Pinned && (w.Oldest.IsZero() || t.Before(w.Oldest)) {
				w.Oldest = t
			}
		}
		w.Posts += fresh
		// The next page: the creator list's cursor is a time (no cursor in
		// its answer, the C2 run); the app's list returns one.
		var next any
		more := p.Get("hasMore").Bool() || p.Get("hasMorePrevious").Bool()
		if variant == "creator_list" {
			var c int64
			c, more = ttclient.NextCreatorCursor(p, cursor)
			if len(posts) == 0 {
				w.EmptyPages++
			}
			cursor, next = c, c
		} else {
			next = p.Get("cursor").String()
		}
		switch {
		case !more:
			w.Stop = "end"
			return
		case len(posts) > 0 && fresh == 0:
			w.Stop = "stuck"
			return
		case w.Pages >= r.o.MaxListPages:
			w.Stop = "cap"
			return
		case r.o.MaxAge > 0 && !w.Oldest.IsZero() && r.o.Now().Sub(w.Oldest) > r.o.MaxAge:
			w.Stop = "age"
			return
		case r.o.MaxAge > 0 && variant == "creator_list" && r.o.Now().Sub(time.UnixMilli(cursor)) > r.o.MaxAge:
			w.Stop = "age" // empty pages stepped the cursor past the window
			return
		}
		vars := map[string]any{"sec_uid": a.Data.SecUID, "count": 15, "cursor": next}
		if variant == "post_list" {
			vars["count"] = 16
		}
		nextResp, s := r.call(ctx, r.o.Client, id, variant+"_page", variant, vars, a.URL)
		if s.Class == stopped {
			w.Stop = stopped
			return
		}
		w.Pages++
		if s.Class != detect.OK.String() {
			w.Stop, w.Failed = "wall:"+s.Class, &s
			return
		}
		resp = nextResp
		if r.o.Progress != nil && w.Pages%progressEvery == 0 {
			r.o.Progress(fmt.Sprintf("%s: %d posts so far, back to %s", a.Username, w.Posts, w.Oldest.Format("2006-01-02")))
		}
	}
}

// depth reads one creator-list page at each of DepthDays back (T11): the
// cursor is a time, so how far back the list answers needs no walk.
func (r *runner) depth(ctx context.Context, id *session.Identity, a *Account, sec string) bool {
	for _, d := range r.o.DepthDays {
		cursor := r.o.Now().Add(-time.Duration(d) * 24 * time.Hour).UnixMilli()
		resp, s := r.call(ctx, r.o.Client, id, "creator_list_depth", "creator_list", map[string]any{"sec_uid": sec, "count": 15, "cursor": cursor}, a.URL)
		if s.Class == stopped {
			return true
		}
		countStep(&s, resp)
		dp := Depth{Days: d, Step: s}
		if resp != nil && len(resp.Parts) > 0 {
			for _, it := range resp.Parts[0].Get("itemList").Array() {
				t := time.Unix(it.Get("createTime").Int(), 0).UTC()
				if dp.Newest.IsZero() || t.After(dp.Newest) {
					dp.Newest = t
				}
				if dp.Oldest.IsZero() || t.Before(dp.Oldest) {
					dp.Oldest = t
				}
			}
		}
		a.Depth = append(a.Depth, dp)
	}
	return false
}

// fresh returns a new identity bootstrapped on the account's page, or nil.
func (r *runner) fresh(ctx context.Context, a *Account) *session.Identity {
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		return nil
	}
	p, took, err := r.load(ctx, id, a.URL)
	s, _ := r.pageStep("profile_page_second", p, err, took)
	if s.Class != detect.OK.String() || !id.Bootstrapped() {
		r.o.Pool.Release(id)
		return nil
	}
	return id
}

func (r *runner) portability(ctx context.Context, a *Account, sec string) {
	if a.Walk == nil || len(a.Posts) < 2 {
		return
	}
	other := r.fresh(ctx, a)
	if other == nil {
		return
	}
	defer r.o.Pool.Release(other)
	last := a.Posts[min(len(a.Posts)-1, 14)]
	cursor := strconv.FormatInt(last.PostedAt*1000, 10)
	resp, s := r.call(ctx, r.o.Client, other, "cursor_portability", a.Walk.Variant, map[string]any{"sec_uid": sec, "count": 15, "cursor": cursor}, a.URL)
	countStep(&s, resp)
	ok := s.Class == detect.OK.String() && s.Items > 0
	a.CursorPortable = &ok
}

func (r *runner) needs(ctx context.Context, id *session.Identity, a *Account, sec string) {
	vars := map[string]any{"sec_uid": sec, "count": 15, "cursor": nowMs(r.o.Now())}
	resp, s := r.call(ctx, &r.noMs, id, "needs_no_mstoken", "creator_list", vars, a.URL)
	countStep(&s, resp)
	a.Needs["no_mstoken"] = s
	tok := id.Tokens()
	for _, c := range []struct {
		name string
		drop string
	}{{"no_cookies", ""}, {"no_device_id", "device_id"}} {
		bare, err := r.o.Pool.Acquire(ctx, r.o.Tier)
		if err != nil {
			continue
		}
		t := session.Tokens{}
		for k, v := range tok {
			if k != c.drop {
				t[k] = v
			}
		}
		bare.SetTokens(t) // values only: this identity never loaded a page, so it has no cookies
		resp, s := r.call(ctx, r.o.Client, bare, "needs_"+c.name, "creator_list", vars, a.URL)
		countStep(&s, resp)
		a.Needs[c.name] = s
		r.o.Pool.Release(bare)
	}
}

func (r *runner) postPages(ctx context.Context, id *session.Identity, a *Account) {
	var video, photo *Post
	for i := range a.Posts {
		p := &a.Posts[i]
		if p.Pinned {
			continue
		}
		if p.Kind == "photo" && photo == nil {
			photo = p
		}
		if p.Kind == "video" && video == nil {
			video = p
		}
	}
	for _, x := range []struct {
		p    *Post
		kind string
	}{{video, "post_page"}, {photo, "photo_page"}} {
		if x.p == nil {
			continue
		}
		pl, took, err := r.load(ctx, id, x.p.Link)
		s, _ := r.pageStep(x.kind, pl, err, took)
		if pl != nil {
			if vd, ok := htmlx.Scope(pl.HTML, htmlx.ScopeVideoDetail); ok {
				st := vd.Get("itemInfo.itemStruct")
				s.Items = 1
				if x.kind == "post_page" {
					a.PostPageStats = st.Get("stats").Exists() || st.Get("statsV2").Exists()
					a.pageVideoURL = smallestVideo(st.Get("video"))
				}
			}
			if r.pages < r.o.RawPages+2 {
				r.saveRaw(a.Username+"/"+x.kind+".html", pl.HTML)
			}
		}
		if x.kind == "post_page" {
			a.PostPage = &s
		} else {
			a.PhotoPage = &s
		}
	}
}

// comments reads the 3 newest posts' comments and some replies. It reports
// whether the operator stopped the run.
func (r *runner) comments(ctx context.Context, id *session.Identity, a *Account) bool {
	var picked []Post
	for _, p := range a.Posts {
		if !p.Pinned && p.Comments > 0 && len(picked) < 3 {
			picked = append(picked, p)
		}
	}
	type withReplies struct{ post, cid string }
	var threads []withReplies
	for _, p := range picked {
		read := CommentRead{PostID: p.ID}
		seen := map[string]bool{}
		var times []int64
		cursor := "0"
		for page := 1; page <= r.o.MaxCommentPages; page++ {
			resp, s := r.call(ctx, r.o.Client, id, "comments", "comments", map[string]any{"item_id": p.ID, "count": 20, "cursor": cursor}, p.Link)
			if s.Class == stopped {
				return true
			}
			countStep(&s, resp)
			if s.Class != detect.OK.String() {
				read.Failed = &s
				break
			}
			read.Pages++
			body := resp.Parts[0]
			read.Total = body.Get("total").Int()
			for _, c := range body.Get("comments").Array() {
				cid := c.Get("cid").String()
				if seen[cid] {
					continue
				}
				seen[cid] = true
				cm := comment(p.ID, "", c, page)
				a.Comments = append(a.Comments, cm)
				times = append(times, cm.CreatedAt)
				read.ReplySum += 1 + cm.Replies
				if cm.Replies > 0 && len(threads) < 3 {
					threads = append(threads, withReplies{p.ID, cid})
				}
			}
			if body.Get("has_more").Int() != 1 {
				read.Ended = true
				break
			}
			cursor = body.Get("cursor").String()
		}
		read.Read = len(seen)
		if len(times) > 1 {
			read.Order = "newest_first"
			for i := 1; i < len(times); i++ {
				if times[i] > times[i-1] {
					read.Order = "not_time_ordered"
					break
				}
			}
		}
		a.CommentReads = append(a.CommentReads, read)
	}
	for i, th := range threads {
		read := CommentRead{PostID: th.post}
		cursor := "0"
		for page := 1; page <= 3; page++ {
			resp, s := r.call(ctx, r.o.Client, id, "replies", "replies", map[string]any{"item_id": th.post, "comment_id": th.cid, "count": 3, "cursor": cursor}, a.URL)
			if s.Class == stopped {
				return true
			}
			if s.Class != detect.OK.String() {
				read.Failed = &s
				break
			}
			read.Pages++
			body := resp.Parts[0]
			read.Total = body.Get("total").Int()
			for _, c := range body.Get("comments").Array() {
				a.Comments = append(a.Comments, comment(th.post, th.cid, c, page))
				read.Read++
			}
			if body.Get("has_more").Int() != 1 {
				read.Ended = true
				break
			}
			cursor = body.Get("cursor").String()
		}
		a.ReplyReads = append(a.ReplyReads, read)
		if i == 0 {
			resp, s := r.call(ctx, r.o.Client, id, "replies_count_20", "replies", map[string]any{"item_id": th.post, "comment_id": th.cid, "count": 20, "cursor": 0}, a.URL)
			countStep(&s, resp)
			a.ReplyCount20 = &s
		}
	}
	return false
}

// get fetches a URL with the identity's cookies (when it has any) and
// returns the body and its step.
func (r *runner) get(ctx context.Context, id *session.Identity, kind, raw, referer, accept string) ([]byte, Step) {
	return r.getWith(ctx, id, kind, raw, [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", id.FP.UserAgent}, {"accept", accept}, {"sec-fetch-site", "cross-site"}, {"sec-fetch-mode", "no-cors"},
		{"sec-fetch-dest", "empty"}, {"referer", referer}, {"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", r.o.AcceptLanguage}})
}

// getWith fetches a URL with these headers and the identity's cookies.
// 200 and 206 (a range) are both answers.
func (r *runner) getWith(ctx context.Context, id *session.Identity, kind, raw string, hdr [][2]string) ([]byte, Step) {
	if err := id.Wait(ctx); err != nil {
		return nil, Step{Class: stopped}
	}
	start := time.Now()
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: raw, Header: hdr})
	if err != nil {
		s := Step{Class: detect.Transport.String(), Reason: redact(err.Error()), Ms: time.Since(start).Milliseconds()}
		r.book(kind, s)
		return nil, s
	}
	s := Step{Class: detect.OK.String(), Status: resp.Status, Ms: time.Since(start).Milliseconds(), WireBytes: resp.BytesIn + resp.BytesOut, Items: len(resp.Body)}
	if resp.Status != 200 && resp.Status != 206 {
		s.Class = detect.SoftError.String()
		s.Reason = "http " + strconv.Itoa(resp.Status)
	}
	r.book(kind, s)
	return resp.Body, s
}

func (r *runner) saveAvatar(ctx context.Context, id *session.Identity, a *Account) {
	if r.o.ImageDir == "" || a.Data.AvatarURL == "" {
		return
	}
	body, s := r.get(ctx, id, "avatar", a.Data.AvatarURL, a.URL, "image/avif,image/webp,image/*,*/*;q=0.8")
	a.Media["avatar"] = s
	if s.Class == detect.OK.String() && len(body) > 0 {
		a.Data.AvatarFile = r.saveFile(r.o.ImageDir, "avatars", safe(a.Username)+".jpg", body)
	}
}

func (r *runner) media(ctx context.Context, id *session.Identity, a *Account) {
	covers, videos := 0, 0
	for i := range a.Posts {
		p := &a.Posts[i]
		if r.o.ImageDir != "" && covers < r.o.Covers && p.CoverURL != "" {
			covers++
			body, s := r.get(ctx, id, "cover", p.CoverURL, a.URL, "image/avif,image/webp,image/*,*/*;q=0.8")
			if covers == 1 {
				a.Media["cover"] = s
			}
			if s.Class == detect.OK.String() && len(body) > 0 {
				p.CoverFile = r.saveFile(r.o.ImageDir, "covers", p.ID+".jpg", body)
			}
		}
		if r.o.VideoDir != "" && videos < r.o.Videos && p.Kind == "video" && p.VideoURL != "" {
			videos++
			r.video(ctx, id, a, p)
		}
	}
}

// mediaHeaders are the headers Chrome sends for a <video> element's file
// on TikTok's page: same site, a range from the start, the site as referer.
func mediaHeaders(id *session.Identity, acceptLanguage string) [][2]string {
	return [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", id.FP.UserAgent}, {"accept", "*/*"}, {"sec-fetch-site", "same-site"}, {"sec-fetch-mode", "no-cors"},
		{"sec-fetch-dest", "video"}, {"referer", "https://www.tiktok.com/"}, {"accept-encoding", "identity;q=1, *;q=0"},
		{"accept-language", acceptLanguage}, {"range", "bytes=0-"},
	}
}

// setHeader replaces a header's value in h.
func setHeader(h [][2]string, name, value string) {
	for i := range h {
		if h[i][0] == name {
			h[i][1] = value
		}
	}
}

// video downloads one post's video file several ways (T18): the list's
// link with the identity's cookies and plain headers, the same without
// cookies, the same with a browser's media headers, and the post page's
// own link with those headers. A refusal's body is kept under raw/failed.
func (r *runner) video(ctx context.Context, id *session.Identity, a *Account, p *Post) {
	if u, err := url.Parse(p.VideoURL); err == nil {
		for k := range id.Doer.Cookies(u) {
			a.VideoCookies = append(a.VideoCookies, k)
		}
		slices.Sort(a.VideoCookies)
	}
	keep := func(kind string, body []byte, s Step) {
		if s.Class != detect.OK.String() && len(body) > 0 {
			r.saveRaw("failed/"+kind+"-"+strconv.Itoa(a.Index)+".txt", body[:min(len(body), 4096)])
		}
	}
	saved := false
	save := func(body []byte, s Step) {
		if !saved && (s.Status == 200 || s.Status == 206) && len(body) > 0 {
			saved = true
			r.saveFile(r.o.VideoDir, "", p.ID+".mp4", body)
		}
	}
	// Once one way brought the file, the others ask for its first KB only:
	// enough to tell whether they would, without paying for the file again.
	media := func() [][2]string {
		h := mediaHeaders(id, r.o.AcceptLanguage)
		if saved {
			setHeader(h, "range", "bytes=0-1023")
		}
		return h
	}
	body, s := r.get(ctx, id, "video", p.VideoURL, p.Link, "*/*")
	a.Media["video_with_cookies"] = s
	keep("video_with_cookies", body, s)
	save(body, s)
	if bare, err := r.o.Pool.Acquire(ctx, r.o.Tier); err == nil {
		h := media()
		setHeader(h, "referer", p.Link)
		_, s := r.getWith(ctx, bare, "video_no_cookies", p.VideoURL, h)
		a.Media["video_without_cookies"] = s
		r.o.Pool.Release(bare)
	}
	body, s = r.getWith(ctx, id, "video_media_headers", p.VideoURL, media())
	a.Media["video_media_headers"] = s
	keep("video_media_headers", body, s)
	save(body, s)
	if a.pageVideoURL != "" {
		body, s = r.getWith(ctx, id, "video_post_page", a.pageVideoURL, media())
		a.Media["video_post_page"] = s
		keep("video_post_page", body, s)
		save(body, s)
	}
}

func (r *runner) oembed(ctx context.Context, id *session.Identity, a *Account) {
	if len(a.Posts) > 0 {
		_, s := r.get(ctx, id, "oembed", r.o.BaseURL+"/oembed?url="+url.QueryEscape(a.Posts[0].Link), a.URL, "application/json")
		a.OEmbed = &s
	}
	if a.Embed == nil {
		r.embed(ctx, id, a.Username, a)
	}
}

var frontity = regexp.MustCompile(`(?s)<script[^>]*id="__FRONTITY_CONNECT_STATE__"[^>]*>(.*?)</script>`)

// embed loads the creator embed and returns the secUid it carries.
func (r *runner) embed(ctx context.Context, id *session.Identity, name string, a *Account) string {
	body, s := r.get(ctx, id, "creator_embed", r.o.BaseURL+"/embed/@"+url.PathEscape(name), a.URL, "text/html")
	a.Embed = &s
	m := frontity.FindSubmatch(body)
	if m == nil {
		return ""
	}
	var sec string
	gjson.GetBytes(m[1], "source.data").ForEach(func(_, v gjson.Result) bool {
		sec = v.Get("userInfo.secUid").String()
		s.Items = len(v.Get("videoList").Array())
		return sec == ""
	})
	a.Embed = &s
	return sec
}

// signed is the browser part (T7, T27 and the fallback): a headless Chrome
// page on the account's profile, through the identity's exit. It records
// the page's own API calls, repeats tks's calls from the page so its script
// signs them, asks for the next page of the page's own list, then follows a
// post's link and records that page's own calls (comments) and asks for
// their next page. Every signed URL is replayed by plain HTTP.
func (r *runner) signed(ctx context.Context, id *session.Identity, a *Account, sec string) *Signed {
	out := &Signed{Calls: map[string]*SignedCall{}}
	var proxyURL *url.URL
	if id.Endpoint.URL != "" {
		proxyURL, _ = url.Parse(id.Endpoint.URL)
	}
	start := time.Now()
	sess, err := browser.OpenSession(ctx, r.o.BrowserBinary, browser.Options{URL: a.URL, Proxy: proxyURL, UserAgent: id.FP.UserAgent,
		AcceptLanguage: r.o.AcceptLanguage, Hints: browser.HintsFrom(id.FP.SecChUa, id.FP.SecChUaPlatform, id.FP.SecChUaMobile),
		BlockMedia: true, HideAutomation: true, NoSandbox: r.o.BrowserNoSandbox, MaxSettle: 15 * time.Second})
	if err != nil {
		out.Error = redact(err.Error())
		out.Page = Step{Class: detect.Transport.String(), Reason: out.Error, Ms: time.Since(start).Milliseconds()}
		r.book("browser_page", out.Page)
		return out
	}
	defer sess.Close()
	out.Page = Step{Class: detect.OK.String(), Status: sess.Status, Ms: time.Since(start).Milliseconds(), WireBytes: sess.Bytes()}
	r.book("browser_page", out.Page)
	html, _ := sess.HTML(ctx)
	tok, ok := htmlx.Extract(html)
	if !ok {
		out.Error = "the browser page carried no page values"
		return out
	}
	out.BotType = tok["botType"]
	if v, err := sess.Evaluate(ctx, `({webdriver: navigator.webdriver === true, time_zone: Intl.DateTimeFormat().resolvedOptions().timeZone,`+
		` screen: screen.width + "x" + screen.height, language: navigator.language, history: history.length})`); err == nil {
		_ = json.Unmarshal(v, &out.Env)
	}
	// The profile page's own calls; a scroll when the load made no list call.
	own := sess.Captured(ctx, 0)
	if findCall(own, postListPath, false) == nil {
		mark := sess.Mark()
		if err := sess.Scroll(ctx); err == nil {
			own = append(own, sess.Captured(ctx, mark)...)
		}
	}
	out.PageCalls = r.pageCalls(own)
	if x := findCall(own, postListPath, true); x != nil && a.AppListPinned == 0 {
		if parts, _, _ := ttclient.Decode(x.Body); len(parts) > 0 {
			a.AppListPinned = pinnedIn(parts[0])
		}
	}
	// tks's own calls, built with the browser page's values; the page's
	// script adds the signatures and msToken.
	page := &session.Identity{FP: id.FP, Base: id.Base, Doer: noCookies{}}
	page.SetTokens(tok)
	builder := ttclient.Builder{AcceptLanguage: r.o.AcceptLanguage, NoMsToken: true}
	reg := r.o.Client.Registry
	calls := []struct {
		name string
		vars map[string]any
	}{
		{"user_detail", map[string]any{"username": a.Username}},
		{"creator_list", map[string]any{"sec_uid": sec, "count": 15, "cursor": nowMs(r.o.Now())}},
		{"post_list", map[string]any{"sec_uid": sec, "count": 16, "cursor": 0}},
	}
	commented := firstCommented(a.Posts)
	if commented != nil {
		calls = append(calls, struct {
			name string
			vars map[string]any
		}{"comments", map[string]any{"item_id": commented.ID, "count": 20, "cursor": 0}})
	}
	var hdr [][2]string
	for _, c := range calls {
		q, err := reg.Get(c.name)
		if err != nil {
			continue
		}
		vars, err := renderVars(q.Variables, c.vars)
		if err != nil {
			continue
		}
		b := builder.Build(page, q, vars, reg.ClientParams(), a.URL)
		hdr = b.Header
		raw := q.Endpoint + "?" + b.Query
		if c.name == "post_list" {
			if x := findCall(own, postListPath, false); x != nil {
				out.ParamsMissing, out.ParamsExtra = paramDiff(x.URL, raw)
			}
		}
		body := r.signedCall(ctx, sess, id, out, c.name, raw, hdr)
		if c.name == "post_list" && a.AppListPinned == 0 {
			if parts, _, _ := ttclient.Decode(body); len(parts) > 0 {
				a.AppListPinned = pinnedIn(parts[0])
			}
		}
	}
	// The next page of the page's own list call.
	if x := findCall(own, postListPath, true); x != nil {
		if next := nextPageURL(x, "cursor"); next != "" {
			r.signedCall(ctx, sess, id, out, "post_list_from_page", next, hdr)
		}
	}
	// Follow a post's link: the post page loads its comments itself.
	if commented == nil {
		return out
	}
	mark, before := sess.Mark(), sess.Bytes()
	start = time.Now()
	err = sess.Navigate(ctx, commented.Link)
	vp := Step{Class: detect.OK.String(), Status: sess.Status, Ms: time.Since(start).Milliseconds(), WireBytes: sess.Bytes() - before}
	if err != nil {
		vp.Class, vp.Reason = detect.Transport.String(), redact(err.Error())
	}
	r.book("browser_video_page", vp)
	out.VideoPage = &vp
	if err != nil {
		return out
	}
	vc := sess.Captured(ctx, mark)
	out.VideoPageCalls = r.pageCalls(vc)
	if x := findCall(vc, commentListPath, true); x != nil {
		if next := nextPageURL(x, "cursor"); next != "" {
			r.signedCall(ctx, sess, id, out, "comments_from_page", next, hdr)
		}
	}
	return out
}

const (
	postListPath    = "/api/post/item_list/"
	commentListPath = "/api/comment/list/"
)

// ownKinds are the page calls booked under the summary's calls (as
// browser_own_<name>); the page makes many others, recorded in PageCalls.
var ownKinds = map[string]string{
	postListPath: "post_list", "/api/creator/item_list/": "creator_list", commentListPath: "comments",
	"/api/comment/list/reply/": "replies", "/api/user/detail/": "user_detail", "/api/item/detail/": "item_detail",
}

// pageCalls records a page's own API calls; the ones tks cares about are
// booked and their first answers kept under raw/.
func (r *runner) pageCalls(xs []browser.Exchange) []PageCall {
	var out []PageCall
	for _, x := range xs {
		u, err := url.Parse(x.URL)
		if err != nil {
			continue
		}
		pc := PageCall{Path: u.Path, Status: x.Status, Bytes: len(x.Body), Items: itemsIn(x.Body),
			Empty: len(strings.TrimSpace(string(x.Body))) == 0, Signed: u.Query().Get("X-Gnarly") != ""}
		out = append(out, pc)
		if kind, ok := ownKinds[u.Path]; ok {
			cls := detect.OK.String()
			if pc.Items == 0 {
				cls = detect.RegistryStale.String()
			}
			r.book("browser_own_"+kind, Step{Class: cls, Status: pc.Status, Empty: pc.Empty, WireBytes: int64(pc.Bytes)})
			r.keepRaw("browser_own_"+kind, x.Body, cls != detect.OK.String())
		}
	}
	return out
}

// signedCall makes one call from the page and replays the URL the page
// sent by plain HTTP from the probe's identity (T27). It returns the
// page's answer.
func (r *runner) signedCall(ctx context.Context, sess *browser.Session, id *session.Identity, out *Signed, name, raw string, hdr [][2]string) []byte {
	f, err := sess.Fetch(ctx, raw)
	if err != nil {
		out.Calls[name] = &SignedCall{Status: 0}
		r.book("signed_"+name, Step{Class: detect.Transport.String(), Reason: redact(err.Error())})
		return nil
	}
	sc := &SignedCall{Status: f.Status, Ms: f.Elapsed.Milliseconds(), Empty: len(strings.TrimSpace(string(f.Body))) == 0, Items: itemsIn(f.Body)}
	cls := classOfSigned(sc)
	r.book("signed_"+name, Step{Class: cls, Status: f.Status, Ms: sc.Ms, Empty: sc.Empty})
	r.keepRaw("signed_"+name, f.Body, cls != detect.OK.String())
	if f.SentURL != "" {
		sc.Added = added(raw, f.SentURL)
		s := r.replay(ctx, id, f.SentURL, hdr)
		sc.Replay = &s
	}
	out.Calls[name] = sc
	return f.Body
}

// itemsIn counts what an answer carries: list items, comments, or one
// account or post.
func itemsIn(body []byte) int {
	parts, _, _ := ttclient.Decode(body)
	if len(parts) == 0 {
		return 0
	}
	p := parts[0]
	switch {
	case p.Get("userInfo.user.id").String() != "", p.Get("itemInfo.itemStruct.id").String() != "":
		return 1
	case p.Get("comments").Exists():
		return len(p.Get("comments").Array())
	default:
		return len(p.Get("itemList").Array())
	}
}

// findCall returns the last captured call to path (with items, when
// withItems), or nil.
func findCall(xs []browser.Exchange, path string, withItems bool) *browser.Exchange {
	for i := len(xs) - 1; i >= 0; i-- {
		u, err := url.Parse(xs[i].URL)
		if err != nil || u.Path != path {
			continue
		}
		if !withItems || itemsIn(xs[i].Body) > 0 {
			return &xs[i]
		}
	}
	return nil
}

// nextPageURL is the page's own call asking for its next page: the same
// parameters, the signatures removed (the page's script adds new ones) and
// the cursor its answer gave. Empty when the answer has no more.
func nextPageURL(x *browser.Exchange, cursorKey string) string {
	parts, _, _ := ttclient.Decode(x.Body)
	if len(parts) == 0 {
		return ""
	}
	p := parts[0]
	if !p.Get("hasMore").Bool() && p.Get("has_more").Int() != 1 {
		return ""
	}
	u, err := url.Parse(x.URL)
	if err != nil {
		return ""
	}
	q := u.Query()
	for _, k := range []string{"X-Bogus", "X-Gnarly", "X-Dynosaur", "msToken"} {
		q.Del(k)
	}
	q.Set("cursor", p.Get(cursorKey).String())
	u.RawQuery = q.Encode()
	return u.String()
}

// paramDiff lists the query parameter names the page's call has and tks's
// does not (signatures aside), and the reverse.
func paramDiff(pageURL, ours string) (missing, extra []string) {
	a, err1 := url.Parse(pageURL)
	b, err2 := url.Parse(ours)
	if err1 != nil || err2 != nil {
		return nil, nil
	}
	pa, pb := a.Query(), b.Query()
	for k := range pa {
		if _, ok := pb[k]; !ok && !slices.Contains([]string{"X-Bogus", "X-Gnarly", "X-Dynosaur", "msToken"}, k) {
			missing = append(missing, k)
		}
	}
	for k := range pb {
		if _, ok := pa[k]; !ok {
			extra = append(extra, k)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	return missing, extra
}

// firstCommented is the newest unpinned post with comments, or nil.
func firstCommented(posts []Post) *Post {
	for i := range posts {
		if !posts[i].Pinned && posts[i].Comments > 0 {
			return &posts[i]
		}
	}
	return nil
}

func classOfSigned(sc *SignedCall) string {
	if sc.Status == 200 && !sc.Empty {
		return detect.OK.String()
	}
	return detect.RegistryStale.String()
}

// replay sends a page-signed URL by plain HTTP from the probe's identity.
func (r *runner) replay(ctx context.Context, id *session.Identity, raw string, hdr [][2]string) Step {
	if err := id.Wait(ctx); err != nil {
		return Step{Class: stopped}
	}
	start := time.Now()
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: raw, Header: hdr})
	if err != nil {
		s := Step{Class: detect.Transport.String(), Reason: redact(err.Error())}
		r.book("signed_replay", s)
		return s
	}
	parts, errs, hasData := ttclient.Decode(resp.Body)
	cls, reason := r.o.Client.Rules.Classify(detect.Input{Status: resp.Status, Location: resp.Location, ContentType: resp.Header.Get("Content-Type"),
		Body: resp.Body, Errors: errs, HasData: hasData, Expect: "graphql"})
	if cls == detect.OK && len(parts) == 0 {
		cls, reason = detect.RegistryStale, "empty answer"
	}
	s := Step{Class: cls.String(), Status: resp.Status, Reason: reason, Ms: time.Since(start).Milliseconds(), WireBytes: resp.BytesIn + resp.BytesOut,
		Empty: len(strings.TrimSpace(string(resp.Body))) == 0}
	r.book("signed_replay", s)
	return s
}

// added lists the query parameters the page added to a URL.
func added(sent, got string) []string {
	a, err1 := url.Parse(sent)
	b, err2 := url.Parse(got)
	if err1 != nil || err2 != nil {
		return nil
	}
	have := a.Query()
	var out []string
	for k := range b.Query() {
		if _, ok := have[k]; !ok {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// noCookies is the doer of the browser page's stand-in identity: it is
// only asked for cookies, and has none.
type noCookies struct{ transport.Doer }

func (noCookies) Cookie(*url.URL, string) string { return "" }

func (r *runner) keepRaw(kind string, body []byte, failed bool) {
	if r.o.RawDir == "" {
		return
	}
	if !failed && r.rawSeen[kind] {
		return
	}
	r.rawSeen[kind] = true
	name := kind + ".json"
	if failed {
		name = fmt.Sprintf("failed/%s-%d.json", kind, len(r.rawSeen)+r.rep.Summary.Captchas)
	}
	r.saveRaw(name, body)
}

func (r *runner) saveRaw(name string, body []byte) {
	if r.o.RawDir == "" {
		return
	}
	p := filepath.Join(r.o.RawDir, name)
	_ = os.MkdirAll(filepath.Dir(p), 0o700)
	_ = os.WriteFile(p, body, 0o600)
}

func (r *runner) saveFile(dir, sub, name string, body []byte) string {
	d := filepath.Join(dir, sub)
	if err := os.MkdirAll(d, 0o700); err != nil {
		return ""
	}
	p := filepath.Join(d, name)
	if err := os.WriteFile(p, body, 0o600); err != nil {
		return ""
	}
	return filepath.Join(filepath.Base(dir), sub, name)
}

var credentials = regexp.MustCompile(`://[^/@\s]+@`)

// redact removes proxy credentials from error text.
func redact(s string) string { return credentials.ReplaceAllString(s, "://***@") }

var unsafe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func safe(s string) string { return unsafe.ReplaceAllString(s, "_") }

// ErrNoAccounts is returned by the command when the list is empty.
var ErrNoAccounts = errors.New("probe: no accounts")
