package discprobe

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Where a signed call's signer page was opened.
const (
	SignOnProfile = "profile" // a profile page, as the crawler's pool opens them
	SignOnSource  = "source"  // the call's own page (search, the feed, a tag)
)

// lane is one identity working through its sources.
type lane struct {
	k       *Kit
	n       int
	id      *session.Identity
	exit    string
	region  string
	profile string // the bootstrap account's page: where signer pages open
	chosen  map[string]chosen
	tried   map[string]int
	secs    map[int]account // the owner's accounts read so far, by position
}

// chosen is the variant a source walks with.
type chosen struct {
	q      registry.Query
	signOn string
	none   bool // nothing answered on the items tried: the source gives up
}

type account struct {
	sec  string
	name string
}

func (l *lane) release() {
	if l.id != nil {
		l.k.Stack.Pool.Release(l.id)
		l.id = nil
	}
}

// bootstrap takes an identity and loads one of the owner's profile pages
// for its page values, trying up to three accounts.
func (l *lane) bootstrap(ctx context.Context) bool {
	k := l.k
	accts := k.Cfg.Accounts
	for t := range 3 {
		if ctx.Err() != nil {
			return false
		}
		i := (l.n + 2*t) % len(accts)
		id, err := k.Stack.Pool.Acquire(ctx, k.Cfg.Tier)
		r := Record{Source: "lane", Op: OpBootstrap, Item: i + 1}
		if err != nil {
			r.Class, r.Reason = detect.Transport.String(), phase0.Clean(err.Error())
			l.add(r)
			continue
		}
		start := time.Now()
		pl, err := k.Stack.Loader.Load(ctx, id, k.Stack.BaseURL+"/@"+accts[i])
		r.Ms, r.Exit = time.Since(start).Milliseconds(), k.exits.Label(id)
		if pl == nil {
			r.Class = detect.Transport.String()
			if err != nil {
				r.Reason = phase0.Clean(err.Error())
			}
			l.add(r)
			k.Stack.Pool.Release(id)
			continue
		}
		r.Class, r.Status, r.Wire, r.Region = pl.Class.String(), pl.Status, pl.Bytes, id.Tokens()["region"]
		l.add(r)
		if pl.Class != detect.OK || id.Tokens()["device_id"] == "" {
			k.Stack.Pool.Release(id)
			continue
		}
		l.id, l.exit, l.region, l.profile = id, r.Exit, r.Region, k.Stack.BaseURL+"/@"+accts[i]
		l.chosen, l.tried, l.secs = map[string]chosen{}, map[string]int{}, map[int]account{}
		return true
	}
	return false
}

// add records r with the lane's exit and region.
func (l *lane) add(r Record) {
	r.At = l.k.now().UTC()
	if r.Exit == "" {
		r.Exit = l.exit
	}
	if r.Region == "" {
		r.Region = l.region
	}
	r.Reason = phase0.Clean(r.Reason)
	l.k.rec.add(r)
}

// refSigner signs on the pool's page for referer, whatever page the call
// itself names: the crawler's pages open on a profile (open_on: profile).
type refSigner struct {
	pool    *sign.Pool
	referer string
}

func (s refSigner) SignURL(ctx context.Context, id *session.Identity, q, rawURL, _ string) (string, error) {
	return s.pool.SignURL(ctx, id, q, rawURL, s.referer)
}

func (s refSigner) SignOutcome(ctx context.Context, sentURL string, cls detect.Class) {
	s.pool.SignOutcome(ctx, sentURL, cls)
}

// answer is one call's outcome.
type answer struct {
	found Found
	rec   Record
	ok    bool
	body  gjson.Result
}

// call sends one variant of an operation and records it.
// callDeadline bounds one call of the probe, from its signature to its
// answer: the pacing, a signer page's opening (tt.signer.page_timeout) and
// signing, and the request itself fit well within it.
const callDeadline = 3 * time.Minute

func (l *lane) call(ctx context.Context, src string, item, page int, q registry.Query, signOn string, vars map[string]any, referer string, try bool) answer {
	k := l.k
	client := &graphql.Client{Registry: registry.NewStatic(registry.Seed{ClientParams: k.seed.ClientParams, Queries: []registry.Query{q}}),
		Rules: k.Stack.Rules, Builder: ttclient.Builder{AcceptLanguage: k.Cfg.AcceptLanguage}, Decode: ttclient.Decode}
	pool, ref := k.profile, l.profile
	if signOn == SignOnSource && k.source != nil {
		pool, ref = k.source, referer
	}
	if pool != nil {
		client.Signer = ttclient.SignerFor(config.SignerBrowser, refSigner{pool: pool, referer: ref})
	}
	r := Record{Source: src, Op: q.Name, Variant: q.DocID, Item: item, Page: page, Try: try}
	if ttclient.Signed(q) && pool != nil {
		r.Signed, r.SignPage = true, first(signOn, SignOnProfile)
	}
	// A safety net: one call, its signature included, never holds the lane
	// longer than callDeadline (C6 run 1 waited 36 minutes on a signer page
	// that would not close).
	cctx, cancel := context.WithTimeout(ctx, callDeadline)
	resp, err := client.Do(cctx, l.id, graphql.Request{Query: q.Name, Vars: vars, Referer: referer})
	cancel()
	if err != nil {
		r.Class, r.Reason = detect.Transport.String(), err.Error()
		if isSignErr(err) {
			r.Class = "sign_error"
		}
		l.add(r)
		return answer{rec: r}
	}
	r.Class, r.Status, r.Reason, r.Ms, r.Wire = resp.Class.String(), resp.Status, resp.Reason, resp.Elapsed.Milliseconds(), resp.BytesIn+resp.BytesOut
	r.SignRetried = resp.SignRetried
	if len(resp.Errors) > 0 {
		r.Code = resp.Errors[0].Code
	}
	a := answer{rec: r, ok: resp.Class == detect.OK}
	if len(resp.Parts) > 0 {
		a.body = resp.Parts[0]
		a.found = Extract(a.body)
		r.Keys, r.Shape, r.HasMore = Keys(a.body), a.found.Shape, a.found.HasMore
		r.Posts = len(a.found.Posts)
		if detailID(a.body) != "" {
			r.Results = 1 // a hashtag's or a sound's detail
		}
		r.Langs = map[string]int{}
		for _, p := range a.found.Posts {
			r.Langs[first(p.Lang, "none")]++
		}
		for _, u := range a.found.Users {
			r.Users++
			if k.person(src, u) {
				r.NewUsers++
			}
		}
	}
	if resp.Class == detect.Checkpoint {
		if k.captchas.Add(1) >= int64(k.Cfg.MaxCaptchas) {
			k.stop()
		}
	}
	k.keepRaw(src, q.Name, q.DocID, r.Class, resp.Body)
	a.rec = r
	l.add(r)
	return a
}

// signOns are where a variant's signer pages open: one place for an
// unsigned variant, the profile and (when compared) the call's own page
// for a signed one.
func (l *lane) signOns(q registry.Query) []string {
	if !ttclient.Signed(q) || l.k.profile == nil {
		return []string{""}
	}
	if l.k.source != nil {
		return []string{SignOnProfile, SignOnSource}
	}
	return []string{SignOnProfile}
}

// first sends an operation's first page: on a source's first two items,
// every variant (and signer page) is tried and the best kept for the rest
// of the source; after that, the chosen one. ok is false when nothing
// answered.
func (l *lane) first(ctx context.Context, src string, item int, name string, vars map[string]any, referer string) (answer, chosen, bool) {
	key := src + "|" + name
	if c, ok := l.chosen[key]; ok {
		if c.none {
			return answer{}, c, false
		}
		a := l.call(ctx, src, item, 1, c.q, c.signOn, vars, referer, false)
		return a, c, a.ok
	}
	var best, okOnly *answer
	var bestC, okC chosen
	var last answer
	for _, q := range candidates(l.k.seed, name) {
		for _, on := range l.signOns(q) {
			if ctx.Err() != nil {
				return answer{}, chosen{}, false
			}
			a := l.call(ctx, src, item, 1, q, on, vars, referer, true)
			last = a
			switch {
			case !a.ok:
			case best == nil && (a.rec.Posts > 0 || a.rec.Users > 0 || detailID(a.body) != ""):
				best, bestC = &a, chosen{q: q, signOn: on}
			case okOnly == nil:
				okOnly, okC = &a, chosen{q: q, signOn: on}
			}
		}
	}
	if best == nil && okOnly != nil {
		best, bestC = okOnly, okC
	}
	if best == nil {
		l.tried[key]++
		if l.tried[key] >= 2 {
			l.chosen[key] = chosen{none: true}
			l.k.issue(src + ": " + name + " answered no variant on two items; the source stopped")
		}
		return last, chosen{}, false
	}
	// A crawler would sign on a profile page: keep that one when both
	// answered.
	if bestC.signOn == SignOnSource {
		bestC.signOn = SignOnProfile
	}
	l.chosen[key] = bestC
	return *best, bestC, true
}

// walkResult sums one item's pages.
type walkResult struct {
	pages, posts, users, newUsers int
	stop                          string
}

// walk reads an item's list page after page: the first page as first
// does, then the chosen variant, until TikTok says there is no more, stops
// answering, or the cap. vars builds a page's values from the offset and
// the previous answer.
func (l *lane) walk(ctx context.Context, src string, item int, name string, maxPages int, referer string,
	vars func(offset int, prev Found) map[string]any, onFound func(Found)) walkResult {
	var res walkResult
	sum := Record{Source: src, Op: name, Item: item, Summary: true}
	defer func() {
		sum.Pages, sum.Stop, sum.Posts, sum.Users, sum.NewUsers = res.pages, res.stop, res.posts, res.users, res.newUsers
		l.add(sum)
	}()
	a, c, ok := l.first(ctx, src, item, name, vars(0, Found{}), referer)
	switch {
	case c.none:
		res.stop = "gave_up" // no variant answered on the source's first items
		return res
	case !ok:
		res.stop = stopOf(a.rec, "refused")
		return res
	}
	sum.Variant, sum.Signed, sum.SignPage = c.q.DocID, ttclient.Signed(c.q) && l.k.profile != nil, c.signOn
	seen := map[string]bool{}
	offset, stuck := 0, 0
	for page := 1; ; page++ {
		res.pages++
		res.posts += a.rec.Posts
		res.users += a.rec.Users
		res.newUsers += a.rec.NewUsers
		if onFound != nil {
			onFound(a.found)
		}
		fresh := 0
		for _, p := range a.found.Posts {
			if !seen[p.ID] {
				seen[p.ID], fresh = true, fresh+1
			}
		}
		for _, u := range a.found.Users {
			if !seen["u"+u.ID+u.Username] {
				seen["u"+u.ID+u.Username], fresh = true, fresh+1
			}
		}
		switch {
		case a.rec.Posts+a.rec.Users == 0:
			res.stop = "empty"
			return res
		case a.found.HasMore != nil && !*a.found.HasMore:
			res.stop = "end"
			return res
		case page >= maxPages:
			res.stop = "cap"
			return res
		case ctx.Err() != nil:
			res.stop = "stopped"
			return res
		}
		if fresh == 0 {
			if stuck++; stuck >= 2 {
				res.stop = "stuck"
				return res
			}
		} else {
			stuck = 0
		}
		next := offset + a.rec.Posts + a.rec.Users
		if n, err := strconv.Atoi(a.found.Cursor); err == nil && n > offset {
			next = n
		}
		offset = next
		a = l.call(ctx, src, item, page+1, c.q, c.signOn, vars(offset, a.found), referer, false)
		if !a.ok {
			res.stop = stopOf(a.rec, "refused")
			return res
		}
	}
}

// stopOf names why a walk stopped at a record: its class, with TikTok's
// code when it gave one.
func stopOf(r Record, def string) string {
	if r.Class == "" {
		return def
	}
	if r.Code != 0 {
		return r.Class + ":" + strconv.FormatInt(r.Code, 10)
	}
	return r.Class
}

func isSignErr(err error) bool { return errors.Is(err, graphql.ErrSign) }
