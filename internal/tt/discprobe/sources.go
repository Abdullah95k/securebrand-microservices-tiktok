package discprobe

import (
	"context"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// Item names one of a source's items in the report: its position, where
// it came from, and its label when it is not personal (a keyword of the
// Iraq list, or a hashtag made of one); hashtags and sounds found in
// results, and posts and accounts, have none.
type Item struct {
	Index  int    `json:"index"`
	Origin string `json:"origin"`
	Label  string `json:"label,omitempty"`
}

func (k *Kit) addItem(src string, it Item) {
	k.mu.Lock()
	k.items[src] = append(k.items[src], it)
	k.mu.Unlock()
}

func (k *Kit) foundPosts(f Found) {
	k.mu.Lock()
	k.posts = append(k.posts, f.Posts...)
	k.mu.Unlock()
}

func (k *Kit) snapshot() []Post {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]Post(nil), k.posts...)
}

// run runs one source.
func (l *lane) run(ctx context.Context, src string) {
	switch src {
	case SrcSearch:
		l.search(ctx, SrcSearch, OpSearch, l.k.Cfg.Limits.Keywords, l.k.Cfg.Limits.SearchPages, "/search")
	case SrcSearchUser:
		l.search(ctx, SrcSearchUser, OpSearchUser, l.k.Cfg.Limits.UserKeywords, l.k.Cfg.Limits.UserPages, "/search/user")
	case SrcFeed:
		l.feed(ctx)
	case SrcHashtag:
		l.hashtags(ctx)
	case SrcSound:
		l.sounds(ctx)
	case SrcRelated:
		l.related(ctx)
	case SrcEntry:
		l.entry(ctx)
	case SrcFollowing:
		l.follows(ctx, SrcFollowing, sceneFollowing)
	case SrcFollowers:
		l.follows(ctx, SrcFollowers, sceneFollowers)
	case SrcReposts:
		l.reposts(ctx)
	case SrcVanished:
		l.vanished(ctx)
	case SrcShortLinks:
		l.shortLinks(ctx)
	}
}

// Scenes of the following and followers lists (public sources; T36).
const (
	sceneFollowing = 21
	sceneFollowers = 67
)

// search reads n keywords' results, maxPages each; the first keyword as
// deep as DeepPages, to find where TikTok stops.
func (l *lane) search(ctx context.Context, src, op string, n, maxPages int, path string) {
	k := l.k
	for i, kw := range k.Cfg.Keywords[:min(n, len(k.Cfg.Keywords))] {
		if ctx.Err() != nil {
			return
		}
		k.addItem(src, Item{Index: i + 1, Origin: kw.Group + "/" + kw.Language, Label: kw.Term})
		pages := maxPages
		if i == 0 && op == OpSearch {
			pages = max(pages, k.Cfg.Limits.DeepPages)
		}
		referer := k.Stack.BaseURL + path + "?q=" + url.QueryEscape(kw.Term)
		l.walk(ctx, src, i+1, op, pages, referer, func(off int, prev Found) map[string]any {
			v := map[string]any{"keyword": kw.Term, "offset": off}
			if prev.SearchID != "" {
				v["search_id"] = prev.SearchID
			}
			return v
		}, k.foundPosts)
	}
}

// feed reads the For You feed FeedReads times and counts the posts each
// read brings that the earlier ones did not.
func (l *lane) feed(ctx context.Context) {
	k := l.k
	seen := map[string]bool{}
	referer := k.Stack.BaseURL + "/foryou"
	for i := range k.Cfg.Limits.FeedReads {
		if ctx.Err() != nil {
			return
		}
		a, _, ok := l.first(ctx, SrcFeed, i+1, OpRecommend, map[string]any{"pull": i + 1}, referer)
		sum := Record{Source: SrcFeed, Op: OpRecommend, Item: i + 1, Summary: true, Pages: 1}
		if !ok {
			sum.Stop = stopOf(a.rec, "refused")
			l.add(sum)
			continue
		}
		for _, p := range a.found.Posts {
			if !seen[p.ID] {
				seen[p.ID] = true
				sum.NewPosts++
			}
		}
		sum.Posts, sum.Users, sum.NewUsers, sum.Stop = a.rec.Posts, a.rec.Users, a.rec.NewUsers, "read"
		l.add(sum)
		k.foundPosts(a.found)
	}
}

// ranked returns the most frequent values first (ties in first-seen
// order).
func ranked(vals []string) []string {
	count := map[string]int{}
	var order []string
	for _, v := range vals {
		if v == "" {
			continue
		}
		if count[v] == 0 {
			order = append(order, v)
		}
		count[v]++
	}
	sort.SliceStable(order, func(i, j int) bool { return count[order[i]] > count[order[j]] })
	return order
}

// hashtags reads hashtag pages and their lists: half made of keywords,
// half the hashtags search and the feed found most.
func (l *lane) hashtags(ctx context.Context) {
	k := l.k
	type tag struct{ name, origin, label string }
	var tags []tag
	have := map[string]bool{}
	for _, kw := range k.Cfg.Keywords {
		if len(tags) >= (k.Cfg.Limits.Hashtags+1)/2 {
			break
		}
		if h := hashtagOf(kw.Term); h != "" && !have[h] {
			have[h] = true
			tags = append(tags, tag{h, "keyword", "#" + h})
		}
	}
	var found []string
	for _, p := range k.snapshot() {
		for _, h := range p.Hashtags {
			found = append(found, strings.ToLower(h))
		}
	}
	for _, h := range ranked(found) {
		if len(tags) >= k.Cfg.Limits.Hashtags {
			break
		}
		if !have[h] {
			have[h] = true
			tags = append(tags, tag{h, "results", ""})
		}
	}
	for i, t := range tags {
		if ctx.Err() != nil {
			return
		}
		k.addItem(SrcHashtag, Item{Index: i + 1, Origin: t.origin, Label: t.label})
		pageURL := k.Stack.BaseURL + "/tag/" + url.PathEscape(t.name)
		id := l.page(ctx, SrcHashtag, i+1, pageURL, "webapp.challenge-detail")
		if a, _, ok := l.first(ctx, SrcHashtag, i+1, OpChallengeDetail, map[string]any{"name": t.name}, pageURL); ok && id == "" {
			id = detailID(a.body)
		}
		if id == "" {
			l.add(Record{Source: SrcHashtag, Op: OpChallengeList, Item: i + 1, Summary: true, Stop: "no_id"})
			continue
		}
		l.walk(ctx, SrcHashtag, i+1, OpChallengeList, k.Cfg.Limits.HashtagPages, pageURL, func(off int, _ Found) map[string]any {
			return map[string]any{"id": id, "offset": off}
		}, nil)
	}
}

// sounds reads the pages and lists of the sounds found posts use most.
func (l *lane) sounds(ctx context.Context) {
	k := l.k
	var ids []string
	for _, p := range k.snapshot() {
		ids = append(ids, p.MusicID)
	}
	for i, id := range ranked(ids)[:min(k.Cfg.Limits.Sounds, len(ranked(ids)))] {
		if ctx.Err() != nil {
			return
		}
		k.addItem(SrcSound, Item{Index: i + 1, Origin: "results"})
		pageURL := k.Stack.BaseURL + "/music/sound-" + id
		l.page(ctx, SrcSound, i+1, pageURL, "webapp.music-detail")
		l.first(ctx, SrcSound, i+1, OpMusicDetail, map[string]any{"id": id}, pageURL)
		l.walk(ctx, SrcSound, i+1, OpMusicList, k.Cfg.Limits.SoundPages, pageURL, func(off int, _ Found) map[string]any {
			return map[string]any{"id": id, "offset": off}
		}, nil)
	}
}

// page loads a source's page by plain HTTP and records its scope's status
// code; it returns the id the scope names (a hashtag's or a sound's).
func (l *lane) page(ctx context.Context, src string, item int, pageURL, scope string) string {
	k := l.k
	start := time.Now()
	pl, err := k.Stack.Loader.Load(ctx, l.id, pageURL)
	r := Record{Source: src, Op: OpPage, Item: item, Page: 1, Ms: time.Since(start).Milliseconds()}
	if pl == nil {
		r.Class = detect.Transport.String()
		if err != nil {
			r.Reason = err.Error()
		}
		l.add(r)
		return ""
	}
	r.Class, r.Status, r.Wire = pl.Class.String(), pl.Status, pl.Bytes
	if htmlx.IsChallenge(pl.HTML) {
		r.Class = "challenge"
	}
	id := ""
	if s, ok := htmlx.Scope(pl.HTML, scope); ok {
		r.Code, r.Keys = s.Get("statusCode").Int(), Keys(s)
		id = detailID(s)
		if id != "" {
			r.Note = "id"
		}
	} else if ds, ok := htmlx.Rehydration(pl.HTML); ok {
		r.Keys, r.Note = Keys(ds), "no "+scope
	}
	k.keepRaw(src, OpPage, "", r.Class, pl.HTML)
	l.add(r)
	return id
}

// related reads the related posts of found posts, one per author.
func (l *lane) related(ctx context.Context) {
	k := l.k
	authors := map[string]bool{}
	n := 0
	for _, p := range k.snapshot() {
		if n >= k.Cfg.Limits.Related || ctx.Err() != nil {
			return
		}
		if p.ID == "" || p.Author.Username == "" || authors[p.Author.Username] {
			continue
		}
		authors[p.Author.Username] = true
		n++
		k.addItem(SrcRelated, Item{Index: n, Origin: "results"})
		referer := k.Stack.BaseURL + "/@" + p.Author.Username + "/video/" + p.ID
		a, _, ok := l.first(ctx, SrcRelated, n, OpRelated, map[string]any{"item_id": p.ID}, referer)
		sum := Record{Source: SrcRelated, Op: OpRelated, Item: n, Summary: true, Pages: 1, Stop: "read"}
		if !ok {
			sum.Stop = stopOf(a.rec, "refused")
		} else {
			sum.Posts, sum.Users, sum.NewUsers = a.rec.Posts, a.rec.Users, a.rec.NewUsers
		}
		l.add(sum)
	}
}

// entry reads one creator-list page of found authors who are not on the
// owner's list, as the entry check does: where they post from (search
// results carry no post region; discovery-plan §3, §8.3).
func (l *lane) entry(ctx context.Context) {
	k := l.k
	done := map[string]bool{}
	n := 0
	q, ok := query(k.seed, OpCreatorList)
	if !ok {
		return
	}
	for _, p := range k.snapshot() {
		u := p.Author
		if n >= k.Cfg.Limits.Entry || ctx.Err() != nil {
			return
		}
		if u.SecUID == "" || u.Private || done[u.SecUID] || k.known[strings.ToLower(u.Username)] {
			continue
		}
		done[u.SecUID] = true
		n++
		k.addItem(SrcEntry, Item{Index: n, Origin: "results"})
		referer := k.Stack.BaseURL + "/@" + u.Username
		a := l.call(ctx, SrcEntry, n, 1, q, "", map[string]any{"sec_uid": u.SecUID, "count": 15, "cursor": (k.now().Unix() + 1) * 1000}, referer, false)
		sum := Record{Source: SrcEntry, Op: OpCreatorList, Item: n, Summary: true, Pages: 1, Stop: stopOf(a.rec, "read")}
		if a.ok {
			sum.Stop, sum.Posts = "read", a.rec.Posts
			sum.Locations, sum.Langs = map[string]int{}, a.rec.Langs
			for _, p := range a.found.Posts {
				sum.Locations[first(p.Region, "none")]++
			}
		}
		l.add(sum)
	}
}

// ownerAccount reads one of the owner's profile pages (once per lane): its
// secUid, whether it shows its following list, and its counts.
func (l *lane) ownerAccount(ctx context.Context, src string, i int) (account, bool) {
	if a, ok := l.secs[i]; ok {
		return a, a.sec != ""
	}
	k := l.k
	name := k.Cfg.Accounts[i]
	start := time.Now()
	pl, err := k.Stack.Loader.Load(ctx, l.id, k.Stack.BaseURL+"/@"+name)
	r := Record{Source: src, Op: OpProfile, Item: i + 1, Ms: time.Since(start).Milliseconds()}
	a := account{name: name}
	if pl == nil {
		r.Class = detect.Transport.String()
		if err != nil {
			r.Reason = err.Error()
		}
	} else {
		r.Class, r.Status, r.Wire = pl.Class.String(), pl.Status, pl.Bytes
		if s, ok := htmlx.Scope(pl.HTML, htmlx.ScopeUserDetail); ok {
			u := s.Get("userInfo.user")
			a.sec = u.Get("secUid").String()
			r.Code = s.Get("statusCode").Int()
			r.Total = s.Get("userInfo.stats.followerCount").Int()
			r.Note = "followingVisibility=" + first(u.Get("followingVisibility").String(), "absent")
			if u.Get("privateAccount").Bool() {
				r.Note += ",private"
				a.sec = ""
			}
		}
	}
	l.add(r)
	l.secs[i] = a
	return a, a.sec != ""
}

// spreadIdx picks n positions spread over m.
func spreadIdx(m, n int) []int {
	n = min(n, m)
	out := make([]int, 0, n)
	for i := range n {
		out = append(out, i*m/n)
	}
	return out
}

// follows reads the following (scene 21) or followers (scene 67) lists of
// a few of the owner's accounts: whether a logged-out visitor gets them at
// all (discovery-plan §9).
func (l *lane) follows(ctx context.Context, src string, scene int) {
	k := l.k
	for _, i := range spreadIdx(len(k.Cfg.Accounts), k.Cfg.Limits.FollowAccounts) {
		if ctx.Err() != nil {
			return
		}
		a, ok := l.ownerAccount(ctx, src, i)
		if !ok {
			l.add(Record{Source: src, Op: OpUserList, Item: i + 1, Summary: true, Stop: "no_account"})
			continue
		}
		k.addItem(src, Item{Index: i + 1, Origin: "owner"})
		referer := k.Stack.BaseURL + "/@" + a.name
		l.walk(ctx, src, i+1, OpUserList, k.Cfg.Limits.FollowPages, referer, func(off int, _ Found) map[string]any {
			return map[string]any{"sec_uid": a.sec, "scene": scene, "offset": off}
		}, nil)
	}
}

// reposts reads the first page of the owner's accounts' reposts.
func (l *lane) reposts(ctx context.Context) {
	k := l.k
	for _, i := range spreadIdx(len(k.Cfg.Accounts), k.Cfg.Limits.FollowAccounts) {
		if ctx.Err() != nil {
			return
		}
		a, ok := l.ownerAccount(ctx, SrcReposts, i)
		if !ok {
			continue
		}
		k.addItem(SrcReposts, Item{Index: i + 1, Origin: "owner"})
		l.walk(ctx, SrcReposts, i+1, OpReposts, 1, k.Stack.BaseURL+"/@"+a.name, func(off int, _ Found) map[string]any {
			return map[string]any{"sec_uid": a.sec, "offset": off}
		}, nil)
	}
}

// vanished is T34: posts an earlier probe listed that the creator list no
// longer returns between its neighbours are read by their pages, as M8b
// confirms a candidate; and the owner's links to posts known to be gone,
// with a creator-list page at their date.
func (l *lane) vanished(ctx context.Context) {
	k := l.k
	byName := map[string][]KnownPost{}
	var names []string
	for _, p := range k.Cfg.Known {
		n := strings.ToLower(p.Username)
		if byName[n] == nil {
			names = append(names, n)
		}
		byName[n] = append(byName[n], p)
	}
	index := map[string]int{}
	for i, a := range k.Cfg.Accounts {
		index[strings.ToLower(a)] = i
	}
	pagesLeft := k.Cfg.Limits.VanishedPages
	q, _ := query(k.seed, OpCreatorList)
	for _, j := range spreadIdx(len(names), k.Cfg.Limits.VanishedAccounts) {
		if ctx.Err() != nil || pagesLeft <= 0 {
			break
		}
		name := names[j]
		i, ok := index[name]
		if !ok {
			continue
		}
		a, ok := l.ownerAccount(ctx, SrcVanished, i)
		sum := Record{Source: SrcVanished, Op: OpCreatorList, Item: i + 1, Summary: true}
		if !ok {
			sum.Stop = "no_account"
			l.add(sum)
			continue
		}
		listed := map[string]bool{}
		var lo, hi int64
		cursor := (k.now().Unix() + 1) * 1000
		for page := 1; page <= 2; page++ {
			r := l.call(ctx, SrcVanished, i+1, page, q, "", map[string]any{"sec_uid": a.sec, "count": 15, "cursor": cursor}, k.Stack.BaseURL+"/@"+a.name, false)
			if !r.ok {
				break
			}
			sum.Pages++
			for _, p := range r.found.Posts {
				listed[p.ID] = true
				if p.Created > 0 && (lo == 0 || p.Created < lo) {
					lo = p.Created
				}
				hi = max(hi, p.Created)
			}
			next, more := ttclient.NextCreatorCursor(r.body, cursor)
			if !more {
				break
			}
			cursor = next
		}
		// Candidates: strictly between the oldest and newest posts listed,
		// as the account stage compares (ADR 0007 §17).
		var cands []KnownPost
		for _, p := range byName[name] {
			t := p.Created.Unix()
			if t > lo && t < hi && !listed[p.ID] {
				cands = append(cands, p)
			}
		}
		sum.Results, sum.Stop = len(cands), "read"
		l.add(sum)
		for _, c := range cands[:min(3, len(cands))] {
			if pagesLeft <= 0 {
				break
			}
			pagesLeft--
			l.postPage(ctx, i+1, k.Stack.BaseURL+"/@"+a.name+"/video/"+c.ID, "candidate")
		}
	}
	for g, link := range k.Cfg.Gone {
		if ctx.Err() != nil {
			return
		}
		k.addItem(SrcVanished, Item{Index: g + 1, Origin: "owner_gone"})
		l.postPage(ctx, g+1, link, "owner_gone")
		l.goneListed(ctx, g+1, link)
	}
}

// postPage reads a post's page and records its status code (T34) and
// whether the page still holds the post.
func (l *lane) postPage(ctx context.Context, item int, link, why string) {
	k := l.k
	start := time.Now()
	pl, err := k.Stack.Loader.Load(ctx, l.id, link)
	r := Record{Source: SrcVanished, Op: OpPostPage, Item: item, Ms: time.Since(start).Milliseconds(), Note: why}
	if pl == nil {
		r.Class = detect.Transport.String()
		if err != nil {
			r.Reason = err.Error()
		}
		l.add(r)
		return
	}
	r.Class, r.Status, r.Wire = pl.Class.String(), pl.Status, pl.Bytes
	if s, ok := htmlx.Scope(pl.HTML, htmlx.ScopeVideoDetail); ok {
		r.Code, r.Keys = s.Get("statusCode").Int(), Keys(s)
		if s.Get("itemInfo.itemStruct.id").String() != "" {
			r.Note += ",public"
		}
	} else {
		r.Note += ",no video-detail"
	}
	k.keepRaw(SrcVanished, OpPostPage, "", r.Class+"-"+strconv.FormatInt(r.Code, 10), pl.HTML)
	l.add(r)
}

var postLink = regexp.MustCompile(`/@([^/?#]+)/(?:video|photo)/(\d+)`)

// goneListed reads the creator list at a gone post's date (its id's time,
// T8): does the list still return it?
func (l *lane) goneListed(ctx context.Context, item int, link string) {
	k := l.k
	m := postLink.FindStringSubmatch(link)
	if m == nil {
		return
	}
	id, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return
	}
	pl, err := k.Stack.Loader.Load(ctx, l.id, k.Stack.BaseURL+"/@"+m[1])
	if err != nil || pl == nil {
		return
	}
	s, ok := htmlx.Scope(pl.HTML, htmlx.ScopeUserDetail)
	sec := s.Get("userInfo.user.secUid").String()
	if !ok || sec == "" {
		return
	}
	q, _ := query(k.seed, OpCreatorList)
	at := (id >> 32) + 3600 // an hour after the post
	a := l.call(ctx, SrcVanished, item, 1, q, "", map[string]any{"sec_uid": sec, "count": 15, "cursor": at * 1000}, k.Stack.BaseURL+"/@"+m[1], false)
	listed := false
	for _, p := range a.found.Posts {
		listed = listed || p.ID == m[2]
	}
	l.add(Record{Source: SrcVanished, Op: OpCreatorList, Item: item, Summary: true, Pages: 1, Stop: stopOf(a.rec, "read"),
		Listed: &listed, Note: "owner_gone"})
}

// shortLinks resolves short links: the redirect's status and the shape of
// where it points (ids and names replaced), then that page.
func (l *lane) shortLinks(ctx context.Context) {
	k := l.k
	for i, link := range k.Cfg.ShortLinks {
		if ctx.Err() != nil {
			return
		}
		k.addItem(SrcShortLinks, Item{Index: i + 1, Origin: "owner"})
		if err := l.id.Wait(ctx); err != nil {
			return
		}
		hdr := [][2]string{{"user-agent", l.id.FP.UserAgent}, {"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
			{"accept-language", k.Cfg.AcceptLanguage}}
		start := time.Now()
		resp, err := l.id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: link, Header: hdr})
		r := Record{Source: SrcShortLinks, Op: OpShortLink, Item: i + 1, Ms: time.Since(start).Milliseconds()}
		if err != nil {
			r.Class, r.Reason = detect.Transport.String(), err.Error()
			l.add(r)
			continue
		}
		r.Status, r.Wire, r.Class = resp.Status, resp.BytesIn+resp.BytesOut, "ok"
		loc := resp.Location
		if loc == "" {
			loc = resp.Header.Get("Location")
		}
		r.Location = Pattern(link, loc)
		l.add(r)
		if loc == "" {
			continue
		}
		target, err := url.Parse(link)
		if err != nil {
			continue
		}
		if u, err := target.Parse(loc); err == nil && strings.HasSuffix(u.Hostname(), hostOf(k.Stack.BaseURL)) {
			if !strings.Contains(u.Path, "/video/") && !strings.Contains(u.Path, "/photo/") {
				continue
			}
			l.postPage(ctx, 1000+i+1, u.String(), "short_link")
		}
	}
}

func hostOf(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(u.Hostname(), "www.")
}

var (
	digits = regexp.MustCompile(`\d{6,}`)
	handle = regexp.MustCompile(`/@[^/?#]+`)
)

// Pattern is a redirect target's shape, with nothing personal: host, path
// with ids as {id} and usernames as @{user}, and the query's parameter
// names. A relative target is resolved against from.
func Pattern(from, loc string) string {
	if loc == "" {
		return ""
	}
	base, _ := url.Parse(from)
	u, err := url.Parse(loc)
	if err != nil {
		return "unparsable"
	}
	if base != nil {
		u = base.ResolveReference(u)
	}
	path := handle.ReplaceAllString(u.Path, "/@{user}")
	path = digits.ReplaceAllString(path, "{id}")
	var names []string
	for k := range u.Query() {
		names = append(names, k)
	}
	sort.Strings(names)
	out := u.Hostname() + path
	if len(names) > 0 {
		out += "?" + strings.Join(names, "&")
	}
	return out
}
