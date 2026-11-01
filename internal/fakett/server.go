package fakett

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Faults model walls and the open questions (docs/tiktok/open-questions.md).
type Faults struct {
	// RequireSignature answers every API call without an X-Gnarly
	// parameter with an empty 200, as TikTok is reported to (T4); pages
	// then carry a script that signs the page's own fetch calls, so the
	// browser signer can be tested.
	RequireSignature bool
	// SignedPaths limits RequireSignature to these API paths (empty: all).
	SignedPaths []string
	// Challenge answers page loads with the JavaScript challenge page (T28).
	Challenge bool
	// ShellPages serves client-rendered profile pages with no account
	// data (T1).
	ShellPages bool
	// RateLimitEvery answers every Nth API call with HTTP 429.
	RateLimitEvery int
	// CaptchaAfter answers a device's API calls after the first N with
	// statusCode 10000 and a verify_event (T6).
	CaptchaAfter int
	// ListWallAfter answers list pages that start at or past this many
	// posts with an empty 200 (T11).
	ListWallAfter int
	// DeviceBanAfter makes the creator list repeat its first page for a
	// device after N calls (T29).
	DeviceBanAfter int
	// CursorBound makes a list cursor work only for the device it was
	// issued to (T22).
	CursorBound bool
	// NoGuestComments answers the comment calls with an empty list, as
	// some public sources report for guests (T12).
	NoGuestComments bool
	// MediaNeedsCookie refuses media without the page's tt_chain_token
	// cookie (T18).
	MediaNeedsCookie bool
	// ListWindow makes the creator list serve only posts within this long
	// before its cursor, as yt-dlp reports TikTok does (about a week): an
	// account quiet for longer answers an empty page, and after a gap a
	// page holds only the post at the cursor's second.
	ListWindow time.Duration
	// ListErrorEvery answers every Nth creator-list call with statusCode
	// 10102, the occasional error of the C3 full pass.
	ListErrorEvery int
}

// Options configure a server.
type Options struct {
	Faults Faults
}

// Server serves a World the way logged-out TikTok's web app is served.
type Server struct {
	World *World
	opts  Options

	mu        sync.Mutex
	devices   map[string]*device
	minted    uint64
	apiCalls  atomic.Int64
	listCalls atomic.Int64
	// Requests counts API calls by path.
	Requests sync.Map
}

type device struct {
	calls  int
	chain  string
	cursor map[string]bool // cursors issued to this device (CursorBound)
}

// NewServer returns a server for w.
func NewServer(w *World, o Options) *Server {
	return &Server{World: w, opts: o, devices: map[string]*device{}}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.World.mu.RLock()
	defer s.World.mu.RUnlock()
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/api/"):
		s.api(w, r)
	case strings.HasPrefix(p, "/fake-cdn/"):
		s.media(w, r)
	case p == "/oembed":
		s.oembed(w, r)
	case strings.HasPrefix(p, "/embed/@"):
		s.embed(w, r)
	case strings.HasPrefix(p, "/login"):
		page(w, `<html><body>Log in to TikTok</body></html>`)
	case strings.HasPrefix(p, "/@"):
		s.page(w, r)
	default:
		http.NotFound(w, r)
	}
}

// signer is the page script RequireSignature adds: it signs the page's
// own fetch calls, as TikTok's script is reported to.
const signer = `<script>(function(){const f=window.fetch;window.fetch=function(i,o){const u=new URL(i,location.href);` +
	`if(u.pathname.startsWith("/api/")){u.searchParams.set("X-Bogus","1");u.searchParams.set("X-Gnarly","fake-gnarly-"+u.searchParams.toString().length);}` +
	`return f(u.toString(),o);};})();</script>`

// page serves a profile, video or photo page: the rehydration document
// with the app context (a fresh device) and the account or the post.
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	if s.opts.Faults.Challenge {
		page(w, `<html><head><title>Please wait...</title></head><body><div id="cs" data-x="eyJ2IjoxfQ=="></div>`+
			`<script>document.cookie="_wafchallengeid=pending; max-age=1"</script></body></html>`)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	acc := s.World.Account(strings.TrimPrefix(parts[0], "@"))
	if acc == nil && len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	wid, odin, created := s.mint()
	chain := "chain-" + wid
	s.mu.Lock()
	s.devices[wid] = &device{chain: chain, cursor: map[string]bool{}}
	s.mu.Unlock()
	for _, c := range []*http.Cookie{{Name: "ttwid", Value: "fake-ttwid-" + wid}, {Name: "tt_chain_token", Value: chain},
		{Name: "msToken", Value: "fake-mstoken-" + wid}, {Name: "tt_csrf_token", Value: "csrf-" + odin}} {
		c.Path = "/"
		http.SetCookie(w, c)
	}
	w.Header().Set("x-ms-token", "fake-mstoken-"+wid)
	scope := map[string]any{"webapp.app-context": map[string]any{"language": "en", "region": "IQ", "appId": 1988, "wid": wid,
		"webIdCreatedTime": created, "odinId": odin, "botType": "others", "csrfToken": "csrf-" + odin}}
	switch {
	case len(parts) == 3 && (parts[1] == "video" || parts[1] == "photo"):
		id, _ := strconv.ParseInt(parts[2], 10, 64)
		post := s.World.Post(id)
		if post == nil || post.Owner != acc {
			http.NotFound(w, r)
			return
		}
		scope["webapp.video-detail"] = map[string]any{"itemInfo": map[string]any{"itemStruct": s.item(post, r.Host)}, "statusCode": 0}
	case len(parts) == 1 && acc == nil:
		// No account under this username (renamed or never there): a page
		// that names nothing, with the app context. Its statusCode is a
		// hypothesis (public sources' 10202; T1).
		scope["webapp.user-detail"] = map[string]any{"statusCode": 10202, "statusMsg": "user not exist"}
	case len(parts) == 1 && acc.Gone:
		// The full pass's shells of banned accounts (C3; T1).
		scope["webapp.user-detail"] = map[string]any{"statusCode": 10221, "statusMsg": ""}
	case len(parts) == 1:
		if !s.opts.Faults.ShellPages {
			scope["webapp.user-detail"] = map[string]any{"userInfo": s.userInfo(acc, r.Host), "statusCode": 0}
		}
	default:
		http.NotFound(w, r)
		return
	}
	blob, _ := json.Marshal(map[string]any{"__DEFAULT_SCOPE__": scope})
	script := ""
	if s.opts.Faults.RequireSignature {
		script = signer
	}
	page(w, `<!doctype html><html><head>`+script+`</head><body><div id="app"></div>`+
		`<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">`+string(blob)+`</script>`+ownCalls+`</body></html>`)
}

// ownCalls is the page script making the page's own first API call, as
// TikTok's web app does once it starts: a profile page asks for the post
// list, a post page for its comments. Only a browser runs it.
const ownCalls = `<script>(function(){const d=JSON.parse(document.getElementById("__UNIVERSAL_DATA_FOR_REHYDRATION__").textContent).__DEFAULT_SCOPE__;` +
	`const c=d["webapp.app-context"],u=d["webapp.user-detail"],v=d["webapp.video-detail"];` +
	`const base="aid=1988&device_id="+c.wid+"&odinId="+c.odinId;` +
	`if(u&&u.userInfo){fetch("/api/post/item_list/?"+base+"&count=16&cursor=0&secUid="+encodeURIComponent(u.userInfo.user.secUid));}` +
	`if(v&&v.itemInfo){fetch("/api/comment/list/?"+base+"&aweme_id="+v.itemInfo.itemStruct.id+"&count=20&cursor=0");}` +
	`})();</script>`

func (s *Server) mint() (wid, odin, created string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minted++
	return strconv.FormatUint(7_300_000_000_000_000_000+s.minted, 10), strconv.FormatUint(7_400_000_000_000_000_000+s.minted, 10),
		strconv.FormatInt(s.World.Now.Unix()-3600, 10)
}

// api serves the web API's GET calls, checked the way TikTok checks them.
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	n := s.apiCalls.Add(1)
	if v, _ := s.Requests.LoadOrStore(r.URL.Path, new(atomic.Int64)); v != nil {
		v.(*atomic.Int64).Add(1)
	}
	f := s.opts.Faults
	if f.RateLimitEvery > 0 && n%int64(f.RateLimitEvery) == 0 {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	s.mu.Lock()
	dev := s.devices[q.Get("device_id")]
	if dev != nil {
		dev.calls++
	}
	s.mu.Unlock()
	if q.Get("device_id") == "" || dev == nil {
		// No device id (or an unknown one): an empty 200 (T2).
		w.Header().Set("tt_orcas_res", "1")
		w.WriteHeader(http.StatusOK)
		return
	}
	if f.RequireSignature && s.signed(r.URL.Path) && q.Get("X-Gnarly") == "" {
		if r.URL.Path == "/api/user/detail/" {
			// The account call is refused with an empty account, not an
			// empty body (the C2 run).
			writeJSON(w, map[string]any{"userInfo": map[string]any{"user": map[string]any{}, "stats": map[string]any{}, "shareMeta": map[string]any{}}})
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if f.CaptchaAfter > 0 && dev.calls > f.CaptchaAfter {
		writeJSON(w, map[string]any{"statusCode": 10000, "statusMsg": "", "verify_event": map[string]any{"type": "slide", "subtype": "3d"}})
		return
	}
	switch r.URL.Path {
	case "/api/creator/item_list/":
		s.creatorList(w, r, dev)
	case "/api/post/item_list/":
		s.postList(w, r, dev)
	case "/api/user/detail/":
		acc := s.World.AccountBySec(q.Get("secUid"))
		if acc == nil {
			acc = s.World.Account(q.Get("uniqueId"))
		}
		if acc == nil {
			writeJSON(w, map[string]any{"statusCode": 10202, "statusMsg": "user not exist"})
			return
		}
		writeJSON(w, map[string]any{"userInfo": s.userInfo(acc, r.Host), "statusCode": 0})
	case "/api/item/detail/":
		id, _ := strconv.ParseInt(q.Get("itemId"), 10, 64)
		post := s.World.Post(id)
		if post == nil {
			writeJSON(w, map[string]any{"statusCode": 10204, "statusMsg": "item not exist"})
			return
		}
		writeJSON(w, map[string]any{"itemInfo": map[string]any{"itemStruct": s.item(post, r.Host)}, "statusCode": 0})
	case "/api/comment/list/":
		id, _ := strconv.ParseInt(q.Get("aweme_id"), 10, 64)
		post := s.World.Post(id)
		if post == nil {
			writeJSON(w, map[string]any{"status_code": 0, "comments": nil, "has_more": 0, "total": 0})
			return
		}
		s.commentPage(w, q, post.Comments, post.ID, post.CommentCount(), "0")
	case "/api/comment/list/reply/":
		id, _ := strconv.ParseInt(q.Get("comment_id"), 10, 64)
		c := s.World.Comment(id)
		item, _ := strconv.ParseInt(q.Get("item_id"), 10, 64)
		if c == nil {
			writeJSON(w, map[string]any{"status_code": 0, "comments": nil, "has_more": 0, "total": 0})
			return
		}
		s.commentPage(w, q, c.Replies, item, int64(len(c.Replies)), strconv.FormatInt(c.ID, 10))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) signed(path string) bool {
	if len(s.opts.Faults.SignedPaths) == 0 {
		return true
	}
	for _, p := range s.opts.Faults.SignedPaths {
		if p == path {
			return true
		}
	}
	return false
}

// listable returns an account's posts for a list call, or writes the
// answer itself and returns nil.
func (s *Server) listable(w http.ResponseWriter, sec string) *Account {
	acc := s.World.AccountBySec(sec)
	if acc == nil || acc.Gone {
		writeJSON(w, map[string]any{"statusCode": 10202, "statusMsg": "user not exist"})
		return nil
	}
	if acc.Private {
		writeJSON(w, map[string]any{"statusCode": 10222, "statusMsg": "private account"})
		return nil
	}
	return acc
}

func (s *Server) checkCursor(w http.ResponseWriter, dev *device, cursor string) bool {
	if !s.opts.Faults.CursorBound || cursor == "" || cursor == "0" {
		return true
	}
	s.mu.Lock()
	ok := dev.cursor[cursor]
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusOK)
	}
	return ok
}

func (s *Server) issue(dev *device, cursor string) {
	s.mu.Lock()
	dev.cursor[cursor] = true
	s.mu.Unlock()
}

// creatorList is /api/creator/item_list/: posts older than the cursor (a
// time in ms, starting at now), newest first, pinned posts at their own
// date with isPinnedItem (C2 run 2).
func (s *Server) creatorList(w http.ResponseWriter, r *http.Request, dev *device) {
	q := r.URL.Query()
	acc := s.listable(w, q.Get("secUid"))
	if acc == nil || !s.checkCursor(w, dev, q.Get("cursor")) {
		return
	}
	count := atoi(q.Get("count"), 15)
	cursor, _ := strconv.ParseInt(q.Get("cursor"), 10, 64)
	if cursor <= 0 {
		cursor = (s.World.Now.Unix() + 1) * 1000
	}
	if s.opts.Faults.DeviceBanAfter > 0 && dev.calls > s.opts.Faults.DeviceBanAfter {
		cursor = (s.World.Now.Unix() + 1) * 1000 // a banned device sees its first page again
	}
	if n := s.opts.Faults.ListErrorEvery; n > 0 && s.listCalls.Add(1)%int64(n) == 0 {
		writeJSON(w, map[string]any{"statusCode": 10102, "status_code": 0, "status_msg": ""})
		return
	}
	var page []*Post
	skipped, older := 0, 0
	for _, p := range acc.Posts {
		ms := p.Created.Unix() * 1000
		if ms >= cursor {
			skipped++
			continue
		}
		older++
		if len(page) == count {
			continue
		}
		if win := s.opts.Faults.ListWindow; win > 0 && ms < cursor-win.Milliseconds() {
			continue
		}
		page = append(page, p)
	}
	if s.opts.Faults.ListWallAfter > 0 && skipped >= s.opts.Faults.ListWallAfter {
		w.WriteHeader(http.StatusOK)
		return
	}
	next := cursor
	if len(page) > 0 {
		next = page[len(page)-1].Created.Unix() * 1000
	}
	more := older > len(page)
	s.issue(dev, strconv.FormatInt(next, 10))
	// Like TikTok's (the C2 run): no cursor and no hasMore in the answer;
	// the next cursor is the last post's createTime in ms, and
	// hasMorePrevious says whether older posts remain.
	writeJSON(w, map[string]any{"itemList": s.items(page, r.Host), "hasMorePrevious": more, "statusCode": 0})
}

// postList is /api/post/item_list/: pinned posts first on the first page,
// then the rest newest first, the cursor a time in ms.
func (s *Server) postList(w http.ResponseWriter, r *http.Request, dev *device) {
	q := r.URL.Query()
	acc := s.listable(w, q.Get("secUid"))
	if acc == nil || !s.checkCursor(w, dev, q.Get("cursor")) {
		return
	}
	count := atoi(q.Get("count"), 16)
	cursor, _ := strconv.ParseInt(q.Get("cursor"), 10, 64)
	var page []*Post
	if cursor == 0 {
		for _, p := range acc.Posts {
			if p.Pinned {
				page = append(page, p)
			}
		}
		cursor = (s.World.Now.Unix() + 1) * 1000
	}
	skipped, taken := 0, 0
	for _, p := range acc.Posts {
		if p.Pinned {
			continue
		}
		if p.Created.Unix()*1000 >= cursor {
			skipped++
			continue
		}
		if taken == count {
			break
		}
		page = append(page, p)
		taken++
	}
	if s.opts.Faults.ListWallAfter > 0 && skipped >= s.opts.Faults.ListWallAfter {
		w.WriteHeader(http.StatusOK)
		return
	}
	next := cursor
	if n := len(page); n > 0 {
		next = page[n-1].Created.Unix() * 1000
	}
	pinned := 0
	for _, p := range acc.Posts {
		if p.Pinned {
			pinned++
		}
	}
	more := skipped+taken < len(acc.Posts)-pinned
	s.issue(dev, strconv.FormatInt(next, 10))
	writeJSON(w, map[string]any{"itemList": s.items(page, r.Host), "cursor": strconv.FormatInt(next, 10), "hasMore": more,
		"statusCode": 0, "status_code": 0, "status_msg": ""})
}

func (s *Server) commentPage(w http.ResponseWriter, q url.Values, list []*Comment, item, total int64, parent string) {
	if s.opts.Faults.NoGuestComments {
		writeJSON(w, map[string]any{"status_code": 0, "comments": nil, "has_more": 0, "total": 0})
		return
	}
	count := atoi(q.Get("count"), 20)
	off := atoi(q.Get("cursor"), 0)
	end := min(off+count, len(list))
	var out []any
	for _, c := range list[min(off, end):end] {
		out = append(out, comment(c, item, parent))
	}
	more := 0
	if end < len(list) {
		more = 1
	}
	writeJSON(w, map[string]any{"status_code": 0, "status_msg": "", "comments": out, "cursor": end, "has_more": more, "total": total})
}

func comment(c *Comment, item int64, parent string) map[string]any {
	return map[string]any{"cid": strconv.FormatInt(c.ID, 10), "text": c.Text, "create_time": c.Created.Unix(), "digg_count": c.Likes,
		"reply_comment_total": len(c.Replies), "aweme_id": strconv.FormatInt(item, 10), "reply_id": parent, "reply_to_reply_id": "0",
		"is_author_digged": false, "stick_position": 0, "author_pin": false, "status": 1, "comment_language": "ar",
		"user": map[string]any{"uid": strconv.FormatInt(c.Author.UID, 10), "unique_id": c.Author.Username, "sec_uid": c.Author.SecUID,
			"nickname": c.Author.Nickname, "avatar_thumb": map[string]any{"url_list": []string{"/fake-cdn/avatar/" + strconv.FormatInt(c.Author.UID, 10) + ".jpg"}}}}
}

func (s *Server) userInfo(acc *Account, host string) map[string]any {
	cdn := "http://" + host + "/fake-cdn/avatar/" + strconv.FormatInt(acc.UID, 10)
	round := func(v int64) int64 {
		if v >= 1_000_000 {
			return v / 100_000 * 100_000
		}
		return v
	}
	return map[string]any{
		"user": map[string]any{"id": strconv.FormatInt(acc.UID, 10), "shortId": "", "uniqueId": acc.Username, "nickname": acc.Nickname,
			"avatarLarger": cdn + "-1080.jpg", "avatarMedium": cdn + "-720.jpg", "avatarThumb": cdn + "-100.jpg", "signature": acc.Bio,
			"createTime": acc.CreateTime.Unix(), "verified": acc.Verified, "secUid": acc.SecUID, "privateAccount": acc.Private,
			"bioLink": map[string]any{"link": acc.BioLink, "risk": 3}, "uniqueIdModifyTime": 0, "nickNameModifyTime": 0, "isOrganization": 0},
		"stats": map[string]any{"followerCount": round(acc.Followers), "followingCount": acc.Following, "heart": round(acc.Hearts),
			"heartCount": round(acc.Hearts), "videoCount": len(acc.Posts), "diggCount": acc.Diggs, "friendCount": acc.Friends},
		"statsV2": map[string]any{"followerCount": i64(acc.Followers), "followingCount": i64(acc.Following), "heart": i64(acc.Hearts),
			"heartCount": i64(acc.Hearts), "videoCount": strconv.Itoa(len(acc.Posts)), "diggCount": i64(acc.Diggs), "friendCount": i64(acc.Friends)},
		"itemList": []any{},
	}
}

func (s *Server) items(ps []*Post, host string) []any {
	out := make([]any, 0, len(ps))
	for _, p := range ps {
		out = append(out, s.item(p, host))
	}
	return out
}

func (s *Server) item(p *Post, host string) map[string]any {
	id := strconv.FormatInt(p.ID, 10)
	cdn := "http://" + host + "/fake-cdn/"
	exp := strconv.FormatInt(s.World.Now.Unix()+48*3600, 10)
	var extra []any
	var challenges []any
	off := strings.Index(p.Desc, "#")
	for _, h := range p.Hashtags {
		extra = append(extra, map[string]any{"hashtagName": h, "start": off, "end": off + 1 + len(h), "type": 1, "subType": 0, "isCommerce": false, "awemeId": ""})
		challenges = append(challenges, map[string]any{"id": strconv.Itoa(len(h) * 1000), "title": h, "desc": ""})
		off += len(h) + 2
	}
	it := map[string]any{
		"id": id, "desc": p.Desc, "createTime": p.Created.Unix(), "isAd": p.Ad, "AIGCDescription": "", "ShowAIGC": false,
		"CategoryType": 120, "textLanguage": "ar", "itemCommentStatus": 0, "privateItem": false,
		"author": map[string]any{"id": strconv.FormatInt(p.Owner.UID, 10), "uniqueId": p.Owner.Username, "nickname": p.Owner.Nickname,
			"secUid": p.Owner.SecUID, "verified": p.Owner.Verified},
		"music": map[string]any{"id": strconv.FormatInt(p.Music.ID, 10), "title": p.Music.Title, "authorName": p.Music.Author,
			"original": p.Music.Original, "duration": p.Music.Duration},
		"stats": map[string]any{"playCount": p.Plays, "diggCount": p.Likes, "commentCount": p.CommentCount(), "shareCount": p.Shares, "collectCount": p.Saves},
		"statsV2": map[string]any{"playCount": i64(p.Plays), "diggCount": i64(p.Likes), "commentCount": i64(p.CommentCount()),
			"shareCount": i64(p.Shares), "collectCount": i64(p.Saves), "repostCount": i64(p.Reposts)},
		"textExtra": extra, "challenges": challenges,
	}
	if p.Pinned {
		it["isPinnedItem"] = true
	}
	cover := cdn + "cover/" + id + ".jpg?x-expires=" + exp + "&x-signature=fake"
	if p.Kind == "photo" {
		it["imagePost"] = map[string]any{"title": "", "cover": map[string]any{"imageURL": map[string]any{"urlList": []string{cover}}},
			"images": []any{
				map[string]any{"imageURL": map[string]any{"urlList": []string{cdn + "photo/" + id + "-1.jpg?x-expires=" + exp}}, "imageWidth": 1080, "imageHeight": 1350},
				map[string]any{"imageURL": map[string]any{"urlList": []string{cdn + "photo/" + id + "-2.jpg?x-expires=" + exp}}, "imageWidth": 1080, "imageHeight": 1350},
			}}
		it["video"] = map[string]any{"duration": 0, "cover": cover}
	} else {
		play := cdn + "video/" + id + ".mp4?expire=" + exp + "&signature=fake"
		it["video"] = map[string]any{"id": id, "duration": p.Duration, "width": 576, "height": 1024, "cover": cover, "originCover": cover,
			"dynamicCover": cover, "playAddr": play, "downloadAddr": play,
			"bitrateInfo": []any{map[string]any{"Bitrate": 400000, "QualityType": 20, "PlayAddr": map[string]any{"UrlList": []string{play}, "DataSize": 4096}}}}
	}
	return it
}

func (s *Server) media(w http.ResponseWriter, r *http.Request) {
	if s.opts.Faults.MediaNeedsCookie && strings.HasPrefix(r.URL.Path, "/fake-cdn/video/") {
		if _, err := r.Cookie("tt_chain_token"); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/fake-cdn/video/"):
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(append([]byte("\x00\x00\x00\x18ftypmp42"), make([]byte, 4096)...))
	default:
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("\xff\xd8\xff\xe0fake-jpeg" + r.URL.Path))
	}
}

func (s *Server) oembed(w http.ResponseWriter, r *http.Request) {
	u, err := url.Parse(r.URL.Query().Get("url"))
	if err != nil {
		http.Error(w, "bad url", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 3 {
		http.Error(w, "bad url", http.StatusBadRequest)
		return
	}
	id, _ := strconv.ParseInt(parts[2], 10, 64)
	p := s.World.Post(id)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{"version": "1.0", "type": "video", "title": p.Desc, "author_name": p.Owner.Nickname,
		"author_url": "https://www.tiktok.com/@" + p.Owner.Username, "author_unique_id": p.Owner.Username, "provider_name": "TikTok",
		"thumbnail_url": "http://" + r.Host + "/fake-cdn/cover/" + parts[2] + ".jpg", "thumbnail_width": 576, "thumbnail_height": 1024,
		"html": "<blockquote></blockquote>"})
}

func (s *Server) embed(w http.ResponseWriter, r *http.Request) {
	acc := s.World.Account(strings.TrimPrefix(strings.Trim(r.URL.Path, "/"), "embed/@"))
	if acc == nil {
		http.NotFound(w, r)
		return
	}
	var list []any
	for _, p := range acc.Posts[:min(6, len(acc.Posts))] {
		list = append(list, map[string]any{"id": strconv.FormatInt(p.ID, 10), "desc": p.Desc})
	}
	blob, _ := json.Marshal(map[string]any{"source": map[string]any{"data": map[string]any{"/embed/@" + acc.Username: map[string]any{
		"userInfo": map[string]any{"secUid": acc.SecUID, "uniqueId": acc.Username, "nickname": acc.Nickname}, "videoList": list}}}})
	page(w, `<html><body><script id="__FRONTITY_CONNECT_STATE__" type="application/json">`+string(blob)+`</script></body></html>`)
}

func page(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func atoi(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n >= 0 {
		return n
	}
	return def
}

func i64(v int64) string { return strconv.FormatInt(v, 10) }
