package fakett

import (
	"hash/fnv"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Discovery: the pages and calls that find accounts tks does not monitor
// yet (docs/tiktok/discovery-plan.md §8, §9). Search and the For You feed
// are shaped as the owner's recording of 2026-11-04 shows them
// (docs/tiktok/har-findings-search-2026-11-04.md); the hashtag, sound,
// related, following, followers and repost calls are shaped as public
// sources describe them, hypotheses until C6 (T36). Everything is
// synthetic.

// LoginCode is fakett's answer to a list a logged-out visitor may not read
// further (Faults.LoginAfter, FollowListsNeedLogin). TikTok's own answer is
// unknown (T36); the probe records whatever it gets.
const LoginCode = 2483

// Scenes of /api/user/list/: following and followers, as public sources
// give them (T36).
const (
	SceneFollowing = 21
	SceneFollowers = 67
)

// ChallengeID is a hashtag's id: TikTok's are numbers (har-findings §3).
func ChallengeID(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(name)))
	return int64(h.Sum64() >> 2)
}

// ShortLink is the path of a post's short link (vm.tiktok.com/<code>/ on
// TikTok; the server's own /t/<code>/ here), which redirects to the post.
func ShortLink(p *Post) string { return "/t/" + strconv.FormatInt(p.ID, 36) + "/" }

// discoveryPage serves the discovery pages: search (both tabs), the For
// You feed, a hashtag's and a sound's page, and short links. It reports
// whether the path was one of them.
func (s *Server) discoveryPage(w http.ResponseWriter, r *http.Request) bool {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/t/"):
		id, err := strconv.ParseInt(strings.Trim(strings.TrimPrefix(p, "/t/"), "/"), 36, 64)
		post := s.World.Post(id)
		if err != nil || post == nil {
			http.Redirect(w, r, "/", http.StatusMovedPermanently)
			return true
		}
		http.Redirect(w, r, "/@"+post.Owner.Username+"/video/"+strconv.FormatInt(post.ID, 10)+"?_r=1&_t=fake", http.StatusMovedPermanently)
		return true
	case p == "/search" || p == "/search/user" || p == "/foryou" || p == "/explore":
		s.writeDoc(w, s.newDevice(w))
		return true
	case strings.HasPrefix(p, "/tag/"):
		name, _ := url.PathUnescape(strings.TrimPrefix(p, "/tag/"))
		scope := s.newDevice(w)
		scope["webapp.challenge-detail"] = map[string]any{"statusCode": 0, "challengeInfo": map[string]any{
			"challenge": map[string]any{"id": strconv.FormatInt(ChallengeID(name), 10), "title": name},
			"stats":     map[string]any{"videoCount": len(s.tagged(name))}}}
		s.writeDoc(w, scope)
		return true
	case strings.HasPrefix(p, "/music/"):
		slug := strings.TrimPrefix(p, "/music/")
		id, _ := strconv.ParseInt(slug[strings.LastIndex(slug, "-")+1:], 10, 64)
		scope := s.newDevice(w)
		if ps := s.withMusic(id); len(ps) > 0 {
			scope["webapp.music-detail"] = map[string]any{"statusCode": 0, "musicInfo": map[string]any{
				"music": map[string]any{"id": strconv.FormatInt(id, 10), "title": ps[0].Music.Title}, "stats": map[string]any{"videoCount": len(ps)}}}
		} else {
			scope["webapp.music-detail"] = map[string]any{"statusCode": 10203, "statusMsg": "music not exist"}
		}
		s.writeDoc(w, scope)
		return true
	}
	return false
}

// discoveryAPI serves the discovery calls; it reports whether path was one.
func (s *Server) discoveryAPI(w http.ResponseWriter, r *http.Request, path string, q url.Values) bool {
	host := r.Host
	switch path {
	case "/api/search/general/full/":
		off, n := atoi(q.Get("offset"), 0), atoi(q.Get("count"), 12)
		if s.loginWall(w, off) {
			return true
		}
		ps := s.searched(q.Get("keyword"))
		page, more := window(ps, off, n)
		var data []any
		if off == 0 {
			// The account card the recording shows on the first page.
			if users := s.authors(ps, 2); len(users) > 0 {
				var list []any
				for _, a := range users {
					list = append(list, map[string]any{"user_info": s.userCard(a)})
				}
				data = append(data, map[string]any{"type": 4, "user_list": list})
			}
		}
		for _, p := range page {
			data = append(data, map[string]any{"type": 1, "item": s.item(p, host)})
		}
		writeJSON(w, map[string]any{"status_code": 0, "data": data, "cursor": off + len(page), "has_more": b2i(more),
			"extra": map[string]any{"search_request_id": "fake-search-" + strconv.Itoa(off)}, "log_pb": map[string]any{"impr_id": "fake"}, "ad_info": map[string]any{}})
	case "/api/search/user/full/":
		off := atoi(q.Get("cursor"), 0)
		if s.loginWall(w, off) {
			return true
		}
		users, more := window(s.authors(s.searched(q.Get("keyword")), 0), off, 10)
		var list []any
		for _, a := range users {
			list = append(list, map[string]any{"user_info": s.userCard(a)})
		}
		writeJSON(w, map[string]any{"status_code": 0, "user_list": list, "cursor": off + len(users), "has_more": b2i(more),
			"rid": "fake-user-search-" + strconv.Itoa(off)})
	case "/api/challenge/detail/":
		name := q.Get("challengeName")
		writeJSON(w, map[string]any{"statusCode": 0, "challengeInfo": map[string]any{
			"challenge": map[string]any{"id": strconv.FormatInt(ChallengeID(name), 10), "title": name}, "stats": map[string]any{"videoCount": len(s.tagged(name))}}})
	case "/api/challenge/item_list/":
		id, _ := strconv.ParseInt(q.Get("challengeID"), 10, 64)
		s.itemList(w, q, s.taggedID(id), host)
	case "/api/music/detail/":
		id, _ := strconv.ParseInt(q.Get("musicId"), 10, 64)
		ps := s.withMusic(id)
		if len(ps) == 0 {
			writeJSON(w, map[string]any{"statusCode": 10203, "statusMsg": "music not exist"})
			return true
		}
		writeJSON(w, map[string]any{"statusCode": 0, "musicInfo": map[string]any{
			"music": map[string]any{"id": q.Get("musicId"), "title": ps[0].Music.Title, "original": ps[0].Music.Original}, "stats": map[string]any{"videoCount": len(ps)}}})
	case "/api/music/item_list/":
		id, _ := strconv.ParseInt(q.Get("musicID"), 10, 64)
		s.itemList(w, q, s.withMusic(id), host)
	case "/api/recommend/item_list/":
		// A new window of posts on every call, as a feed brings new ones
		// (whether TikTok's does is a question for C6).
		ps := s.visible()
		n := atoi(q.Get("count"), 12)
		calls := int(s.count("/api/recommend/item_list/"))
		var page []*Post
		for i := range min(n, len(ps)) {
			page = append(page, ps[((calls-1)*n+i)%len(ps)])
		}
		writeJSON(w, map[string]any{"statusCode": 0, "itemList": s.items(page, host), "hasMore": true})
	case "/api/related/item_list/":
		id, _ := strconv.ParseInt(q.Get("itemID"), 10, 64)
		p := s.World.Post(id)
		var rel []*Post
		if p != nil {
			for _, o := range s.visible() {
				if o.Owner != p.Owner && sharesTag(o, p) {
					rel = append(rel, o)
				}
				if len(rel) == atoi(q.Get("count"), 16) {
					break
				}
			}
		}
		writeJSON(w, map[string]any{"statusCode": 0, "itemList": s.items(rel, host)})
	case "/api/user/list/":
		s.userList(w, q)
	case "/api/repost/item_list/":
		acc := s.World.AccountBySec(q.Get("secUid"))
		var reposts []*Post
		if acc != nil {
			for _, o := range s.visible() {
				if o.Owner != acc && len(reposts) < 5 {
					reposts = append(reposts, o)
				}
			}
		}
		writeJSON(w, map[string]any{"statusCode": 0, "itemList": s.items(reposts, host), "hasMore": false, "cursor": "0"})
	default:
		return false
	}
	return true
}

// loginWall answers LoginCode for a list asked from off results on, when
// Faults.LoginAfter says so.
func (s *Server) loginWall(w http.ResponseWriter, off int) bool {
	if n := s.faults().LoginAfter; n > 0 && off >= n {
		writeJSON(w, map[string]any{"status_code": LoginCode, "status_msg": "login required"})
		return true
	}
	return false
}

// itemList pages a list of posts by its cursor (an offset).
func (s *Server) itemList(w http.ResponseWriter, q url.Values, ps []*Post, host string) {
	off := atoi(q.Get("cursor"), 0)
	if s.loginWall(w, off) {
		return
	}
	page, more := window(ps, off, atoi(q.Get("count"), 30))
	writeJSON(w, map[string]any{"statusCode": 0, "itemList": s.items(page, host), "hasMore": more, "cursor": strconv.Itoa(off + len(page))})
}

// userList serves the following (scene 21) and followers (scene 67) lists:
// an account follows the other monitored accounts and is followed by the
// people who commented on its posts.
func (s *Server) userList(w http.ResponseWriter, q url.Values) {
	f := s.faults()
	if f.FollowListsNeedLogin {
		writeJSON(w, map[string]any{"statusCode": LoginCode, "statusMsg": "login required"})
		return
	}
	acc := s.World.AccountBySec(q.Get("secUid"))
	if acc == nil {
		writeJSON(w, map[string]any{"statusCode": 10202, "statusMsg": "user not exist"})
		return
	}
	off := atoi(q.Get("minCursor"), 0)
	if s.loginWall(w, off) {
		return
	}
	var people []map[string]any
	switch atoi(q.Get("scene"), 0) {
	case SceneFollowing:
		for _, o := range s.World.accounts {
			if o != acc {
				people = append(people, map[string]any{"user": s.person(o.Person, o.Private), "stats": map[string]any{"followerCount": o.Followers, "videoCount": len(o.Posts)}})
			}
		}
	case SceneFollowers:
		seen := map[int64]bool{}
		for _, p := range acc.Posts {
			for _, c := range p.Comments {
				if !seen[c.Author.UID] {
					seen[c.Author.UID] = true
					people = append(people, map[string]any{"user": s.person(c.Author, false), "stats": map[string]any{"followerCount": 0, "videoCount": 0}})
				}
			}
		}
	default:
		writeJSON(w, map[string]any{"statusCode": 10101, "statusMsg": "bad scene"})
		return
	}
	page, more := window(people, off, atoi(q.Get("count"), 30))
	writeJSON(w, map[string]any{"statusCode": 0, "userList": page, "hasMore": more, "minCursor": off + len(page), "maxCursor": 0, "total": len(people)})
}

func (s *Server) person(p Person, private bool) map[string]any {
	return map[string]any{"id": strconv.FormatInt(p.UID, 10), "uniqueId": p.Username, "nickname": p.Nickname, "secUid": p.SecUID, "privateAccount": private}
}

// userCard is an account in search's snake_case form (user_info).
func (s *Server) userCard(a *Account) map[string]any {
	return map[string]any{"uid": strconv.FormatInt(a.UID, 10), "unique_id": a.Username, "nickname": a.Nickname, "sec_uid": a.SecUID,
		"signature": a.Bio, "follower_count": a.Followers, "verification_type": b2i(a.Verified)}
}

// visible returns every public post a visitor can see, newest id first.
func (s *Server) visible() []*Post {
	var out []*Post
	for _, p := range s.World.posts {
		if p.Unavailable == 0 && !p.Owner.Private && !p.Owner.Gone {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b *Post) int {
		switch {
		case a.ID > b.ID:
			return -1
		case a.ID < b.ID:
			return 1
		}
		return 0
	})
	return out
}

// searched returns the posts whose caption or hashtags hold the keyword.
func (s *Server) searched(keyword string) []*Post {
	k := strings.ToLower(strings.TrimSpace(keyword))
	if k == "" {
		return nil
	}
	var out []*Post
	for _, p := range s.visible() {
		if strings.Contains(strings.ToLower(p.Desc), k) || slices.ContainsFunc(p.Hashtags, func(h string) bool { return strings.EqualFold(h, k) }) {
			out = append(out, p)
		}
	}
	return out
}

func (s *Server) tagged(name string) []*Post { return s.taggedID(ChallengeID(name)) }

func (s *Server) taggedID(id int64) []*Post {
	var out []*Post
	for _, p := range s.visible() {
		if slices.ContainsFunc(p.Hashtags, func(h string) bool { return ChallengeID(h) == id }) {
			out = append(out, p)
		}
	}
	return out
}

func (s *Server) withMusic(id int64) []*Post {
	var out []*Post
	for _, p := range s.visible() {
		if p.Music.ID == id {
			out = append(out, p)
		}
	}
	return out
}

// authors returns the distinct authors of ps in order, at most n (0: all).
func (s *Server) authors(ps []*Post, n int) []*Account {
	var out []*Account
	for _, p := range ps {
		if !slices.Contains(out, p.Owner) {
			out = append(out, p.Owner)
		}
		if n > 0 && len(out) == n {
			break
		}
	}
	return out
}

// count is the number of calls made to path so far.
func (s *Server) count(path string) int64 {
	if v, ok := s.Requests.Load(path); ok {
		return v.(interface{ Load() int64 }).Load()
	}
	return 0
}

func sharesTag(a, b *Post) bool {
	for _, h := range a.Hashtags {
		if slices.Contains(b.Hashtags, h) {
			return true
		}
	}
	return false
}

func window[T any](xs []T, off, n int) ([]T, bool) {
	if n <= 0 {
		n = 12
	}
	if off >= len(xs) {
		return nil, false
	}
	end := min(off+n, len(xs))
	return xs[off:end], end < len(xs)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
