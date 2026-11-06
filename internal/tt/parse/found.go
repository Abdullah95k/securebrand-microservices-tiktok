package parse

import (
	"strings"

	"github.com/tidwall/gjson"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// Discovery part C's answers (docs/tiktok/discovery-plan.md §8; ADR 0007
// §27). A search answer holds results under data, each a post (type 1,
// item) or a card of accounts (type 4, user_list), with cursor, has_more
// and extra.search_request_id (the owner's recording of 2026-11-04,
// har-findings-search §3); the hashtag, sound, related and feed calls hold
// posts under itemList with hasMore and cursor (C6 run 2; their shape as
// the discovery probe recorded it, T36). Every post is the same full item
// as an account's own list, with its author.

// FoundResult is one page of posts found on a discovery page.
type FoundResult struct {
	Posts []PostBundle
	// Authors are the posts' authors as their items carry them, each once,
	// with their avatars (AuthorMedia).
	Authors     []*ttv1.Account
	AuthorMedia []*ttv1.Media
	// Cards are the accounts a search's account cards show (type 4).
	Cards []FollowEntry
	// HasMore says the list goes on; Cursor is the next page's (an offset),
	// empty when the answer gives none; SearchID is search's
	// search_request_id, sent with its later pages.
	HasMore  bool
	Cursor   string
	SearchID string
	// StatusCode is the answer's status (0: served); a non-zero one is not
	// drift, the caller classes it.
	StatusCode int64
	// Shape names where the posts were (itemList, data.item).
	Shape   string
	Dropped Dropped
}

// Found parses a page of a discovery call's posts: search, a hashtag's or
// a sound's posts, related posts or the feed. via is how the posts were
// found. An empty body (TikTok's refusal of a call, T4) and an answer with
// neither posts nor a status nor has_more are drift.
func Found(body []byte, via ttv1.DiscoveredVia, o Options) (FoundResult, Report) {
	var rep Report
	res := FoundResult{Dropped: Dropped{}}
	if !gjson.ValidBytes(body) || len(strings.TrimSpace(string(body))) == 0 {
		rep.AddMissing("body")
		return res, rep
	}
	doc := gjson.ParseBytes(body)
	code, hasCode := statusCode(doc)
	res.StatusCode = code
	if code != 0 {
		return res, rep
	}
	more := firstOf(doc, "hasMore", "has_more")
	res.HasMore = more.Bool() || more.Int() != 0
	if c := doc.Get("cursor"); c.Exists() && c.String() != "" {
		res.Cursor = c.String()
	}
	res.SearchID = firstOf(doc, "extra.search_request_id", "log_pb.impr_id").String()

	seen := map[int64]bool{}
	authors := map[int64]bool{}
	add := func(it gjson.Result) {
		if id, ok, _ := ID(it.Get("id")); ok && seen[id] {
			return // an item repeated in one page is read once
		}
		b, ok := post(it, via, o, &rep, res.Dropped)
		if !ok {
			return
		}
		seen[b.Post.PostId] = true
		res.Posts = append(res.Posts, b)
		if uid := b.Post.UserId; uid != 0 && !authors[uid] && it.Get("author.id").Exists() {
			authors[uid] = true
			a, m := account(it.Get("author"), it.Get("authorStats"), it.Get("authorStatsV2"),
				ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, o, &Report{}, res.Dropped)
			if a != nil {
				res.Authors = append(res.Authors, a)
				res.AuthorMedia = append(res.AuthorMedia, m...)
			}
		}
	}
	found := false
	for _, p := range []string{"itemList", "item_list", "items"} {
		if list := doc.Get(p); list.IsArray() {
			found, res.Shape = true, p
			for _, it := range list.Array() {
				add(it)
			}
			break
		}
	}
	if data := doc.Get("data"); !found && data.IsArray() {
		found, res.Shape = true, "data.item"
		for _, r := range data.Array() {
			if it := r.Get("item"); it.IsObject() {
				add(it)
				continue
			}
			for _, u := range r.Get("user_list").Array() {
				if e, _, ok := followEntry(u, o, res.Dropped); ok {
					res.Cards = append(res.Cards, e)
				}
			}
		}
	}
	if !found && !more.Exists() && !hasCode {
		rep.AddMissing("itemList")
	}
	return res, rep
}

// ChallengeResult is a hashtag's detail: its id, which its post list needs
// (the tag page's HTML holds none: C6 run 2), its name and post count.
type ChallengeResult struct {
	ID         int64
	Title      string
	Posts      *int64
	StatusCode int64
}

// ChallengeDetail parses /api/challenge/detail/. A hashtag TikTok does not
// know answers a status code (or no challenge id), which is not drift.
func ChallengeDetail(body []byte) (ChallengeResult, Report) {
	var rep Report
	var res ChallengeResult
	if !gjson.ValidBytes(body) || len(strings.TrimSpace(string(body))) == 0 {
		rep.AddMissing("body")
		return res, rep
	}
	doc := gjson.ParseBytes(body)
	code, hasCode := statusCode(doc)
	res.StatusCode = code
	if code != 0 {
		return res, rep
	}
	ch := doc.Get("challengeInfo.challenge")
	if !ch.Exists() {
		if !hasCode {
			rep.AddMissing("challengeInfo.challenge")
		}
		return res, rep
	}
	if id, ok, _ := ID(ch.Get("id")); ok {
		res.ID = id
	}
	res.Title = ch.Get("title").String()
	st, st2 := doc.Get("challengeInfo.stats"), doc.Get("challengeInfo.statsV2")
	res.Posts = count(st2.Get("videoCount"), st.Get("videoCount"))
	return res, rep
}
