package parse

import (
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// The following and followers lists (/api/user/list/, scenes 21 and 67)
// are served logged out (C6 run 1), but their answer's shape is still the
// public sources' (T36): people under userList, each a user with its stats,
// hasMore and minCursor. The parser takes every place public sources put
// them, as the discovery probe does (internal/tt/discprobe), and says which
// one held them.

// userListPaths hold lists of people, each the person or a wrapper.
var userListPaths = []string{"userList", "user_list", "users", "followings", "followers"}

// FollowEntry is one person on a list page, with what the list showed of
// them (discovery's cheap pre-checks: a private account or one with no
// post needs no read).
type FollowEntry struct {
	Person                     *ttv1.Person
	Private                    *bool
	Videos, Followers, Follows *int64
}

// FollowListResult is one page of a following or followers list.
type FollowListResult struct {
	People []FollowEntry
	// HasMore says the list goes on; Cursor is the next page's (minCursor),
	// empty when the answer gives none.
	HasMore bool
	Cursor  string
	// Total is the list's size when the answer says it.
	Total *int64
	// Shape names where the people were (userList, user_list.user_info…).
	Shape   string
	Dropped Dropped
}

// FollowList parses a page of a following or followers list. An answer
// with neither a list of people nor hasMore is drift; an empty list with
// hasMore false is the end of the list.
func FollowList(body []byte, o Options) (FollowListResult, Report) {
	var rep Report
	res := FollowListResult{Dropped: Dropped{}}
	r := gjson.ParseBytes(body)
	found := false
	for _, p := range userListPaths {
		list := r.Get(p)
		if !list.IsArray() {
			continue
		}
		found = true
		inner := ""
		for _, it := range list.Array() {
			e, w, ok := followEntry(it, o, res.Dropped)
			if ok {
				res.People = append(res.People, e)
			}
			if inner == "" {
				inner = w
			}
		}
		res.Shape = p
		if inner != "" {
			res.Shape += "." + inner
		}
		break
	}
	more := firstOf(r, "hasMore", "has_more")
	if more.Exists() {
		res.HasMore = more.Bool() || more.Int() != 0
	} else {
		rep.AddOptional("hasMore")
	}
	if !found && !more.Exists() {
		rep.AddMissing("userList")
		return res, rep
	}
	for _, k := range []string{"minCursor", "cursor", "offset"} {
		if v := r.Get(k); v.Exists() && v.String() != "" && v.String() != "0" {
			res.Cursor = v.String()
			break
		}
	}
	if t := firstOf(r, "total", "totalCount", "total_count"); t.Exists() && t.Type == gjson.Number {
		res.Total = proto.Int64(t.Int())
	}
	return res, rep
}

// followEntry reads one person in any of its forms: a wrapper holding the
// user (user or user_info) and its stats, or the user itself, in the web's
// camelCase or search's snake_case. It returns the wrapper's key.
func followEntry(it gjson.Result, o Options, dropped Dropped) (FollowEntry, string, bool) {
	u, stats, wrapper := it, it.Get("stats"), ""
	for _, k := range []string{"user", "user_info"} {
		if w := it.Get(k); w.IsObject() {
			u, wrapper = w, k
			break
		}
	}
	if !stats.IsObject() {
		stats = u.Get("stats")
	}
	id, ok, inRange := ID(firstOf(u, "id", "uid"))
	if !ok {
		if !inRange {
			dropped.add(DropIDRange)
		} else {
			dropped.add(DropNoUserID)
		}
		return FollowEntry{}, wrapper, false
	}
	p := &ttv1.Person{AuthorKey: o.key(id), UserId: proto.Int64(id),
		SecUid:    firstOf(u, "secUid", "sec_uid").String(),
		Username:  firstOf(u, "uniqueId", "unique_id").String(),
		Nickname:  u.Get("nickname").String(),
		AvatarUrl: firstOf(u, "avatarThumb", "avatarMedium", "avatarLarger").String(),
		Verified:  optBool(u.Get("verified"))}
	if p.AvatarUrl == "" {
		p.AvatarUrl = firstOf(u, "avatar_thumb.url_list.0", "avatar_medium.url_list.0").String()
	}
	e := FollowEntry{Person: p, Private: optBool(firstOf(u, "privateAccount", "secret")),
		Videos:    count(stats.Get("videoCount"), firstOf(u, "aweme_count", "videoCount")),
		Followers: count(stats.Get("followerCount"), firstOf(u, "follower_count", "followerCount")),
		Follows:   count(stats.Get("followingCount"), firstOf(u, "following_count", "followingCount"))}
	return e, wrapper, true
}

// FollowCursor is the cursor a list page is asked with: TikTok's own, or 0
// for the first page.
func FollowCursor(c string) string {
	if c = strings.TrimSpace(c); c == "" {
		return "0"
	}
	return c
}
