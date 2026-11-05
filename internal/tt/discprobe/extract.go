package discprobe

import (
	"slices"
	"sort"
	"strings"

	"github.com/tidwall/gjson"
)

// The shapes of a discovery answer are known for search and the feed (the
// owner's recording) and guessed for the rest (T36), so the probe looks
// for posts and people in every place public sources put them and records
// which one held them: M12 and M13 write their parsers from that.

// postPaths hold lists of post objects; searchPosts are search's results
// (type 1 carries the post in "item").
var postPaths = []string{"itemList", "item_list", "items", "aweme_list"}

// userPaths hold lists of people, each either the person or a wrapper.
var userPaths = []string{"userList", "user_list", "users", "followings", "followers"}

// Post is what the probe keeps of a found post (in memory; the report
// holds counts only).
type Post struct {
	ID       string
	Created  int64 // createTime, Unix seconds
	Author   User
	Lang     string
	Region   string // locationCreated, on lists that carry it
	Hashtags []string
	MusicID  string
	Original bool
}

// User is a found person.
type User struct {
	ID, Username, SecUID string
	Private              bool
	Followers            int64
}

// Found is one answer's results.
type Found struct {
	Posts []Post
	Users []User
	// Shape names where they were: "data.item", "user_list.user_info", ...
	Shape   string
	HasMore *bool
	// Cursor is the next page's cursor, when the answer gives one.
	Cursor string
	// SearchID is search's request id, sent with its next pages.
	SearchID string
}

// Extract reads posts, people, the next cursor and has_more from an
// answer.
func Extract(r gjson.Result) Found {
	var f Found
	var shapes []string
	if data := r.Get("data"); data.IsArray() {
		for _, d := range data.Array() {
			if it := d.Get("item"); it.Exists() {
				f.Posts = append(f.Posts, post(it))
				addShape(&shapes, "data.item")
			}
			for _, u := range d.Get("user_list").Array() {
				f.Users = append(f.Users, user(u))
				addShape(&shapes, "data.user_list")
			}
			if u := d.Get("user_info"); u.Exists() {
				f.Users = append(f.Users, user(d))
				addShape(&shapes, "data.user_info")
			}
		}
	}
	for _, p := range postPaths {
		for _, it := range r.Get(p).Array() {
			f.Posts = append(f.Posts, post(it))
			addShape(&shapes, p)
		}
	}
	for _, p := range userPaths {
		for _, u := range r.Get(p).Array() {
			f.Users = append(f.Users, user(u))
			addShape(&shapes, p)
		}
	}
	// Each post's author is a person found too.
	for _, p := range f.Posts {
		if p.Author.ID != "" || p.Author.Username != "" {
			f.Users = append(f.Users, p.Author)
		}
	}
	f.Shape = strings.Join(shapes, ",")
	for _, k := range []string{"has_more", "hasMore"} {
		if v := r.Get(k); v.Exists() {
			b := v.Bool() || v.Int() != 0
			f.HasMore = &b
			break
		}
	}
	for _, k := range []string{"cursor", "minCursor", "maxCursor", "offset"} {
		if v := r.Get(k); v.Exists() && v.String() != "" && v.String() != "0" {
			f.Cursor = v.String()
			break
		}
	}
	f.SearchID = first(r.Get("extra.search_request_id").String(), r.Get("log_pb.impr_id").String(), r.Get("rid").String())
	return f
}

func addShape(shapes *[]string, s string) {
	if !slices.Contains(*shapes, s) {
		*shapes = append(*shapes, s)
	}
}

func post(it gjson.Result) Post {
	p := Post{ID: it.Get("id").String(), Created: it.Get("createTime").Int(), Lang: it.Get("textLanguage").String(), Region: it.Get("locationCreated").String(),
		Author: user(it.Get("author")), MusicID: it.Get("music.id").String(), Original: it.Get("music.original").Bool()}
	if p.Author.Followers == 0 {
		p.Author.Followers = it.Get("authorStats.followerCount").Int()
	}
	for _, x := range it.Get("textExtra").Array() {
		if h := x.Get("hashtagName").String(); h != "" {
			p.Hashtags = append(p.Hashtags, h)
		}
	}
	return p
}

// user reads a person in any of its forms: the web's camelCase, search's
// snake_case under user_info, or a list entry wrapping it under user.
func user(u gjson.Result) User {
	for _, k := range []string{"user_info", "user"} {
		if w := u.Get(k); w.IsObject() {
			x := user(w)
			if x.Followers == 0 {
				x.Followers = first64(u.Get("stats.followerCount").Int(), u.Get("follower_count").Int())
			}
			return x
		}
	}
	return User{ID: first(u.Get("id").String(), u.Get("uid").String()), Username: first(u.Get("uniqueId").String(), u.Get("unique_id").String()),
		SecUID: first(u.Get("secUid").String(), u.Get("sec_uid").String()), Private: u.Get("privateAccount").Bool() || u.Get("secret").Bool(),
		Followers: first64(u.Get("followerCount").Int(), u.Get("follower_count").Int())}
}

// Keys lists an answer's top-level keys (names only: no values), for the
// shapes M12 and M13 parse.
func Keys(r gjson.Result) []string {
	var out []string
	r.ForEach(func(k, _ gjson.Result) bool {
		out = append(out, k.String())
		return true
	})
	sort.Strings(out)
	return out
}

// detailID reads a hashtag's or a sound's id from its detail call or page.
func detailID(r gjson.Result) string {
	return first(r.Get("challengeInfo.challenge.id").String(), r.Get("musicInfo.music.id").String())
}

func first(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func first64(vs ...int64) int64 {
	for _, v := range vs {
		if v != 0 {
			return v
		}
	}
	return 0
}
