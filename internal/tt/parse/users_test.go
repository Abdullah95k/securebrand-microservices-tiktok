package parse_test

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

// Synthetic list pages in the shapes public sources give (T36), all
// invented.
func TestFollowListWebShape(t *testing.T) {
	body := `{"statusCode":0,"hasMore":true,"minCursor":1761946200,"maxCursor":0,"total":3,"userList":[
	 {"user":{"id":"6900000000000000002","uniqueId":"one","nickname":"One","secUid":"MS4wLjABAAAAa","privateAccount":false,
	   "avatarThumb":"https://p16-sign.tiktokcdn.com/a.jpeg"},"stats":{"followerCount":12,"followingCount":3,"videoCount":0}},
	 {"user":{"id":"6900000000000000003","uniqueId":"two","secUid":"MS4wLjABAAAAb","privateAccount":true},"stats":{}},
	 {"user":{"uniqueId":"noid"}},
	 {"user":{"id":"99999999999999999999","uniqueId":"big"}}]}`
	res, rep := parse.FollowList([]byte(body), parse.Options{AuthorKey: key})
	if !rep.OK() || !res.HasMore || res.Cursor != "1761946200" || res.Total == nil || *res.Total != 3 || res.Shape != "userList.user" {
		t.Fatalf("%+v %+v", rep, res)
	}
	if len(res.People) != 2 || res.Dropped[parse.DropNoUserID] != 1 || res.Dropped[parse.DropIDRange] != 1 {
		t.Fatalf("people %d dropped %v", len(res.People), res.Dropped)
	}
	a := res.People[0]
	if a.Person.GetUserId() != 6900000000000000002 || a.Person.SecUid != "MS4wLjABAAAAa" || a.Person.Username != "one" ||
		len(a.Person.AuthorKey) == 0 || a.Person.AvatarUrl == "" {
		t.Fatalf("person %v", a.Person)
	}
	if a.Private == nil || *a.Private || a.Videos == nil || *a.Videos != 0 || *a.Followers != 12 || *a.Follows != 3 {
		t.Fatalf("entry %+v", a)
	}
	if b := res.People[1]; b.Private == nil || !*b.Private || b.Videos != nil {
		t.Fatalf("entry %+v", b)
	}
}

func TestFollowListOtherShapes(t *testing.T) {
	// snake_case people under user_list, has_more as a number, no cursor.
	res, rep := parse.FollowList([]byte(`{"status_code":0,"has_more":0,"user_list":[
	 {"user_info":{"uid":"6900000000000000004","unique_id":"three","sec_uid":"MS4wLjABAAAAc","secret":true,"follower_count":5,"aweme_count":7}}]}`),
		parse.Options{})
	if !rep.OK() || res.HasMore || res.Cursor != "" || res.Shape != "user_list.user_info" || len(res.People) != 1 {
		t.Fatalf("%+v %+v", rep, res)
	}
	e := res.People[0]
	if e.Person.Username != "three" || !*e.Private || *e.Videos != 7 || *e.Followers != 5 {
		t.Fatalf("entry %+v %v", e, e.Person)
	}
	// The end of a list: no people, hasMore false.
	res, rep = parse.FollowList([]byte(`{"statusCode":0,"hasMore":false,"userList":[]}`), parse.Options{})
	if !rep.OK() || res.HasMore || len(res.People) != 0 {
		t.Fatalf("%+v %+v", rep, res)
	}
	// Neither people nor hasMore: drift.
	if _, rep := parse.FollowList([]byte(`{"statusCode":0,"something":[]}`), parse.Options{}); rep.OK() {
		t.Fatal("drift not reported")
	}
	if parse.FollowCursor("") != "0" || parse.FollowCursor(" 42 ") != "42" {
		t.Fatal("cursor")
	}
}

func TestFollowingVisibility(t *testing.T) {
	page := func(fv string) string {
		return `<html><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{"webapp.user-detail":{"statusCode":0,
		"userInfo":{"user":{"id":"6800000000000000001","uniqueId":"acct","secUid":"MS4wLjABAAAAx"` + fv + `},"stats":{"followerCount":1}}}}}</script></html>`
	}
	res, rep := parse.ProfilePage([]byte(page(`,"followingVisibility":2`)), parse.Options{})
	if !rep.OK() || res.Account.FollowingVisibility == nil || res.Account.GetFollowingVisibility() != 2 {
		t.Fatalf("%+v %v", rep, res.Account)
	}
	res, _ = parse.ProfilePage([]byte(page("")), parse.Options{})
	if res.Account.FollowingVisibility != nil {
		t.Fatalf("absent visibility read as %v", res.Account.GetFollowingVisibility())
	}
}
