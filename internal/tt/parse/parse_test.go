package parse_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
)

const item = `{"id":"7702921642179910309","createTime":1761946200,"desc":"hi #tag @who","isPinnedItem":true,"isAd":false,
 "author":{"id":"6800000000000000001","uniqueId":"acct","nickname":"Acct","secUid":"MS4wLjABAAAAx","avatarLarger":"https://p16-sign.tiktokcdn.com/a.jpeg?x-expires=1793653200&x-signature=s"},
 "authorStats":{"followerCount":15900000},"authorStatsV2":{"followerCount":"15900000"},
 "stats":{"playCount":1000,"diggCount":10},"statsV2":{"playCount":"1234","diggCount":"10","repostCount":"2"},
 "textExtra":[{"start":3,"end":7,"hashtagName":"tag","type":1},{"start":8,"end":12,"userId":"6900000000000000002","userUniqueId":"who","secUid":"MS4wLjABAAAAy","type":0},
   {"start":0,"end":2,"userUniqueId":"noid","type":0}],
 "challenges":[{"id":"1234567","title":"tag"},{"id":"7654321","title":"other"}],
 "music":{"id":"7702380883188833719","title":"t","authorName":"a","original":true,"duration":30,"coverLarge":"https://p16-sign.tiktokcdn.com/m.jpeg"},
 "video":{"duration":32,"width":576,"height":1024,"cover":"https://p16-sign.tiktokcdn.com/c.jpeg?x-expires=1793653200",
   "bitrateInfo":[{"Bitrate":900,"PlayAddr":{"UrlList":["https://v16-webapp-prime.tiktok.com/lo?expire=1793653300"],"Width":576,"Height":1024,"DataSize":100}},
                  {"Bitrate":300,"PlayAddr":{"UrlList":["https://v16-webapp-prime.tiktok.com/hi"],"Width":360,"Height":640,"DataSize":50}}]}}`

func TestPostListItem(t *testing.T) {
	res, rep := parse.PostList([]byte(`{"statusCode":0,"hasMorePrevious":true,"itemList":[`+item+`,`+item+`]}`), parse.Options{AuthorKey: key})
	if !rep.OK() || len(res.Posts) != 1 || !res.HasMore || res.Cursor != "" {
		t.Fatalf("%+v %+v", rep, res)
	}
	b := res.Posts[0]
	p := b.Post
	if p.PostId != 7702921642179910309 || p.UserId != 6800000000000000001 || !p.GetPinned() || p.GetAd() || p.Kind != ttv1.PostKind_POST_KIND_VIDEO {
		t.Fatalf("post %v", p)
	}
	if p.Counts.GetPlays() != 1234 || p.Counts.GetReposts() != 2 { // statsV2 wins over the rounded stats
		t.Fatalf("counts %v", p.Counts)
	}
	if len(p.Hashtags) != 2 || p.Hashtags[0].Id != 1234567 || p.Hashtags[1].Name != "other" {
		t.Fatalf("hashtags %v", p.Hashtags)
	}
	if len(p.TextRanges) != 2 || p.TextRanges[1].Person.GetUserId() != 6900000000000000002 || len(p.TextRanges[1].Person.AuthorKey) == 0 {
		t.Fatalf("ranges %v", p.TextRanges)
	}
	if res.Dropped[parse.DropNoUserID] != 1 {
		t.Fatalf("a mention without a user id is dropped and counted: %v", res.Dropped)
	}
	var sd, hd *ttv1.Media
	for _, m := range b.Media {
		if m.Kind == ttv1.MediaKind_MEDIA_KIND_VIDEO {
			if m.Variant == "sd" {
				sd = m
			} else {
				hd = m
			}
		}
	}
	if sd == nil || hd == nil || sd.Bitrate != 300 || hd.Bitrate != 900 || hd.SourceExpiresAt.GetSeconds() != 1793653300 {
		t.Fatalf("renditions %v %v", sd, hd)
	}
	if b.Music.GetMusicId() != 7702380883188833719 || p.GetMusicId() != b.Music.MusicId || b.Music.DurationMs != 30000 {
		t.Fatalf("music %v", b.Music)
	}
	a := res.Account
	if a.Source != ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST || a.Counts.Exact || a.Counts.GetFollowers() != 15900000 {
		t.Fatalf("a list's author counts are rounded, never exact: %v", a)
	}
	if res.AccountMedia[0].SourceExpiresAt.GetSeconds() != 1793653200 || a.AvatarMediaId != res.AccountMedia[0].MediaId {
		t.Fatalf("avatar %v", res.AccountMedia)
	}
}

func TestPostListRefusalsAndErrors(t *testing.T) {
	for _, body := range []string{"", "  ", "<html>"} {
		if _, rep := parse.PostList([]byte(body), parse.Options{}); rep.OK() {
			t.Errorf("%q: an empty or non-JSON answer is a refusal, reported missing", body)
		}
	}
	res, rep := parse.PostList([]byte(`{"statusCode":10201,"status_msg":"invalid count parameter"}`), parse.Options{})
	if !rep.OK() || res.StatusCode != 10201 || len(res.Posts) != 0 {
		t.Fatalf("an error status is returned, not drift: %+v %+v", rep, res)
	}
	res, rep = parse.PostList([]byte(`{"statusCode":0,"hasMorePrevious":true}`), parse.Options{})
	if !rep.OK() || !res.HasMore || len(rep.Optional) != 1 {
		t.Fatalf("a page without items is not drift (the list's window): %+v %+v", rep, res)
	}
}

func TestIDRange(t *testing.T) {
	big := strings.Replace(item, `"id":"7702921642179910309"`, `"id":"9923372036854775808"`, 1)
	res, _ := parse.PostList([]byte(`{"statusCode":0,"itemList":[`+big+`]}`), parse.Options{})
	if len(res.Posts) != 0 || res.Dropped[parse.DropIDRange] != 1 {
		t.Fatalf("an id past int64 is dropped and counted, never wrapped: %+v", res)
	}
	if _, ok, inRange := parse.ID(jsonResult(`"-5"`)); ok || inRange {
		t.Fatal("a negative id is out of range")
	}
	if id, ok, _ := parse.ID(jsonResult(`7702921642179910309`)); !ok || id != 7702921642179910309 {
		t.Fatal("a numeric id reads exactly")
	}
	if got := parse.IDTime(7702921642179910309).Unix(); got != 7702921642179910309>>32 {
		t.Fatal(got)
	}
}

func TestProfilePage(t *testing.T) {
	page := `<html><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{"webapp.user-detail":` +
		`{"statusCode":0,"userInfo":{"user":{"id":"6800000000000000001","uniqueId":"acct","secUid":"s","privateAccount":true,` +
		`"isOrganization":1,"bioLink":{"link":"example.com","risk":3},"createTime":1639486414,"uniqueIdModifyTime":0},` +
		`"stats":{"followerCount":15900000},"statsV2":{"followerCount":"15858019","heart":"7"}}}}}</script></html>`
	res, rep := parse.ProfilePage([]byte(page), parse.Options{})
	a := res.Account
	if !rep.OK() || a == nil || a.Counts.GetFollowers() != 15858019 || !a.Counts.Exact || a.Counts.GetHearts() != 7 {
		t.Fatalf("%+v %v", rep, a)
	}
	if a.Status != ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE || !a.GetOrganization() || a.GetBioLink() != "example.com" ||
		a.GetBioLinkRisk() != 3 || a.UsernameModifiedAt != nil || a.CreatedAt.GetSeconds() != 1639486414 {
		t.Fatalf("%v", a)
	}
	for code, want := range map[int64]ttv1.AccountStatus{10221: ttv1.AccountStatus_ACCOUNT_STATUS_BANNED,
		209002: ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE} {
		shell := `<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">{"__DEFAULT_SCOPE__":{"webapp.user-detail":{"statusCode":` +
			itoa(code) + `}}}</script>`
		res, rep := parse.ProfilePage([]byte(shell), parse.Options{})
		if !rep.OK() || res.Account != nil || res.Status != want {
			t.Errorf("%d: %+v %+v", code, rep, res)
		}
	}
	if _, rep := parse.ProfilePage([]byte(`<html>Please wait...</html>`), parse.Options{}); rep.OK() {
		t.Error("a page without the document is drift")
	}
}

func TestComments(t *testing.T) {
	body := `{"status_code":0,"total":3,"has_more":1,"cursor":20,"comments":[
	 {"cid":"7682439766627038953","aweme_id":"7682427663735590109","text":"hello @x","create_time":1757171405,"digg_count":4,"reply_comment_total":2,
	  "is_author_digged":true,"author_pin":false,"stick_position":1,"comment_language":"ar","reply_id":"0","reply_to_reply_id":"0",
	  "user":{"uid":"7173266195505352160","sec_uid":"MS4","unique_id":"u","nickname":"N","predicted_age_group":"secret-band",
	   "avatar_thumb":{"url_list":["https://p16-sign.tiktokcdn.com/av.jpeg?x-expires=1"]}},
	  "text_extra":[{"start":6,"end":8,"user_id":"7000000000000000003","sec_uid":"MS5","type":0}],
	  "image_list":[{"origin_url":{"url_list":["https://p16-sign.tiktokcdn.com/img.jpeg"],"width":10,"height":20}}],
	  "reply_comment":[{"cid":"7682439766627038999","aweme_id":"7682427663735590109","text":"r","create_time":1757171500,
	   "reply_id":"7682439766627038953","reply_to_reply_id":"0","user":{"uid":"7173266195505352161"}}]},
	 {"cid":"7682439766627038954","aweme_id":"7682427663735590109","text":"anon","create_time":1757171405,"user":{"unique_id":"no-id"}}]}`
	res, rep := parse.Comments([]byte(body), parse.CommentContext{UserID: 42}, parse.Options{AuthorKey: key})
	if !rep.OK() || res.Total != 3 || !res.HasMore || res.Cursor != 20 || len(res.Comments) != 2 {
		t.Fatalf("%+v %+v", rep, res)
	}
	c, r := res.Comments[0].Comment, res.Comments[1].Comment
	if c.PostId != 7682427663735590109 || c.UserId != 42 || c.Depth != 0 || c.ParentCommentId != nil || !c.GetLikedByCreator() ||
		c.GetReplyCount() != 2 || c.StickPosition != 1 || len(c.ImageMediaIds) != 1 || len(c.TextRanges) != 1 {
		t.Fatalf("comment %v", c)
	}
	if r.Depth != 1 || r.GetParentCommentId() != c.CommentId || r.ReplyToCommentId != nil {
		t.Fatalf("an inline reply is a depth-1 comment under its top-level comment: %v", r)
	}
	if len(res.Threads) != 1 || res.Threads[0].ReplyCount != 2 || res.Threads[0].Inline != 1 {
		t.Fatalf("threads %+v", res.Threads)
	}
	if res.Dropped[parse.DropNoUserID] != 1 {
		t.Fatalf("a commenter without a numeric id is dropped and counted: %v", res.Dropped)
	}
	for _, b := range res.Comments {
		out, _ := protojson.Marshal(b.Comment)
		if strings.Contains(string(out), "secret-band") {
			t.Fatal("predicted_age_group is never read")
		}
	}
	m := res.Comments[0].Media[0]
	if m.GetCommentId() != c.CommentId || m.Width != 10 || string(m.AuthorKey) != string(c.Author.AuthorKey) {
		t.Fatalf("comment image %v", m)
	}
	// A reply page: reply_to_reply_id names the reply answered.
	reply := `{"status_code":0,"has_more":0,"comments":[{"cid":"7682439766627039000","aweme_id":"7682427663735590109","create_time":1,
	 "reply_id":"7682439766627038953","reply_to_reply_id":"7682439766627038999","user":{"uid":"1"}}]}`
	res, _ = parse.Comments([]byte(reply), parse.CommentContext{Replies: true}, parse.Options{})
	if c := res.Comments[0].Comment; c.Depth != 1 || c.GetReplyToCommentId() != 7682439766627038999 || len(res.Threads) != 0 {
		t.Fatalf("%v", c)
	}
}

func TestMediaID(t *testing.T) {
	a := parse.MediaID(1, ttv1.MediaKind_MEDIA_KIND_COVER, "original")
	if a != parse.MediaID(1, ttv1.MediaKind_MEDIA_KIND_COVER, "original") || a == parse.MediaID(1, ttv1.MediaKind_MEDIA_KIND_AVATAR, "original") ||
		a == parse.MediaID(2, ttv1.MediaKind_MEDIA_KIND_COVER, "original") {
		t.Fatal("media ids are deterministic per owner, kind and variant")
	}
	if parse.URLExpiry("https://x/a?x-expires=10").GetSeconds() != 10 || parse.URLExpiry("https://x/a?expire=11").GetSeconds() != 11 ||
		parse.URLExpiry("https://x/a") != nil {
		t.Fatal("expiry")
	}
}
