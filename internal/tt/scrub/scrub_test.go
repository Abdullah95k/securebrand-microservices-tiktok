package scrub_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scrub"
)

// Distinctive values in every place TikTok puts personal data. None may
// survive a scrub.
var secrets = []string{
	"7041539490274267690",      // the account's user id
	"MS4wLjABAAAAsecretSecUid", // its secUid
	"realhandle",               // its username
	"Real Nickname",            // its nickname
	"MyBioText",                // its bio
	"mysite.example",           // its bio link
	"7702921642179910309",      // a post id
	"CaptionWords",             // a caption
	"mentioned_one",            // a mentioned username
	"7000000000000000003",      // a mentioned user id
	"7702380883188833719",      // a music id
	"original sound - Real Nickname",
	"7682439766627038953", // a comment id
	"7173266195505352160", // a commenter's id
	"commenter.name",      // a commenter's username
	"Commenter Nick",      // a commenter's nickname
	"CommentBody",         // a comment's text
	"x-signature=SIG",     // a signed image link
	"signature=VIDSIG",    // a signed video link
	"secret-age-band",     // predicted_age_group
	"7459999999999999999", // a subtitle id, under a key the scrubber does not know
	"csrfTokenValue",      // the page's tokens
	"1234567890123456789", // the browser's device id (wid)
}

const list = `{"statusCode":0,"hasMorePrevious":true,"itemList":[{"id":"7702921642179910309","createTime":1761946200,
 "desc":"CaptionWords #tag @mentioned_one 🌙","author":{"id":"7041539490274267690","uniqueId":"realhandle","nickname":"Real Nickname",
 "secUid":"MS4wLjABAAAAsecretSecUid","signature":"MyBioText","avatarLarger":"https://p16-common-sign.tiktokcdn-us.com/tos/abc~tplv.jpeg?x-expires=1793653200&x-signature=SIG"},
 "textExtra":[{"start":13,"end":18,"hashtagName":"tag"},{"start":19,"end":33,"userId":"7000000000000000003","userUniqueId":"mentioned_one","secUid":"MS4wLjABAAAAsecretSecUid"}],
 "music":{"id":"7702380883188833719","title":"original sound - Real Nickname","authorName":"Real Nickname"},
 "video":{"playAddr":"https://www.tiktok.com/aweme/v1/play/?file_id=f&item_id=7702921642179910309&signature=VIDSIG&tk=tt_chain_token",
  "claInfo":{"captionInfos":[{"claSubtitleID":"7459999999999999999"}]},
  "shareUrl":"https://www.tiktok.com/@realhandle/video/7702921642179910309?lang=en"}}]}`

const comments = `{"status_code":0,"total":1,"has_more":0,"cursor":20,"comments":[{"cid":"7682439766627038953","aweme_id":"7702921642179910309",
 "text":"CommentBody 😀","create_time":1757171405,"reply_id":"0",
 "share_info":{"desc":"Commenter Nick's comment: CommentBody","url":"https://www.tiktok.com/@realhandle/video/7702921642179910309"},
 "user":{"uid":"7173266195505352160","unique_id":"commenter.name","nickname":"Commenter Nick","predicted_age_group":"secret-age-band",
  "avatar_thumb":{"url_list":["https://platform-lookaside.fbsbx.com/platform/profilepic/?asid=1&hash=h"]}}}]}`

const page = `<!DOCTYPE html><html><head><title>Real Nickname (@realhandle) | TikTok</title>
<meta property="og:url" content="https://www.tiktok.com/@realhandle"></head><body>
<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{
 "webapp.app-context":{"wid":"1234567890123456789","odinId":"7000000000000000099","region":"IQ","csrfToken":"csrfTokenValue","nonce":"n","userAgent":"Mozilla"},
 "webapp.i18n-translation":{"x":"y"},
 "webapp.user-detail":{"statusCode":0,"shareMeta":{"title":"Real Nickname on TikTok","desc":"@realhandle 15.9m Followers"},
  "userInfo":{"user":{"id":"7041539490274267690","uniqueId":"realhandle","nickname":"Real Nickname","secUid":"MS4wLjABAAAAsecretSecUid",
  "signature":"MyBioText","bioLink":{"link":"mysite.example","risk":3}},"statsV2":{"followerCount":"15858019"}}}}}</script></body></html>`

func TestNothingPersonalSurvives(t *testing.T) {
	for _, c := range []struct {
		kind scrub.Kind
		in   string
	}{{scrub.CreatorList, list}, {scrub.Comments, comments}, {scrub.ProfilePage, page}} {
		out, err := scrub.Scrub([]byte(c.in), c.kind, scrub.Options{})
		if err != nil {
			t.Fatal(c.kind, err)
		}
		for _, s := range secrets {
			if strings.Contains(string(out), s) {
				t.Errorf("%s: %q survived:\n%s", c.kind, s, out)
			}
		}
		for _, marker := range []string{"x-signature", "signature=", "tt_chain_token", "fbsbx", "og:url", "i18n"} {
			if strings.Contains(string(out), marker) {
				t.Errorf("%s: %q survived", c.kind, marker)
			}
		}
	}
}

func TestStructureSurvives(t *testing.T) {
	out, err := scrub.Scrub([]byte(list), scrub.CreatorList, scrub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var in, got map[string]any
	_ = json.Unmarshal([]byte(list), &in)
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	it := got["itemList"].([]any)[0].(map[string]any)
	orig := in["itemList"].([]any)[0].(map[string]any)
	if u16(it["desc"].(string)) != u16(orig["desc"].(string)) || !strings.Contains(it["desc"].(string), "🌙") ||
		!strings.Contains(it["desc"].(string), "#xxx @") {
		t.Errorf("a caption keeps its length in UTF-16 units, its marks and emoji: %q", it["desc"])
	}
	te := it["textExtra"].([]any)
	if te[0].(map[string]any)["hashtagName"] != "xxx" {
		t.Errorf("hashtags are blanked like the caption: %v", te[0])
	}
	author := it["author"].(map[string]any)
	mention := te[1].(map[string]any)
	if author["secUid"] != mention["secUid"] || !strings.HasPrefix(author["secUid"].(string), "MS4wLjABAAAA") {
		t.Error("a secUid gets one pseudonym wherever it appears")
	}
	id := it["id"].(string)
	if v, err := strconv.ParseInt(id, 10, 64); err != nil || v>>32 != 7702921642179910309>>32 {
		t.Errorf("a post id keeps its time and int64 range: %s", id)
	}
	share := it["video"].(map[string]any)["shareUrl"].(string)
	if !strings.Contains(share, "/@"+author["uniqueId"].(string)+"/video/"+id) {
		t.Errorf("links keep their shape with pseudonyms: %s", share)
	}
	av := author["avatarLarger"].(string)
	if !strings.HasPrefix(av, "https://p16-common-sign.tiktokcdn-us.com/scrubbed/") || !strings.HasSuffix(av, ".jpeg?x-expires=1793653200") {
		t.Errorf("a media link keeps its host, type and expiry: %s", av)
	}
	if it["createTime"].(float64) != 1761946200 {
		t.Error("times are kept")
	}
}

func TestPseudoID(t *testing.T) {
	for _, id := range []string{"7702921642179910309", "6800000000000000001", "9223372036854775807", "107955", "123456789012345"} {
		p := scrub.PseudoID(id)
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil || p == id || len(p) != len(id) || p != scrub.PseudoID(id) {
			t.Errorf("%s -> %s: a pseudonym is stable, as long, different and an int64", id, p)
		}
		if orig, _ := strconv.ParseInt(id, 10, 64); orig>>32 >= 1_400_000_000 && orig>>32 <= 2_100_000_000 && v>>32 != orig>>32 {
			t.Errorf("%s -> %s: a time-carrying id keeps its time", id, p)
		}
	}
}

func TestPageKeepsOnlyTheParsersScopes(t *testing.T) {
	out, err := scrub.Scrub([]byte(page), scrub.ProfilePage, scrub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	k, err := scrub.Detect(out)
	if err != nil || k != scrub.ProfilePage {
		t.Fatalf("%v %v", k, err)
	}
	m := regexp.MustCompile(`(?s)<script[^>]*>(.*?)</script>`).FindSubmatch(out)
	var doc map[string]map[string]map[string]any
	if err := json.Unmarshal(m[1], &doc); err != nil {
		t.Fatal(err)
	}
	ac := doc["__DEFAULT_SCOPE__"]["webapp.app-context"]
	if _, ok := ac["userAgent"]; ok || ac["region"] != "IQ" || ac["csrfToken"] != "scrubbed" {
		t.Errorf("app context: %v", ac)
	}
	if _, ok := doc["__DEFAULT_SCOPE__"]["webapp.i18n-translation"]; ok {
		t.Error("other scopes are dropped")
	}
	if _, err := scrub.Scrub([]byte("<html>no data</html>"), scrub.ProfilePage, scrub.Options{}); err == nil {
		t.Error("a page without the document cannot become a fixture")
	}
}

func TestTrimKeepsShapes(t *testing.T) {
	var items []string
	for i := 0; i < 10; i++ {
		x := `{"id":"77029216421799103` + strconv.Itoa(10+i) + `","createTime":1}`
		switch i {
		case 7:
			x = `{"id":"7702921642179910399","isPinnedItem":true}`
		case 8:
			x = `{"id":"7702921642179910398","imagePost":{"images":[]} ,"desc":"p"}`
		}
		items = append(items, x)
	}
	out, err := scrub.Scrub([]byte(`{"statusCode":0,"itemList":[`+strings.Join(items, ",")+`]}`), scrub.PostList, scrub.Options{Items: 3})
	if err != nil {
		t.Fatal(err)
	}
	var got struct{ ItemList []map[string]any }
	_ = json.Unmarshal(out, &got)
	if len(got.ItemList) != 3 || got.ItemList[1]["isPinnedItem"] != true || got.ItemList[2]["imagePost"] == nil {
		t.Fatalf("trim keeps the pinned and photo posts, in order: %s", out)
	}
}

func TestFromHARAndWrite(t *testing.T) {
	har := `{"log":{"entries":[
	 {"request":{"url":"https://www.tiktok.com/api/comment/list/?aweme_id=1"},"response":{"content":{"text":""}}},
	 {"request":{"url":"https://www.tiktok.com/api/comment/list/reply/?x=1"},"response":{"content":{"text":"{}"}}},
	 {"request":{"url":"https://www.tiktok.com/api/comment/list/?aweme_id=1"},"response":{"content":{"text":"` +
		`eyJzdGF0dXNfY29kZSI6MCwiY29tbWVudHMiOltdfQ==","encoding":"base64"}}}]}}`
	b, err := scrub.FromHAR([]byte(har), scrub.Comments, 1)
	if err != nil || string(b) != `{"status_code":0,"comments":[]}` {
		t.Fatalf("the first comments answer with a body: %q %v", b, err)
	}
	if _, err := scrub.FromHAR([]byte(har), scrub.Comments, 2); err == nil {
		t.Fatal("there is no second")
	}
	root := t.TempDir()
	dir, err := scrub.Write(root, scrub.Comments, "case", []byte(comments), map[string]any{"source": "har"}, scrub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	m, err := scrub.ReadMeta(dir)
	if err != nil || !m.Scrubbed || m.Scrubber != scrub.Version || m.Kind != scrub.Comments || m.Source != "har" {
		t.Fatalf("%+v %v", m, err)
	}
	body, _ := os.ReadFile(filepath.Join(dir, "response.json"))
	if strings.Contains(string(body), "CommentBody") {
		t.Fatal("only the scrubbed copy is written")
	}
	if _, err := scrub.Write(root, scrub.Comments, "../escape", []byte(comments), nil, scrub.Options{}); err == nil {
		t.Fatal("a fixture name is one path element")
	}
}

func TestDetect(t *testing.T) {
	for in, want := range map[string]scrub.Kind{
		`{"statusCode":0,"hasMorePrevious":false,"itemList":[]}`:             scrub.CreatorList,
		`{"statusCode":0,"hasMore":true,"itemList":[]}`:                      scrub.PostList,
		`{"userInfo":{"user":{}}}`:                                           scrub.UserDetail,
		`{"status_code":0,"comments":[{"cid":"1","reply_id":"0"}]}`:          scrub.Comments,
		`{"status_code":0,"comments":[{"cid":"2","reply_id":"1"}]}`:          scrub.Replies,
		`<html><script>{"webapp.video-detail":{}}</script></html>`:           scrub.PostPage,
		`<html><script>{"webapp.user-detail":{"statusCode":10221}}</script>`: scrub.ProfilePage,
	} {
		if got, err := scrub.Detect([]byte(in)); err != nil || got != want {
			t.Errorf("%s: %v %v, want %v", in, got, err, want)
		}
	}
}
