package client_test

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// do bootstraps a fresh identity and makes one call.
func do(t *testing.T, env *tttest.Env, query string, vars map[string]any) *graphql.Response {
	t.Helper()
	ident := env.Identity(t, env.World.Accounts()[0].Username)
	resp, err := env.Client.Do(context.Background(), ident, graphql.Request{Query: query, Vars: vars})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// The whole unsigned flow against fakett, over both transports: bootstrap
// on the profile page, both post lists, the account call, a post, comments
// and replies; and the request format of the recordings.
func TestClientAgainstFakett(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		t.Run("tls="+strconv.FormatBool(useTLS), func(t *testing.T) {
			env := tttest.New(t, tttest.Options{UseTLS: useTLS})
			acc := env.World.Accounts()[0]
			id := env.Identity(t, acc.Username)
			tok := id.Tokens()
			if tok["device_id"] == "" || tok["odinId"] == "" || tok["WebIdLastTime"] == "" || tok["region"] != "IQ" {
				t.Fatalf("tokens %v", tok)
			}
			call := func(query string, vars map[string]any) *graphql.Response {
				t.Helper()
				resp, err := env.Client.Do(context.Background(), id, graphql.Request{Query: query, Vars: vars, Referer: env.HTTP.URL + "/@" + acc.Username})
				if err != nil {
					t.Fatal(err)
				}
				if resp.Class != detect.OK || len(resp.Parts) != 1 {
					t.Fatalf("%s: %v (%s) body %.200s", query, resp.Class, resp.Reason, resp.Body)
				}
				return resp
			}
			// The creator list pages by time back to the first post: the
			// cursor is the oldest createTime read (NextCreatorCursor).
			seen, pages := map[string]bool{}, 0
			cursor := env.World.Now.Add(time.Second).UnixMilli()
			for {
				resp := call("creator_list", map[string]any{"sec_uid": acc.SecUID, "count": 15, "cursor": cursor})
				for _, it := range resp.Parts[0].Get("itemList").Array() {
					seen[it.Get("id").String()] = true
				}
				pages++
				next, more := ttclient.NextCreatorCursor(resp.Parts[0], cursor)
				if !more || pages > 20 {
					break
				}
				cursor = next
			}
			if len(seen) != len(acc.Posts) {
				t.Fatalf("creator list: %d posts in %d pages, want %d", len(seen), pages, len(acc.Posts))
			}
			// The app's own list puts the pinned post first.
			first := call("post_list", map[string]any{"sec_uid": acc.SecUID, "count": 16, "cursor": 0}).Parts[0].Get("itemList.0")
			if !first.Get("isPinnedItem").Bool() || first.Get("statsV2.repostCount").String() == "" {
				t.Fatalf("first item %s", first.Raw)
			}
			if got := call("user_detail", map[string]any{"username": acc.Username}).Parts[0].Get("userInfo.user.secUid").String(); got != acc.SecUID {
				t.Fatalf("user detail secUid %q", got)
			}
			post := acc.Posts[0]
			for _, p := range acc.Posts {
				if len(p.Comments) > 0 {
					post = p
					break
				}
			}
			pid := strconv.FormatInt(post.ID, 10)
			if call("item_detail", map[string]any{"item_id": pid}).Parts[0].Get("itemInfo.itemStruct.id").String() != pid {
				t.Fatal("item detail")
			}
			cm := call("comments", map[string]any{"item_id": pid, "count": 20, "cursor": 0}).Parts[0]
			if int(cm.Get("total").Int()) != int(post.CommentCount()) || len(cm.Get("comments").Array()) == 0 {
				t.Fatalf("comments %.300s", cm.Raw)
			}
			for _, c := range post.Comments {
				if len(c.Replies) > 0 {
					rp := call("replies", map[string]any{"item_id": pid, "comment_id": strconv.FormatInt(c.ID, 10), "count": 3, "cursor": 0}).Parts[0]
					if rp.Get("comments.0.reply_id").String() != strconv.FormatInt(c.ID, 10) {
						t.Fatalf("replies %.300s", rp.Raw)
					}
					break
				}
			}
			// The request format of the recordings: GET, parameters sorted
			// with capitals first, no signature, the page's device values.
			reqs := env.Requests("/api/creator/item_list/")
			if len(reqs) == 0 {
				t.Fatal("no request recorded")
			}
			r := reqs[0]
			u, _ := url.Parse(r.URL)
			raw := strings.Split(u.RawQuery, "&")
			if r.Method != "GET" || r.Body != nil || !strings.HasPrefix(raw[0], "WebIdLastTime=") || !strings.HasPrefix(raw[1], "aid=1988") {
				t.Fatalf("request %s %s", r.Method, u.RawQuery)
			}
			qv := u.Query()
			for k, want := range map[string]string{"device_id": tok["device_id"], "odinId": tok["odinId"], "region": "IQ", "app_name": "tiktok_web",
				"device_platform": "web_pc", "os": "windows", "browser_platform": "Win32", "secUid": acc.SecUID, "type": "1"} {
				if qv.Get(k) != want {
					t.Errorf("%s = %q, want %q", k, qv.Get(k), want)
				}
			}
			if qv.Has("X-Gnarly") || qv.Has("X-Bogus") || qv.Get("msToken") == "" {
				t.Errorf("unsigned call: X-Gnarly %q X-Bogus %q msToken %q", qv.Get("X-Gnarly"), qv.Get("X-Bogus"), qv.Get("msToken"))
			}
		})
	}
}

// The faults reach the right classes: a refused (unsigned) call is
// registry-stale, a captcha a checkpoint, a private account unavailable, a
// challenge page fails the bootstrap.
func TestClientClassesFakettFaults(t *testing.T) {
	env := tttest.New(t, tttest.Options{Faults: fakett.Faults{RequireSignature: true}})
	acc := env.World.Accounts()[0]
	if c := do(t, env, "creator_list", map[string]any{"sec_uid": acc.SecUID, "count": 15}).Class; c != detect.RegistryStale {
		t.Fatalf("unsigned call: %v", c)
	}
	env = tttest.New(t, tttest.Options{Faults: fakett.Faults{CaptchaAfter: 1}})
	acc = env.World.Accounts()[0]
	id := env.Identity(t, acc.Username)
	var last *graphql.Response
	for i := 0; i < 2; i++ {
		var err error
		if last, err = env.Client.Do(context.Background(), id, graphql.Request{Query: "user_detail", Vars: map[string]any{"username": acc.Username}}); err != nil {
			t.Fatal(err)
		}
	}
	if last.Class != detect.Checkpoint {
		t.Fatalf("captcha: %v (%s)", last.Class, last.Reason)
	}
	env = tttest.New(t, tttest.Options{})
	acc = env.World.Accounts()[1]
	acc.Private = true
	if c := do(t, env, "creator_list", map[string]any{"sec_uid": acc.SecUID, "count": 15}).Class; c != detect.ContentUnavailable {
		t.Fatalf("private account: %v", c)
	}
	env = tttest.New(t, tttest.Options{Faults: fakett.Faults{Challenge: true}})
	id, err := env.Pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Loader.Bootstrap(context.Background(), id, env.HTTP.URL+"/@"+env.World.Accounts()[0].Username); err == nil {
		t.Fatal("a challenge page must not bootstrap")
	}
}
