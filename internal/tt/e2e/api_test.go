package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	ttapi "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
)

// client calls tks-api as one API key.
type client struct {
	t   *testing.T
	url string
	key string
}

func (c *client) do(method, path string, body any, out any) int {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, c.url+"/v1"+path, rd)
	req.Header.Set("Authorization", "Bearer "+c.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			c.t.Fatalf("%s %s: %v: %s", method, path, err, b)
		}
	}
	return resp.StatusCode
}

func (c *client) want(code int, method, path string, body any, out any) {
	c.t.Helper()
	if got := c.do(method, path, body, out); got != code {
		c.t.Fatalf("%s %s: %d, want %d", method, path, got, code)
	}
}

// lines reads an NDJSON stream's lines.
func (c *client) lines(path string) []map[string]any {
	c.t.Helper()
	req, _ := http.NewRequest("GET", c.url+"/v1"+path, nil)
	req.Header.Set("Authorization", "Bearer "+c.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out []map[string]any
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			c.t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

// TestAPI drives tks through its API (tks-api as cmd/tks-api wires it)
// with the scheduler's planner and finalizer, the crawler
// and the persister against fakett:
//  1. accounts registered through the API are backfilled by the planner;
//     what the API answers equals fakett's world (accounts, every post,
//     every comment and reply), personal fields only with pii:read;
//  2. an on-demand job runs, and its results stream the account, its posts
//     and their comments to an end line;
//  3. nothing is deleted (ADR 0007 §14): an account every client
//     unregistered keeps its data and answers, a commenter's comments all
//     stay, and there is no erasure endpoint.
func TestAPI(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 23, Accounts: 3, PostsPerAccount: 10, MaxComments: 12, MaxReplies: 3,
		PostSpacing: 30 * time.Hour}})
	w := s.World
	accs := w.Accounts()
	ctx := context.Background()
	fresh := w.AddPost(accs[0], 5) // a commenter comments here again later
	sc := newSched(s)
	srv := &ttapi.Server{Cfg: s.Cfg, DB: s.DB, Redis: s.Redis, Now: w.Clock,
		Dispatch: &dispatch.Dispatcher{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer}}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	mk := func(scopes ...string) *client {
		key, _, err := ttapi.CreateClient(ctx, s.DB, strings.Join(scopes, "+"), scopes, 0)
		if err != nil {
			t.Fatal(err)
		}
		return &client{t: t, url: hs.URL, key: key}
	}
	user := mk(ttapi.ScopeDataRead, ttapi.ScopeAccountsWrite, ttapi.ScopeJobsWrite)
	pii := mk(ttapi.ScopeDataRead, ttapi.ScopePIIRead)

	// 1. Registration and backfill.
	for _, a := range accs {
		var reg map[string]any
		user.want(201, "POST", "/accounts", map[string]any{"account": "https://www.tiktok.com/@" + a.Username}, &reg)
	}
	got := sc.tick(t)
	if len(got) != len(accs) {
		t.Fatalf("%d jobs started, want %d", len(got), len(accs))
	}
	for ref, j := range got {
		if j.kind != dispatch.KindBackfill || j.status != "completed" {
			t.Fatalf("%s: %+v", ref, j)
		}
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
		sameAPI(t, user, pii, a)
	}

	// 2. An on-demand job and its results.
	w.Advance(time.Hour)
	np := w.AddPost(accs[1], 2)
	var j map[string]any
	user.want(202, "POST", "/jobs", map[string]any{"account": "@" + accs[1].Username, "mode": "incremental"}, &j)
	s.Wait(t, 3*time.Minute, j["job_id"].(string))
	deadline := time.Now().Add(time.Minute)
	for j["status"] == "running" || j["status"] == "finalizing" {
		if time.Now().After(deadline) {
			t.Fatalf("job not finalised: %v", j)
		}
		if err := sc.fin.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		user.want(200, "GET", "/jobs/"+j["job_id"].(string), nil, &j)
	}
	if j["status"] != "completed" || j["kind"] != "on_demand" || j["lane"] != "p0" || j["user_id"] != strconv.FormatInt(accs[1].UID, 10) {
		t.Fatalf("job %v", j)
	}
	lines := user.lines("/jobs/" + j["job_id"].(string) + "/results?include=comments")
	if len(lines) < 3 || lines[0]["type"] != "account" || lines[len(lines)-1]["type"] != "end" {
		t.Fatalf("results: %d lines, first %v, last %v", len(lines), lines[0], lines[len(lines)-1])
	}
	posts, comments := map[string]bool{}, 0
	for _, l := range lines {
		switch l["type"] {
		case "post":
			posts[l["post"].(map[string]any)["post_id"].(string)] = true
		case "comment":
			comments++
		}
	}
	if !posts[strconv.FormatInt(np.ID, 10)] || comments < 2 {
		t.Fatalf("results lack the new post or its comments: %d posts, %d comments", len(posts), comments)
	}
	sameStore(t, s, accs[1], truth{})

	// 3. Nothing is deleted: an account every client unregistered keeps
	// all its data and stays readable; a commenter seen before is stored
	// with every comment when they comment again.
	kept := accs[2]
	user.want(204, "DELETE", "/accounts/@"+kept.Username, nil, nil)
	commenter := fresh.Comments[0].Author
	w.Advance(7 * time.Hour)
	w.AddComments(fresh, 3)
	w.Mutate(func() { fresh.Comments[len(fresh.Comments)-1].Author = commenter })
	got = sc.tick(t)
	if _, ok := got["u:"+kept.Username]; ok {
		t.Error("an unregistered account was checked")
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
	sameAPI(t, user, pii, kept)
	key := emit.AuthorKey([]byte(s.Cfg.Privacy.AuthorHMACKey), commenter.UID)
	if n := count(t, s, `SELECT count(*) FROM comments WHERE author_key = $1 AND text IS NOT NULL`, key); n < 2 {
		t.Errorf("%d comments of the returning commenter stored, want both", n)
	}
	if code := user.do("POST", "/erasure-requests", map[string]any{"subject_type": "author", "user_id": "1"}, nil); code != 404 && code != 405 {
		t.Errorf("an erasure endpoint answers %d", code)
	}
}

// sameAPI compares what the API answers about an account with fakett's
// world: the account, every post (paged), and every comment and reply of
// each post; without pii:read no name, text or author is shown.
func sameAPI(t *testing.T, user, pii *client, a *fakett.Account) {
	t.Helper()
	var acc struct {
		Account  map[string]any `json:"account"`
		Schedule map[string]any `json:"schedule"`
	}
	pii.want(200, "GET", "/accounts/@"+a.Username, nil, &acc)
	if acc.Account["user_id"] != strconv.FormatInt(a.UID, 10) || acc.Account["username"] != a.Username ||
		acc.Account["follower_count"] != float64(a.Followers) || acc.Schedule["account_ref"] != "u:"+strings.ToLower(a.Username) {
		t.Errorf("account %s: %v %v", a.Username, acc.Account, acc.Schedule)
	}
	var bare struct {
		Account map[string]any `json:"account"`
	}
	user.want(200, "GET", "/accounts/id:"+strconv.FormatInt(a.UID, 10), nil, &bare)
	if bare.Account["username"] != nil || bare.Account["bio"] != nil || bare.Account["user_id"] != strconv.FormatInt(a.UID, 10) {
		t.Errorf("account %s without pii:read: %v", a.Username, bare.Account)
	}
	want := map[string]*fakett.Post{}
	for _, p := range a.Posts {
		want[strconv.FormatInt(p.ID, 10)] = p
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		var page struct {
			Posts []map[string]any `json:"posts"`
			Next  string           `json:"next_cursor"`
		}
		user.want(200, "GET", "/accounts/sec:"+a.SecUID+"/posts?limit=4&cursor="+cursor, nil, &page)
		for _, p := range page.Posts {
			id := p["post_id"].(string)
			wp := want[id]
			counts := p["counts"].(map[string]any)
			if wp == nil || seen[id] || counts["like_count"] != float64(wp.Likes) || counts["comment_count"] != float64(wp.CommentCount()) {
				t.Errorf("post %s: %v", id, p)
				continue
			}
			seen[id] = true
			ids := map[string]bool{}
			var cs struct {
				Comments []map[string]any `json:"comments"`
			}
			pii.want(200, "GET", "/posts/"+id+"/comments?limit=500", nil, &cs)
			for _, c := range cs.Comments {
				ids[c["comment_id"].(string)] = true
				if c["text"] == nil || c["author"] == nil {
					t.Errorf("comment %v with pii:read lacks its text or author", c["comment_id"])
				}
			}
			for _, c := range wp.Comments {
				if !ids[strconv.FormatInt(c.ID, 10)] {
					t.Errorf("post %s: comment %d missing", id, c.ID)
				}
				for _, r := range c.Replies {
					if !ids[strconv.FormatInt(r.ID, 10)] {
						t.Errorf("post %s: reply %d missing", id, r.ID)
					}
				}
			}
			if int64(len(cs.Comments)) != wp.CommentCount() {
				t.Errorf("post %s: %d comments, want %d", id, len(cs.Comments), wp.CommentCount())
			}
			var one struct {
				Comments []map[string]any `json:"comments"`
			}
			user.want(200, "GET", "/posts/"+id+"/comments?limit=1", nil, &one)
			if len(one.Comments) == 1 && (one.Comments[0]["text"] != nil || one.Comments[0]["author"] != nil) {
				t.Errorf("post %s: a comment's text or author without pii:read", id)
			}
		}
		if cursor = page.Next; cursor == "" {
			break
		}
	}
	if len(seen) != len(want) {
		t.Errorf("account %s: %d posts through the API, want %d", a.Username, len(seen), len(want))
	}
}
