package api

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

const secret = "tks-api-test-secret"

// Synthetic ids: the post ids carry their time (T8), as TikTok's do.
const (
	acctID   = int64(7001)
	otherID  = int64(7002)
	mentID   = int64(8001) // a person a caption and a comment mention
	authorID = int64(9001) // a commenter
	replyID  = int64(9002) // a replier
)

var t0 = time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)

func postID(i int) int64 { return t0.Add(time.Duration(i)*time.Hour).Unix()<<32 | int64(i) }

// env is an API on a fresh schema tt holding one account read twice
// (renamed in between) with three posts, comments, replies, metrics,
// media and a sound, and API clients with different scopes.
type env struct {
	t    *testing.T
	db   *pgxpool.Pool
	srv  *httptest.Server
	keys map[string]string
	ids  map[string]uuid.UUID
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := ttstore.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 3, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	seed(t, &ttpersist.Writer{DB: db})
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Privacy.AuthorHMACKey = secret
	e := &env{t: t, db: db, keys: map[string]string{}, ids: map[string]uuid.UUID{}}
	for name, scopes := range map[string][]string{
		"reader": {ScopeDataRead},
		"pii":    {ScopeDataRead, ScopePIIRead},
		"a":      {ScopeDataRead, ScopeAccountsWrite, ScopeJobsWrite},
		"b":      {ScopeDataRead, ScopeAccountsWrite, ScopeJobsWrite},
		"admin":  {ScopeAdmin},
	} {
		key, id, err := CreateClient(ctx, db, name, scopes, 0)
		if err != nil {
			t.Fatal(err)
		}
		e.keys[name], e.ids[name] = key, id
	}
	s := &Server{Cfg: cfg, DB: db, Dispatch: &dispatch.Dispatcher{DB: db}}
	e.srv = httptest.NewServer(s.Handler())
	t.Cleanup(e.srv.Close)
	return e
}

func obs(at time.Time, hash string) *commonv1.Observation {
	return &commonv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(hash)}
}

func key(uid int64) []byte { return emit.AuthorKey([]byte(secret), uid) }

func person(uid int64, name string) *ttv1.Person {
	return &ttv1.Person{AuthorKey: key(uid), UserId: proto.Int64(uid), SecUid: fmt.Sprintf("sec-%d", uid), Username: name,
		Nickname: "Nick " + name}
}

func seed(t *testing.T, w *ttpersist.Writer) {
	t.Helper()
	ctx := context.Background()
	page := ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE
	acct := func(at time.Time, uid int64, name string, followers int64) *ttv1.Account {
		return &ttv1.Account{Obs: obs(at, name+fmt.Sprint(followers)), UserId: uid, SecUid: fmt.Sprintf("sec-%d", uid),
			Username: name, Nickname: "Nick " + name, Bio: "a bio", BioLink: proto.String("https://example.org/" + name),
			AvatarUrl: "https://example.org/avatar.jpg", AuthorKey: key(uid), Source: page, Verified: proto.Bool(true),
			Counts: &ttv1.AccountCounts{Followers: proto.Int64(followers), Videos: proto.Int64(3), Exact: true}}
	}
	if err := w.Accounts(ctx, []*ttv1.Account{acct(t0, acctID, "acct_old", 100), acct(t0.Add(time.Hour), acctID, "acct_new", 120),
		acct(t0, otherID, "other", 5)}); err != nil {
		t.Fatal(err)
	}
	var posts []*ttv1.Post
	ms := map[int][]*ttv1.PostMetrics{} // a batch keeps one capture per post
	for i := 1; i <= 3; i++ {
		kind := ttv1.PostKind_POST_KIND_VIDEO
		if i == 2 {
			kind = ttv1.PostKind_POST_KIND_PHOTO
		}
		p := &ttv1.Post{Obs: obs(t0.Add(30*time.Hour), fmt.Sprint("p", i)), PostId: postID(i), UserId: acctID, Kind: kind,
			CreatedAt: timestamppb.New(t0.Add(time.Duration(i) * time.Hour)), Caption: "caption #tag @ment",
			Hashtags: []*ttv1.Hashtag{{Id: 5, Name: "tag"}}, MusicId: proto.Int64(9), Pinned: proto.Bool(i == 1),
			TextRanges: []*ttv1.TextRange{{Offset: 13, Length: 5, Type: "mention", Name: "ment", Person: person(mentID, "ment")}},
			Counts:     &ttv1.PostCounts{Plays: proto.Int64(100), Likes: proto.Int64(int64(10 * i)), Comments: proto.Int64(3)}}
		posts = append(posts, p)
		for j, at := range []time.Time{t0.Add(29 * time.Hour), t0.Add(30 * time.Hour)} {
			ms[j] = append(ms[j], &ttv1.PostMetrics{Obs: obs(at, "m"), PostId: p.PostId, UserId: acctID, PostCreatedAt: p.CreatedAt,
				CapturedAt: timestamppb.New(at), Counts: &ttv1.PostCounts{Plays: proto.Int64(int64(90 + 10*j)), Likes: p.Counts.Likes}})
		}
	}
	if err := w.Posts(ctx, posts); err != nil {
		t.Fatal(err)
	}
	for j := range 2 {
		if err := w.PostMetrics(ctx, ms[j]); err != nil {
			t.Fatal(err)
		}
	}
	p1 := posts[0]
	cm := func(id, parent int64, author *ttv1.Person, text string, ranges ...*ttv1.TextRange) *ttv1.Comment {
		c := &ttv1.Comment{Obs: obs(t0.Add(31*time.Hour), text), CommentId: id, PostId: p1.PostId, UserId: acctID,
			PostCreatedAt: p1.CreatedAt, Author: author, Text: text, TextRanges: ranges,
			CreatedAt: timestamppb.New(t0.Add(time.Duration(2+id%10) * time.Hour)), LikeCount: proto.Int64(1),
			Status: ttv1.CommentStatus_COMMENT_STATUS_VISIBLE}
		if parent != 0 {
			c.ParentCommentId, c.Depth = proto.Int64(parent), 1
		}
		return c
	}
	if err := w.Comments(ctx, []*ttv1.Comment{
		cm(101, 0, person(authorID, "commenter"), "first"),
		cm(102, 101, person(replyID, "replier"), "@ment reply", &ttv1.TextRange{Offset: 0, Length: 5, Type: "mention", Name: "ment",
			Person: person(mentID, "ment")}),
		cm(103, 0, person(replyID, "replier"), "second"),
	}); err != nil {
		t.Fatal(err)
	}
	cover := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cover")).String()
	avatar := uuid.NewSHA1(uuid.NameSpaceOID, []byte("avatar")).String()
	if err := w.Media(ctx, []*ttv1.Media{
		{Obs: obs(t0, "c"), MediaId: cover, Kind: ttv1.MediaKind_MEDIA_KIND_COVER, Variant: "original", UserId: acctID,
			PostId: proto.Int64(p1.PostId), SourceUrl: "https://example.org/cover.jpg", PostCreatedAt: p1.CreatedAt},
		{Obs: obs(t0, "a"), MediaId: avatar, Kind: ttv1.MediaKind_MEDIA_KIND_AVATAR, Variant: "original",
			SourceUrl: "https://example.org/person.jpg", AuthorKey: key(authorID)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Music(ctx, []*ttv1.Music{{Obs: obs(t0, "music"), MusicId: 9, Title: "original sound", AuthorName: "acct_new",
		Original: proto.Bool(true)}}); err != nil {
		t.Fatal(err)
	}
}

// do sends a request as client who and decodes a JSON answer into out.
func (e *env) do(who, method, path string, body any, out any) int {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, e.srv.URL+"/v1"+path, rd)
	if who != "" {
		req.Header.Set("Authorization", "Bearer "+e.keys[who])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			e.t.Fatalf("%s %s: %v: %s", method, path, err, b)
		}
	}
	return resp.StatusCode
}

func (e *env) want(code int, who, method, path string, body any, out any) {
	e.t.Helper()
	if got := e.do(who, method, path, body, out); got != code {
		e.t.Fatalf("%s %s as %s: %d, want %d", method, path, who, got, code)
	}
}

func TestAuthAndScopes(t *testing.T) {
	e := newEnv(t)
	e.want(401, "", "GET", "/accounts", nil, nil)
	e.keys["forged"] = e.keys["reader"][:len(e.keys["reader"])-2] + "xx"
	e.want(401, "forged", "GET", "/accounts", nil, nil)
	e.want(403, "reader", "POST", "/accounts", map[string]any{"account": "@x"}, nil)
	e.want(403, "reader", "POST", "/jobs", map[string]any{"account": "@x"}, nil)
	e.want(403, "reader", "GET", "/accounts/id:7001/usernames", nil, nil)
	e.want(403, "a", "POST", "/erasure-requests", map[string]any{"subject_type": "author", "user_id": "1"}, nil)
	e.want(403, "pii", "GET", "/admin/operations", nil, nil)
	e.want(200, "admin", "GET", "/admin/operations", nil, nil)
	e.want(200, "admin", "GET", "/admin/canary/runs", nil, nil)
	if _, err := e.db.Exec(context.Background(), `UPDATE api_clients SET revoked_at = now() WHERE name = 'b'`); err != nil {
		t.Fatal(err)
	}
	e.want(401, "b", "GET", "/accounts", nil, nil)
	if _, _, err := CreateClient(context.Background(), e.db, "bad", []string{"pages:write"}, 0); err == nil {
		t.Fatal("a key with an unknown scope was made")
	}
}

// An account is found by id, secUid, its current or a former username, a
// link; personal fields need pii:read; ids are strings.
func TestAccountReadsAndPII(t *testing.T) {
	e := newEnv(t)
	for _, ref := range []string{"id:7001", "sec:sec-7001", "u:acct_new", "acct_old", "@ACCT_NEW",
		url.PathEscape("https://www.tiktok.com/@acct_new?lang=en")} {
		var out struct {
			Account map[string]any `json:"account"`
		}
		e.want(200, "pii", "GET", "/accounts/"+ref, nil, &out)
		if out.Account["user_id"] != "7001" || out.Account["username"] != "acct_new" || out.Account["bio_link"] != "https://example.org/acct_new" {
			t.Fatalf("%s: %v", ref, out.Account)
		}
	}
	var out struct {
		Account map[string]any `json:"account"`
	}
	e.want(200, "reader", "GET", "/accounts/id:7001", nil, &out)
	for _, f := range []string{"username", "nickname", "bio", "bio_link", "avatar_url"} {
		if _, ok := out.Account[f]; ok {
			t.Errorf("%s shown without pii:read", f)
		}
	}
	if out.Account["follower_count"] != float64(120) || out.Account["author_key"] != hex.EncodeToString(key(acctID)) {
		t.Errorf("account without pii: %v", out.Account)
	}
	e.want(404, "reader", "GET", "/accounts/u:nobody", nil, nil)
	e.want(400, "reader", "GET", "/accounts/u:bad%20name", nil, nil)
	var names struct {
		Usernames []struct{ Username string } `json:"usernames"`
	}
	e.want(200, "pii", "GET", "/accounts/id:7001/usernames", nil, &names)
	if len(names.Usernames) != 2 || names.Usernames[0].Username != "acct_new" || names.Usernames[1].Username != "acct_old" {
		t.Fatalf("usernames %+v", names.Usernames)
	}
	var snaps struct {
		Snapshots []map[string]any `json:"snapshots"`
	}
	e.want(200, "reader", "GET", "/accounts/id:7001/snapshots", nil, &snaps)
	if len(snaps.Snapshots) == 0 {
		t.Fatal("no snapshots")
	}
	for _, s := range snaps.Snapshots {
		if s["username"] != nil || s["nickname"] != nil {
			t.Fatalf("snapshot names without pii:read: %v", s)
		}
	}
}

func TestPostsCommentsMetricsMedia(t *testing.T) {
	e := newEnv(t)
	// Newest first, in pages of 2 without repeats.
	var seen []string
	cursor := ""
	for range 3 {
		var page struct {
			Posts []map[string]any `json:"posts"`
			Next  string           `json:"next_cursor"`
		}
		e.want(200, "reader", "GET", "/accounts/acct_new/posts?limit=2&cursor="+cursor, nil, &page)
		for _, p := range page.Posts {
			seen = append(seen, p["post_id"].(string))
		}
		if cursor = page.Next; cursor == "" {
			break
		}
	}
	want := []string{fmt.Sprint(postID(3)), fmt.Sprint(postID(2)), fmt.Sprint(postID(1))}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("posts %v, want %v", seen, want)
	}
	e.want(400, "reader", "GET", "/accounts/acct_new/posts?cursor=nonsense", nil, nil)
	var photos struct {
		Posts []map[string]any `json:"posts"`
	}
	e.want(200, "reader", "GET", "/accounts/id:7001/posts?kind=photo", nil, &photos)
	if len(photos.Posts) != 1 || photos.Posts[0]["kind"] != "photo" {
		t.Fatalf("photo posts %v", photos.Posts)
	}
	// Without pii:read the mention keeps its place and key only, and the
	// link (which names the username) is left out.
	p1 := fmt.Sprint(postID(1))
	var p map[string]any
	e.want(200, "reader", "GET", "/posts/"+p1, nil, &p)
	r := p["text_ranges"].([]any)[0].(map[string]any)
	if p["url"] != nil || r["name"] != nil || len(r["person"].(map[string]any)) != 1 || r["offset"] != float64(13) {
		t.Fatalf("post without pii: url %v range %v", p["url"], r)
	}
	if p["caption"] != "caption #tag @ment" || p["music_id"] != "9" || p["is_pinned"] != true {
		t.Fatalf("post %v", p)
	}
	e.want(200, "pii", "GET", "/posts/"+p1, nil, &p)
	if p["url"] != "https://www.tiktok.com/@acct_new/video/"+p1 {
		t.Fatalf("url %v", p["url"])
	}
	if r := p["text_ranges"].([]any)[0].(map[string]any); r["person"].(map[string]any)["username"] != "ment" {
		t.Fatalf("range with pii %v", r)
	}
	e.want(404, "reader", "GET", "/posts/12345", nil, nil)

	var cs struct {
		Comments []map[string]any `json:"comments"`
	}
	e.want(200, "reader", "GET", "/posts/"+p1+"/comments", nil, &cs)
	if len(cs.Comments) != 3 {
		t.Fatalf("%d comments", len(cs.Comments))
	}
	for _, c := range cs.Comments {
		if c["text"] != nil || c["author"] != nil || c["text_ranges"] != nil || c["author_key"] == "" {
			t.Fatalf("comment without pii: %v", c)
		}
	}
	e.want(200, "pii", "GET", "/posts/"+p1+"/comments?parent_id=101", nil, &cs)
	if len(cs.Comments) != 1 || cs.Comments[0]["text"] != "@ment reply" || cs.Comments[0]["parent_comment_id"] != "101" ||
		cs.Comments[0]["author"].(map[string]any)["username"] != "replier" {
		t.Fatalf("replies %v", cs.Comments)
	}
	e.want(200, "pii", "GET", "/posts/"+p1+"/comments?depth=0&limit=1", nil, &cs)
	if len(cs.Comments) != 1 || cs.Comments[0]["depth"] != float64(0) {
		t.Fatalf("top level %v", cs.Comments)
	}
	e.want(400, "pii", "GET", "/posts/"+p1+"/comments?depth=2", nil, nil)

	var ms struct {
		Metrics []map[string]any `json:"metrics"`
	}
	e.want(200, "reader", "GET", "/posts/"+p1+"/metrics", nil, &ms)
	if len(ms.Metrics) != 2 || ms.Metrics[0]["play_count"] != float64(100) || ms.Metrics[1]["play_count"] != float64(90) {
		t.Fatalf("metrics %v", ms.Metrics)
	}
	var media struct {
		Media []map[string]any `json:"media"`
	}
	e.want(200, "reader", "GET", "/posts/"+p1+"/media", nil, &media)
	if len(media.Media) != 1 || media.Media[0]["kind"] != "cover" || media.Media[0]["source_url"] == nil {
		t.Fatalf("media %v", media.Media)
	}
	var m map[string]any
	avatar := uuid.NewSHA1(uuid.NameSpaceOID, []byte("avatar")).String()
	e.want(200, "reader", "GET", "/media/"+avatar, nil, &m)
	if m["source_url"] != nil || m["author_key"] != hex.EncodeToString(key(authorID)) {
		t.Fatalf("a person's avatar without pii: %v", m)
	}
	e.want(200, "reader", "GET", "/music/9", nil, &m)
	if m["author_name"] != nil || m["title"] != "original sound" {
		t.Fatalf("music without pii %v", m)
	}
}

// Registration puts an account on the schedule due now, once however it
// is named; the schedule is shared, so only an admin may slow it while
// others are registered; unregistering disables it once nobody is.
func TestRegistrations(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	var reg struct {
		Ref      string         `json:"account_ref"`
		Created  bool           `json:"created"`
		Schedule map[string]any `json:"schedule"`
	}
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "https://www.tiktok.com/@Fresh_one"}, &reg)
	if reg.Ref != "u:fresh_one" || !reg.Created || reg.Schedule["interval_seconds"] != float64(9900) || reg.Schedule["enabled"] != true {
		t.Fatalf("registration %+v", reg)
	}
	if next, _ := time.Parse(time.RFC3339Nano, reg.Schedule["next_run_at"].(string)); time.Since(next) > time.Minute || time.Until(next) > time.Second {
		t.Fatalf("next run %v, want now", next)
	}
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "@fresh_one"}, &reg)
	if reg.Created {
		t.Fatal("a second registration created a schedule")
	}
	e.want(201, "b", "POST", "/accounts", map[string]any{"account": "fresh_one", "schedule": map[string]any{"interval_minutes": 60}}, &reg)
	if reg.Schedule["interval_seconds"] != float64(3600) {
		t.Fatalf("an explicit interval did not shorten the schedule: %v", reg.Schedule)
	}
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "fresh_one", "schedule": map[string]any{"interval_minutes": 600}}, &reg)
	if reg.Schedule["interval_seconds"] != float64(3600) {
		t.Fatalf("a longer interval slowed a shared schedule: %v", reg.Schedule)
	}
	e.want(400, "a", "POST", "/accounts", map[string]any{"account": "x", "schedule": map[string]any{"interval_minutes": 5}}, nil)
	e.want(400, "a", "POST", "/accounts", map[string]any{"account": "x", "options": map[string]any{"video_policy": "4k"}}, nil)
	e.want(422, "a", "POST", "/accounts", map[string]any{"account": "id:999"}, nil)
	// Shared: b may not disable it or slow it; it may shorten it; admin may.
	e.want(409, "b", "PATCH", "/accounts/fresh_one", map[string]any{"enabled": false}, nil)
	e.want(409, "b", "PATCH", "/accounts/fresh_one", map[string]any{"interval_minutes": 120}, nil)
	e.want(204, "b", "PATCH", "/accounts/fresh_one", map[string]any{"interval_minutes": 45}, nil)
	e.want(403, "pii", "PATCH", "/accounts/fresh_one", map[string]any{"enabled": true}, nil)
	e.want(204, "admin", "PATCH", "/accounts/fresh_one", map[string]any{"interval_minutes": 165, "options": map[string]any{"max_age_days": 30}}, nil)
	var got struct {
		Schedule map[string]any `json:"schedule"`
	}
	e.want(200, "reader", "GET", "/accounts/fresh_one", nil, &got)
	if got.Schedule["interval_seconds"] != float64(9900) || got.Schedule["options"].(map[string]any)["max_age_days"] != float64(30) {
		t.Fatalf("schedule after the admin's change %v", got.Schedule)
	}
	// A read account is one schedule by any name.
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "@acct_new"}, &reg)
	e.want(201, "b", "POST", "/accounts", map[string]any{"account": "sec:sec-7001"}, &reg)
	if reg.Ref != "u:acct_new" || reg.Created || reg.Schedule["user_id"] != "7001" {
		t.Fatalf("by secUid: %+v", reg)
	}
	e.want(201, "b", "POST", "/accounts", map[string]any{"account": "acct_old"}, &reg)
	if reg.Ref != "u:acct_new" {
		t.Fatalf("by a former username: %+v", reg)
	}
	var list struct {
		Accounts []map[string]any `json:"accounts"`
		Next     string           `json:"next_cursor"`
	}
	e.want(200, "a", "GET", "/accounts?limit=1", nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0]["account_ref"] != "u:acct_new" || list.Accounts[0]["username"] != nil || list.Next == "" {
		t.Fatalf("a's accounts %v", list)
	}
	e.want(200, "a", "GET", "/accounts?limit=1&cursor="+list.Next, nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0]["account_ref"] != "u:fresh_one" {
		t.Fatalf("a's second page %v", list)
	}
	e.want(200, "pii", "GET", "/accounts", nil, &list)
	if len(list.Accounts) != 0 {
		t.Fatalf("a client saw others' registrations: %v", list.Accounts)
	}
	// Unregistering: the schedule stays on while b is registered.
	e.want(204, "a", "DELETE", "/accounts/fresh_one", nil, nil)
	if !one[bool](t, e.db, `SELECT enabled FROM account_schedule WHERE account_ref = 'u:fresh_one'`) {
		t.Fatal("disabled while b is still registered")
	}
	e.want(404, "a", "PATCH", "/accounts/fresh_one", map[string]any{"interval_minutes": 60}, nil)
	e.want(204, "b", "DELETE", "/accounts/fresh_one", nil, nil)
	if one[bool](t, e.db, `SELECT enabled FROM account_schedule WHERE account_ref = 'u:fresh_one'`) {
		t.Fatal("still enabled with nobody registered")
	}
	e.want(403, "a", "DELETE", "/accounts/acct_new?purge=true", nil, nil)
	var er map[string]any
	e.want(202, "admin", "DELETE", "/accounts/acct_new?purge=true", nil, &er)
	if er["subject_type"] != "account" || one[int64](t, e.db, `SELECT user_id FROM erasure_requests WHERE id = $1`, er["id"]) != acctID {
		t.Fatalf("purge %v", er)
	}
	// An erased account is not registered again.
	if _, err := e.db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('account', '7002')`); err != nil {
		t.Fatal(err)
	}
	e.want(409, "a", "POST", "/accounts", map[string]any{"account": "@other"}, nil)
}

func TestErasureRequests(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		body    map[string]any
		code    int
		subject string
		key     []byte
		user    int64
	}{
		{map[string]any{"subject_type": "author", "username": "@Commenter"}, 202, "author", key(authorID), 0},
		{map[string]any{"subject_type": "author", "sec_uid": "sec-9002"}, 202, "author", key(replyID), 0},
		{map[string]any{"subject_type": "author", "user_id": "123456789"}, 202, "author", key(123456789), 0},
		{map[string]any{"subject_type": "author", "author_key": hex.EncodeToString(key(mentID))}, 202, "author", key(mentID), 0},
		{map[string]any{"subject_type": "account", "username": "acct_old"}, 202, "account", nil, acctID},
		{map[string]any{"subject_type": "account", "user_id": "7002"}, 202, "account", nil, otherID},
		{map[string]any{"subject_type": "account", "user_id": "424242"}, 202, "account", nil, 424242},
		{map[string]any{"subject_type": "author", "username": "nobody"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "account", "sec_uid": "sec-none"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "author", "user_id": "12x"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "author", "user_id": "007"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "author", "author_key": "abcd"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "account", "author_key": hex.EncodeToString(key(1))}, 400, "", nil, 0},
		{map[string]any{"subject_type": "author", "user_id": "1", "username": "x"}, 400, "", nil, 0},
		{map[string]any{"subject_type": "page", "user_id": "1"}, 400, "", nil, 0},
	}
	for i, c := range cases {
		var out map[string]any
		e.want(c.code, "admin", "POST", "/erasure-requests", c.body, &out)
		if c.code != 202 {
			continue
		}
		var subject string
		var k []byte
		var uid *int64
		if err := e.db.QueryRow(context.Background(), `SELECT subject_type, author_key, user_id FROM erasure_requests WHERE id = $1`,
			out["id"]).Scan(&subject, &k, &uid); err != nil {
			t.Fatal(err)
		}
		if subject != c.subject || hex.EncodeToString(k) != hex.EncodeToString(c.key) || (c.user != 0) != (uid != nil) ||
			(uid != nil && *uid != c.user) {
			t.Errorf("case %d: stored %s %x %v", i, subject, k, uid)
		}
		var got map[string]any
		e.want(200, "admin", "GET", "/erasure-requests/"+out["id"].(string), nil, &got)
		if got["status"] != "pending" || got["subject_type"] != c.subject {
			t.Errorf("case %d: %v", i, got)
		}
	}
}

// Jobs: a client sees its own jobs and the scheduled jobs of accounts it
// registered, never another's; results stream the account, its posts seen
// during the job and their comments, and end with an end line.
func TestJobsOwnershipAndResults(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "@acct_new"}, nil)
	mk := func(client *uuid.UUID, kind string) uuid.UUID {
		id, _ := uuid.NewV7()
		if _, err := e.db.Exec(ctx, `INSERT INTO jobs (job_id, api_client_id, kind, lane, mode, account_ref, user_id, params, status,
			created_at, finished_at) VALUES ($1,$2,$3,0,'incremental','u:acct_new',7001,'{}','completed',$4,$4)`,
			id, client, kind, t0.Add(29*time.Hour)); err != nil {
			t.Fatal(err)
		}
		return id
	}
	aid, bid := e.ids["a"], e.ids["b"]
	ownA, ownB, sched := mk(&aid, "on_demand"), mk(&bid, "on_demand"), mk(nil, "scheduled")
	e.want(200, "a", "GET", "/jobs/"+ownA.String(), nil, nil)
	e.want(200, "a", "GET", "/jobs/"+sched.String(), nil, nil)
	e.want(404, "a", "GET", "/jobs/"+ownB.String(), nil, nil)
	e.want(404, "b", "GET", "/jobs/"+sched.String(), nil, nil) // b did not register the account
	e.want(404, "a", "POST", "/jobs/"+sched.String()+"/cancel", nil, nil)
	e.want(400, "a", "GET", "/jobs/not-a-uuid", nil, nil)
	var list struct {
		Jobs []map[string]any `json:"jobs"`
	}
	e.want(200, "a", "GET", "/jobs?account=@acct_new", nil, &list)
	if len(list.Jobs) != 2 {
		t.Fatalf("a sees %d jobs, want 2", len(list.Jobs))
	}
	if list.Jobs[0]["user_id"] != "7001" {
		t.Fatalf("job %v", list.Jobs[0])
	}
	e.want(200, "admin", "GET", "/jobs", nil, &list)
	if len(list.Jobs) != 3 {
		t.Fatalf("admin sees %d jobs", len(list.Jobs))
	}

	req, _ := http.NewRequest("GET", e.srv.URL+"/v1/jobs/"+ownA.String()+"/results?include=comments", nil)
	req.Header.Set("Authorization", "Bearer "+e.keys["a"])
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var types []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var line map[string]any
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		types = append(types, line["type"].(string))
	}
	if got := strings.Join(types, ","); got != "account,post,post,post,comment,comment,comment,end" {
		t.Fatalf("results %s", got)
	}
}

func one[T any](t *testing.T, db *pgxpool.Pool, q string, args ...any) T {
	t.Helper()
	var v T
	if err := db.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func TestScrubRanges(t *testing.T) {
	in := `[{"offset":0,"length":4,"type":"hashtag","name":"tag"},{"offset":5,"length":5,"type":"mention","name":"ment",` +
		`"person":{"author_key":"a2V5","user_id":"8001","username":"ment"}}]`
	got := string(scrubRanges(json.RawMessage(in)))
	want := `[{"length":4,"name":"tag","offset":0,"type":"hashtag"},{"length":5,"offset":5,"person":{"author_key":"a2V5"},"type":"mention"}]`
	if got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	if scrubRanges(json.RawMessage(`{`)) != nil {
		t.Fatal("unreadable ranges kept")
	}
	if u := PostURL("name", "photo", 42); u != "https://www.tiktok.com/@name/photo/42" {
		t.Fatal(u)
	}
}
