package probe_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/probe"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

func run(t *testing.T, f fakett.Faults, mod func(*probe.Options)) (probe.Report, *tttest.Env, string) {
	t.Helper()
	return runSpec(t, fakett.Spec{}, f, mod)
}

// runSpec is run on a world built from spec (the default world when zero).
func runSpec(t *testing.T, spec fakett.Spec, f fakett.Faults, mod func(*probe.Options)) (probe.Report, *tttest.Env, string) {
	t.Helper()
	env := tttest.New(t, tttest.Options{Spec: spec, Faults: f})
	var names []string
	for _, a := range env.World.Accounts() {
		names = append(names, a.Username)
	}
	dir := t.TempDir()
	o := probe.Options{BaseURL: env.HTTP.URL, Accounts: names, Tier: "direct", Pool: env.Pool, Loader: env.Loader, Client: env.Client,
		AcceptLanguage: tttest.AcceptLanguage, Fingerprint: "chrome_152", MaxListPages: 40, MaxCommentPages: 5, CountChecks: 1,
		Covers: 2, Videos: 1, OEmbed: true, ImageDir: filepath.Join(dir, "images"), VideoDir: filepath.Join(dir, "videos"),
		RawDir: filepath.Join(dir, "raw"), RawPages: 1, RetryPause: time.Millisecond}
	if mod != nil {
		mod(&o)
	}
	return probe.Run(context.Background(), o), env, dir
}

// The whole probe against fakett: every account read, every question
// answered, and the files written 0600.
func TestProbeAgainstFakett(t *testing.T) {
	rep, env, dir := run(t, fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/post/item_list/"}}, func(o *probe.Options) {
		o.DepthChecks, o.DepthDays = 1, []int{1, 3650}
	})
	s := rep.Summary
	if s.Accounts != 3 || s.Bootstrapped != 3 || s.PageForms["ssr"] != 3 {
		t.Fatalf("bootstrap: %+v", s)
	}
	if s.Calls["creator_list"]["ok"] != 3 || s.Calls["post_list"]["registry_stale_empty"] != 3 {
		t.Fatalf("unsigned lists: %v", s.Calls)
	}
	if s.Calls["needs_no_device_id"]["registry_stale_empty"] != 3 || s.Calls["needs_no_mstoken"]["ok"] != 3 {
		t.Fatalf("needs: %v", s.Calls)
	}
	if s.Posts != 180 || s.PostsWithStats != 180 || s.IDTimeMatches != 180 || s.PhotoSeen != 3 || s.AdSeen != 3 || s.WalkStops["end"] != 3 {
		t.Fatalf("posts: %+v", s)
	}
	// The creator list's time cursor keeps the oldest post's second, so
	// fakett (which compares in ms) serves it again on the next page.
	if s.WalkRepeats == 0 {
		t.Fatalf("walk repeats: %+v", s)
	}
	if s.DepthServed["1"] != 1 || s.DepthServed["3650"] != 0 || len(rep.Accounts[0].Depth) != 2 {
		t.Fatalf("depth: %v %+v", s.DepthServed, rep.Accounts[0].Depth)
	}
	if s.CommentsServed == 0 || s.OrderVerdicts["not_time_ordered"] == 0 || s.RepliesServed == 0 || s.CursorPortable != 3 {
		t.Fatalf("comments: %+v", s)
	}
	for _, r := range s.CompleteRatios {
		if r != 1 {
			t.Fatalf("fakett serves every comment: ratio %v", r)
		}
	}
	a := rep.Accounts[0]
	acc := env.World.Account(a.Username)
	if a.Data == nil || a.Data.SecUID != acc.SecUID || a.Data.Followers != acc.Followers || a.Data.BioLink != acc.BioLink || a.Data.AvatarFile == "" {
		t.Fatalf("account data %+v", a.Data)
	}
	if a.PostPage == nil || !a.PostPageStats || a.PhotoPage == nil || a.OEmbed == nil || a.Media["video_with_cookies"].Class != "ok" {
		t.Fatalf("pages and media: %+v %v %+v %+v", a.PostPage, a.PostPageStats, a.PhotoPage, a.Media)
	}
	if err := probe.Write(dir, rep); err != nil {
		t.Fatal(err)
	}
	bundle, err := probe.Bundle(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"report.json", "accounts.csv", "posts.csv", "comments.csv", bundle} {
		p := f
		if !filepath.IsAbs(p) {
			p = filepath.Join(dir, f)
		}
		st, err := os.Stat(p)
		if err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v %v", f, err, st)
		}
	}
	csv, _ := os.ReadFile(filepath.Join(dir, "posts.csv"))
	if !strings.Contains(string(csv), acc.Posts[0].Desc) {
		t.Fatal("posts.csv lacks the captions")
	}
}

// The faults land in the report: a challenge page reads nothing; captchas
// stop the run; comments refused to guests and a list wall are recorded.
func TestProbeFindsTheWalls(t *testing.T) {
	rep, _, _ := run(t, fakett.Faults{Challenge: true}, nil)
	if rep.Summary.PageForms["challenge"] != 3 || rep.Summary.Challenges < 3 || rep.Summary.Posts != 0 {
		t.Fatalf("challenge: %+v", rep.Summary)
	}
	rep, _, _ = run(t, fakett.Faults{CaptchaAfter: 2}, func(o *probe.Options) { o.MaxCaptchas = 2 })
	if rep.Summary.StoppedEarly != "captchas" || rep.Summary.Captchas < 2 {
		t.Fatalf("captchas: %+v", rep.Summary)
	}
	rep, _, _ = run(t, fakett.Faults{NoGuestComments: true}, nil)
	if rep.Summary.CommentsServed != 0 || rep.Summary.CommentReads == 0 {
		t.Fatalf("no guest comments: %+v", rep.Summary)
	}
	// An unsigned account call is refused with an empty account; the page
	// still gives the account.
	rep, _, _ = run(t, fakett.Faults{RequireSignature: true}, nil)
	if rep.Summary.Calls["user_detail"]["registry_stale"] != 3 || rep.Accounts[0].Data == nil || rep.Accounts[0].Data.Source != "page" {
		t.Fatalf("refused account call: %v %+v", rep.Summary.Calls, rep.Accounts[0].Data)
	}
	// A shell page is kept for a look.
	rep, _, dir := run(t, fakett.Faults{ShellPages: true}, nil)
	if rep.Summary.PageForms["shell"] != 3 {
		t.Fatalf("shell: %v", rep.Summary.PageForms)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "failed", rep.Accounts[0].Username+"-profile_page.html")); err != nil {
		t.Fatalf("shell page not kept: %v", err)
	}
	rep, _, _ = run(t, fakett.Faults{ListWallAfter: 30}, nil)
	if rep.Summary.WalkStops["wall:registry_stale"] != 3 {
		t.Fatalf("list wall: %v", rep.Summary.WalkStops)
	}
	rep, _, _ = run(t, fakett.Faults{}, func(o *probe.Options) { o.MaxListPages = 1 })
	if rep.Summary.WalkStops["cap"] != 3 {
		t.Fatalf("cap: %v", rep.Summary.WalkStops)
	}
	rep, _, _ = run(t, fakett.Faults{}, func(o *probe.Options) { o.MaxAge = 10 * 24 * time.Hour })
	if rep.Summary.WalkStops["age"] != 3 {
		t.Fatalf("age: %v", rep.Summary.WalkStops)
	}
}

// A stop by the operator is a stop: the run ends, the report says so.
func TestProbeStop(t *testing.T) {
	env := tttest.New(t, tttest.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep := probe.Run(ctx, probe.Options{BaseURL: env.HTTP.URL, Accounts: []string{env.World.Accounts()[0].Username}, Tier: "direct",
		Pool: env.Pool, Loader: env.Loader, Client: env.Client, AcceptLanguage: tttest.AcceptLanguage})
	if rep.Summary.StoppedEarly != "operator" {
		t.Fatalf("stop: %+v", rep.Summary)
	}
}

// The browser signer, where Chromium exists: the page's script signs the
// calls, and the signed URL replays by plain HTTP (fakett accepts it).
func TestProbeBrowserSigner(t *testing.T) {
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	rep, _, _ := run(t, fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/post/item_list/"}}, func(o *probe.Options) {
		o.Signer, o.BrowserBinary, o.BrowserNoSandbox = "browser", bin, true
		o.Accounts = o.Accounts[:1]
	})
	sg := rep.Accounts[0].Signed
	if sg == nil || sg.Error != "" || sg.Page.Class != "ok" {
		t.Fatalf("signed: %+v", sg)
	}
	pl := sg.Calls["post_list"]
	if pl == nil || pl.Status != 200 || pl.Items == 0 || len(pl.Added) == 0 || pl.Replay == nil || pl.Replay.Class != "ok" {
		t.Fatalf("signed post_list: %+v", pl)
	}
	if rep.Accounts[0].AppListPinned != 1 {
		t.Fatalf("pinned from the signed app list: %d", rep.Accounts[0].AppListPinned)
	}
	// The page's own calls: the profile page's list, signed by its script;
	// its next page asked from the page; then the post page's comments.
	if sg.Env["webdriver"] != false || sg.BotType != "others" {
		t.Fatalf("browser env %v bot type %q", sg.Env, sg.BotType)
	}
	own := false
	for _, pc := range sg.PageCalls {
		if pc.Path == "/api/post/item_list/" && pc.Items > 0 && pc.Signed {
			own = true
		}
	}
	if !own {
		t.Fatalf("the page's own list call: %+v", sg.PageCalls)
	}
	if n := sg.Calls["post_list_from_page"]; n == nil || n.Items == 0 || n.Replay == nil || n.Replay.Class != "ok" {
		t.Fatalf("the page list's next page: %+v", n)
	}
	if len(sg.ParamsExtra) == 0 {
		t.Fatalf("tks sends parameters fakett's page does not: %v %v", sg.ParamsMissing, sg.ParamsExtra)
	}
	if sg.VideoPage == nil || sg.VideoPage.Class != "ok" {
		t.Fatalf("video page: %+v", sg.VideoPage)
	}
	comments := false
	for _, pc := range sg.VideoPageCalls {
		if pc.Path == "/api/comment/list/" && pc.Items > 0 {
			comments = true
		}
	}
	if !comments {
		t.Fatalf("the post page's own comment call: %+v", sg.VideoPageCalls)
	}
	if rep.Summary.Calls["browser_own_post_list"]["ok"] != 1 || rep.Summary.Calls["browser_own_comments"]["ok"] != 1 {
		t.Fatalf("own calls booked: %v", rep.Summary.Calls)
	}
	// Comments and replies read from the page, and the first signed comment
	// URL replayed from another identity, modified, and later.
	if rep.Summary.CommentsByVia["browser"] == 0 || rep.Summary.CommentsByVia["plain"] == 0 {
		t.Fatalf("comments by via: %v", rep.Summary.CommentsByVia)
	}
	c := sg.Calls["comments"]
	if c == nil || c.ReplayOther == nil || c.ReplayOther.Class != "ok" || c.ReplayModified == nil || c.ReplayLater == nil ||
		c.ReplayLater.Class != "ok" || !strings.Contains(c.ReplayLater.Reason, "after") {
		t.Fatalf("comment replays: %+v", c)
	}
	seen := map[string]bool{}
	for _, cm := range rep.Accounts[0].Comments {
		if seen[cm.ID] {
			t.Fatalf("comment %s kept twice", cm.ID)
		}
		seen[cm.ID] = true
	}
	if c.Class != "ok" || sg.Calls["post_list"].Class != "ok" {
		t.Fatalf("signed call classes: comments %q post_list %q", c.Class, sg.Calls["post_list"].Class)
	}
	if rep.Accounts[0].Media["video_chain_cookie"].Class != "ok" {
		t.Fatalf("video with the chain cookie: %+v", rep.Accounts[0].Media)
	}
}

// Accounts read in parallel give the same answers as one after another,
// in the accounts file's order (run with -race).
func TestProbeParallel(t *testing.T) {
	seq, _, _ := run(t, fakett.Faults{}, nil)
	par, env, _ := run(t, fakett.Faults{}, func(o *probe.Options) { o.Parallel = 3 })
	if par.Summary.Accounts != 3 || par.Summary.Posts != seq.Summary.Posts || par.Summary.CommentsServed != seq.Summary.CommentsServed ||
		par.Summary.WalkStops["end"] != 3 || par.Summary.StoppedEarly != "" {
		t.Fatalf("parallel %+v\nsequential %+v", par.Summary, seq.Summary)
	}
	for i, a := range env.World.Accounts() {
		if par.Accounts[i].Username != a.Username || par.Accounts[i].Index != i+1 {
			t.Fatalf("account %d: %s, want %s", i, par.Accounts[i].Username, a.Username)
		}
	}
	if par.Summary.Calls["creator_list"]["ok"] != 3 || par.Summary.Requests["creator_list"].N != 3 {
		t.Fatalf("booked calls: %v", par.Summary.Calls)
	}
	// Captchas still stop the run.
	rep, _, _ := run(t, fakett.Faults{CaptchaAfter: 2}, func(o *probe.Options) { o.MaxCaptchas, o.Parallel = 2, 2 })
	if rep.Summary.StoppedEarly != "captchas" || rep.Summary.Captchas < 2 || rep.Summary.Accounts == 3 {
		t.Fatalf("captchas in parallel: %+v", rep.Summary)
	}
}

// TikTok's list window and its occasional errors (the C3 full pass): with
// posts 10 days apart and a 7-day window, pages alternate between the post
// at the cursor alone, an empty page and the next post; every 4th list
// call answers statusCode 10102. The walk still reads every post, retrying
// the errors, and ends at the first post.
func TestProbeWalkThroughWindowAndErrors(t *testing.T) {
	spec := fakett.Spec{Seed: 2, Accounts: 2, PostsPerAccount: 8, MaxComments: 3, MaxReplies: 1, PostSpacing: 10 * 24 * time.Hour}
	rep, env, _ := runSpec(t, spec, fakett.Faults{ListWindow: 7 * 24 * time.Hour, ListErrorEvery: 4}, func(o *probe.Options) {
		o.MaxListPages, o.CountChecks = 200, 0
	})
	for i, acc := range env.World.Accounts() {
		a := rep.Accounts[i]
		if a.Walk == nil || a.Walk.Stop != "end" || a.Walk.Posts != len(acc.Posts) {
			t.Fatalf("account %d: walk %+v, want all %d posts and end", i, a.Walk, len(acc.Posts))
		}
	}
	retries, empty, repeats := 0, 0, 0
	for _, a := range rep.Accounts {
		retries += a.Walk.Retries
		empty += a.Walk.EmptyPages
		repeats += a.Walk.Repeats
	}
	if retries == 0 || empty == 0 || repeats == 0 {
		t.Fatalf("retries %d, empty pages %d, repeats %d: the window and the errors were not exercised", retries, empty, repeats)
	}
}
