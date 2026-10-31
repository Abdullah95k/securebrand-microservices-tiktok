package probe_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

func run(t *testing.T, f fakeig.Faults, extra ...string) (probe.Report, *igtest.Env) {
	return runImages(t, f, "", extra...)
}

func runImages(t *testing.T, f fakeig.Faults, imageDir string, extra ...string) (probe.Report, *igtest.Env) {
	t.Helper()
	env := igtest.New(t, igtest.Options{Faults: f})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	names = append(names, extra...)
	rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct",
		MaxGridPages: 10, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Fingerprint: "chrome_146", Version: "test", ImageDir: imageDir})
	return rep, env
}

// No faults: every question gets its "works" answer, and the report holds
// what fakeig served: profile details, every post, the newest post's first
// comments and the profile picture.
func TestProbeAgainstFakeig(t *testing.T) {
	dir := t.TempDir()
	rep, env := runImages(t, fakeig.Faults{}, filepath.Join(dir, "images"), "no_such_profile")
	s := rep.Summary
	n := len(env.World.Profiles())
	posts := len(env.World.Profiles()[0].Posts)
	if s.Profiles != n+1 || s.Bootstrapped != n || s.GridEnds != n || s.PagedPast24 != n || s.MaxPosts != posts {
		t.Fatalf("summary %+v", s)
	}
	if s.CursorPortable != n || s.CursorNotPortable != 0 || s.GridByPKWorks != n || s.GridByPKFails != 0 {
		t.Fatalf("U5/U9: %+v", s)
	}
	if s.CSRFCookie != n || s.Cookies["csrftoken"] != n || s.Cookies["mid"] != n || s.Cookies["ig_did"] != n {
		t.Fatalf("U8: %+v", s)
	}
	if s.QueriesWithoutClientState["ok"] == 0 || len(s.QueriesWithoutClientState) != 1 {
		t.Fatalf("U3: %v", s.QueriesWithoutClientState)
	}
	for _, k := range []string{"lsd", "csrf_token", "app_id", "jazoest"} {
		if s.TokensFound[k] != n {
			t.Errorf("token %s found %d times", k, s.TokensFound[k])
		}
	}
	for i, p := range rep.Profiles[:n] {
		want := env.World.Profiles()[i]
		if p.CommentsRead == nil || p.CommentsRead.Class != "ok" || p.Highlights == nil || p.Highlights.Items == 0 || p.Grid.Posts != posts {
			t.Fatalf("profile %d: %+v", p.Index, p)
		}
		d := p.Data
		if p.Username != want.Username || p.URL != env.HTTP.URL+"/"+want.Username+"/" || d == nil ||
			d.FullName != want.FullName || d.Followers != want.Followers || d.PK != strconv.FormatInt(want.PK, 10) ||
			d.Verified != want.Verified || len(d.BioLinks) != 1 || len(d.Highlights) != len(want.Highlights) || d.Highlights[0].Items == 0 {
			t.Fatalf("profile %d data: %+v", p.Index, d)
		}
		if len(p.Posts) != len(want.Posts) || p.Posts[0].Shortcode != want.Posts[0].Code || p.Posts[0].Caption != want.Posts[0].Caption ||
			!strings.HasSuffix(p.Posts[0].URL, "/"+want.Posts[0].Code+"/") || p.Posts[0].MediaType == "" || p.Posts[0].ThumbnailURL == "" {
			t.Fatalf("profile %d posts: %d, first %+v", p.Index, len(p.Posts), p.Posts[0])
		}
		// Each step keeps its own outcome (they once shared one variable).
		if p.GridFirst.Items != fakeig.GridPageSize || !p.GridFirst.HasNext || p.GridByPK.Items != fakeig.GridPageSize ||
			p.CommentsRead.Items != len(p.Comments) || p.Highlights.Items != d.Highlights[0].Items || p.Profile.Items != 0 {
			t.Fatalf("profile %d steps: first %+v, by pk %+v, comments %+v, highlights %+v, profile %+v",
				p.Index, p.GridFirst, p.GridByPK, p.CommentsRead, p.Highlights, p.Profile)
		}
		newest := want.Posts[0]
		np := p.NewestPost
		if np == nil || np.Class != "ok" || np.CommentsEmbedded != min(len(newest.Comments), fakeig.CommentPageSize) ||
			np.LikeCount == nil || *np.LikeCount != newest.Likes || !np.TakenAt.Equal(newest.TakenAt) ||
			np.CommentCount == nil || *np.CommentCount != int64(len(newest.Comments)) {
			t.Fatalf("profile %d newest post page: %+v", p.Index, np)
		}
		if len(p.CommentsRead.Errors) != 0 {
			t.Fatalf("profile %d comments errors: %v", p.Index, p.CommentsRead.Errors)
		}
		if len(p.Comments) != min(len(newest.Comments), fakeig.CommentPageSize) {
			t.Fatalf("profile %d comments: %d of %d", p.Index, len(p.Comments), len(newest.Comments))
		}
		if len(p.Comments) > 0 && (p.Comments[0].Text != newest.Comments[0].Text || p.Comments[0].Username != newest.Comments[0].Username ||
			p.Comments[0].PostShortcode != newest.Code || !p.Comments[0].CreatedAt.Equal(newest.Comments[0].CreatedAt)) {
			t.Fatalf("profile %d first comment: %+v", p.Index, p.Comments[0])
		}
		if b, err := os.ReadFile(filepath.Join(dir, d.ProfilePicFile)); err != nil || len(b) == 0 || d.ProfilePicFile != filepath.Join("images", want.Username+".jpg") {
			t.Fatalf("profile %d picture %q: %v", p.Index, d.ProfilePicFile, err)
		}
	}
	// The unknown profile's page is a 404 and its profile query finds no
	// user; it is reported by its line number only.
	if last := rep.Profiles[n]; last.Index != n+1 || last.Bootstrap.Class == "ok" || last.Profile == nil ||
		last.Profile.Reason != "no user in the response" || last.Grid != nil {
		t.Fatalf("unknown profile: %+v", last)
	}
	if s.Requests["page"].N != n+1 || s.Requests["grid"].N == 0 || s.Requests["grid"].MeanBytes == 0 {
		t.Fatalf("costs %+v", s.Requests)
	}
	// The files: the report, three spreadsheets and the bundle, all 0600.
	if err := probe.Write(dir, rep); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "report.json"))
	var back probe.Report
	if err != nil || json.Unmarshal(b, &back) != nil || len(back.Profiles) != n+1 {
		t.Fatalf("report.json: %v", err)
	}
	for name, rows := range map[string]int{"profiles.csv": n + 2, "posts.csv": n*posts + 1} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		recs, err := csv.NewReader(f).ReadAll()
		_ = f.Close()
		if err != nil || len(recs) != rows {
			t.Fatalf("%s: %d rows, want %d (%v)", name, len(recs), rows, err)
		}
		if fi, _ := os.Stat(filepath.Join(dir, name)); fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %v", name, fi.Mode().Perm())
		}
	}
	bundle, err := probe.Bundle(dir)
	if fi, serr := os.Stat(bundle); err != nil || serr != nil || fi.Size() == 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("bundle %s: %v %v", bundle, err, serr)
	}
}

// Each fault shows in its question's answer.
func TestProbeFindsTheWalls(t *testing.T) {
	t.Run("grid wall at 24 (U1)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{GridWallAfter: 24})
		n := len(env.World.Profiles())
		s := rep.Summary
		if s.GridWalls["login_wall"] != n || s.PagedPast24 != 0 || s.MaxPosts != 24 || len(s.WalledAtPosts) != n || s.WalledAtPosts[0] != 24 {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("cursor bound to its identity (U5)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{CursorBound: true})
		if s := rep.Summary; s.CursorNotPortable != len(env.World.Profiles()) || s.CursorPortable != 0 || s.GridEnds != len(env.World.Profiles()) {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("no grid by pk alone (U9)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{NoPKGrid: true})
		if s := rep.Summary; s.GridByPKFails != len(env.World.Profiles()) || s.GridByPKWorks != 0 {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("client state required (U3)", func(t *testing.T) {
		rep, _ := run(t, fakeig.Faults{RequireClientState: true})
		if s := rep.Summary; s.QueriesWithoutClientState["ok"] != 0 || s.QueriesWithoutClientState["soft_error"] == 0 {
			t.Fatalf("summary %+v", s.QueriesWithoutClientState)
		}
	})
	t.Run("grid depth cap", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Spec: fakeig.Spec{Seed: 2, Profiles: 1, PostsPerProfile: 100, MaxComments: 3, Highlights: 0}})
		rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: []string{env.World.Profiles()[0].Username},
			Tier: "direct", MaxGridPages: 3, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL})
		if g := rep.Profiles[0].Grid; g.Stop != "cap" || g.Posts != 48 || !rep.Profiles[0].NoHighlights {
			t.Fatalf("grid %+v", g)
		}
	})
}

// A grid streamed in parts, as Relay sends a connection (edges in their own
// parts, the real page_info deferred), is read whole: every post, the walk
// to the end, the other identity's cursor and the grid by pk. The raw
// answers and the first pages' HTML are saved.
func TestProbeReadsAStreamedGrid(t *testing.T) {
	env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{StreamGrid: true}})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	raw := filepath.Join(t.TempDir(), "raw")
	rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct",
		MaxGridPages: 10, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, RawDir: raw, RawPages: 2})
	n := len(names)
	posts := len(env.World.Profiles()[0].Posts)
	s := rep.Summary
	if s.GridEnds != n || s.MaxPosts != posts || s.PagedPast24 != n || s.CursorPortable != n || s.GridByPKWorks != n {
		t.Fatalf("summary %+v", s)
	}
	for _, p := range rep.Profiles {
		if len(p.Posts) != posts || p.GridFirst.Items != fakeig.GridPageSize || !p.GridFirst.HasNext {
			t.Fatalf("profile %d: %d posts, first page %+v", p.Index, len(p.Posts), p.GridFirst)
		}
	}
	first := filepath.Join(raw, "001-"+names[0])
	for _, pattern := range []string{"page.html", "*-profile-ok.json", "*-grid_first-ok.json", "*-grid-ok.json", "*-comments-ok.json"} {
		if m, _ := filepath.Glob(filepath.Join(first, pattern)); len(m) == 0 {
			t.Errorf("no %s in %s", pattern, first)
		}
	}
	if m, _ := filepath.Glob(filepath.Join(raw, "003-*", "page.html")); len(m) != 0 {
		t.Error("page HTML saved past RawPages")
	}
	// One grid page per profile is enough to check the shape; failures and
	// empty pages would be kept too.
	if m, _ := filepath.Glob(filepath.Join(first, "*-grid-ok.json")); len(m) != 1 {
		t.Errorf("grid pages saved: %v", m)
	}
}

// A post's time comes from its pk, and MaxAge stops the walk at the
// backfill window.
func TestProbePostedAtAndAgeLimit(t *testing.T) {
	env := igtest.New(t, igtest.Options{Spec: fakeig.Spec{Seed: 3, Profiles: 1, PostsPerProfile: 60, MaxComments: 2, PostSpacing: 24 * time.Hour}})
	p := env.World.Profiles()[0]
	now := func() time.Time { return fakeig.Epoch.Add(time.Hour) }
	rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: []string{p.Username}, Tier: "direct",
		MaxGridPages: 20, MaxAge: 20 * 24 * time.Hour, Now: now, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL})
	got := rep.Profiles[0]
	// Posts a day apart, 12 a page: the second page (days 12-23) reaches
	// past the 20-day window, so the walk stops after 24 posts.
	if got.Grid.Stop != "age" || len(got.Posts) != 24 || rep.Summary.GridAgeStops != 1 {
		t.Fatalf("grid %+v, %d posts", got.Grid, len(got.Posts))
	}
	for i, x := range got.Posts {
		if d := x.PostedAt.Sub(p.Posts[i].TakenAt); d < -time.Second || d > time.Second {
			t.Fatalf("post %d posted %s, taken %s", i, x.PostedAt, p.Posts[i].TakenAt)
		}
	}
	if !got.Grid.Oldest.Equal(got.Posts[23].PostedAt) {
		t.Fatalf("oldest %s", got.Grid.Oldest)
	}
	if probe.PostedAt("not-a-pk").IsZero() != true || probe.PostedAt("12").IsZero() != true {
		t.Fatal("implausible pks must give no time")
	}
}

// A long walk reports progress every 10 pages, and stopping the run
// (Ctrl-C) mid-walk is recorded as a stop, not a wall: the profile is
// marked partial and counts for no question.
func TestProbeProgressAndStop(t *testing.T) {
	env := igtest.New(t, igtest.Options{Spec: fakeig.Spec{Seed: 4, Profiles: 2, PostsPerProfile: 200, MaxComments: 2, PostSpacing: time.Hour}})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lines []string
	rep := probe.Run(ctx, probe.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct", MaxGridPages: 100,
		Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Progress: func(line string) {
			lines = append(lines, line)
			if strings.Contains(line, "posts so far") {
				cancel() // the operator presses Ctrl-C after the first progress line
			}
		}})
	if len(lines) != 2 || !strings.Contains(lines[0], names[0]+": 132 posts so far, back to ") ||
		!strings.Contains(lines[1], "profile 1/2 "+names[0]+": stopped (Ctrl-C) after 132 posts") {
		t.Fatalf("progress %q", lines)
	}
	s := rep.Summary
	if len(rep.Profiles) != 1 || !s.StoppedEarly || !rep.Profiles[0].Stopped || s.Profiles != 1 {
		t.Fatalf("profiles %d, summary %+v", len(rep.Profiles), s)
	}
	g := rep.Profiles[0].Grid
	if g.Stop != "stopped" || g.Failed != nil || len(s.GridWalls) != 0 || len(s.WalledAtPosts) != 0 || s.GridByPKFails != 0 || s.GridByPKWorks != 0 {
		t.Fatalf("grid %+v, summary %+v", g, s)
	}
	if len(rep.Profiles[0].Posts) != 132 || rep.Profiles[0].GridByPK != nil || s.QueriesWithoutClientState["transport"] != 0 {
		t.Fatalf("profile %+v", rep.Profiles[0])
	}
}
