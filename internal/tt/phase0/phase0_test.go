package phase0_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// page signs as fakett's page script does.
type page struct{}

func (page) Sign(_ context.Context, raw string) (string, error) {
	time.Sleep(2 * time.Millisecond) // a browser's time, so it is counted
	return fakett.Sign(raw)
}
func (page) Close() {}

func opener(context.Context, *session.Identity, string) (sign.Page, error) { return page{}, nil }

func newPool(t *testing.T, base string, rps float64) *session.Pool {
	t.Helper()
	b, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(b)
	if err != nil {
		t.Fatal(err)
	}
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := session.NewPool(session.PoolConfig{BaseURL: base, Profile: "chrome_152", Timeout: 10 * time.Second, IdentityRPS: rps, IdentityBurst: 1,
		MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 32}, proxies, fps,
		func(o transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// rotated is the dry run's world: TikTok has started signing the creator
// list too, so the seed's unsigned variant is stale and the variants step
// must find the signed one.
func rotated() fakett.Faults {
	return fakett.Faults{RequireSignature: true, MediaNeedsCookie: true, MaxRPS: 40,
		SignedPaths: []string{"/api/post/item_list/", "/api/creator/item_list/", "/api/comment/list/", "/api/comment/list/reply/"}}
}

func kit(t *testing.T, f fakett.Faults) (*phase0.Kit, *tttest.Env, string) {
	t.Helper()
	env := tttest.New(t, tttest.Options{Faults: f})
	var names []string
	for _, a := range env.World.Accounts() {
		names = append(names, a.Username)
	}
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	base := env.HTTP.URL
	k := &phase0.Kit{
		Stack: phase0.Stack{BaseURL: base, Pool: newPool(t, base, 8), HTTP: http.DefaultClient, Rules: ttdetect.Rules(),
			Loader: &session.Loader{Rules: ttdetect.Rules(), AcceptLanguage: tttest.AcceptLanguage, Hooks: ttclient.Hooks()},
			Opener: opener, Signer: config.Signer{Mode: config.SignerBrowser, Pages: 2, PageMaxSigns: 500, PageMaxAge: time.Hour,
				SignTimeout: 5 * time.Second, PageTimeout: 5 * time.Second},
			LadderPools: func(tier string, n int, rps float64) ([]phase0.LadderIP, error) {
				return []phase0.LadderIP{{Pool: newPool(t, base, 1000), Tier: "direct"}}, nil
			}},
		Cfg: phase0.Config{Out: out, Tiers: []string{"direct"}, Accounts: names, Seed: seed, Parallel: 2, SoftPause: time.Millisecond,
			Run:        phase0.Limits{BackfillDays: 365, MaxListPages: 30, CommentPosts: 3, MaxCommentPages: 10, ReplyThreads: 3, MaxReplyPages: 5, Videos: 2, Replays: 3},
			Smoke:      phase0.Limits{BackfillDays: 365, MaxListPages: 1, CommentPosts: 1, MaxCommentPages: 1, ReplyThreads: 1, MaxReplyPages: 1, Videos: 1},
			ReplayAges: []time.Duration{100 * time.Millisecond, 300 * time.Millisecond}, ReplayWait: time.Second, AcceptLanguage: tttest.AcceptLanguage,
			CookieDomains: []string{"tiktok.com"}, Secrets: []string{"s3cr3t-password"},
			Ladder:     phase0.LadderConfig{Rates: []float64{5, 20, 120}, Step: 1500 * time.Millisecond, IPs: 1, Accounts: 2},
			Model:      phase0.Model{IPMonthUSD: 3, BrowserHourUSD: 0.05, ComputeUSD: 0.0001, StorageGBMonth: 0.02},
			Thresholds: phase0.Thresholds{BudgetUSD: 1}},
	}
	return k, env, out
}

// The kit end to end against fakett with rotated variants: every step
// works, the creator list's signed variant is found, the run reaches GO,
// the ladder finds the fake IP's limit, and the bundle holds no username,
// secUid, link or credential.
func TestDryRunReachesGO(t *testing.T) {
	k, env, out := kit(t, rotated())
	rep, err := k.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Verdict != "GO" {
		t.Fatalf("verdict %s:\n%s", rep.Verdict, phase0.FormatReport(rep))
	}
	t.Log("\n" + phase0.FormatReport(rep))
	var v phase0.VariantsResult
	readJSON(t, filepath.Join(out, phase0.FileVariants), &v)
	if v.Chosen["creator_list"] != "creator-signed" || v.Chosen["comments"] != "web-signed" || v.Chosen["replies"] != "web-signed" {
		t.Fatalf("chosen %v", v.Chosen)
	}
	b, _ := os.ReadFile(filepath.Join(out, phase0.FileRegistry))
	seed, err := registry.ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	if q, _ := registry.NewStatic(seed).Get("creator_list"); q.DocID != "creator-signed" {
		t.Fatalf("registry's creator_list is %s", q.DocID)
	}

	tr := rep.Tiers[0]
	s := tr.Summary
	if s.Accounts != 3 || s.Measured != 3 || s.Drift != 0 || s.Captchas != 0 {
		t.Fatalf("summary %+v", s)
	}
	// fakett's accounts post every 20 h for 60 posts: the window holds
	// them all, 1.2 a day.
	if s.Posts != 180 || s.PostsPerDay.P50 < 1.1 || s.PostsPerDay.P50 > 1.3 {
		t.Fatalf("posts %d, %+v", s.Posts, s.PostsPerDay)
	}
	if c := s.Ops["comments"]; c == nil || c.Signed != c.N || c.SignedOK != c.N || c.N == 0 {
		t.Fatalf("comments %+v", c)
	}
	if l := s.Ops["list_check"]; l == nil || l.Signed != l.N || l.OK != 3 {
		t.Fatalf("list checks %+v", l)
	}
	if s.Ops["replies"] == nil || s.Ops["replies"].OK == 0 || s.CommentComplete == 0 || s.Served.P50 < 0.99 {
		t.Fatalf("replies %+v, reads %d whole %d served %+v", s.Ops["replies"], s.CommentReads, s.CommentComplete, s.Served)
	}
	if r := s.Ops["sig_replay"]; r == nil || r.OK == 0 {
		t.Fatalf("replays %+v", s.Replays)
	}
	if d, p := s.Ops["video_direct"], s.Ops["video_proxy"]; d == nil || p == nil || d.OK != 2 || p.OK != 2 || env.Fake.MediaRefused.Load() != 0 {
		t.Fatalf("videos direct %+v proxy %+v refused %d", d, p, env.Fake.MediaRefused.Load())
	}
	// fakett's limit is 40 a second: 5 and 20 hold, 120 does not.
	steps := s.Ladder["direct/2"]
	if len(steps) == 0 {
		for _, st := range s.Ladder {
			steps = st
		}
	}
	if tol, reached := phase0.Tolerance(steps, 0.01); tol != 20 || reached != 120 {
		t.Fatalf("ladder %+v: tolerance %v reached %v", steps, tol, reached)
	}
	e := tr.Estimate
	if len(e.Sizes) != 4 || e.Sizes[3].Accounts != 300_000 || e.Sizes[3].IPs == 0 || e.Sizes[3].SignerPages == 0 || len(e.Unpriced) != 0 {
		t.Fatalf("estimate %+v", e)
	}
	if e.Day.CommentCalls <= 0 || e.Day.ListCalls < e.Day.Checks || e.Day.RPSPerIP != 20 {
		t.Fatalf("day %+v", e.Day)
	}

	// The files are private and the bundle is safe to send.
	for _, f := range []string{phase0.FileReport, phase0.FileRegistry, phase0.FileBundle, phase0.RunFile("direct")} {
		st, err := os.Stat(filepath.Join(out, f))
		if err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v %v", f, err, st)
		}
	}
	files := untar(t, filepath.Join(out, phase0.FileBundle))
	for _, want := range []string{"run-direct.jsonl", "ladder-direct.jsonl", "report.txt", "variants.json", "registry.yaml", "meta.json", "preflight.json"} {
		if _, ok := files["tks-phase0/"+want]; !ok {
			t.Fatalf("bundle lacks %s: %v", want, keys(files))
		}
	}
	for name, b := range files {
		for _, a := range env.World.Accounts() {
			for _, secret := range []string{a.Username, a.SecUID, a.Nickname} {
				if bytes.Contains(b, []byte(secret)) {
					t.Fatalf("%s holds %q", name, secret)
				}
			}
		}
		for _, bad := range []string{"http://", "https://", "s3cr3t-password"} {
			if bytes.Contains(b, []byte(bad)) {
				t.Fatalf("%s holds %q", name, bad)
			}
		}
	}

	// Resuming at the run measures nothing again and reports the same.
	k2, _, _ := kit(t, rotated())
	k2.Cfg.Out, k2.Cfg.From, k2.Cfg.Accounts = out, phase0.StepRun, k.Cfg.Accounts
	k2.Stack.LadderPools = nil
	before, _ := os.ReadFile(filepath.Join(out, phase0.RunFile("direct")))
	rep2, err := k2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(out, phase0.RunFile("direct")))
	if !bytes.Equal(before, after) || rep2.Verdict != "GO" {
		t.Fatalf("resume changed the run (%d -> %d bytes) or the verdict %s", len(before), len(after), rep2.Verdict)
	}
}

// A challenge on every page stops the kit at preflight.
func TestPreflightStopsOnChallenge(t *testing.T) {
	f := rotated()
	f.Challenge = true
	k, _, out := kit(t, f)
	_, err := k.Run(context.Background())
	var se *phase0.StepError
	if !errors.As(err, &se) || se.Step != phase0.StepPreflight {
		t.Fatalf("err %v", err)
	}
	var pf phase0.PreflightResult
	readJSON(t, filepath.Join(out, phase0.FilePreflight), &pf)
	if pf.OK || pf.Tiers["direct"] != "page_challenge" {
		t.Fatalf("preflight %+v", pf)
	}
}

// Comments not served to guests (T12) stop the kit at the variants step
// with the bundle written.
func TestVariantsNeedAction(t *testing.T) {
	f := rotated()
	f.NoGuestComments = true
	k, _, out := kit(t, f)
	_, err := k.Run(context.Background())
	var se *phase0.StepError
	if !errors.As(err, &se) || se.Step != phase0.StepVariants || !se.NeedsAction || !strings.Contains(err.Error(), "comments") {
		t.Fatalf("err %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, phase0.FileBundle)); err != nil {
		t.Fatal("no bundle:", err)
	}
}

// Captchas in the run stop it at the cap and give NO-GO.
func TestCaptchasGiveNoGo(t *testing.T) {
	recs := []phase0.Record{}
	for i := range 200 {
		c := "ok"
		if i%20 == 0 {
			c = "checkpoint"
		}
		recs = append(recs, phase0.Record{Op: phase0.OpListCheck, Class: c, Wire: 40000, Exit: "isp/1"})
	}
	rep := phase0.BuildReport(map[string][]phase0.Record{"isp": recs}, phase0.ReportOptions{})
	if rep.Verdict != "NO-GO" {
		t.Fatal(rep.Verdict)
	}
	found := false
	for _, c := range rep.Tiers[0].Checks {
		if c.Name == "captchas and challenges" {
			found = c.Status == phase0.Fail
		}
	}
	if !found {
		t.Fatalf("checks %+v", rep.Tiers[0].Checks)
	}
}

func TestTolerance(t *testing.T) {
	steps := []phase0.LadderStep{{Rate: 1, Calls: 600, Blocks: 0}, {Rate: 0.33, Calls: 200}, {Rate: 2, Calls: 1200, Blocks: 30}, {Rate: 4, Calls: 10, Blocks: 10}}
	if r, reached := phase0.Tolerance(steps, 0.01); r != 1 || reached != 2 {
		t.Fatalf("tolerance %v reached %v", r, reached)
	}
	if r, _ := phase0.Tolerance([]phase0.LadderStep{{Rate: 0.33, Calls: 100, Blocks: 5}}, 0.01); r != 0 {
		t.Fatal(r)
	}
}

// The model's arithmetic on a known day: an account posting 2 a day with
// 40 comments a post.
func TestEstimate(t *testing.T) {
	s := phase0.Summary{Posts: 100, VideoPosts: 100, CommentCount: 4000, CommentPagesEst: 200, PagesFactor: 1.5,
		PostsPerDay: phase0.Dist{N: 1, Mean: 2}, Ops: map[string]*phase0.OpStats{
			"list_check": {Wire: phase0.Dist{N: 1, Mean: 40000}},
			"comments":   {Wire: phase0.Dist{N: 1, Mean: 20000}, SignMs: phase0.Dist{N: 1, Mean: 500}},
			"page":       {Wire: phase0.Dist{N: 1, Mean: 100000}},
		}}
	m := phase0.Model{CommentWindows: []phase0.Window{{MaxAge: 48 * time.Hour, Interval: 6 * time.Hour}}, MetricsWalks: []phase0.Walk{},
		IPMonthUSD: 3, Sizes: []int{1000}}
	e := m.Estimate(s, 1, 0)
	d := e.Day
	checks := 24 / 2.75
	if d.Checks < checks-0.01 || d.Checks > checks+0.01 {
		t.Fatal(d.Checks)
	}
	// 2 posts × 2 pages × 1.5 × 8 reads.
	if d.CommentCalls != 48 {
		t.Fatal(d.CommentCalls)
	}
	if d.BrowserSecs != 24 {
		t.Fatal(d.BrowserSecs)
	}
	z := e.Sizes[0]
	if z.IPs != 1 || z.ProxyUSD != 0.1 || len(e.Unpriced) != 3 {
		t.Fatalf("%+v %v", z, e.Unpriced)
	}
}

func TestCandidatesAndChoose(t *testing.T) {
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, q := range phase0.Candidates(seed, "comments") {
		got = append(got, q.DocID)
	}
	if strings.Join(got, ",") != "web-signed,web" {
		t.Fatal(got)
	}
	chosen := phase0.Choose(seed, map[string]string{"creator_list": "creator-signed", "comments": "web-signed"})
	r := registry.NewStatic(chosen)
	if q, _ := r.Get("creator_list"); q.DocID != "creator-signed" {
		t.Fatal(q.DocID)
	}
	if q, ok := r.Fallback("creator_list", "creator-signed"); !ok || q.DocID != "creator" {
		t.Fatal("no unsigned fallback", q)
	}
	if q, _ := r.Get("comments"); q.DocID != "web-signed" {
		t.Fatal(q.DocID)
	}
}

func TestCleanAndRecords(t *testing.T) {
	if got := phase0.Clean(`Get "https://u:p@www.tiktok.com/api/x?secUid=MS4w": EOF`); strings.Contains(got, "MS4w") || strings.Contains(got, "u:p") {
		t.Fatal(got)
	}
	var b bytes.Buffer
	rc := phase0.NewRecorder(&b, "run", "isp")
	rc.Add(phase0.Record{Op: "page", Class: "ok", Reason: "see http://x/y"})
	b.WriteString(`{"op":"torn`)
	recs, err := phase0.ReadRecords(&b)
	if err != nil || len(recs) != 1 || recs[0].Step != "run" || recs[0].Tier != "isp" || recs[0].Reason != "see <url>" {
		t.Fatalf("%v %+v", err, recs)
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsonUnmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func untar(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	out := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		out[h.Name] = b
	}
}

func keys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// TikTok refusing some signed calls (C4's third attempt: about one in nine
// comment pages signed on a page opened for another account): the client
// signs each refused call once more on another page, the report counts the
// retries, and the run still reaches GO.
func TestRefusedSignedCallsAreRetried(t *testing.T) {
	f := rotated()
	f.SignerRefuseEvery = 7
	k, _, _ := kit(t, f)
	rep, err := k.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	txt := phase0.FormatReport(rep)
	c := rep.Tiers[0].Summary.Ops["comments"]
	if rep.Verdict != "GO" || c == nil || c.SignRetried == 0 || c.SignRetriedOK != c.SignRetried || !strings.Contains(txt, "sent again on another page") {
		t.Fatalf("verdict %s, comments %+v:\n%s", rep.Verdict, c, txt)
	}
}
