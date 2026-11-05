package discprobe_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discprobe"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

type page struct{}

func (page) Sign(_ context.Context, raw string) (string, error) { return fakett.Sign(raw) }
func (page) Close()                                             {}

func opener(context.Context, *session.Identity, string) (sign.Page, error) { return page{}, nil }

func pool(t *testing.T, base string) *session.Pool {
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
	p, err := session.NewPool(session.PoolConfig{BaseURL: base, Profile: "chrome_152", Timeout: 10 * time.Second, IdentityRPS: 1000, IdentityBurst: 10,
		MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 16}, proxies, fps,
		func(o transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// world is fakett as the recording shows TikTok: search and the feed
// signed (and comments), lists that stop after three pages of search, a
// found account posting from Iraq, a post made private and one deleted
// since an earlier probe listed them.
type world struct {
	env             *tttest.Env
	names           []string
	known           []discprobe.KnownPost
	gone, short     []string
	hidden, deleted *fakett.Post
	found           *fakett.Account
}

func newWorld(t *testing.T, f fakett.Faults) *world {
	t.Helper()
	env := tttest.New(t, tttest.Options{Faults: f})
	w := &world{env: env}
	for _, a := range env.World.Accounts() {
		w.names = append(w.names, a.Username)
	}
	w.found = env.World.AddAccount("found_iraqi", "", 5, time.Hour, 24*time.Hour)
	env.World.Mutate(func() {
		for _, p := range w.found.Posts {
			p.Desc, p.Hashtags, p.Region = "from Baghdad #iraq", []string{"iraq"}, "IQ"
		}
	})
	acc := env.World.Accounts()[0]
	for _, p := range acc.Posts[:20] {
		w.known = append(w.known, discprobe.KnownPost{Username: acc.Username, ID: strconv.FormatInt(p.ID, 10), Created: p.Created})
	}
	w.hidden, w.deleted = acc.Posts[4], acc.Posts[6]
	env.World.HidePost(w.hidden, 10216)
	env.World.DeletePost(w.deleted)
	other := env.World.Accounts()[1]
	w.gone = []string{env.HTTP.URL + "/@" + acc.Username + "/video/" + strconv.FormatInt(w.deleted.ID, 10)}
	w.short = []string{env.HTTP.URL + fakett.ShortLink(other.Posts[0])}
	return w
}

func signed() fakett.Faults {
	return fakett.Faults{RequireSignature: true, LoginAfter: 36,
		SignedPaths: []string{"/api/search/general/full/", "/api/recommend/item_list/", "/api/comment/list/", "/api/comment/list/reply/"}}
}

func newKit(t *testing.T, w *world, mod func(*discprobe.Kit)) (*discprobe.Kit, string) {
	t.Helper()
	out := t.TempDir()
	base := w.env.HTTP.URL
	lim := discprobe.DefaultLimits()
	lim.Keywords, lim.DeepPages, lim.VanishedAccounts = 3, 10, 5
	k := &discprobe.Kit{
		Stack: discprobe.Stack{BaseURL: base, Pool: pool(t, base), Rules: ttdetect.Rules(),
			Loader: &session.Loader{Rules: ttdetect.Rules(), AcceptLanguage: tttest.AcceptLanguage, Hooks: ttclient.Hooks()},
			Opener: opener, Signer: config.Signer{Mode: config.SignerBrowser, Pages: 2, PageMaxSigns: 500, PageMaxAge: time.Hour,
				SignTimeout: 5 * time.Second, PageTimeout: 5 * time.Second, OpenOn: config.SignerOpenOnProfile}},
		Cfg: discprobe.Config{Out: out, Tier: "direct", Accounts: w.names, Limits: lim, AcceptLanguage: tttest.AcceptLanguage,
			Keywords: []discprobe.Keyword{{Term: "iraq", Language: "en", Group: "iraq"}, {Term: "Baghdad", Language: "en", Group: "baghdad"},
				{Term: "nothing here", Language: "en", Group: "none"}},
			Known: w.known, Gone: w.gone, ShortLinks: w.short, CompareSignPages: true, Secrets: []string{"s3cr3t-password"}, Version: "test"},
	}
	if mod != nil {
		mod(k)
	}
	return k, out
}

func summaries(t *testing.T, out, src string) []discprobe.Record {
	t.Helper()
	recs, err := discprobe.ReadCalls(filepath.Join(out, discprobe.FileCalls))
	if err != nil {
		t.Fatal(err)
	}
	var s []discprobe.Record
	for _, r := range recs {
		if r.Source == src && r.Summary {
			s = append(s, r)
		}
	}
	return s
}

// Every source against fakett: search answers only signed, walks to the
// login wall; the feed brings new posts each read; hashtags, sounds,
// related posts, following and followers lists and reposts are read; the
// entry sample finds the author posting from Iraq; T34's candidate pages
// give 10216 and the gone post's 10204; the short link's shape is kept
// without its id; and the bundle's summary files hold no username.
func TestProbeAgainstFakett(t *testing.T) {
	w := newWorld(t, signed())
	k, out := newKit(t, w, nil)
	rep, err := k.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	txt := discprobe.FormatReport(rep)
	t.Log("\n" + txt)

	var unsigned, profile, source *discprobe.VariantStat
	for i, v := range rep.Variants {
		if v.Op != discprobe.OpSearch {
			continue
		}
		switch {
		case !v.Signed:
			unsigned = &rep.Variants[i]
		case v.SignPage == discprobe.SignOnSource:
			source = &rep.Variants[i]
		default:
			profile = &rep.Variants[i]
		}
	}
	if unsigned == nil || unsigned.OK != 0 || profile == nil || profile.WithResults == 0 || source == nil || source.OK == 0 {
		t.Fatalf("search variants: unsigned %+v profile %+v source %+v", unsigned, profile, source)
	}
	if !strings.Contains(strings.Join(profile.Shapes, ","), "data.item") {
		t.Fatalf("search shape %v", profile.Shapes)
	}
	s := summaries(t, out, discprobe.SrcSearch)
	if len(s) != 3 || !strings.HasSuffix(s[0].Stop, strconv.Itoa(fakett.LoginCode)) || s[0].Pages != 3 || s[2].Stop != "empty" {
		t.Fatalf("search walks %+v", s)
	}
	if feed := summaries(t, out, discprobe.SrcFeed); len(feed) != k.Cfg.Limits.FeedReads || feed[1].NewPosts == 0 {
		t.Fatalf("feed %+v", feed)
	}
	for _, src := range []string{discprobe.SrcHashtag, discprobe.SrcSound, discprobe.SrcRelated, discprobe.SrcFollowing,
		discprobe.SrcFollowers, discprobe.SrcReposts, discprobe.SrcSearchUser} {
		st := rep.Sources[src]
		if st == nil || st.Posts+st.Users == 0 {
			t.Fatalf("%s: %+v", src, st)
		}
	}
	if e := rep.Sources[discprobe.SrcEntry]; e == nil || e.Notes["authors posting from IQ"] != 1 || e.Locations["IQ"] != 5 {
		t.Fatalf("entry %+v", e)
	}
	v := rep.Sources[discprobe.SrcVanished]
	if v.Codes["post_page:10216 (candidate)"] != 1 || v.Codes["post_page:10204 (candidate)"] != 1 || v.Codes["post_page:10204 (owner_gone)"] != 1 ||
		v.Notes["gone post still listed=false"] != 1 {
		t.Fatalf("vanished %+v", v)
	}
	if !strings.Contains(txt, "short link 301 -> 127.0.0.1/@{user}/video/{id}?_r&_t") {
		t.Fatalf("short links: %+v", rep.Sources[discprobe.SrcShortLinks])
	}
	if rep.Regions["IQ"] == 0 || rep.Captchas != 0 {
		t.Fatalf("regions %v captchas %d", rep.Regions, rep.Captchas)
	}
	if len(rep.Issues) != 1 || !strings.Contains(rep.Issues[0], "no browser") {
		t.Fatalf("issues %v", rep.Issues)
	}

	files := untar(t, filepath.Join(out, discprobe.FileBundle))
	raw := 0
	for name, b := range files {
		if strings.HasPrefix(name, "tks-discover/raw/") {
			raw++
			continue
		}
		for _, a := range w.env.World.Accounts() {
			for _, secret := range []string{a.Username, a.SecUID, a.Nickname} {
				if bytes.Contains(b, []byte(secret)) {
					t.Fatalf("%s holds %q", name, secret)
				}
			}
		}
		for _, bad := range []string{"found_iraqi", w.found.SecUID, strconv.FormatInt(w.deleted.ID, 10), "http://"} {
			if bytes.Contains(b, []byte(bad)) {
				t.Fatalf("%s holds %q", name, bad)
			}
		}
	}
	if raw == 0 || files["tks-discover/report.txt"] == nil || files["tks-discover/calls.jsonl"] == nil {
		t.Fatalf("bundle: %d raw files, %d in all", raw, len(files))
	}
	st, err := os.Stat(filepath.Join(out, discprobe.FileCalls))
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("calls file %v %v", err, st)
	}

	// The report again from the files alone.
	k2, _ := newKit(t, w, func(k *discprobe.Kit) { k.Cfg.Out, k.Cfg.ReportOnly = out, true })
	rep2, err := k2.Run(context.Background())
	if err != nil || rep2.Requests != rep.Requests {
		t.Fatalf("report only: %v, %d requests (was %d)", err, rep2.Requests, rep.Requests)
	}
}

// Logged out, the follow lists may need a login (discovery-plan §9): the
// probe records TikTok's code and the walk stops there; without a signer,
// search's signed variant is refused and the source says so.
func TestFollowListsAndNoSigner(t *testing.T) {
	f := signed()
	f.FollowListsNeedLogin = true
	w := newWorld(t, f)
	k, out := newKit(t, w, func(k *discprobe.Kit) {
		k.Stack.Opener, k.Cfg.Sources = nil, []string{discprobe.SrcSearch, discprobe.SrcFollowers}
	})
	rep, err := k.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range summaries(t, out, discprobe.SrcFollowers) {
		if (i < 2 && !strings.HasSuffix(s.Stop, strconv.Itoa(fakett.LoginCode))) || (i >= 2 && s.Stop != "gave_up") {
			t.Fatalf("followers %+v", s)
		}
	}
	if st := rep.Sources[discprobe.SrcSearch]; st.OK != 0 {
		t.Fatalf("search answered unsigned: %+v", st)
	}
	if !strings.Contains(strings.Join(rep.Issues, ";"), "search_general answered no variant") {
		t.Fatalf("issues %v", rep.Issues)
	}
	if rep.Sources[discprobe.SrcFeed] != nil {
		t.Fatal("feed ran though not asked")
	}
}

// The probe stops at the captcha cap.
func TestCaptchaCap(t *testing.T) {
	f := signed()
	f.CaptchaAfter = 3
	w := newWorld(t, f)
	k, _ := newKit(t, w, func(k *discprobe.Kit) { k.Cfg.MaxCaptchas = 2 })
	rep, err := k.Run(context.Background())
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(rep.Issues, ";"), "captchas") || rep.Requests > 60 {
		t.Fatalf("issues %v, %d requests", rep.Issues, rep.Requests)
	}
}

func TestPickKeywords(t *testing.T) {
	seed, err := discovery.IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	ks := discprobe.PickKeywords(seed, 16)
	if len(ks) != 16 {
		t.Fatalf("%d keywords", len(ks))
	}
	langs, groups := map[string]int{}, map[string]bool{}
	for _, k := range ks {
		langs[k.Language]++
		groups[k.Group] = true
	}
	if langs["ar"] == 0 || langs["en"] == 0 || langs["ckb"] == 0 || len(groups) < 10 || ks[0].Group != "iraq" || ks[0].Language != "ar" {
		t.Fatalf("keywords %+v", ks)
	}
}

func TestPattern(t *testing.T) {
	for _, c := range [][3]string{
		{"https://vm.tiktok.com/ZMabc/", "https://www.tiktok.com/@some.one/video/7400000000000000001?_r=1&_t=x", "www.tiktok.com/@{user}/video/{id}?_r&_t"},
		{"https://vm.tiktok.com/ZMabc/", "/@x/photo/7400000000000000001", "vm.tiktok.com/@{user}/photo/{id}"},
		{"https://vm.tiktok.com/ZMabc/", "", ""},
	} {
		if got := discprobe.Pattern(c[0], c[1]); got != c[2] {
			t.Errorf("Pattern(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}

// The page captures in Chromium against fakett: each page's own API calls
// are kept with their parameters' names.
func TestPageCapturesWithChromium(t *testing.T) {
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip("no Chromium:", err)
	}
	w := newWorld(t, signed())
	k, _ := newKit(t, w, func(k *discprobe.Kit) {
		k.Cfg.Sources = []string{discprobe.SrcSearch, discprobe.SrcPages}
		k.Cfg.Limits.PageScrolls = 1
		k.Stack.Browser = func(ctx context.Context, id *session.Identity, url string) (discprobe.Navigator, error) {
			return browser.OpenSession(ctx, bin, browser.Options{URL: url, UserAgent: id.FP.UserAgent, NoSandbox: true, BlockMedia: true,
				HideAutomation: true, MaxSettle: 5 * time.Second, CallTimeout: 10 * time.Second})
		}
	})
	rep, err := k.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var profile *discprobe.PageCapture
	for i, p := range rep.Pages {
		if p.Page == "profile" {
			profile = &rep.Pages[i]
		}
	}
	if len(rep.Pages) < 5 || profile == nil || len(profile.Calls) == 0 || profile.Calls[0].Path != "/api/post/item_list/" {
		t.Fatalf("pages %+v", rep.Pages)
	}
	if v := profile.Calls[0].Values; v["count"] != "16" || v["secUid"] != "" {
		t.Fatalf("values %v", v)
	}
}

func untar(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
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
