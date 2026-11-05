package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discprobe"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// DiscoverDryRunFaults is the discovery dry run's fake TikTok: search and
// the feed signed (as the owner's recording shows), comments too, and
// lists that ask to log in after three pages of search.
func DiscoverDryRunFaults() fakett.Faults {
	return fakett.Faults{RequireSignature: true, LoginAfter: 36,
		SignedPaths: []string{"/api/search/general/full/", "/api/recommend/item_list/", "/api/comment/list/", "/api/comment/list/reply/"}}
}

func runDiscover(args []string) int {
	fs := flag.NewFlagSet("tks-spike discover", flag.ExitOnError)
	accountsFile := fs.String("accounts", "", "your accounts file (one public account per line): bootstrap pages, the follow lists' accounts")
	cfgPath := fs.String("config", os.Getenv("TKS_CONFIG"), "config file with proxy tiers under fb.proxy (phase0/proxies.yaml)")
	tier := fs.String("tier", "isp", "proxy tier (direct = this machine's own IP)")
	outDir := fs.String("out-dir", "phase0/out-tt-discover", "output directory")
	keywords := fs.Int("keywords", 16, "keywords of the Iraq list searched, spread over its groups and languages")
	keywordFile := fs.String("keyword-file", "", "search these keywords instead (one per line)")
	sources := fs.String("sources", "", "sources to run, comma-separated (default all: "+strings.Join(discprobe.Sources(), ", ")+")")
	knownPosts := fs.String("known-posts", "", "an earlier probe's posts.csv (the full pass's): its posts no longer listed are read (T34)")
	gonePosts := fs.String("gone-posts", "", "file of links to posts you know were deleted or made private (T34)")
	shortLinks := fs.String("short-links", "", "file of short links (vm.tiktok.com/...) to resolve")
	signer := fs.String("signer", "browser", `"browser" signs in headless Chromium as the crawler does and captures the pages' own calls; "none" does neither`)
	signerPages := fs.Int("signer-pages", 2, "signer pages open at once")
	compare := fs.Bool("compare-sign-pages", true, "also sign each signed call's first items on a page opened on the call's own page")
	rps := fs.Float64("rps", 0, "requests per second per identity (default tt.identity_rps: the crawler's pace)")
	fingerprint := fs.String("fingerprint", "", "browser fingerprint profile (default tt.fingerprint_profile)")
	maxCaptchas := fs.Int("max-captchas", 5, "stop after this many captchas, so the exits are not burnt")
	reportOnly := fs.Bool("report-only", false, "rebuild the report and the bundle from --out-dir's files")
	base := fs.String("base", "", "TikTok base URL (default tt.base_url)")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake TikTok (no traffic, no proxies)")
	lim := discprobe.DefaultLimits()
	fs.IntVar(&lim.SearchPages, "search-pages", lim.SearchPages, "search pages per keyword (12 results each)")
	fs.IntVar(&lim.DeepPages, "deep-pages", lim.DeepPages, "search pages for the first keyword: how deep TikTok serves")
	fs.IntVar(&lim.UserKeywords, "user-keywords", lim.UserKeywords, "keywords searched for accounts")
	fs.IntVar(&lim.UserPages, "user-pages", lim.UserPages, "account search pages per keyword")
	fs.IntVar(&lim.Hashtags, "hashtags", lim.Hashtags, "hashtags read (half made of keywords, half found in results)")
	fs.IntVar(&lim.HashtagPages, "hashtag-pages", lim.HashtagPages, "list pages per hashtag")
	fs.IntVar(&lim.Sounds, "sounds", lim.Sounds, "sounds read (the ones found posts use most)")
	fs.IntVar(&lim.SoundPages, "sound-pages", lim.SoundPages, "list pages per sound")
	fs.IntVar(&lim.FeedReads, "feed-reads", lim.FeedReads, "reads of the For You feed")
	fs.IntVar(&lim.Related, "related", lim.Related, "found posts whose related posts are read")
	fs.IntVar(&lim.FollowAccounts, "follow-accounts", lim.FollowAccounts, "your accounts whose following, followers and reposts are read")
	fs.IntVar(&lim.FollowPages, "follow-pages", lim.FollowPages, "pages per following or followers list")
	fs.IntVar(&lim.Entry, "entry", lim.Entry, "found authors whose list's first page is read (where they post from)")
	fs.IntVar(&lim.VanishedAccounts, "vanished-accounts", lim.VanishedAccounts, "accounts of --known-posts whose lists are compared")
	fs.IntVar(&lim.VanishedPages, "vanished-pages", lim.VanishedPages, "post pages read for posts no longer listed")
	fs.IntVar(&lim.PageScrolls, "page-scrolls", lim.PageScrolls, "scrolls of each page in the browser")
	_ = fs.Parse(args)
	lim.Keywords = *keywords

	var cfg *config.Config
	var err error
	if *dryRun {
		cfg, err = config.Load("")
	} else {
		cfg, err = config.Load(*cfgPath)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "discover: config:", err)
		return 1
	}
	if *base == "" {
		*base = cfg.TT.BaseURL
	}
	if *fingerprint == "" {
		*fingerprint = cfg.TT.FingerprintProfile
	}
	if *rps <= 0 {
		*rps = cfg.TT.IdentityRPS
	}
	var kws []discprobe.Keyword
	if *keywordFile != "" {
		lines, err := readLines(*keywordFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "discover:", err)
			return 1
		}
		for _, l := range lines {
			kws = append(kws, discprobe.Keyword{Term: l, Language: "file", Group: "file"})
		}
		lim.Keywords = len(kws)
	} else {
		seed, err := discovery.IraqSeed()
		if err != nil {
			fmt.Fprintln(os.Stderr, "discover:", err)
			return 1
		}
		kws = discprobe.PickKeywords(seed, *keywords)
	}
	dc := discprobe.Config{Out: *outDir, Tier: *tier, Keywords: kws, Sources: splitList(*sources), Limits: lim, CompareSignPages: *compare,
		MaxCaptchas: *maxCaptchas, AcceptLanguage: cfg.TT.AcceptLanguage, ReportOnly: *reportOnly, Secrets: proxySecrets(cfg.FB.Proxy), Version: version}
	if *dryRun {
		w := fakett.NewWorld(fakett.DefaultSpec())
		hs := httptest.NewServer(fakett.NewServer(w, fakett.Options{Faults: DiscoverDryRunFaults()}))
		defer hs.Close()
		*base, *tier, *rps, dc.Tier = hs.URL, "direct", 20, "direct"
		dc.Accounts, dc.Known, dc.Gone, dc.ShortLinks = dryRunWorld(w, hs.URL)
		dc.Keywords = append([]discprobe.Keyword{{Term: "iraq", Language: "en", Group: "iraq"}}, kws...)
		cfg.FB.Proxy = config.Proxy{}
		if *outDir == "phase0/out-tt-discover" {
			dc.Out = "phase0/out-tt-discover-dryrun"
		}
	} else {
		if *accountsFile == "" && !*reportOnly {
			fmt.Fprintln(os.Stderr, "discover: --accounts is required (or --dry-run)")
			return 2
		}
		if *accountsFile != "" {
			if dc.Accounts, err = readAccounts(*accountsFile); err != nil {
				fmt.Fprintln(os.Stderr, "discover:", err)
				return 1
			}
		}
		if *knownPosts != "" {
			if dc.Known, err = readKnownPosts(*knownPosts); err != nil {
				fmt.Fprintln(os.Stderr, "discover:", err)
				return 1
			}
		}
		for _, f := range []struct {
			path string
			into *[]string
		}{{*gonePosts, &dc.Gone}, {*shortLinks, &dc.ShortLinks}} {
			if f.path == "" {
				continue
			}
			if *f.into, err = readLines(f.path); err != nil {
				fmt.Fprintln(os.Stderr, "discover:", err)
				return 1
			}
		}
	}
	stack := discprobe.Stack{BaseURL: *base, Rules: ttdetect.Rules(), Signer: cfg.TT.Signer,
		Loader: &session.Loader{Rules: ttdetect.Rules(), AcceptLanguage: cfg.TT.AcceptLanguage, Hooks: ttclient.Hooks()}}
	stack.Signer.Pages = *signerPages
	if *signer == "browser" && !*reportOnly {
		bin, err := browser.FindBinary("")
		switch {
		case err == nil:
			noSandbox := os.Getenv("TKS_BROWSER_NO_SANDBOX") != "" || os.Geteuid() == 0
			stack.Opener = sign.BrowserOpener(sign.BrowserConfig{Binary: bin, NoSandbox: noSandbox, AcceptLanguage: cfg.TT.AcceptLanguage,
				CallTimeout: 30 * time.Second, Freeze: cfg.TT.Signer.Freeze, Lite: cfg.TT.Signer.Lite})
			stack.Browser = pageBrowser(bin, noSandbox, cfg.TT.AcceptLanguage, cfg.TT.Signer.Lite)
		case *dryRun:
			fmt.Fprintln(os.Stderr, "discover: no Chromium: the dry run signs as fakett's page script does and captures no pages")
			stack.Opener = func(context.Context, *session.Identity, string) (sign.Page, error) { return simulatedPage{}, nil }
		default:
			fmt.Fprintln(os.Stderr, "discover: --signer browser:", err, "(run: make tks-browser-check)")
			return 1
		}
	}
	if !*reportOnly {
		if stack.Pool, err = identityPool(cfg.FB.Proxy, *base, *fingerprint, *rps, 8); err != nil {
			fmt.Fprintln(os.Stderr, "discover:", err)
			return 1
		}
	}
	k := &discprobe.Kit{Stack: stack, Cfg: dc, Log: os.Stderr}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "discover: %d keywords, %d accounts, tier %s, %.2f requests/s per identity, out %s; Ctrl-C stops and writes the report\n",
		len(dc.Keywords), len(dc.Accounts), dc.Tier, *rps, dc.Out)
	if _, err := k.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "\ndiscover:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "\ndiscover: done. Put %s/%s in your Drive folder and paste its link into the build session (it holds TikTok's answers: never by chat or e-mail).\n",
		dc.Out, discprobe.FileBundle)
	return 0
}

// pageBrowser opens the page captures' browser on the identity's exit, as
// the signer's pages open (no automation flag, media blocked).
func pageBrowser(bin string, noSandbox bool, acceptLanguage string, lite bool) discprobe.Browser {
	return func(ctx context.Context, id *session.Identity, pageURL string) (discprobe.Navigator, error) {
		var proxyURL *url.URL
		if id.Endpoint.URL != "" {
			u, err := url.Parse(id.Endpoint.URL)
			if err != nil {
				return nil, fmt.Errorf("exit: %w", err)
			}
			proxyURL = u
		}
		return browser.OpenSession(ctx, bin, browser.Options{URL: pageURL, Proxy: proxyURL, UserAgent: id.FP.UserAgent, AcceptLanguage: acceptLanguage,
			Hints: browser.HintsFrom(id.FP.SecChUa, id.FP.SecChUaPlatform, id.FP.SecChUaMobile), BlockMedia: true, HideAutomation: true,
			NoSandbox: noSandbox, MaxSettle: 15 * time.Second, CallTimeout: 30 * time.Second, Lite: lite})
	}
}

// dryRunWorld adds to the fake world what the dry run reads: an account
// that posts from Iraq, a post made private and one deleted since an
// "earlier probe" listed them, and a short link.
func dryRunWorld(w *fakett.World, base string) (accounts []string, known []discprobe.KnownPost, gone, short []string) {
	for _, a := range w.Accounts() {
		accounts = append(accounts, a.Username)
	}
	found := w.AddAccount("found_iraqi", "", 5, time.Hour, 24*time.Hour)
	w.Mutate(func() {
		for _, p := range found.Posts {
			p.Desc, p.Hashtags, p.Region = "from Baghdad #iraq", []string{"iraq"}, "IQ"
		}
	})
	acc := w.Accounts()[0]
	for _, p := range acc.Posts[:20] {
		known = append(known, discprobe.KnownPost{Username: acc.Username, ID: strconv.FormatInt(p.ID, 10), Created: p.Created})
	}
	w.HidePost(acc.Posts[4], 10216)
	deleted := acc.Posts[6]
	w.DeletePost(deleted)
	gone = []string{base + "/@" + acc.Username + "/video/" + strconv.FormatInt(deleted.ID, 10)}
	short = []string{base + fakett.ShortLink(w.Accounts()[1].Posts[0])}
	return accounts, known, gone, short
}

// readKnownPosts reads a probe's posts.csv: username, id and posted_at.
func readKnownPosts(path string) ([]discprobe.KnownPost, error) {
	f, err := os.Open(path) //nolint:gosec // the owner's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	head, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	col := map[string]int{}
	for i, h := range head {
		col[h] = i
	}
	for _, need := range []string{"username", "id", "posted_at"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("%s: no %s column (expected a probe's posts.csv)", path, need)
		}
	}
	var out []discprobe.KnownPost
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		t, err := time.Parse(time.RFC3339, row[col["posted_at"]])
		if err != nil || row[col["id"]] == "" {
			continue
		}
		out = append(out, discprobe.KnownPost{Username: row[col["username"]], ID: row[col["id"]], Created: t})
	}
}

// readLines reads a file's non-empty lines, without # comments.
func readLines(path string) ([]string, error) {
	f, err := os.Open(path) //nolint:gosec // the owner's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}
