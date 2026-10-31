// Command tks-spike holds the tools the owner runs against real TikTok
// from the Phase 0 droplet (docs/runbooks/tks-probe.md). Today: probe
// (checkpoints C2 and C3), which answers the open questions of
// docs/tiktok/open-questions.md for a list of public accounts and writes
// what it read: the accounts, their posts, comments and replies, avatars,
// covers and sampled videos, as JSON, CSV and a .tar.gz to download.
//
//	tks-spike probe --dry-run                                     # rehearse against fakett
//	tks-spike probe --accounts phase0/tt-accounts.txt --config phase0/proxies.yaml --tier isp
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/probe"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "probe":
		os.Exit(runProbe(os.Args[2:]))
	case "browser-check":
		os.Exit(browserCheck())
	case "version", "--version":
		fmt.Println(version)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: tks-spike probe --accounts FILE [--config FILE] [--tier NAME] [--out-dir DIR] [flags]
       tks-spike probe --dry-run
       tks-spike browser-check

probe loads each account's page and reads it with the logged-out web API,
pacing like a person (one request every few seconds). It writes report.json,
accounts.csv, posts.csv, comments.csv, images/ and videos/ to --out-dir, and
the same as a .tar.gz beside it. The files hold personal data: keep them off
git. --dry-run rehearses against a built-in fake TikTok, with no traffic.
browser-check reports whether a Chromium for --signer browser is installed.
`)
	os.Exit(2)
}

const acceptLanguage = "en-US,en;q=0.9,ar;q=0.8"

func runProbe(args []string) int {
	fs := flag.NewFlagSet("tks-spike probe", flag.ExitOnError)
	accountsFile := fs.String("accounts", "", "file with one public account per line (username, @username or profile link); # starts a comment")
	cfgPath := fs.String("config", os.Getenv("TKS_CONFIG"), "config file with proxy tiers under fb.proxy (phase0/proxies.yaml)")
	tier := fs.String("tier", "direct", "proxy tier to use (direct = this machine's own IP)")
	outDir := fs.String("out-dir", "", "output directory (default phase0/out-tt/<tier>-<time>)")
	rps := fs.Float64("rps", 0.3, "requests per second per identity (0.3 = one every ~3.3 s)")
	maxPages := fs.Int("max-list-pages", 40, "post-list pages to read per account after the first (15 posts each)")
	maxAge := fs.Int("max-age-days", 0, "stop an account's list once its posts are older than this many days (0 = no limit; the backfill window is 365)")
	maxComments := fs.Int("max-comment-pages", 5, "comment pages to read per post (20 comments each)")
	countChecks := fs.Int("count-checks", 5, "how many accounts get the page-size check (count 5 and 30)")
	covers := fs.Int("covers", 12, "covers saved per account (0 = none)")
	videos := fs.Int("videos", 0, "video files downloaded per account (0 = none)")
	signer := fs.String("signer", "none", `"browser" repeats the main calls from inside a headless Chromium page (T7, T27); "none" skips it`)
	oembed := fs.Bool("oembed", false, "also try oEmbed and the creator embed (T26)")
	maxCaptchas := fs.Int("max-captchas", 10, "stop the run after this many captchas, so a bad exit list does not burn IPs")
	fingerprint := fs.String("fingerprint", "chrome_152", "browser fingerprint profile (configs/fingerprints.yaml)")
	altFingerprint := fs.String("alt-fingerprint", "chrome_146", "a second profile for the first account's page (T5; empty = skip)")
	raw := fs.Bool("raw", true, "save TikTok's raw answers under <out-dir>/raw (first of each call kind, every failure, the first 3 pages' HTML)")
	base := fs.String("base", ttclient.BaseURL, "TikTok base URL")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake TikTok (no traffic, no proxies)")
	_ = fs.Parse(args)

	var names []string
	cfg := &config.Config{}
	if *dryRun {
		w := fakett.NewWorld(fakett.DefaultSpec())
		// Like the recordings: the app's own list call needs a signature.
		hs := httptest.NewServer(fakett.NewServer(w, fakett.Options{Faults: fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/post/item_list/"}}}))
		defer hs.Close()
		*base, *tier, *rps = hs.URL, "direct", 1000
		for _, a := range w.Accounts() {
			names = append(names, a.Username)
		}
		if *outDir == "" {
			*outDir = "phase0/out-tt-dryrun"
		}
		*videos = max(*videos, 1)
	} else {
		if *accountsFile == "" {
			fmt.Fprintln(os.Stderr, "probe: --accounts is required (or --dry-run)")
			return 2
		}
		var err error
		if names, err = readAccounts(*accountsFile); err != nil {
			fmt.Fprintln(os.Stderr, "probe:", err)
			return 1
		}
		if cfg, err = config.Load(*cfgPath); err != nil {
			fmt.Fprintln(os.Stderr, "probe: config:", err)
			return 1
		}
	}
	pool, err := newPool(cfg, *base, *fingerprint, *rps)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	var altPool *session.Pool
	if *altFingerprint != "" {
		if altPool, err = newPool(cfg, *base, *altFingerprint, *rps); err != nil {
			fmt.Fprintln(os.Stderr, "probe:", err)
			return 1
		}
	}
	seed, err := ttclient.Seed()
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	rules := ttdetect.Rules()
	var bin string
	if *signer == "browser" {
		if bin, err = browser.FindBinary(""); err != nil {
			fmt.Fprintln(os.Stderr, "probe: --signer browser:", err, "(run: make tks-browser-check)")
			return 1
		}
	}
	if *outDir == "" {
		*outDir = filepath.Join("phase0", "out-tt", *tier+"-"+time.Now().UTC().Format("20060102-150405"))
	}
	imageDir, videoDir, rawDir := filepath.Join(*outDir, "images"), filepath.Join(*outDir, "videos"), ""
	if *raw {
		rawDir = filepath.Join(*outDir, "raw")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "probe: %d accounts, tier %s, %.2f requests/s per identity, signer %s; Ctrl-C stops and keeps what was read\n",
		len(names), *tier, *rps, *signer)
	rep := probe.Run(ctx, probe.Options{BaseURL: *base, Accounts: names, Tier: *tier, Pool: pool, AltPool: altPool, AltFingerprint: *altFingerprint,
		Loader:         &session.Loader{Rules: rules, AcceptLanguage: acceptLanguage, Hooks: ttclient.Hooks()},
		Client:         &graphql.Client{Registry: registry.NewStatic(seed), Rules: rules, Builder: ttclient.Builder{AcceptLanguage: acceptLanguage}, Decode: ttclient.Decode},
		AcceptLanguage: acceptLanguage, Fingerprint: *fingerprint, Version: version,
		MaxListPages: *maxPages, MaxAge: time.Duration(*maxAge) * 24 * time.Hour, MaxCommentPages: *maxComments, CountChecks: *countChecks,
		Covers: *covers, Videos: *videos, Signer: *signer, BrowserBinary: bin, BrowserNoSandbox: os.Getenv("TKS_BROWSER_NO_SANDBOX") != "" || os.Geteuid() == 0,
		OEmbed: *oembed || *dryRun, MaxCaptchas: *maxCaptchas, ImageDir: imageDir, VideoDir: videoDir, RawDir: rawDir, RawPages: 3,
		Progress: func(line string) { fmt.Fprintln(os.Stderr, "probe:", line) }})
	if err := probe.Write(*outDir, rep); err != nil {
		fmt.Fprintln(os.Stderr, "probe: write:", err)
		return 1
	}
	bundle, err := probe.Bundle(*outDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	s := rep.Summary
	fmt.Fprintf(os.Stderr, "probe: wrote %s and %s\n  bootstrapped %d/%d; page forms %v; captchas %d, challenges %d\n"+
		"  calls %v\n  posts %d (with counts %d, pinned %d, ads %d, photos %d); id time matches %d, mismatches %d\n"+
		"  list stops %v, oldest %s; comments served %d in %d reads, order %v, replies %d; cursor portable %d/%d\n",
		*outDir, bundle, s.Bootstrapped, s.Accounts, s.PageForms, s.Captchas, s.Challenges, s.Calls,
		s.Posts, s.PostsWithStats, s.PinnedSeen, s.AdSeen, s.PhotoSeen, s.IDTimeMatches, s.IDTimeMismatches,
		s.WalkStops, s.OldestReached.Format("2006-01-02"), s.CommentsServed, s.CommentReads, s.OrderVerdicts, s.RepliesServed,
		s.CursorPortable, s.CursorPortable+s.CursorNotPortable)
	if s.StoppedEarly != "" {
		fmt.Fprintf(os.Stderr, "  stopped early (%s) after %d of %d accounts\n", s.StoppedEarly, s.Accounts, len(names))
	}
	return 0
}

func newPool(cfg *config.Config, base, fingerprint string, rps float64) (*session.Pool, error) {
	fpBytes, err := configs.FS.ReadFile("fingerprints.yaml")
	if err != nil {
		return nil, err
	}
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	if _, ok := fps[fingerprint]; !ok {
		return nil, fmt.Errorf("unknown fingerprint %q (configs/fingerprints.yaml)", fingerprint)
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return nil, err
	}
	return session.NewPool(session.PoolConfig{BaseURL: base, Profile: fingerprint, Timeout: 30 * time.Second,
		IdentityRPS: rps, IdentityBurst: 1, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 8}, proxies, fps, nil)
}

func browserCheck() int {
	bin, err := browser.FindBinary("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "no Chromium found. To install Google Chrome on the droplet (Ubuntu), run these two commands:")
		fmt.Fprintln(os.Stderr, "  wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb")
		fmt.Fprintln(os.Stderr, "  apt-get install -y ./google-chrome-stable_current_amd64.deb")
		return 1
	}
	fmt.Println("Chromium found:", bin)
	return 0
}

// readAccounts reads usernames: one per line, @ and profile links accepted,
// blank lines and # comments skipped.
func readAccounts(path string) ([]string, error) {
	f, err := os.Open(path) //nolint:gosec // the owner's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if u := username(sc.Text()); u != "" {
			out = append(out, u)
		}
	}
	if len(out) == 0 && sc.Err() == nil {
		return nil, fmt.Errorf("%s holds no accounts", path)
	}
	return out, sc.Err()
}

func username(line string) string {
	line, _, _ = strings.Cut(line, "#")
	line = strings.TrimSpace(line)
	for _, p := range []string{"https://", "http://", "www.", "m.", "tiktok.com/"} {
		line = strings.TrimPrefix(line, p)
	}
	line = strings.TrimPrefix(line, "@")
	line, _, _ = strings.Cut(line, "/")
	line, _, _ = strings.Cut(line, "?")
	return strings.TrimSpace(line)
}
