// Package discprobe is the C6 probe (docs/tiktok/discovery-plan.md §8, §9,
// §15; build-status M10b): what TikTok's discovery pages and calls give a
// logged-out visitor through tks's own exits, before M12 and M13 build the
// sources on them.
//
// With the crawler's identities, client, detect rules and browser signer
// (calls that need TikTok's signature are signed by TikTok's own script in
// a browser page, D11), it reads, each under a cap:
//   - keyword search (posts and accounts) for keywords of the Iraq list,
//     page after page, to learn whether a signature from the pool is
//     accepted and how deep it goes before TikTok stops or asks to log in;
//   - the For You feed several times (new posts per read);
//   - hashtag and sound pages and their post lists;
//   - related posts of found posts;
//   - the following and followers lists and reposts of a few of the
//     owner's accounts (whether a logged-out visitor gets them at all);
//   - one creator-list page of found authors: how many post from Iraq,
//     since search results carry no post region (the exit-country
//     question, §8.3);
//   - post pages of posts no longer listed since an earlier probe, and of
//     posts the owner knows are gone (T34);
//   - short links (how vm.tiktok.com redirects);
//   - and, in a browser, each source's page with a few scrolls: the calls
//     TikTok's own page makes (paths, parameter names, answers' sizes),
//     for the calls not recorded yet (T36).
//
// Every call is tried as seeded and as its signed or unsigned twin on its
// source's first item, and the first that answers is walked.
//
// Output (all 0600, under phase0/): calls.jsonl (one record per request:
// what was asked, how it was answered, how many posts and people it held,
// with keywords, hashtags, sounds and accounts by position), pages.json
// (the browser's captured calls), report.json and report.txt, raw/ (the
// first answers of each call, for M12's golden fixtures: they hold
// personal data), and a bundle. The records and the report hold no
// username, name, id, link or text, the keywords of the Iraq list aside;
// raw/ does, so the bundle goes back through the owner's Drive only.
package discprobe

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Sources, in the order they run (two lanes, each on its own identity).
const (
	SrcSearch     = "search"
	SrcFeed       = "feed"
	SrcHashtag    = "hashtag"
	SrcSound      = "sound"
	SrcRelated    = "related"
	SrcEntry      = "entry"
	SrcSearchUser = "search_user"
	SrcFollowing  = "following"
	SrcFollowers  = "followers"
	SrcReposts    = "reposts"
	SrcVanished   = "vanished"
	SrcShortLinks = "short_links"
	SrcPages      = "pages"
)

// Lanes run at once, each source after the one before it in its lane:
// the first lane's later sources use what search and the feed found.
var Lanes = [][]string{
	{SrcSearch, SrcFeed, SrcHashtag, SrcSound, SrcRelated, SrcEntry},
	{SrcSearchUser, SrcFollowing, SrcFollowers, SrcReposts, SrcVanished, SrcShortLinks},
}

// Sources lists every source: the lanes', then the browser's pages.
func Sources() []string {
	var out []string
	for _, l := range Lanes {
		out = append(out, l...)
	}
	return append(out, SrcPages)
}

// Files written to the output directory.
const (
	FileCalls     = "calls.jsonl"
	FilePages     = "pages.json"
	FileReport    = "report.json"
	FileReportTxt = "report.txt"
	FileMeta      = "meta.json"
	FileBundle    = "tks-discover-bundle.tar.gz"
	DirRaw        = "raw"
)

// Browser opens a browser page on url through id's exit, for the page
// captures; cmd/tks-spike opens a browser.Session.
type Browser func(ctx context.Context, id *session.Identity, url string) (Navigator, error)

// Navigator is the part of browser.Session the page captures use.
type Navigator interface {
	Navigate(ctx context.Context, url string) error
	Scroll(ctx context.Context) error
	Mark() int
	Captured(ctx context.Context, mark int) []browser.Exchange
	MemoryMB() float64
	Close()
}

// Stack holds the collaborators. cmd/tks-spike wires the real ones; tests
// wire fakett.
type Stack struct {
	BaseURL string
	Pool    *session.Pool
	Loader  *session.Loader
	Rules   detect.Rules
	// Opener opens signer pages (sign.BrowserOpener); nil signs nothing.
	Opener sign.Opener
	Signer config.Signer
	// Browser opens the page captures' browser; nil skips them.
	Browser Browser
}

// Keyword is a search term of the Iraq list.
type Keyword struct {
	Term     string `json:"term"`
	Language string `json:"language"`
	Group    string `json:"group"`
}

// KnownPost is a post an earlier probe listed (its posts.csv), for T34.
type KnownPost struct {
	Username string
	ID       string
	Created  time.Time
}

// Limits cap every source. Zero skips what it counts.
type Limits struct {
	Keywords, SearchPages, DeepPages int
	UserKeywords, UserPages          int
	Hashtags, HashtagPages           int
	Sounds, SoundPages               int
	FeedReads                        int
	Related                          int
	FollowAccounts, FollowPages      int
	Entry                            int
	VanishedAccounts, VanishedPages  int
	PageScrolls                      int
	// RawPerOp is how many answers of each call and class are kept in
	// raw/.
	RawPerOp int
	// PageMB stops a captured page's scrolling past this memory (TikTok's
	// video pages grew Chrome past 1 GB in 30 s: S1).
	PageMB float64
}

// DefaultLimits are about an hour at the crawler's pace, with two
// identities.
func DefaultLimits() Limits {
	return Limits{Keywords: 16, SearchPages: 5, DeepPages: 30, UserKeywords: 6, UserPages: 5, Hashtags: 8, HashtagPages: 5,
		Sounds: 5, SoundPages: 3, FeedReads: 6, Related: 6, FollowAccounts: 5, FollowPages: 3, Entry: 30,
		VanishedAccounts: 40, VanishedPages: 30, PageScrolls: 4, RawPerOp: 3, PageMB: 900}
}

// MaxRequests is the most API calls and page loads the limits allow
// (variant tries and bootstraps included), for the run's estimate.
func (l Limits) MaxRequests(shortLinks, gonePosts int) int {
	tries := 4 * 10 // each call's variants on its first item, both signer pages
	n := l.Keywords*l.SearchPages + max(0, l.DeepPages-l.SearchPages) + l.UserKeywords*l.UserPages +
		l.Hashtags*(2+l.HashtagPages) + l.Sounds*(2+l.SoundPages) + l.FeedReads + l.Related +
		l.FollowAccounts*(1+2*l.FollowPages+1) + l.Entry + l.VanishedAccounts*3 + l.VanishedPages + 2*shortLinks + gonePosts
	return n + tries + 4
}

// Config configures a run.
type Config struct {
	Out  string
	Tier string
	// Accounts are the owner's usernames (in memory only; records name
	// them by position): the follow lists' accounts, the identities'
	// bootstrap pages, and the list a found account is "new" against.
	Accounts []string
	Keywords []Keyword
	// Sources to run (default all).
	Sources []string
	Limits  Limits
	// ShortLinks are short links to resolve; Known are an earlier probe's
	// posts; Gone are links to posts the owner knows are no longer public.
	ShortLinks []string
	Known      []KnownPost
	Gone       []string
	// CompareSignPages also signs each signed call's first item on a page
	// opened on the call's own page (search, feed), not only on a profile
	// page as the crawler does.
	CompareSignPages bool
	MaxCaptchas      int
	AcceptLanguage   string
	// ReportOnly rebuilds the report and the bundle from the files.
	ReportOnly bool
	Secrets    []string
	Version    string
	Now        func() time.Time
}

// Kit runs the probe.
type Kit struct {
	Stack Stack
	Cfg   Config
	Log   io.Writer

	seed     registry.Seed
	exits    phase0.Exits
	profile  *sign.Pool // pages opened on a profile, as the crawler's
	source   *sign.Pool // pages opened on the call's own page
	rec      *recorder
	captchas atomic.Int64
	stop     context.CancelFunc
	started  time.Time

	logMu  sync.Mutex
	mu     sync.Mutex
	known  map[string]bool // the owner's usernames, lowercased
	seen   map[string]bool // people found so far, by source and id or username
	raw    map[string]int  // answers kept per op|variant|class
	pages  []PageCapture
	items  map[string][]Item
	posts  []Post // what search and the feed found (lane 1)
	issues []string
}

func (k *Kit) logf(format string, args ...any) {
	if k.Log != nil {
		k.logMu.Lock()
		_, _ = fmt.Fprintf(k.Log, format+"\n", args...)
		k.logMu.Unlock()
	}
}

func (k *Kit) now() time.Time {
	if k.Cfg.Now != nil {
		return k.Cfg.Now()
	}
	return time.Now()
}

func (k *Kit) path(name string) string { return filepath.Join(k.Cfg.Out, name) }

func (k *Kit) wants(src string) bool {
	return len(k.Cfg.Sources) == 0 || contains(k.Cfg.Sources, src)
}

// Run probes every source and writes the report and the bundle.
func (k *Kit) Run(ctx context.Context) (Report, error) {
	if k.Cfg.MaxCaptchas <= 0 {
		k.Cfg.MaxCaptchas = 5
	}
	if err := os.MkdirAll(filepath.Join(k.Cfg.Out, DirRaw), 0o700); err != nil {
		return Report{}, err
	}
	k.started = k.now().UTC()
	if !k.Cfg.ReportOnly {
		if err := k.probe(ctx); err != nil {
			return Report{}, err
		}
	}
	rep, err := k.report()
	if err != nil {
		return rep, err
	}
	if err := k.bundle(); err != nil {
		return rep, err
	}
	return rep, ctx.Err()
}

func (k *Kit) probe(ctx context.Context) error {
	if len(k.Cfg.Accounts) == 0 {
		return errors.New("discprobe: needs the owner's accounts (their pages bootstrap the identities)")
	}
	seed, err := Seed()
	if err != nil {
		return err
	}
	k.seed = seed
	k.known, k.seen, k.raw, k.items = map[string]bool{}, map[string]bool{}, map[string]int{}, map[string][]Item{}
	for _, a := range k.Cfg.Accounts {
		k.known[strings.ToLower(a)] = true
	}
	f, err := os.OpenFile(k.path(FileCalls), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	k.rec = &recorder{w: f}
	if k.Stack.Opener != nil {
		k.profile = sign.New(k.Stack.Signer, k.Stack.Opener)
		defer k.profile.Close()
		if k.Cfg.CompareSignPages {
			sc := k.Stack.Signer
			sc.Pages, sc.OpenOn = 1, ""
			k.source = sign.New(sc, k.Stack.Opener)
			defer k.source.Close()
		}
	}
	ctx, k.stop = context.WithCancel(ctx)
	defer k.stop()
	k.logf("%d keywords, %d of the owner's accounts, at most ~%d requests, 2 lanes; tier %s; signer %s",
		len(k.Cfg.Keywords), len(k.Cfg.Accounts), k.Cfg.Limits.MaxRequests(len(k.Cfg.ShortLinks), len(k.Cfg.Gone)), k.Cfg.Tier, k.signerName())
	var wg sync.WaitGroup
	for i, lane := range Lanes {
		wg.Go(func() { k.lane(ctx, i, lane) })
	}
	wg.Wait()
	if k.wants(SrcPages) && ctx.Err() == nil {
		k.capturePages(ctx)
	}
	if err := k.rec.err(); err != nil {
		return err
	}
	if n := k.captchas.Load(); n >= int64(k.Cfg.MaxCaptchas) {
		k.issue(fmt.Sprintf("stopped after %d captchas, so the exits are not burnt", n))
	}
	b, _ := json.MarshalIndent(k.pages, "", "  ")
	return os.WriteFile(k.path(FilePages), append(b, '\n'), 0o600)
}

func (k *Kit) signerName() string {
	switch {
	case k.profile == nil:
		return "none (signed calls are refused)"
	case k.source != nil:
		return "browser (profile pages, and the call's own page for comparison)"
	}
	return "browser (profile pages, as the crawler's)"
}

func (k *Kit) issue(s string) {
	k.mu.Lock()
	k.issues = append(k.issues, s)
	k.mu.Unlock()
	k.logf("note: %s", s)
}

// lane runs its sources one after another on one identity, bootstrapped
// on one of the owner's accounts.
func (k *Kit) lane(ctx context.Context, n int, sources []string) {
	l := &lane{k: k, n: n}
	defer l.release()
	for _, src := range sources {
		if ctx.Err() != nil || !k.wants(src) {
			continue
		}
		if l.id == nil && !l.bootstrap(ctx) {
			k.issue(fmt.Sprintf("lane %d: no identity could load a page; its sources did not run", n+1))
			return
		}
		start := time.Now()
		k.logf("%s: start", src)
		l.run(ctx, src)
		k.logf("%s: done in %s", src, time.Since(start).Round(time.Second))
	}
}

// person records a person a source found; it returns whether it is new
// to tks: not on the owner's list, and not found by this source before.
func (k *Kit) person(src string, u User) bool {
	key := u.ID
	if key == "" {
		key = strings.ToLower(u.Username)
	}
	if key == "" {
		return false
	}
	key = src + "|" + key
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.seen[key] {
		return false
	}
	k.seen[key] = true
	return !k.known[strings.ToLower(u.Username)]
}

// keepRaw saves an answer under raw/, the first few of each call, variant
// and class.
func (k *Kit) keepRaw(src, op, variant, class string, body []byte) {
	if len(body) == 0 || k.Cfg.Limits.RawPerOp <= 0 {
		return
	}
	key := op + "|" + variant + "|" + class
	k.mu.Lock()
	n := k.raw[key]
	if n >= k.Cfg.Limits.RawPerOp {
		k.mu.Unlock()
		return
	}
	k.raw[key] = n + 1
	k.mu.Unlock()
	name := fmt.Sprintf("%s-%s-%s-%s-%d", src, op, safe(variant), class, n+1)
	ext := ".json"
	if bytes.HasPrefix(bytes.TrimSpace(body), []byte("<")) {
		ext = ".html"
	}
	_ = os.WriteFile(filepath.Join(k.Cfg.Out, DirRaw, name+ext), body, 0o600)
}

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

func safe(s string) string {
	if s == "" {
		return "page"
	}
	return unsafeName.ReplaceAllString(s, "_")
}

// recorder writes records as JSON lines.
type recorder struct {
	mu   sync.Mutex
	w    io.Writer
	fail error
}

func (r *recorder) add(rec Record) {
	b, err := json.Marshal(rec)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		_, err = r.w.Write(append(b, '\n'))
	}
	if err != nil && r.fail == nil {
		r.fail = err
	}
}

func (r *recorder) err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

// ReadCalls reads a calls file.
func ReadCalls(path string) ([]Record, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the probe's own output
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, fmt.Errorf("discprobe: %s: %w", path, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func (k *Kit) report() (Report, error) {
	recs, err := ReadCalls(k.path(FileCalls))
	if err != nil {
		return Report{}, err
	}
	var pages []PageCapture
	if b, err := os.ReadFile(k.path(FilePages)); err == nil {
		_ = json.Unmarshal(b, &pages)
	}
	rep := BuildReport(recs, pages, k.Cfg.Keywords)
	rep.Version, rep.Tier, rep.Items = k.Cfg.Version, k.Cfg.Tier, k.items
	rep.Issues = append(rep.Issues, k.issues...)
	txt := FormatReport(rep)
	k.logf("%s", strings.TrimRight(txt, "\n"))
	if err := os.WriteFile(k.path(FileReportTxt), []byte(txt), 0o600); err != nil {
		return rep, err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	return rep, os.WriteFile(k.path(FileReport), append(b, '\n'), 0o600)
}

// credentials finds a URL carrying a user name and password.
var credentials = regexp.MustCompile(`://[^/\s:@"]+:[^/\s@"]+@`)

// ErrUnsafeBundle stops the bundle when a file holds a proxy credential,
// or a summary file (not raw/) one of the owner's usernames.
var ErrUnsafeBundle = errors.New("discprobe: bundle would contain a credential or a username outside raw/")

// bundle packs the outputs. raw/ holds personal data (D13): the bundle
// goes back through the owner's Drive, as the probe's did (C2, C3); the
// other files must hold no credential and none of the owner's usernames.
func (k *Kit) bundle() error {
	meta := map[string]any{"version": k.Cfg.Version, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
		"started_at": k.started, "written_at": k.now().UTC(), "tier": k.Cfg.Tier, "accounts": len(k.Cfg.Accounts),
		"keywords": k.Cfg.Keywords, "limits": k.Cfg.Limits, "sources": k.Cfg.Sources, "short_links": len(k.Cfg.ShortLinks),
		"known_posts": len(k.Cfg.Known), "gone_posts": len(k.Cfg.Gone), "signer": k.Stack.Signer, "signer_on": k.Stack.Opener != nil,
		"compare_sign_pages": k.Cfg.CompareSignPages}
	b, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(k.path(FileMeta), append(b, '\n'), 0o600); err != nil {
		return err
	}
	var files []string
	err := filepath.WalkDir(k.Cfg.Out, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(k.Cfg.Out, p)
		rel = filepath.ToSlash(rel)
		switch {
		case rel == FileCalls, rel == FilePages, rel == FileReport, rel == FileReportTxt, rel == FileMeta, strings.HasPrefix(rel, DirRaw+"/"):
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	names := usernames(k.Cfg.Accounts)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, rel := range files {
		data, err := os.ReadFile(k.path(rel))
		if err != nil {
			return err
		}
		for _, s := range k.Cfg.Secrets {
			if len(s) >= 6 && bytes.Contains(data, []byte(s)) {
				return fmt.Errorf("%w: %s holds a proxy credential", ErrUnsafeBundle, rel)
			}
		}
		if credentials.Match(data) {
			return fmt.Errorf("%w: %s holds a URL with credentials", ErrUnsafeBundle, rel)
		}
		if !strings.HasPrefix(rel, DirRaw+"/") && names != nil && names.Match(data) {
			return fmt.Errorf("%w: %s holds one of your usernames", ErrUnsafeBundle, rel)
		}
		if err := tw.WriteHeader(&tar.Header{Name: "tks-discover/" + rel, Mode: 0o600, Size: int64(len(data)), ModTime: k.now()}); err != nil {
			return err
		}
		if _, err := tw.Write(data); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := os.WriteFile(k.path(FileBundle), buf.Bytes(), 0o600); err != nil {
		return err
	}
	k.logf("wrote %s (%d files): raw/ holds TikTok's answers as they came (names, links, captions): send it through your Drive only", k.path(FileBundle), len(files))
	return nil
}

// usernames matches the owner's usernames (5 characters or more) as whole
// words.
func usernames(accounts []string) *regexp.Regexp {
	var alts []string
	for _, a := range accounts {
		if len(a) >= 5 {
			alts = append(alts, regexp.QuoteMeta(strings.ToLower(a)))
		}
	}
	if len(alts) == 0 {
		return nil
	}
	return regexp.MustCompile(`(?i)(^|[^a-z0-9_.])(` + strings.Join(alts, "|") + `)($|[^a-z0-9_.])`)
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
