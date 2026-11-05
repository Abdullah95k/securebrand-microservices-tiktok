package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

func TestUsername(t *testing.T) {
	for in, want := range map[string]string{
		"someone":                                   "someone",
		"@someone":                                  "someone",
		"https://www.tiktok.com/@some.one":          "some.one",
		"https://www.tiktok.com/@a..b?lang=en":      "a..b",
		"tiktok.com/@x_y/video/7300000000000000000": "x_y",
		"  # a comment":                             "",
		"name # note":                               "name",
	} {
		if got := username(in); got != want {
			t.Errorf("username(%q) = %q, want %q", in, got, want)
		}
	}
}

// The dry run reaches every account of the fake TikTok and writes every
// file: what make tks-probe-dryrun runs.
func TestDryRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	if code := runProbe([]string{"--dry-run", "--out-dir", dir, "--raw=false"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, f := range []string{"report.json", "accounts.csv", "posts.csv", "comments.csv"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(dir + ".tar.gz"); err != nil {
		t.Fatal(err)
	}
}

// The discovery dry run reads every source of the fake TikTok and writes
// the report and the bundle: what make tks-discover-dryrun runs.
func TestDiscoverDryRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	if code := runDiscover([]string{"--dry-run", "--out-dir", dir, "--keywords", "3", "--signer", "none"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, f := range []string{"calls.jsonl", "report.json", "report.txt", "tks-discover-bundle.tar.gz"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := os.ReadFile(filepath.Join(dir, "report.txt"))
	if !strings.Contains(string(b), "search_general") || !strings.Contains(string(b), "short link 301") {
		t.Fatalf("report:\n%s", b)
	}
}

// --parallel is capped at the tier's exit IPs: a static list's endpoints,
// direct's one; a rotating gateway does not cap it.
func TestTierIPs(t *testing.T) {
	cfg := &config.Config{}
	cfg.FB.Proxy.Tiers = []config.ProxyTier{
		{Name: "isp", Kind: "static_list", Endpoints: []string{"http://a:1", "http://b:1", "http://c:1"}},
		{Name: "resi", Kind: "gateway", Gateway: "http://gw:1"},
	}
	for _, c := range []struct {
		tier   string
		n      int
		capped bool
	}{{"isp", 3, true}, {"direct", 1, true}, {"resi", 0, false}, {"missing", 0, false}} {
		if n, capped := tierIPs(cfg, c.tier); n != c.n || capped != c.capped {
			t.Errorf("%s: %d %v, want %d %v", c.tier, n, capped, c.n, c.capped)
		}
	}
}

// The Phase 0 dry run reaches GO against fakett with rotated variants and
// writes its bundle: what make tks-phase0-dryrun runs.
func TestPhase0DryRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	if code := runPhase0([]string{"--dry-run", "--out-dir", dir}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, f := range []string{"report.txt", "report.json", "registry.yaml", "run-direct.jsonl", "ladder-direct.jsonl", "tks-phase0-bundle.tar.gz"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := os.ReadFile(filepath.Join(dir, "registry.yaml"))
	if !strings.Contains(string(b), "creator-signed") {
		t.Fatalf("the rotated creator list's signed variant is not in the registry:\n%s", b)
	}
}

// Accounts are sampled evenly over the file, not from its top.
func TestSpread(t *testing.T) {
	var all []string
	for i := range 10 {
		all = append(all, strconv.Itoa(i))
	}
	if got := strings.Join(spread(all, 4), ","); got != "0,2,5,7" {
		t.Fatal(got)
	}
	if len(spread(all, 0)) != 10 || len(spread(all, 20)) != 10 {
		t.Fatal("all")
	}
}
