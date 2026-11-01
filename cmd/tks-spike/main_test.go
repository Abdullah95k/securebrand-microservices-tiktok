package main

import (
	"os"
	"path/filepath"
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
