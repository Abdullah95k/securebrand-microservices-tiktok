package main

import (
	"os"
	"path/filepath"
	"testing"
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
