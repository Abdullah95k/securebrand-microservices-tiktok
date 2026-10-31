package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

func TestUsername(t *testing.T) {
	for in, want := range map[string]string{
		"brand": "brand", "@brand": "brand", "https://www.instagram.com/brand/": "brand",
		"instagram.com/brand?hl=en": "brand", "  # a comment": "", "brand # note": "brand", "": "",
	} {
		if got := username(in); got != want {
			t.Errorf("username(%q) = %q, want %q", in, got, want)
		}
	}
}

// The command end to end against fakeig over the fingerprinted transport:
// it reads the profiles file and writes the report, the spreadsheets, the
// profile pictures and the bundle, all 0600.
func TestProbeCommand(t *testing.T) {
	w := fakeig.NewWorld(fakeig.DefaultSpec())
	hs := httptest.NewServer(fakeig.NewServer(w, fakeig.Options{}))
	defer hs.Close()
	dir := t.TempDir()
	list := filepath.Join(dir, "profiles.txt")
	var names []string
	for _, p := range w.Profiles() {
		names = append(names, "@"+p.Username)
	}
	if err := os.WriteFile(list, []byte("# canary\n"+strings.Join(names, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	if code := runProbe([]string{"--profiles", list, "--base", hs.URL, "--out-dir", out, "--rps", "1000", "--max-grid-pages", "5"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	b, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rep probe.Report
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Summary.Bootstrapped != len(names) || rep.Summary.GridEnds != len(names) || rep.Profiles[0].Username != w.Profiles()[0].Username {
		t.Fatalf("summary %+v", rep.Summary)
	}
	files := []string{"report.json", "profiles.csv", "posts.csv", "comments.csv"}
	for _, p := range w.Profiles() {
		files = append(files, filepath.Join("images", p.Username+".jpg"))
	}
	for _, f := range files {
		if fi, err := os.Stat(filepath.Join(out, f)); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v", f, err)
		}
	}
	if fi, err := os.Stat(out + ".tar.gz"); err != nil || fi.Size() == 0 {
		t.Fatalf("bundle: %v", err)
	}
}

// --dry-run needs no profiles file, config or network.
func TestDryRun(t *testing.T) {
	out := filepath.Join(t.TempDir(), "dry")
	if code := runProbe([]string{"--dry-run", "--out-dir", out}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Stat(filepath.Join(out, "profiles.csv")); err != nil {
		t.Fatal(err)
	}
}
