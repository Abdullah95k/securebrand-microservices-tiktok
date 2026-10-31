package spike

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
)

// Scrubbing keeps a fixture parseable: every committed fixture, treated as
// a raw capture, still passes CheckFixtures after ScrubDir (people's text
// is blanked, keys keep their order, the page keeps its id and name).
func TestScrubbedFixturesStillParse(t *testing.T) {
	root := t.TempDir()
	src := "../../testdata/fb"
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if info.Name() == "meta.json" { // as a raw capture would have it
			b = []byte(strings.NewReplacer(`"source": "fakefb"`, `"source": "capture"`,
				`"source": "handcrafted"`, `"source": "capture"`, `"__scrubbed": true`, `"__scrubbed": false`).Replace(string(b)))
		}
		dst := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	checks, err := CheckFixtures(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("no fixtures")
	}
	changed := 0
	for _, c := range checks {
		before, _ := os.ReadDir(c.Dir)
		var raw []byte
		for _, e := range before {
			if strings.HasPrefix(e.Name(), "response.") {
				raw, _ = os.ReadFile(filepath.Join(c.Dir, e.Name()))
			}
		}
		if err := fixtures.ScrubDir(c.Dir, nil); err != nil {
			t.Fatal(err)
		}
		for _, e := range before {
			if strings.HasPrefix(e.Name(), "response.") {
				if b, _ := os.ReadFile(filepath.Join(c.Dir, e.Name())); string(b) != string(raw) {
					changed++
				}
			}
		}
	}
	if changed < len(checks)/2 {
		t.Fatalf("only %d of %d fixtures changed: the scrubber did not run", changed, len(checks))
	}
	after, _ := CheckFixtures(root)
	for i, c := range after {
		if !c.OK() || c.Items != checks[i].Items {
			t.Errorf("%s: %d items before scrubbing, after: %+v", c.Dir, checks[i].Items, c)
		}
	}
}
