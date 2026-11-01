package scrub_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scrub"
)

// TestCommittedFixtures guards testdata/tt: every fixture is marked
// scrubbed by this scrubber version, and none holds a signed link, a
// person's age band, a raw page's head, or a real secUid shape.
func TestCommittedFixtures(t *testing.T) {
	dirs, _ := filepath.Glob("../../../testdata/tt/*/*")
	if len(dirs) == 0 {
		t.Fatal("no fixtures under testdata/tt")
	}
	ageBand := regexp.MustCompile(`"predicted_age_group":"([^"]*)"`)
	for _, d := range dirs {
		m, err := scrub.ReadMeta(d)
		if err != nil {
			t.Errorf("%s: %v", d, err)
			continue
		}
		if !m.Synthetic() && (!m.Scrubbed || m.Scrubber < scrub.Version) {
			t.Errorf("%s: not scrubbed by scrubber %d (tksctl fixtures scrub)", d, scrub.Version)
		}
		b, err := os.ReadFile(filepath.Join(d, m.Kind.File()))
		if err != nil {
			t.Errorf("%s: %v", d, err)
			continue
		}
		s := string(b)
		for _, bad := range []string{"x-signature", "signature=", "policy=", "tt_chain_token", "og:", "<meta property"} {
			if strings.Contains(s, bad) {
				t.Errorf("%s: holds %q", d, bad)
			}
		}
		for _, a := range ageBand.FindAllStringSubmatch(s, -1) {
			if a[1] != "scrubbed" && a[1] != "" {
				t.Errorf("%s: holds an age band", d)
			}
		}
	}
}
