package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scrub"
)

// fixturesAdd scrubs one real answer into a golden fixture. The answer is
// a raw body (a probe raw/ file) or the nth answer of its kind in a HAR;
// only the scrubbed copy is written (docs/runbooks/tks-fixtures.md).
func fixturesAdd(args []string) (map[string]any, error) {
	fs := flag.NewFlagSet("fixtures add", flag.ExitOnError)
	from := fs.String("from", "", "a HAR file or a raw answer (JSON body or page HTML)")
	kind := fs.String("kind", "", "answer kind: "+kindList()+" (guessed from a raw answer when empty)")
	name := fs.String("name", "", "fixture name (testdata/tt/<kind>/<name>); never a username")
	entry := fs.Int("entry", 1, "with a HAR: the nth answer of the kind")
	items := fs.Int("items", 6, "keep at most this many list items or comments (0: all)")
	source := fs.String("source", "", "where the answer came from: har | probe (default: from the file)")
	root := fs.String("root", "testdata/tt", "fixtures root")
	_ = fs.Parse(args)
	if *from == "" || *name == "" {
		return nil, fmt.Errorf("fixtures add: --from and --name are required")
	}
	b, err := os.ReadFile(*from)
	if err != nil {
		return nil, err
	}
	k := scrub.Kind(*kind)
	isHAR := strings.HasSuffix(strings.ToLower(*from), ".har")
	switch {
	case isHAR && k == "":
		return nil, fmt.Errorf("fixtures add: --kind is required with a HAR")
	case isHAR:
		if b, err = scrub.FromHAR(b, k, *entry); err != nil {
			return nil, err
		}
	case k == "":
		if k, err = scrub.Detect(b); err != nil {
			return nil, err
		}
	}
	if !validKind(k) {
		return nil, fmt.Errorf("fixtures add: unknown kind %q (%s)", k, kindList())
	}
	src := *source
	if src == "" {
		src = "probe"
		if isHAR {
			src = "har"
		}
	}
	dir, err := scrub.Write(*root, k, *name, b, map[string]any{"source": src}, scrub.Options{Items: *items})
	if err != nil {
		return nil, err
	}
	return map[string]any{"fixture": dir, "kind": k, "next": "go test ./internal/tt/parse -run Golden -update, then review expected.json"}, nil
}

// fixturesScrub scrubs every fixture an older scrubber wrote again.
func fixturesScrub(args []string) (map[string]any, error) {
	fs := flag.NewFlagSet("fixtures scrub", flag.ExitOnError)
	root := fs.String("root", "testdata/tt", "fixtures root")
	_ = fs.Parse(args)
	dirs, _ := filepath.Glob(filepath.Join(*root, "*", "*"))
	n := 0
	for _, d := range dirs {
		if _, err := os.Stat(filepath.Join(d, "meta.json")); err != nil {
			continue
		}
		if err := scrub.ScrubDir(d); err != nil {
			return nil, fmt.Errorf("%s: %w", d, err)
		}
		n++
	}
	return map[string]any{"fixtures": n, "scrubber": scrub.Version}, nil
}

func validKind(k scrub.Kind) bool {
	for _, x := range scrub.Kinds {
		if x == k {
			return true
		}
	}
	return false
}

func kindList() string {
	var s []string
	for _, k := range scrub.Kinds {
		s = append(s, string(k))
	}
	return strings.Join(s, ", ")
}
