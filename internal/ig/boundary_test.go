package ig_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/abdullah95k/securebrand-microservices-igs"

// forbidden are Facebook's packages: Instagram code builds on the neutral
// layer (internal/scrape) only, so the copied Facebook code can be removed
// later without touching it (ADR 0004 §2).
var forbidden = []string{
	module + "/internal/fb",
	module + "/internal/fakefb",
	module + "/gen/go/securebrand/fb/",
}

func TestNoFacebookImports(t *testing.T) {
	fset := token.NewFileSet()
	files := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		files++
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			for _, bad := range forbidden {
				if p == strings.TrimSuffix(bad, "/") || strings.HasPrefix(p, strings.TrimSuffix(bad, "/")+"/") {
					t.Errorf("%s imports %s", path, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("no Go files found")
	}
}
