package scrape_test

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

// forbidden are the import paths no neutral package may use: platform code
// belongs in internal/fb or internal/ig, never below internal/scrape.
var forbidden = []string{
	module + "/internal/fb",
	module + "/internal/ig",
	module + "/internal/fakefb",
	module + "/gen/go/securebrand/fb/",
	module + "/gen/go/securebrand/fbs/",
}

// TestNoPlatformImports enforces the layering of ADR 0004 §2 on every Go
// file under internal/scrape, tests included.
func TestNoPlatformImports(t *testing.T) {
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
				if p == bad || strings.HasPrefix(p, strings.TrimSuffix(bad, "/")+"/") {
					t.Errorf("%s imports %s: internal/scrape must stay platform-neutral", path, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 10 {
		t.Fatalf("only %d Go files found under internal/scrape: is the walk rooted right?", files)
	}
}
