package tt_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/abdullah95k/securebrand-microservices-tiktok"

// forbidden are the imports TikTok code may not use: Facebook's client,
// parsers, fake and contracts (docs/tiktok ADR 0006 §2).
var forbidden = []string{
	module + "/internal/fb",
	module + "/internal/fakefb",
	module + "/gen/go/securebrand/fb/",
	module + "/gen/go/securebrand/fbs/",
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
				if p == bad || strings.HasPrefix(p, strings.TrimSuffix(bad, "/")+"/") {
					t.Errorf("%s imports %s: TikTok code must not depend on Facebook code", path, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 3 {
		t.Fatalf("only %d Go files found under internal/tt: is the walk rooted right?", files)
	}
}
