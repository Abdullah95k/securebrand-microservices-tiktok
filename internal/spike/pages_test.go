package spike

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPages(t *testing.T) {
	csv := `# stratified sample
page,size,category,region,language
https://www.facebook.com/brand_a, small , Brand, US, en

brand_b,mega,news,eu,de
profile.php?id=123,,,apac
`
	ps, err := ReadPages(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 3 || ps[0].Ref != "https://www.facebook.com/brand_a" || ps[0].Labels["category"] != "brand" ||
		ps[0].Labels["size"] != "small" || ps[1].Labels["region"] != "eu" || len(ps[2].Labels) != 1 {
		t.Fatalf("%+v", ps)
	}
	plain, err := ReadPages(strings.NewReader("brand_a\n# c\nbrand_b,ignored\n"))
	if err != nil || len(plain) != 2 || plain[1].Ref != "brand_b" || plain[1].Labels != nil {
		t.Fatalf("%+v %v", plain, err)
	}
	if _, err := ReadPages(strings.NewReader("# nothing\n")); !errors.Is(err, ErrNoPages) {
		t.Fatalf("want ErrNoPages, got %v", err)
	}
}

func TestDoneAndRemaining(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.jsonl")
	done, err := DoneFile(path)
	if err != nil || len(done) != 0 {
		t.Fatal(done, err)
	}
	_ = os.WriteFile(path, []byte(`{"type":"page","page":"v:brand_a"}
{"type":"page_done","page":"v:brand_a"}
{"type":"page","page":"v:brand_b"}
not json
`), 0o644)
	done, err = DoneFile(path)
	if err != nil || !done["v:brand_a"] || done["v:brand_b"] {
		t.Fatal(done, err)
	}
	left := Remaining([]PageRef{{Ref: "https://www.facebook.com/brand_a"}, {Ref: "brand_b"}}, done)
	if len(left) != 1 || left[0].Ref != "brand_b" {
		t.Fatalf("%+v", left)
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		`proxyconnect tcp: http://user-country-us-session-ab12:p4ss@gw.example:7777: refused`: `proxyconnect tcp: http://***@gw.example:7777: refused`,
		`Get "https://www.facebook.com/x": EOF`:                                               `Get "https://www.facebook.com/x": EOF`,
		`socks5://u:p@1.2.3.4:1080 and http://a@b`:                                            `socks5://***@1.2.3.4:1080 and http://***@b`,
	} {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCheckFixtures(t *testing.T) {
	checks, err := CheckFixtures("../../testdata/fb")
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) < 6 {
		t.Fatalf("checks: %d", len(checks))
	}
	for _, c := range checks {
		if !c.OK() {
			t.Fatalf("committed fixture fails: %+v", c)
		}
	}
	root := t.TempDir()
	broken := filepath.Join(root, "timeline", "20260101_broken")
	_ = os.MkdirAll(broken, 0o755)
	_ = os.WriteFile(filepath.Join(broken, "response.ndjson"), []byte(`{"data":{"node":{"__typename":"User"}}}`), 0o644)
	_ = os.WriteFile(filepath.Join(broken, "meta.json"), []byte(`{"page_id":1}`), 0o644)
	unknown := filepath.Join(root, "mystery", "x")
	_ = os.MkdirAll(unknown, 0o755)
	checks, err = CheckFixtures(root)
	if err != nil || len(checks) != 2 {
		t.Fatal(checks, err)
	}
	for _, c := range checks {
		if c.OK() {
			t.Fatalf("must fail: %+v", c)
		}
	}
	if out := FormatFixtureChecks(checks); !strings.Contains(out, "FAIL timeline") || !strings.Contains(out, "no parser") {
		t.Fatal(out)
	}
}

func TestMergeDocIDs(t *testing.T) {
	seed := embeddedSeed(t)
	merged, res := MergeDocIDs(seed, map[string]string{"CommentsListComponentsPaginationQuery": "42", "Unrelated": "1"})
	if len(res.Updated) != 3 || len(res.Missing) != 2 || res.Found["CommentsListComponentsPaginationQuery"] != "42" {
		t.Fatalf("%+v", res)
	}
	for _, q := range merged.Queries {
		if q.FriendlyName == "CommentsListComponentsPaginationQuery" && q.DocID != "42" {
			t.Fatalf("%s not updated", q.Name)
		}
	}
	if seed.Queries[1].DocID == "42" {
		t.Fatal("MergeDocIDs must not modify its input")
	}
}
