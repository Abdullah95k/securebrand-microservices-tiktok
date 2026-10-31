package fixtures

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
)

func TestScrub(t *testing.T) {
	in := `{"author":{"__typename":"User","id":"100012345","name":"Jane Real","url":"https://www.facebook.com/jane.real?x=1"},` +
		`"img":"https://scontent-ams2-1.xx.fbcdn.net/v/t39/123_n.jpg?stp=x&_nc_cat=1&oh=00_ABC&oe=6A1B2C3D",` +
		`"page":{"__typename":"User","id":"100064000","name":"Brand"}}`
	out := string(Scrub([]byte(in), map[string]bool{"100064000": true}))
	for _, leak := range []string{"100012345", "Jane Real", "jane.real", "oh=00_ABC", "oe=6A1B2C3D"} {
		if strings.Contains(out, leak) {
			t.Fatalf("scrubbed output still contains %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, "100064000") {
		t.Fatalf("kept id removed: %s", out)
	}
	if string(Scrub([]byte(in), map[string]bool{"100064000": true})) != out {
		t.Fatal("scrub must be deterministic")
	}
}

// A CDN URL with no path (as page HTML carries, e.g. a preconnect to
// "https://static.xx.fbcdn.net") is replaced too, not a panic.
func TestScrubCDNURLWithoutPath(t *testing.T) {
	in := `<link rel="preconnect" href="https://static.xx.fbcdn.net"><img src="https:\/\/scontent.xx.fbcdn.net?oh=00_X&oe=1">`
	out := string(Scrub([]byte(in), nil))
	if strings.Contains(out, "oh=00_X") || !strings.Contains(out, "https://static.xx.fbcdn.net/scrubbed/") {
		t.Fatalf("scrubbed output: %s", out)
	}
}

// Write never puts the unscrubbed body on disk.
func TestWriteScrubsBeforeWriting(t *testing.T) {
	body := `{"author":{"__typename":"User","id":"100012345","name":"Jane Real"}}`
	d, err := Write(t.TempDir(), "comments", "case", "response.ndjson", []byte(body), map[string]any{"page_id": 1}, []string{"1"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(d, "response.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "Jane Real") || strings.Contains(string(got), "100012345") {
		t.Fatalf("written fixture is not scrubbed: %s", got)
	}
	meta, err := os.ReadFile(filepath.Join(d, "meta.json"))
	if err != nil || !strings.Contains(string(meta), `"__scrubbed": true`) {
		t.Fatalf("meta: %s, %v", meta, err)
	}
}

// Names written with JSON escapes (Facebook writes non-ASCII as \uXXXX)
// are scrubbed too.
func TestScrubEscapedNames(t *testing.T) {
	in := `{"a":{"name":"Jos\u00e9 Garc\u00eda"},"b":{"name":"Ali \"Al\" Hassan"},"c":{"name":"\u0639\u0644\u064a"}}`
	out := string(Scrub([]byte(in), nil))
	for _, leak := range []string{`Jos\u00e9`, `Hassan`, `\u0639\u0644\u064a`} {
		if strings.Contains(out, leak) {
			t.Fatalf("scrubbed output still contains %q: %s", leak, out)
		}
	}
	var v map[string]map[string]string
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("not JSON any more: %v: %s", err, out)
	}
}

// A user id that is part of a longer number (the kept page id) does not
// change that number, and the result does not depend on map order.
func TestScrubReplacesWholeIDsOnly(t *testing.T) {
	in := `{"page":{"__typename":"User","id":"100064000"},"x":{"__typename":"User","id":"1000"},` +
		`"y":{"__typename":"User","id":"10006"},"post":"100064000_77","by":"1000_2"}`
	keep := map[string]bool{"100064000": true}
	out := string(Scrub([]byte(in), keep))
	if !strings.Contains(out, `"id":"100064000"`) || !strings.Contains(out, `"post":"100064000_77"`) {
		t.Fatalf("kept page id changed: %s", out)
	}
	if strings.Contains(out, `"1000"`) || strings.Contains(out, `"10006"`) || strings.Contains(out, `"1000_2"`) {
		t.Fatalf("user ids kept: %s", out)
	}
	for range 20 {
		if string(Scrub([]byte(in), keep)) != out {
			t.Fatal("scrub must be deterministic")
		}
	}
}

// Signed URLs are found however they are escaped or quoted, and nothing
// after them is lost.
func TestScrubSignedURLContexts(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"json in a js string",
			`"{\"u\":\"https:\\\/\\\/scontent.xx.fbcdn.net\\\/v\\\/b.jpg?oh=SECRET\\u0026oe=1\"}"`,
			`"{\"u\":\"https://scontent.xx.fbcdn.net/scrubbed/b.jpg\"}"`},
		{"unquoted attribute",
			`<img src=https://scontent.xx.fbcdn.net/v/a.jpg?oh=SECRET&oe=2><b>x</b>`,
			`<img src=https://scontent.xx.fbcdn.net/scrubbed/a.jpg><b>x</b>`},
		{"entity-encoded json",
			`<i data-store="{&quot;src&quot;:&quot;https:\/\/scontent.xx.fbcdn.net\/a.jpg?oh=SECRET&amp;oe=1&quot;}">`,
			`<i data-store="{&quot;src&quot;:&quot;https://scontent.xx.fbcdn.net/scrubbed/a.jpg&quot;}">`},
		{"single quotes",
			`f('https://scontent.xx.fbcdn.net/a.jpg?_nc_cat=1&oh=SECRET')`,
			`f('https://scontent.xx.fbcdn.net/scrubbed/a.jpg')`},
	} {
		if got := string(Scrub([]byte(tc.in), nil)); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}
	var outer string
	if err := json.Unmarshal(Scrub([]byte(`"{\"u\":\"https:\\\/\\\/scontent.xx.fbcdn.net\\\/b.jpg?oh=S\"}"`), nil), &outer); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(outer), &map[string]string{}); err != nil {
		t.Fatalf("inner JSON broken: %v: %s", err, outer)
	}
}

// ScrubDir scrubs a raw fixture once, keeping the page id from meta.json,
// and leaves a scrubbed one alone.
func TestScrubDirOnce(t *testing.T) {
	dir := t.TempDir()
	raw := `{"page":{"__typename":"User","id":"100064000"},"author":{"__typename":"User","id":"100012345","name":"Jane Real"}}`
	if err := os.WriteFile(filepath.Join(dir, "response.ndjson"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"page_id": 100064000}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ScrubDir(dir, nil); err != nil {
		t.Fatal(err)
	}
	once, _ := os.ReadFile(filepath.Join(dir, "response.ndjson"))
	if strings.Contains(string(once), "Jane Real") || strings.Contains(string(once), "100012345") ||
		!strings.Contains(string(once), `"id":"100064000"`) {
		t.Fatalf("scrubbed: %s", once)
	}
	meta, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	if !strings.Contains(string(meta), `"page_id": 100064000`) || !strings.Contains(string(meta), `"__scrubbed": true`) {
		t.Fatalf("meta: %s", meta)
	}
	if err := ScrubDir(dir, nil); err != nil {
		t.Fatal(err)
	}
	if twice, _ := os.ReadFile(filepath.Join(dir, "response.ndjson")); string(twice) != string(once) {
		t.Fatalf("scrubbed again:\n%s\n%s", once, twice)
	}
}

// The shapes a real logged-out recording carried past the first scrubber:
// a mentioned person (an EntityAtRange whose User entity has __is* keys
// before its id, a short_name, a /people/ URL and the name in the comment
// text), a page owner's profile id inside escaped serialized JSON, an id
// before its __typename, and an escaped profile URL.
func TestScrubRealShapes(t *testing.T) {
	const page, person, owner, other = "100064000000001", "100055512345678", "100044412345678", "100033312345678"
	in := `{"page":{"__typename":"User","id":"` + page + `","name":"Brand"},` +
		`"body":{"text":"Ali Hassan 😀 علي","ranges":[{"offset":0,"length":10,"entity":` +
		`{"__typename":"User","__isEntity":"User","__isNode":"User","id":"` + person + `","short_name":"Ali","name":"Ali Hassan",` +
		`"url":"https:\/\/www.facebook.com\/people\/Ali-Hassan\/` + person + `\/"}}]},` +
		`"political_figure_data":{"owning_profile":{"id":"` + owner + `"}},"badge":{"serialized":"{\"target_id\":` + owner + `}"},` +
		`"x":{"id":"` + other + `","__typename":"User","url":"https:\/\/www.facebook.com\/ali.hassan"},` +
		`"link":"https:\/\/www.facebook.com\/people\/Ali-Hassan\/` + person + `\/",` +
		`"permalink":"https:\/\/www.facebook.com\/permalink.php?story_fbid=123&id=` + page + `","page_url":"https:\/\/www.facebook.com\/BrandPage"}`
	out := string(Scrub([]byte(in), map[string]bool{page: true}))
	for _, leak := range []string{person, owner, other, "Ali", "Hassan", `ع`, "ali.hassan"} {
		if strings.Contains(out, leak) {
			t.Errorf("scrubbed output still contains %q", leak)
		}
	}
	var v struct {
		Page struct{ ID string }
		Body struct {
			Text   string
			Ranges []struct{ Offset, Length int }
		}
	}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("not JSON any more: %v: %s", err, out)
	}
	if v.Page.ID != page {
		t.Errorf("page id changed: %s", v.Page.ID)
	}
	// "Ali Hassan 😀 علي" is 17 UTF-16 units (the emoji is 2); the mention range stays valid.
	if got := utf16Len(v.Body.Text); got != 17 || v.Body.Text[:10] != "xxx xxxxxx" {
		t.Errorf("text %q (%d units)", v.Body.Text, got)
	}
	// The page keeps its name and links; people's links and names go.
	for _, want := range []string{"facebook.com/people/scrubbed/", `"short_name":"Person `, `"name":"Brand"`,
		"permalink.php?story_fbid=123", "https://www.facebook.com/BrandPage", "scrubbed.profile"} {
		if !strings.Contains(out, want) {
			t.Errorf("scrubbed output lacks %q: %s", want, out)
		}
	}
}

func utf16Len(s string) int { return len(utf16.Encode([]rune(s))) }

// A fixture scrubbed by an older scrubber is scrubbed again for what that
// one missed, then left alone.
func TestScrubDirRescrubsOlderVersions(t *testing.T) {
	dir := t.TempDir()
	raw := `{"entity":{"__typename":"User","__isNode":"User","id":"100055512345678","short_name":"Ali"}}`
	if err := os.WriteFile(filepath.Join(dir, "response.ndjson"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"page_id": 1, "__scrubbed": true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ScrubDir(dir, nil); err != nil {
		t.Fatal(err)
	}
	once, _ := os.ReadFile(filepath.Join(dir, "response.ndjson"))
	if strings.Contains(string(once), "100055512345678") || strings.Contains(string(once), "Ali") {
		t.Fatalf("not scrubbed again: %s", once)
	}
	meta, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	if !strings.Contains(string(meta), `"__scrubber": 3`) {
		t.Fatalf("meta: %s", meta)
	}
	if err := ScrubDir(dir, nil); err != nil {
		t.Fatal(err)
	}
	if twice, _ := os.ReadFile(filepath.Join(dir, "response.ndjson")); string(twice) != string(once) {
		t.Fatal("scrubbed again by the same version")
	}
}

// Serialized JSON inside a string is walked too; captions and display
// names go; the page's own text, such as its follower line, stays.
func TestScrubNestedAndPageText(t *testing.T) {
	const page = "100064000000001"
	in := `{"page":{"__typename":"User","id":"` + page + `","name":"Brand","social":[{"text":"1.2M followers"}]},` +
		`"badge":{"serialized":"{\"actor\":{\"__typename\":\"User\",\"id\":\"100055512345678\",\"name\":\"Ali Hassan\"},` +
		`\"body\":{\"text\":\"hello Ali\"}}"},` +
		`"photo":{"accessibility_caption":"May be an image of Ali Hassan"},"who":{"display_name":"Ali Hassan","username":"ali.hassan"}}`
	out := string(Scrub([]byte(in), map[string]bool{page: true}))
	for _, leak := range []string{"Ali", "Hassan", "100055512345678", "ali.hassan"} {
		if strings.Contains(out, leak) {
			t.Errorf("scrubbed output still contains %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, "1.2M followers") || !strings.Contains(out, `"name":"Brand"`) {
		t.Errorf("page text lost: %s", out)
	}
	var v struct{ Badge struct{ Serialized string } }
	if err := json.Unmarshal([]byte(out), &v); err != nil || !json.Valid([]byte(v.Badge.Serialized)) {
		t.Errorf("serialized JSON broken: %v: %s", err, out)
	}
}

// Synthetic fixtures (fakefb, handcrafted) are never scrubbed again: the
// golden files are computed from them as they are.
func TestScrubDirLeavesSyntheticFixtures(t *testing.T) {
	dir := t.TempDir()
	raw := `{"author":{"__typename":"User","id":"100012345","name":"Jane Fake"},"body":{"text":"hi"}}`
	_ = os.WriteFile(filepath.Join(dir, "response.ndjson"), []byte(raw), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"__scrubbed": true, "source": "fakefb"}`), 0o644)
	if err := ScrubDir(dir, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "response.ndjson")); string(got) != raw {
		t.Fatalf("synthetic fixture changed: %s", got)
	}
}

// A response cut off inside a value (a truncated HAR body) is not taken
// as complete: it is scrubbed by pattern, and nothing of it is dropped.
func TestScrubTruncatedJSON(t *testing.T) {
	if _, err := decodeAll(strings.NewReader(`{"a":1}` + "\n" + `{"b":{"c":`)); err == nil {
		t.Fatal("a truncated stream decoded without error")
	}
	in := `{"a":1}` + "\n" + `{"author":{"__typename":"User","id":"100012345","name":"Jane Real"},"rest":`
	out := string(Scrub([]byte(in), nil))
	if strings.Contains(out, "Jane Real") || strings.Contains(out, "100012345") || !strings.HasSuffix(out, `"rest":`) {
		t.Fatalf("scrubbed: %s", out)
	}
}

// Comment text is blanked wherever Facebook nests it: body_renderer in a
// Comment, a post's interesting_top_level_comments, and any text inside a
// comment (an attachment title). The page's own UI text stays.
func TestScrubCommentBodies(t *testing.T) {
	in := `{"interesting_top_level_comments":[{"comment":{"body_renderer":{"__typename":"TextWithEntities","text":"Ali says hi"}}}],` +
		`"edges":[{"node":{"__typename":"Comment","body_renderer":{"__typename":"TextWithEntities","text":"secret words"},` +
		`"attachments":[{"title":{"text":"Hassan gif"}}]}}],` +
		`"intro":{"profile_status_text":{"text":"Mona status"}},"tile":{"title":{"text":"Photos"}}}`
	out := string(Scrub([]byte(in), nil))
	for _, leak := range []string{"Ali", "secret", "Hassan", "Mona"} {
		if strings.Contains(out, leak) {
			t.Errorf("scrubbed output still contains %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, `"text":"Photos"`) {
		t.Errorf("UI text lost: %s", out)
	}
}
