package htmlx

import (
	"fmt"
	"regexp"
	"testing"
)

// Patterns apply in order: a later match replaces an earlier one, a fallback
// fills only a key still empty, and keys without a match are absent.
func TestExtract(t *testing.T) {
	doc := []byte(`{"token":"first"} <input name="tok" value="second"> {"rev":5,"crev":7,"hsi":"99"}`)
	ps := []Pattern{
		{Key: "tok", Re: regexp.MustCompile(`"token":"([^"]+)"`)},
		{Key: "tok", Re: regexp.MustCompile(`name="tok" value="([^"]+)"`)},
		{Key: "rev", Re: regexp.MustCompile(`"rev":(\d+)`)},
		{Key: "rev", Re: regexp.MustCompile(`"crev":(\d+)`), Fallback: true},
		{Key: "crev", Re: regexp.MustCompile(`"nothing":(\d+)`)},
		{Key: "crev", Re: regexp.MustCompile(`"crev":(\d+)`), Fallback: true},
		{Key: "missing", Re: regexp.MustCompile(`"absent":"([^"]+)"`)},
	}
	got := Extract(doc, ps)
	want := map[string]string{"tok": "second", "rev": "5", "crev": "7"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestContainsAny(t *testing.T) {
	doc := []byte(`<form action="/accounts/login/">`)
	if !ContainsAny(doc, [][]byte{[]byte("nope"), []byte(`/accounts/login/`)}) {
		t.Fatal("marker not found")
	}
	if ContainsAny(doc, nil) || ContainsAny(doc, [][]byte{[]byte("nope")}) {
		t.Fatal("found a marker that is not there")
	}
}

// Payloads with extra attributes (Instagram's data-sjs) are read; scripts
// that are not JSON, or not valid JSON, are skipped.
func TestJSONBlobs(t *testing.T) {
	html := []byte(`<script type="application/json" data-content-len="12" data-sjs>{"a":1}</script>` +
		`<script>{"b":2}</script>` +
		`<script type="application/json">{"broken":</script>` +
		`<script type="application/json">
{"c":[3]}
</script>`)
	blobs := JSONBlobs(html)
	if len(blobs) != 2 || blobs[0].Get("a").Int() != 1 || blobs[1].Get("c.0").Int() != 3 {
		t.Fatalf("blobs %v", blobs)
	}
}
