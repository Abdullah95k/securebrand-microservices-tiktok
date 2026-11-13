package graphql

import "testing"

// A bucket gives a page's calls of one kind a limit of their own.
func TestPageKey(t *testing.T) {
	if got := pageKey(Request{PageID: 42}); got != "page:42" {
		t.Errorf("no bucket: %q", got)
	}
	if got := pageKey(Request{PageID: 42, PageBucket: "list"}); got != "page:42:list" {
		t.Errorf("list bucket: %q", got)
	}
}
