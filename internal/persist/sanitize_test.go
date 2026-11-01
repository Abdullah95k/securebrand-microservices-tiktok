package persist

import (
	"strings"
	"testing"
)

// JSON with a \u0000 escape is rewritten without it, and its numbers stay
// exact: a 64-bit Facebook id must not be rounded.
func TestJSONBSafeKeepsNumbers(t *testing.T) {
	got := string(jsonbSafe([]byte(`{"owner_id":100012345678901234,"bio":"a\u0000b"}`)))
	if !strings.Contains(got, `100012345678901234`) || strings.Contains(got, `\u0000`) || !strings.Contains(got, `"ab"`) {
		t.Fatalf("got %s", got)
	}
}
