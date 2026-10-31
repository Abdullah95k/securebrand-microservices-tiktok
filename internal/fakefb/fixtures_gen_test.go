//go:build fixturegen

package fakefb

import (
	"testing"
	"time"
)

// Regenerate with: go test -tags fixturegen ./internal/fakefb -run TestWriteFixtures
func TestWriteFixtures(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	spec := DefaultSpec()
	spec.Now = at
	spec.PostsPerPage = 6
	w := NewWorld(spec)
	if err := WriteFixtures("../../testdata/fb", w, "https://www.facebook.com", at); err != nil {
		t.Fatal(err)
	}
}
