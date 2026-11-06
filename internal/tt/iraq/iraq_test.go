package iraq

import (
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	at := time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)
	if got, want := Format(at), "2026-11-07 12:00 Iraq time (UTC+3)"; got != want {
		t.Fatalf("Format = %q, want %q", got, want)
	}
	if got := Format(time.Time{}); got != "-" {
		t.Fatalf("zero: %q", got)
	}
}
