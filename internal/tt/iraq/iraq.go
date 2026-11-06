// Package iraq gives the owner's time zone: every time a person reads in
// a tks report is in Iraq time and says so (M16, B7).
package iraq

import "time"

// Zone is Iraq's time (UTC+3, no summer time).
var Zone = time.FixedZone("Iraq (UTC+3)", 3*3600)

// Layout is the minute-precision layout reports print times with.
const Layout = "2006-01-02 15:04"

// Label follows a time printed in Iraq time.
const Label = "Iraq time (UTC+3)"

// Format prints t in Iraq time with its label ("2026-11-07 12:00 Iraq time
// (UTC+3)"); the zero time is "-".
func Format(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.In(Zone).Format(Layout) + " " + Label
}
