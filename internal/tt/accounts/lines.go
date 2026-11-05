// Package accounts registers TikTok accounts for scheduled checks: one at a
// time for POST /v1/accounts, in bulk for imports (docs/tiktok/
// discovery-plan.md §6), and, from M10c on, for the accounts discovery
// adds. Every way records where the account came from (account_sources),
// the client's labels, and lifts the client's own block; a new schedule
// waits for paced admission (tt.admission) before its first read.
package accounts

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Line is one account of an import: its 1-based line number in the file,
// the text as given, and what it asks for.
type Line struct {
	N     int
	Input string
	// Ref is the normalised reference (task.NormalizeRef); empty when the
	// line was not accepted, with Reason saying why.
	Ref    string
	Reason string
	// Per-line settings of a CSV import; nil keeps the defaults.
	Labels       []string
	IntervalMins *int
	MaxAgeDays   *int
	IncludeMedia *bool
}

// Reasons a line is not accepted.
const (
	ReasonShortLink = "short links (vm.tiktok.com, tiktok.com/t/) are not supported yet: resolving one is a request to TikTok (M12)"
	ReasonInvalid   = "not a TikTok username, profile or video link, secUid or id: reference"
)

// MaxLines bounds one import: a larger file goes in chunks (the API takes
// MaxAPILines a call).
const (
	MaxLines    = 500_000
	MaxAPILines = 1000
)

// Clean removes what copying and pasting adds to a line: a byte-order
// mark, zero-width spaces and joiners, direction marks and isolates, other
// format characters, a Windows line end, and full-width forms of ASCII
// (＠ becomes @); spaces at either end go too.
func Clean(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\r', r == '\uFEFF', unicode.Is(unicode.Cf, r):
			continue
		case r >= 0xFF01 && r <= 0xFF5E: // full-width ASCII
			r -= 0xFEE0
		case r == 0x3000: // ideographic space
			r = ' '
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

// ParseRef reads one account: a username or @username, a profile or video
// link (any www. or m. host, any query), a secUid, sec:<secUid>,
// u:<username> or id:<user id>. Usernames are lowercased.
func ParseRef(raw string) (string, string) {
	s := Clean(raw)
	low := strings.ToLower(s)
	for _, short := range []string{"vm.tiktok.com/", "vt.tiktok.com/", "tiktok.com/t/"} {
		if strings.Contains(low, short) {
			return "", ReasonShortLink
		}
	}
	if looksLikeSecUID(s) {
		s = "sec:" + s
	}
	ref, err := task.NormalizeRef(s)
	if err != nil {
		return "", ReasonInvalid
	}
	return ref, ""
}

// looksLikeSecUID: a secUid is URL-safe base64 of a fixed header (every
// one seen starts MS4wLjABAAAA) and longer than any username.
func looksLikeSecUID(s string) bool {
	if strings.HasPrefix(s, "MS4wLjABAAAA") {
		return true
	}
	if len(s) <= 64 {
		return false
	}
	for _, r := range s {
		if r != '-' && r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

// Parse reads an import: one account a line, or a CSV whose header names an
// `account` column and optionally `labels` (separated by | or ;),
// `interval` (minutes), `max_age_days` and `include_media`. Blank lines and
// lines starting with # are skipped. Every other line comes back, accepted
// or not.
func Parse(r io.Reader) ([]Line, error) {
	br := bufio.NewReader(r)
	var lines []string
	for {
		s, err := br.ReadString('\n')
		if s != "" || err == nil {
			lines = append(lines, strings.TrimSuffix(s, "\n"))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(lines) > MaxLines {
			return nil, fmt.Errorf("more than %d lines: split the file", MaxLines)
		}
	}
	first := -1
	for i, l := range lines {
		if c := Clean(l); c != "" && !strings.HasPrefix(c, "#") {
			first = i
			break
		}
	}
	if first >= 0 {
		if cols, ok := csvHeader(Clean(lines[first])); ok {
			return parseCSV(lines, first, cols)
		}
	}
	var out []Line
	for i, l := range lines {
		c := Clean(l)
		if c == "" || strings.HasPrefix(c, "#") {
			continue
		}
		ln := Line{N: i + 1, Input: c}
		ln.Ref, ln.Reason = ParseRef(c)
		out = append(out, ln)
	}
	return out, nil
}

// csvHeader recognises a CSV header line by its account column.
func csvHeader(s string) (map[string]int, bool) {
	if !strings.Contains(s, ",") {
		return nil, false
	}
	rec, err := csv.NewReader(strings.NewReader(s)).Read()
	if err != nil {
		return nil, false
	}
	cols := map[string]int{}
	for i, c := range rec {
		cols[strings.ToLower(Clean(c))] = i
	}
	_, ok := cols["account"]
	return cols, ok
}

func parseCSV(lines []string, header int, cols map[string]int) ([]Line, error) {
	var out []Line
	for i := header + 1; i < len(lines); i++ {
		c := Clean(lines[i])
		if c == "" || strings.HasPrefix(c, "#") {
			continue
		}
		ln := Line{N: i + 1, Input: c}
		rd := csv.NewReader(strings.NewReader(c))
		rd.FieldsPerRecord = -1
		rec, err := rd.Read()
		if err != nil {
			ln.Reason = "unreadable CSV line: " + err.Error()
			out = append(out, ln)
			continue
		}
		field := func(name string) string {
			if j, ok := cols[name]; ok && j < len(rec) {
				return Clean(rec[j])
			}
			return ""
		}
		ln.Input = field("account")
		if ln.Ref, ln.Reason = ParseRef(ln.Input); ln.Reason != "" {
			out = append(out, ln)
			continue
		}
		if err := ln.settings(field("labels"), field("interval"), field("max_age_days"), field("include_media")); err != nil {
			ln.Ref, ln.Reason = "", err.Error()
		}
		out = append(out, ln)
	}
	return out, nil
}

func (ln *Line) settings(labels, interval, maxAge, media string) error {
	for _, l := range strings.FieldsFunc(labels, func(r rune) bool { return r == '|' || r == ';' }) {
		if l = strings.TrimSpace(l); l != "" {
			if err := CheckLabel(l); err != nil {
				return err
			}
			ln.Labels = append(ln.Labels, l)
		}
	}
	if interval != "" {
		v, err := strconv.Atoi(interval)
		if err != nil || v < MinIntervalMinutes || v > MaxIntervalMinutes {
			return fmt.Errorf("interval must be minutes between %d and %d", MinIntervalMinutes, MaxIntervalMinutes)
		}
		ln.IntervalMins = &v
	}
	if maxAge != "" {
		v, err := strconv.Atoi(maxAge)
		if err != nil || v < 1 {
			return errors.New("max_age_days must be a positive number of days")
		}
		ln.MaxAgeDays = &v
	}
	if media != "" {
		v, err := strconv.ParseBool(media)
		if err != nil {
			return errors.New("include_media must be true or false")
		}
		ln.IncludeMedia = &v
	}
	return nil
}

// Interval bounds: no faster than every 30 minutes, no slower than monthly.
const (
	MinIntervalMinutes = 30
	MaxIntervalMinutes = 30 * 24 * 60
)

// CheckLabel accepts a label of 1 to 64 printable characters without
// spaces or commas (list:banks, import:2026-11-05).
func CheckLabel(l string) error {
	if l == "" || len([]rune(l)) > 64 || strings.ContainsFunc(l, func(r rune) bool {
		return unicode.IsSpace(r) || r == ',' || !unicode.IsPrint(r)
	}) {
		return fmt.Errorf("label %q must be 1 to 64 printable characters without spaces or commas", l)
	}
	return nil
}
