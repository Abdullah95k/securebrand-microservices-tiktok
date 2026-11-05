package accounts

import (
	"strings"
	"testing"
)

// Every way the plan lists of naming an account reads to one reference;
// what is not an account says why (docs/tiktok/discovery-plan.md §6).
func TestParseRef(t *testing.T) {
	sec := "MS4wLjABAAAA" + strings.Repeat("x", 64)
	cases := []struct{ in, ref, reason string }{
		{"someone", "u:someone", ""},
		{"@SomeOne", "u:someone", ""},
		{"  @someone  ", "u:someone", ""},
		{"https://www.tiktok.com/@some.one?lang=en", "u:some.one", ""},
		{"https://m.tiktok.com/@some_one", "u:some_one", ""},
		{"tiktok.com/@someone/video/7400000000000000001?is_from_webapp=1", "u:someone", ""},
		{"https://www.tiktok.com/@someone/photo/7400000000000000001", "u:someone", ""},
		{sec, "sec:" + sec, ""},
		{"sec:" + sec, "sec:" + sec, ""},
		{"id:7001", "id:7001", ""},
		{"u:Someone", "u:someone", ""},
		{"12345", "u:12345", ""}, // a bare number is a username
		// Copy and paste: a byte-order mark, zero-width spaces, direction
		// marks, a Windows line end, a full-width @.
		{"\uFEFF@someone\r", "u:someone", ""},
		{"some\u200Bone", "u:someone", ""},
		{"\u200F@someone\u200E", "u:someone", ""},
		{"\uFF20someone", "u:someone", ""},
		{"https://vm.tiktok.com/ZMabc123/", "", ReasonShortLink},
		{"https://www.tiktok.com/t/ZTabc123/", "", ReasonShortLink},
		{"some one", "", ReasonInvalid},
		{"someone!", "", ReasonInvalid},
		{"https://www.tiktok.com/", "", ReasonInvalid},
		{"...", "", ReasonInvalid},
		{"id:abc", "", ReasonInvalid},
	}
	for _, c := range cases {
		ref, reason := ParseRef(c.in)
		if ref != c.ref || reason != c.reason {
			t.Errorf("ParseRef(%q) = %q, %q; want %q, %q", c.in, ref, reason, c.ref, c.reason)
		}
	}
}

func TestParsePlainList(t *testing.T) {
	in := "\uFEFF# the banks\r\nfirst\r\n\r\n@second\n  # a comment\nnot valid!\nhttps://vm.tiktok.com/x/\nfirst"
	lines, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		n      int
		ref    string
		reason string
	}{{2, "u:first", ""}, {4, "u:second", ""}, {6, "", ReasonInvalid}, {7, "", ReasonShortLink}, {8, "u:first", ""}}
	if len(lines) != len(want) {
		t.Fatalf("%d lines: %+v", len(lines), lines)
	}
	for i, w := range want {
		if l := lines[i]; l.N != w.n || l.Ref != w.ref || l.Reason != w.reason {
			t.Errorf("line %d: %+v, want %+v", i, l, w)
		}
	}
}

func TestParseCSV(t *testing.T) {
	in := "# exported\nAccount,Labels,Interval,max_age_days,include_media\n" +
		"@first,list:banks|tier:1,60,30,true\n" +
		"second,,,,\n" +
		"third,bad label,,,\n" +
		"fourth,,10,,\n" +
		"fifth,,,,maybe\n" +
		"\"https://www.tiktok.com/@sixth\",list:x;list:y,,,\n"
	lines, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 6 {
		t.Fatalf("%d lines: %+v", len(lines), lines)
	}
	l := lines[0]
	if l.N != 3 || l.Ref != "u:first" || strings.Join(l.Labels, ",") != "list:banks,tier:1" || *l.IntervalMins != 60 ||
		*l.MaxAgeDays != 30 || !*l.IncludeMedia {
		t.Errorf("first: %+v", l)
	}
	if l := lines[1]; l.Ref != "u:second" || l.Labels != nil || l.IntervalMins != nil || l.MaxAgeDays != nil || l.IncludeMedia != nil {
		t.Errorf("second: %+v", l)
	}
	for _, i := range []int{2, 3, 4} {
		if lines[i].Ref != "" || lines[i].Reason == "" {
			t.Errorf("line %d accepted: %+v", lines[i].N, lines[i])
		}
	}
	if l := lines[5]; l.Ref != "u:sixth" || strings.Join(l.Labels, ",") != "list:x,list:y" {
		t.Errorf("sixth: %+v", l)
	}
	// One column with a comma-free header is a plain list, not a CSV.
	lines, _ = Parse(strings.NewReader("account\nsomeone\n"))
	if len(lines) != 2 || lines[0].Ref != "u:account" {
		t.Errorf("a one-column file: %+v", lines)
	}
}

func TestCheckLabel(t *testing.T) {
	for _, ok := range []string{"list:banks", "import:2026-11-05", "قائمة"} {
		if CheckLabel(ok) != nil {
			t.Errorf("%q refused", ok)
		}
	}
	for _, bad := range []string{"", "a b", "a,b", strings.Repeat("x", 65), "a\tb"} {
		if CheckLabel(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
