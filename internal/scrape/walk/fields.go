package walk

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Report lists fields that could not be found.
type Report struct {
	Missing  []string // required fields: the response is drifted
	Optional []string // optional fields: metrics only
}

// AddMissing records a required field that was not found.
func (r *Report) AddMissing(field string) { r.Missing = append(r.Missing, field) }

// AddOptional records an optional field that was not found.
func (r *Report) AddOptional(field string) { r.Optional = append(r.Optional, field) }

// OK reports whether every required field was found.
func (r *Report) OK() bool { return len(r.Missing) == 0 }

// Merge appends other's findings.
func (r *Report) Merge(o Report) {
	r.Missing = append(r.Missing, o.Missing...)
	r.Optional = append(r.Optional, o.Optional...)
}

// Int64 parses a JSON number or numeric string.
func Int64(r gjson.Result) int64 {
	switch r.Type {
	case gjson.Number:
		return r.Int()
	case gjson.String:
		v, _ := strconv.ParseInt(strings.TrimSpace(r.Str), 10, 64)
		return v
	}
	return 0
}

// Unix converts a unix-seconds field to a timestamp.
func Unix(r gjson.Result) *timestamppb.Timestamp {
	s := Int64(r)
	if s <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(s, 0).UTC())
}

var countRe = regexp.MustCompile(`(?i)([\d][\d.,]*)\s*([kmb])?`)

// ParseCount parses "1,234", "1.2K", "3.4M followers" into a number.
func ParseCount(s string) (int64, bool) {
	m := countRe.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	num := m[1]
	mult := 1.0
	switch strings.ToLower(m[2]) {
	case "k":
		mult = 1e3
	case "m":
		mult = 1e6
	case "b":
		mult = 1e9
	}
	if mult == 1 {
		num = strings.NewReplacer(",", "", ".", "").Replace(num)
	} else {
		num = strings.ReplaceAll(num, ",", ".")
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, false
	}
	return int64(f*mult + 0.5), true
}

// URLExpiry reads the hex "oe" expiry of a signed Meta CDN URL (fbcdn.net,
// cdninstagram.com).
func URLExpiry(raw string) *timestamppb.Timestamp {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	oe := u.Query().Get("oe")
	if oe == "" {
		return nil
	}
	v, err := strconv.ParseInt(oe, 16, 64)
	if err != nil || v <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(v, 0).UTC())
}
