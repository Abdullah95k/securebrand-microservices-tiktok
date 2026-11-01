package detect

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Overrides replace parts of a platform's built-in rules from
// configuration, so a code, marker or status TikTok or Facebook starts
// answering with can be classed without a deploy. A list that is set
// replaces the built-in one; a list left unset keeps it. config.Detect has
// the same fields (and converts to Overrides).
type Overrides struct {
	RateLimitCodes   []int64
	SoftCodes        []int64
	LoginCodes       []int64
	CheckpointCodes  []int64
	UnavailableCodes []int64
	// StaleMessages are regular expressions matched against error
	// messages.
	StaleMessages         []string
	LoginMarkers          []string
	BlockMarkers          []string
	CheckpointPaths       []string
	LoginPaths            []string
	CheckpointPageMarkers []string
	LoginPageMarkers      []string
	// Statuses classes HTTP statuses before the built-in rules: the key is
	// a status ("403"), or a kind of request and a status ("html:400",
	// "graphql:404"); the value a class name (Class.String: rate_limited,
	// login_wall, checkpoint, soft_error, registry_stale,
	// content_unavailable, transport, ok). They are added to the rules'
	// own Statuses, ahead of them.
	Statuses map[string]string
}

// ParseClass returns the class named name (Class.String).
func ParseClass(name string) (Class, error) {
	for i, n := range classNames {
		if n == name {
			return Class(i), nil
		}
	}
	return 0, fmt.Errorf("unknown class %q (want one of %s)", name, strings.Join(classNames[:], ", "))
}

// Apply returns r with o's lists in place of r's. It fails on a regular
// expression, status or class it cannot read.
func (r Rules) Apply(o Overrides) (Rules, error) {
	var errs []error
	codes := func(dst *[]int64, src []int64) {
		if src != nil {
			*dst = append([]int64(nil), src...)
		}
	}
	marks := func(dst *[][]byte, src []string) {
		if src != nil {
			*dst = make([][]byte, len(src))
			for i, m := range src {
				(*dst)[i] = []byte(m)
			}
		}
	}
	strs := func(dst *[]string, src []string) {
		if src != nil {
			*dst = append([]string(nil), src...)
		}
	}
	codes(&r.RateLimitCodes, o.RateLimitCodes)
	codes(&r.SoftCodes, o.SoftCodes)
	codes(&r.LoginCodes, o.LoginCodes)
	codes(&r.CheckpointCodes, o.CheckpointCodes)
	codes(&r.UnavailableCodes, o.UnavailableCodes)
	if o.StaleMessages != nil {
		r.StaleMessages = make([]*regexp.Regexp, 0, len(o.StaleMessages))
		for _, m := range o.StaleMessages {
			re, err := regexp.Compile(m)
			if err != nil {
				errs = append(errs, fmt.Errorf("stale_messages: %w", err))
				continue
			}
			r.StaleMessages = append(r.StaleMessages, re)
		}
	}
	marks(&r.LoginMarkers, o.LoginMarkers)
	marks(&r.BlockMarkers, o.BlockMarkers)
	strs(&r.CheckpointPaths, o.CheckpointPaths)
	strs(&r.LoginPaths, o.LoginPaths)
	marks(&r.CheckpointPageMarkers, o.CheckpointPageMarkers)
	marks(&r.LoginPageMarkers, o.LoginPageMarkers)
	if len(o.Statuses) > 0 {
		var added []StatusRule
		for k, v := range o.Statuses {
			expect, status, ok := strings.Cut(k, ":")
			if !ok {
				expect, status = "", k
			}
			n, err := strconv.Atoi(status)
			if err != nil || n < 100 || n > 599 || (expect != "" && expect != "graphql" && expect != "html") {
				errs = append(errs, fmt.Errorf("statuses: %q is not a status or kind:status (graphql or html)", k))
				continue
			}
			c, err := ParseClass(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("statuses[%s]: %w", k, err))
				continue
			}
			added = append(added, StatusRule{Status: n, Expect: expect, Class: c})
		}
		// In a fixed order (not the map's), a rule for one kind of request
		// before one for both.
		slices.SortFunc(added, func(a, b StatusRule) int {
			return cmp.Or(cmp.Compare(a.Status, b.Status), -cmp.Compare(a.Expect, b.Expect))
		})
		r.Statuses = append(added, r.Statuses...)
	}
	return r, errors.Join(errs...)
}
