package discprobe

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/iraq"
)

// Record is one request, or (Summary) one item's walk. It names keywords,
// hashtags, sounds, posts and accounts by position only.
type Record struct {
	At       time.Time `json:"at"`
	Source   string    `json:"source"`
	Op       string    `json:"op"`
	Variant  string    `json:"variant,omitempty"`
	Signed   bool      `json:"signed,omitempty"`
	SignPage string    `json:"sign_page,omitempty"`
	// SignRetried marks a signed call refused once and signed again on
	// another page (S4).
	SignRetried bool `json:"sign_retried,omitempty"`
	// Try marks a variant tried on a source's first items.
	Try     bool   `json:"try,omitempty"`
	Summary bool   `json:"summary,omitempty"`
	Item    int    `json:"item,omitempty"`
	Page    int    `json:"page,omitempty"`
	Class   string `json:"class,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Status  int    `json:"status,omitempty"`
	// Code is TikTok's statusCode (status_code) when it is not 0, or a
	// page scope's.
	Code     int64 `json:"code,omitempty"`
	Posts    int   `json:"posts,omitempty"`
	Users    int   `json:"users,omitempty"`
	NewUsers int   `json:"new_users,omitempty"`
	NewPosts int   `json:"new_posts,omitempty"`
	Results  int   `json:"results,omitempty"`
	// HasMore is what the answer said about a next page.
	HasMore *bool    `json:"has_more,omitempty"`
	Shape   string   `json:"shape,omitempty"`
	Keys    []string `json:"keys,omitempty"`
	Pages   int      `json:"pages,omitempty"`
	Stop    string   `json:"stop,omitempty"`
	// Total is an account's follower count (the follow lists' ratio).
	Total     int64          `json:"total,omitempty"`
	Langs     map[string]int `json:"langs,omitempty"`
	Locations map[string]int `json:"locations,omitempty"`
	Listed    *bool          `json:"listed,omitempty"`
	Location  string         `json:"location,omitempty"`
	Region    string         `json:"region,omitempty"`
	Exit      string         `json:"exit,omitempty"`
	Ms        int64          `json:"ms,omitempty"`
	Wire      int64          `json:"wire,omitempty"`
	Note      string         `json:"note,omitempty"`
}

// VariantStat is how one variant of a call was answered.
type VariantStat struct {
	Op       string `json:"op"`
	Variant  string `json:"variant"`
	Signed   bool   `json:"signed"`
	SignPage string `json:"sign_page,omitempty"`
	Calls    int    `json:"calls"`
	OK       int    `json:"ok"`
	// WithResults counts answers holding posts or people.
	WithResults int            `json:"with_results"`
	Classes     map[string]int `json:"classes"`
	Shapes      []string       `json:"shapes,omitempty"`
	Keys        []string       `json:"keys,omitempty"`
}

// SourceStat sums one source.
type SourceStat struct {
	Requests int            `json:"requests"`
	OK       int            `json:"ok"`
	Classes  map[string]int `json:"classes"`
	Items    int            `json:"items"`
	// Pages per item and why each item's walk stopped.
	Pages     []int          `json:"pages,omitempty"`
	Stops     map[string]int `json:"stops,omitempty"`
	Posts     int            `json:"posts"`
	Users     int            `json:"users"`
	NewUsers  int            `json:"new_users"`
	NewPosts  []int          `json:"new_posts,omitempty"`
	Langs     map[string]int `json:"langs,omitempty"`
	Locations map[string]int `json:"locations,omitempty"`
	Codes     map[string]int `json:"codes,omitempty"`
	Notes     map[string]int `json:"notes,omitempty"`
	Wire      int64          `json:"wire_bytes"`
	// Summary lines for a person.
	Lines []string `json:"lines,omitempty"`
}

// Report is the probe's findings.
type Report struct {
	Version  string                 `json:"version"`
	Tier     string                 `json:"tier"`
	Keywords []Keyword              `json:"keywords"`
	Requests int                    `json:"requests"`
	Captchas int                    `json:"captchas"`
	Regions  map[string]int         `json:"regions"`
	Sources  map[string]*SourceStat `json:"sources"`
	Variants []VariantStat          `json:"variants"`
	Pages    []PageCapture          `json:"pages,omitempty"`
	Items    map[string][]Item      `json:"items,omitempty"`
	Issues   []string               `json:"issues,omitempty"`
	Started  time.Time              `json:"started"`
	Ended    time.Time              `json:"ended"`
}

// BuildReport sums the records.
func BuildReport(recs []Record, pages []PageCapture, keywords []Keyword) Report {
	rep := Report{Keywords: keywords, Regions: map[string]int{}, Sources: map[string]*SourceStat{}, Pages: pages}
	variants := map[string]*VariantStat{}
	src := func(name string) *SourceStat {
		s := rep.Sources[name]
		if s == nil {
			s = &SourceStat{Classes: map[string]int{}, Stops: map[string]int{}, Langs: map[string]int{}, Locations: map[string]int{},
				Codes: map[string]int{}, Notes: map[string]int{}}
			rep.Sources[name] = s
		}
		return s
	}
	for _, r := range recs {
		if rep.Started.IsZero() || r.At.Before(rep.Started) {
			rep.Started = r.At
		}
		if r.At.After(rep.Ended) {
			rep.Ended = r.At
		}
		if r.Summary {
			s := src(r.Source)
			s.Items++
			s.Pages = append(s.Pages, r.Pages)
			s.Stops[first(r.Stop, "none")]++
			s.Posts += r.Posts
			s.Users += r.Users
			s.NewUsers += r.NewUsers
			if r.Source == SrcFeed {
				s.NewPosts = append(s.NewPosts, r.NewPosts)
			}
			for k, v := range r.Locations {
				s.Locations[k] += v
			}
			if r.Locations != nil {
				iq := r.Locations["IQ"] > 0
				s.Notes["authors posting from IQ"] += b2i(iq)
				s.Notes["authors read"]++
			}
			if r.Listed != nil {
				s.Notes["gone post still listed="+strconv.FormatBool(*r.Listed)]++
			}
			if r.Results > 0 {
				s.Notes["candidates"] += r.Results
			}
			continue
		}
		rep.Requests++
		if r.Class == "checkpoint" {
			rep.Captchas++
		}
		if r.Region != "" && r.Op == OpBootstrap {
			rep.Regions[r.Region]++
		}
		s := src(r.Source)
		s.Requests++
		s.Classes[r.Class]++
		s.Wire += r.Wire
		if r.Class == "ok" {
			s.OK++
		}
		for k, v := range r.Langs {
			s.Langs[k] += v
		}
		if r.Code != 0 {
			s.Codes[r.Op+":"+strconv.FormatInt(r.Code, 10)]++
		}
		if r.Op == OpPostPage || r.Op == OpProfile || r.Op == OpPage {
			s.Notes[r.Op+" "+first(r.Note, "-")]++
			if r.Op == OpPostPage {
				s.Codes["post_page:"+strconv.FormatInt(r.Code, 10)+" ("+r.Note+")"]++
			}
		}
		if r.Location != "" {
			s.Notes[fmt.Sprintf("short link %d -> %s", r.Status, r.Location)]++
		}
		if r.Variant == "" {
			continue
		}
		key := r.Op + "|" + r.Variant + "|" + r.SignPage
		v := variants[key]
		if v == nil {
			v = &VariantStat{Op: r.Op, Variant: r.Variant, Signed: r.Signed, SignPage: r.SignPage, Classes: map[string]int{}}
			variants[key] = v
		}
		v.Calls++
		v.Classes[r.Class]++
		if r.Class == "ok" {
			v.OK++
			if r.Posts+r.Users+r.Results > 0 {
				v.WithResults++
			}
		}
		if r.Shape != "" && !contains(v.Shapes, r.Shape) {
			v.Shapes = append(v.Shapes, r.Shape)
		}
		if v.Keys == nil && len(r.Keys) > 0 && r.Class == "ok" {
			v.Keys = r.Keys
		}
	}
	for _, v := range variants {
		rep.Variants = append(rep.Variants, *v)
	}
	sort.Slice(rep.Variants, func(i, j int) bool {
		a, b := rep.Variants[i], rep.Variants[j]
		if a.Op != b.Op {
			return a.Op < b.Op
		}
		return a.Variant+a.SignPage < b.Variant+b.SignPage
	})
	for name, s := range rep.Sources {
		s.Lines = lines(name, s)
	}
	return rep
}

func lines(name string, s *SourceStat) []string {
	var out []string
	if s.Items > 0 {
		out = append(out, fmt.Sprintf("%d items: pages %s; stops %s", s.Items, spreadOf(s.Pages), mapLine(s.Stops)))
	}
	if s.Posts+s.Users > 0 {
		out = append(out, fmt.Sprintf("%d posts, %d people (authors included), %d new to tks (not on your list; each counted once per source)", s.Posts, s.Users, s.NewUsers))
	}
	if len(s.NewPosts) > 0 {
		out = append(out, fmt.Sprintf("new posts per read: %v", s.NewPosts))
	}
	if len(s.Langs) > 0 {
		out = append(out, "post languages: "+mapLine(s.Langs))
	}
	if n := s.Notes["authors read"]; n > 0 && name == SrcEntry {
		out = append(out, fmt.Sprintf("%d of %d found authors have a post made in Iraq on their list's first page; post regions: %s",
			s.Notes["authors posting from IQ"], n, mapLine(s.Locations)))
	}
	if len(s.Codes) > 0 {
		out = append(out, "codes: "+mapLine(s.Codes))
	}
	return out
}

func spreadOf(xs []int) string {
	if len(xs) == 0 {
		return "-"
	}
	s := append([]int(nil), xs...)
	sort.Ints(s)
	return fmt.Sprintf("min %d, median %d, max %d", s[0], s[len(s)/2], s[len(s)-1])
}

func mapLine(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// FormatReport writes the report for a person.
func FormatReport(rep Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tks discovery probe (C6) %s, tier %s\n", rep.Version, rep.Tier)
	fmt.Fprintf(&b, "%d requests from %s to %s, %d captchas; region served to the exits: %s\n\n",
		rep.Requests, iraq.Format(rep.Started), iraq.Format(rep.Ended), rep.Captchas, mapLine(rep.Regions))
	b.WriteString("Calls (each variant on its source's first items, then the one that answered):\n")
	for _, v := range rep.Variants {
		sp := ""
		if v.SignPage != "" {
			sp = " on a " + v.SignPage + " page"
		}
		fmt.Fprintf(&b, "  %-17s %-11s%s: %d/%d answered, %d with results (%s)", v.Op, v.Variant, sp, v.OK, v.Calls, v.WithResults, mapLine(v.Classes))
		if len(v.Shapes) > 0 {
			fmt.Fprintf(&b, "; in %s", strings.Join(v.Shapes, " "))
		}
		b.WriteString("\n")
	}
	for _, name := range Sources() {
		s := rep.Sources[name]
		if s == nil {
			continue
		}
		fmt.Fprintf(&b, "\n%s: %d requests, %d ok (%s), %.1f MB\n", name, s.Requests, s.OK, mapLine(s.Classes), float64(s.Wire)/1e6)
		for _, l := range s.Lines {
			fmt.Fprintf(&b, "  %s\n", l)
		}
		for _, k := range sortedKeys(s.Notes) {
			if strings.HasPrefix(k, "short link") || strings.HasPrefix(k, "profile_page") || strings.HasPrefix(k, "gone post") ||
				strings.HasPrefix(k, "page ") || k == "candidates" {
				fmt.Fprintf(&b, "  %s: %d\n", k, s.Notes[k])
			}
		}
	}
	if len(rep.Pages) > 0 {
		b.WriteString("\nThe browser's own calls on each page (path: times; parameters):\n")
		for _, p := range rep.Pages {
			fmt.Fprintf(&b, "  %s (%s, %.0f MB):\n", p.Page, first(p.Error, "ok"), p.MemoryMB)
			for _, c := range p.Calls {
				fmt.Fprintf(&b, "    %s %d: %d posts, %d people, signed %v; %s\n", c.Path, c.Status, c.Posts, c.Users, c.Signed, strings.Join(c.Params, ","))
			}
		}
	}
	if len(rep.Issues) > 0 {
		b.WriteString("\nNotes:\n")
		for _, i := range rep.Issues {
			fmt.Fprintf(&b, "  %s\n", i)
		}
	}
	return b.String()
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
