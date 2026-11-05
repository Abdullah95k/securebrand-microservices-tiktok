package discovery

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Matcher finds the keyword list's terms (Iraq, the Kurdistan Region, the
// governorates and their cities) in an account's text and its posts'
// captions and hashtags, for the Iraq score's place signals (discovery-plan
// §3). Text is folded by Normalize first. A term in Arabic script matches
// inside a word, since Arabic attaches prefixes (وبغداد, لبغداد); a Latin
// term matches whole words only (Kut is not in "kutie"). In hashtags, which
// run words together (#baghdadlife), a Latin term of minLatinInTag letters
// or more matches inside the tag too.
type Matcher struct {
	arabic []string
	latin  []string
}

// minLatinInTag is the shortest Latin term matched inside a hashtag.
const minLatinInTag = 5

// NewMatcher builds a matcher from normalized terms.
func NewMatcher(normalized []string) *Matcher {
	m := &Matcher{}
	seen := map[string]bool{}
	for _, t := range normalized {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		if arabicScript(t) {
			m.arabic = append(m.arabic, t)
		} else {
			m.latin = append(m.latin, t)
		}
	}
	return m
}

// Match returns the terms text names, sorted.
func (m *Matcher) Match(text string) []string {
	if m == nil || strings.TrimSpace(text) == "" {
		return nil
	}
	n := Normalize(text)
	var out []string
	for _, t := range m.arabic {
		if strings.Contains(n, t) {
			out = append(out, t)
		}
	}
	for _, t := range m.latin {
		if containsWord(n, t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// MatchTags returns the terms hashtags name, sorted.
func (m *Matcher) MatchTags(tags []string) []string {
	if m == nil || len(tags) == 0 {
		return nil
	}
	found := map[string]bool{}
	for _, tag := range tags {
		n := Normalize(strings.TrimPrefix(tag, "#"))
		if n == "" {
			continue
		}
		joined := strings.ReplaceAll(n, " ", "")
		for _, t := range m.arabic {
			if strings.Contains(n, t) || strings.Contains(joined, strings.ReplaceAll(t, " ", "")) {
				found[t] = true
			}
		}
		for _, t := range m.latin {
			bare := strings.ReplaceAll(t, " ", "")
			if containsWord(n, t) || (len([]rune(bare)) >= minLatinInTag && strings.Contains(joined, bare)) {
				found[t] = true
			}
		}
	}
	out := make([]string, 0, len(found))
	for t := range found {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// containsWord reports term in text with no letter or digit on either side.
func containsWord(text, term string) bool {
	for i := 0; ; {
		j := strings.Index(text[i:], term)
		if j < 0 {
			return false
		}
		start, end := i+j, i+j+len(term)
		if !wordRuneBefore(text, start) && !wordRuneAfter(text, end) {
			return true
		}
		i = start + 1
		if i >= len(text) {
			return false
		}
	}
}

func wordRuneBefore(s string, i int) bool {
	if i == 0 {
		return false
	}
	r := lastRune(s[:i])
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func wordRuneAfter(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	r := []rune(s[i:])[0]
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func lastRune(s string) rune {
	r := []rune(s)
	return r[len(r)-1]
}

func arabicScript(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Arabic, r) {
			return true
		}
	}
	return false
}

// Keywords keeps the enabled keywords' matcher, reloaded from the store
// every TTL, so a keyword turned on or off counts within minutes.
type Keywords struct {
	DB  DB
	TTL time.Duration

	mu     sync.Mutex
	m      *Matcher
	loaded time.Time
}

// Matcher returns the current matcher; a failed reload keeps the last one.
func (k *Keywords) Matcher(ctx context.Context) (*Matcher, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	ttl := k.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if k.m != nil && time.Since(k.loaded) < ttl {
		return k.m, nil
	}
	rows, err := k.DB.Query(ctx, `SELECT normalized FROM discovery_keywords WHERE enabled`)
	if err != nil {
		if k.m != nil {
			return k.m, nil
		}
		return nil, err
	}
	var terms []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		terms = append(terms, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	k.m, k.loaded = NewMatcher(terms), time.Now()
	return k.m, nil
}
