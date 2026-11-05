package discovery

import (
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

// Signals of the Iraq score (docs/tiktok/discovery-plan.md §3), as named in
// the evidence.
const (
	// Strong.
	SignalPostsInIraq = "posts_in_iraq" // posts made in Iraq (locationCreated)
	SignalTextPlace   = "text_place"    // Iraq or an Iraqi place in the bio or name
	SignalPhone       = "phone"         // an Iraqi phone number in the bio
	SignalIQLink      = "iq_link"       // a .iq link in the bio
	// Medium, strong when many.
	SignalCaptionPlaces    = "caption_places"     // Iraqi places in captions or hashtags
	SignalFollowedByIraqi  = "followed_by_iraqi"  // in monitored accounts' following lists
	SignalMentionedByIraqi = "mentioned_by_iraqi" // mentioned or replied to by monitored accounts
	SignalShownInIraq      = "shown_in_iraq"      // served to viewers in Iraq (search, hashtags, the feed)
	// Weak.
	SignalArabicKurdish = "arabic_kurdish" // posts written in Arabic or Kurdish
	SignalEngagesIraqi  = "engages_iraqi"  // comments on or follows monitored accounts
)

// Strengths.
const (
	Strong = "strong"
	Medium = "medium"
	Weak   = "weak"
)

// Audience signals come from the sources that found the account
// (account_sources): how many different monitored accounts or searches led
// to it, by source. Each source feeds one signal.
var audienceSignal = map[string]string{
	"following": SignalFollowedByIraqi,
	"mention":   SignalMentionedByIraqi,
	"reply":     SignalMentionedByIraqi,
	"commenter": SignalEngagesIraqi,
	"follower":  SignalEngagesIraqi,
}

// ShownInIraqSources are the sources whose finds count as shown to viewers
// in Iraq when TikTok served them to an exit it placed there (the source's
// evidence carries "region": "IQ"; discovery-plan §8.3).
var ShownInIraqSources = []string{"search", "hashtag", "sound", "feed", "related"}

// AccountText is what the score reads of the account itself.
type AccountText struct {
	Nickname string
	Bio      string
	BioLink  string
}

// PostFacts is what the score reads of one post.
type PostFacts struct {
	Region       string
	Caption      string
	Hashtags     []string
	TextLanguage string
}

// Inputs are everything an account's score is computed from: its text, its
// newest posts, and its audience by source (how many different monitored
// accounts or searches led to it).
type Inputs struct {
	Account  AccountText
	Posts    []PostFacts
	Audience map[string]int
	// ShownInIraq: finds served to viewers in Iraq.
	ShownInIraq int
}

// Evidence is one signal found, for the review page and the API: never the
// account's own text, only what matched.
type Evidence struct {
	Signal   string   `json:"signal"`
	Strength string   `json:"strength"`
	Count    int      `json:"count,omitempty"`
	Of       int      `json:"of,omitempty"`
	Terms    []string `json:"terms,omitempty"`
}

// Result is an account's Iraq score.
type Result struct {
	Score    float64    `json:"score"`
	Pass     bool       `json:"pass"`
	Evidence []Evidence `json:"evidence"`
}

// maxTerms bounds the matched terms an evidence entry lists.
const maxTerms = 10

// Score computes an account's Iraq score. Each kind of signal counts once,
// with its strength's weight; weak ones together add at most WeakMax.
func Score(cfg config.IraqScore, m *Matcher, in Inputs) Result {
	var r Result
	add := func(signal, strength string, count, of int, terms []string) {
		if len(terms) > maxTerms {
			terms = terms[:maxTerms]
		}
		r.Evidence = append(r.Evidence, Evidence{Signal: signal, Strength: strength, Count: count, Of: of, Terms: terms})
	}
	n := len(in.Posts)
	if c := countPosts(in.Posts, func(p PostFacts) bool { return slices.Contains(cfg.Regions, strings.ToUpper(p.Region)) }); c > 0 {
		add(SignalPostsInIraq, Strong, c, n, nil)
	}
	if terms := m.Match(in.Account.Nickname + "\n" + in.Account.Bio); len(terms) > 0 {
		add(SignalTextPlace, Strong, len(terms), 0, terms)
	}
	if IraqiPhone(in.Account.Bio) {
		add(SignalPhone, Strong, 0, 0, nil)
	}
	if IQLink(in.Account.BioLink) || IQLink(in.Account.Bio) {
		add(SignalIQLink, Strong, 0, 0, nil)
	}
	var captionTerms []string
	captions := countPosts(in.Posts, func(p PostFacts) bool {
		t := m.Match(p.Caption)
		t = append(t, m.MatchTags(p.Hashtags)...)
		captionTerms = append(captionTerms, t...)
		return len(t) > 0
	})
	if captions > 0 {
		add(SignalCaptionPlaces, many(captions, cfg.CaptionPostsStrong), captions, n, uniq(captionTerms))
	}
	audience := map[string]int{}
	for how, k := range in.Audience {
		if s, ok := audienceSignal[how]; ok && k > 0 {
			audience[s] += k
		}
	}
	if in.ShownInIraq > 0 {
		audience[SignalShownInIraq] += in.ShownInIraq
	}
	for _, s := range []string{SignalFollowedByIraqi, SignalMentionedByIraqi, SignalShownInIraq} {
		if k := audience[s]; k > 0 {
			add(s, many(k, cfg.AudienceStrong), k, 0, nil)
		}
	}
	if c := countPosts(in.Posts, func(p PostFacts) bool { return slices.Contains(cfg.Languages, strings.ToLower(p.TextLanguage)) }); c > 0 {
		add(SignalArabicKurdish, Weak, c, n, nil)
	}
	if k := audience[SignalEngagesIraqi]; k > 0 {
		add(SignalEngagesIraqi, Weak, k, 0, nil)
	}
	var weak float64
	for _, e := range r.Evidence {
		switch e.Strength {
		case Strong:
			r.Score += cfg.Strong
		case Medium:
			r.Score += cfg.Medium
		case Weak:
			weak += cfg.Weak
		}
	}
	r.Score += min(weak, cfg.WeakMax)
	r.Pass = r.Score >= cfg.Pass-1e-9
	return r
}

// many is a medium signal's strength: strong from strongAt on.
func many(n, strongAt int) string {
	if n >= strongAt {
		return Strong
	}
	return Medium
}

func countPosts(ps []PostFacts, f func(PostFacts) bool) int {
	n := 0
	for _, p := range ps {
		if f(p) {
			n++
		}
	}
	return n
}

func uniq(s []string) []string {
	sort.Strings(s)
	return slices.Compact(s)
}

var (
	// +964 or 00964 followed by a number; an Iraqi mobile 07[3-9]x xxx xxxx.
	phoneIntl   = regexp.MustCompile(`(?:\+|00)964[0-9]{7,}`)
	phoneMobile = regexp.MustCompile(`(?:^|[^0-9])07[3-9][0-9]{8}(?:$|[^0-9])`)
	iqDomain    = regexp.MustCompile(`(?i)(?:^|[^a-z0-9.-])(?:[a-z0-9-]+\.)+iq(?:$|[^a-z0-9-])`)
)

// IraqiPhone reports an Iraqi phone number in text: +964 or 00964, or an
// Iraqi mobile number (07[3-9]…, 11 digits), written with Arabic-Indic
// digits or not, with spaces or dashes between the digits.
func IraqiPhone(text string) bool {
	t := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || r == '-' || r == '.' || r == '(' || r == ')' {
			return -1
		}
		return r
	}, Normalize(text))
	return phoneIntl.MatchString(t) || phoneMobile.MatchString(t)
}

// IQLink reports a link to a .iq domain: a URL whose host ends in .iq, or
// such a domain written in text.
func IQLink(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if u, err := url.Parse(s); err == nil && u.Host != "" {
		h := strings.ToLower(u.Hostname())
		return strings.HasSuffix(h, ".iq")
	}
	return iqDomain.MatchString(s)
}
