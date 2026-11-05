package discovery

import (
	"slices"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

func testScoreCfg() config.IraqScore {
	return config.IraqScore{Strong: 1, Medium: 0.5, Weak: 0.1, WeakMax: 0.3, Pass: 1, Regions: []string{"IQ"},
		CaptionPostsStrong: 3, AudienceStrong: 10, Languages: []string{"ar", "ckb", "ku"}}
}

func testMatcher() *Matcher {
	var terms []string
	for _, t := range []string{"Iraq", "Iraqi", "Baghdad", "Kut", "Erbil", "العراق", "بغداد", "هەولێر", "Sulaymaniyah"} {
		terms = append(terms, Normalize(t))
	}
	return NewMatcher(terms)
}

func TestMatcher(t *testing.T) {
	m := testMatcher()
	for _, c := range []struct {
		text string
		want []string
	}{
		{"Coffee in BAGHDAD", []string{"baghdad"}},
		{"cutie kutie", nil},                  // Kut only as a word
		{"from Kut, south", []string{"kut"}},  // punctuation is a boundary
		{"Iraqi chef", []string{"iraqi"}},     // Iraq is not inside Iraqi as a word
		{"وبغداد الجميلة", []string{"بغداد"}}, // an Arabic prefix attached
		{"أهل العراق", []string{"العراق"}},
		{"هه\u200cولێر", []string{Normalize("هەولێر")}}, // a ZWNJ and the older spelling fold
		{"nothing here", nil},
	} {
		if got := m.Match(c.text); !slices.Equal(got, c.want) {
			t.Errorf("Match(%q) = %v, want %v", c.text, got, c.want)
		}
	}
	if got := m.MatchTags([]string{"baghdadlife", "kutie", "#Erbil", "عشاق_بغداد"}); !slices.Equal(got, []string{"baghdad", "erbil", "بغداد"}) {
		t.Errorf("MatchTags = %v", got)
	}
}

func TestIraqiPhoneAndLink(t *testing.T) {
	for s, want := range map[string]bool{
		"call +964 770 123 4567":  true,
		"00964-750-1234567":       true,
		"٠٧٧٠١٢٣٤٥٦٧ للحجز":       true, // Arabic-Indic digits
		"07701234567":             true,
		"0770123456":              false, // one digit short
		"+1 555 0100":             false,
		"order 12307701234567890": false, // inside a longer number
	} {
		if got := IraqiPhone(s); got != want {
			t.Errorf("IraqiPhone(%q) = %v", s, got)
		}
	}
	for s, want := range map[string]bool{
		"https://shop.example.iq/a": true,
		"example.com.iq":            true,
		"visit www.example.iq now":  true,
		"https://example.com/iq":    false,
		"iq test":                   false,
		"":                          false,
	} {
		if got := IQLink(s); got != want {
			t.Errorf("IQLink(%q) = %v", s, got)
		}
	}
}

func TestScore(t *testing.T) {
	cfg, m := testScoreCfg(), testMatcher()
	post := func(region, caption, lang string, tags ...string) PostFacts {
		return PostFacts{Region: region, Caption: caption, TextLanguage: lang, Hashtags: tags}
	}
	signals := func(r Result) map[string]string {
		out := map[string]string{}
		for _, e := range r.Evidence {
			out[e.Signal] = e.Strength
		}
		return out
	}
	for _, c := range []struct {
		name string
		in   Inputs
		pass bool
		want map[string]string
	}{
		{"one post made in Iraq is enough", Inputs{Posts: []PostFacts{post("US", "", ""), post("iq", "", "")}}, true,
			map[string]string{SignalPostsInIraq: Strong}},
		{"a place in the bio", Inputs{Account: AccountText{Bio: "Baghdad bakery"}}, true, map[string]string{SignalTextPlace: Strong}},
		{"a phone number", Inputs{Account: AccountText{Bio: "+9647701234567"}}, true, map[string]string{SignalPhone: Strong}},
		{"a .iq link", Inputs{Account: AccountText{BioLink: "https://a.example.iq"}}, true, map[string]string{SignalIQLink: Strong}},
		{"one caption is medium: not enough", Inputs{Posts: []PostFacts{post("", "Kut today", "")}}, false,
			map[string]string{SignalCaptionPlaces: Medium}},
		{"three captions are strong", Inputs{Posts: []PostFacts{post("", "Kut", ""), post("", "", "", "baghdadnights"), post("", "Erbil", "")}}, true,
			map[string]string{SignalCaptionPlaces: Strong}},
		{"two different medium signals pass", Inputs{Posts: []PostFacts{post("", "Kut", "")}, Audience: map[string]int{"following": 2}}, true,
			map[string]string{SignalCaptionPlaces: Medium, SignalFollowedByIraqi: Medium}},
		{"one medium and every weak signal do not", Inputs{Posts: []PostFacts{post("", "Kut", "ar")},
			Audience: map[string]int{"commenter": 40, "follower": 3}}, false,
			map[string]string{SignalCaptionPlaces: Medium, SignalArabicKurdish: Weak, SignalEngagesIraqi: Weak}},
		{"weak signals alone never pass", Inputs{Posts: []PostFacts{post("", "", "ar"), post("", "", "ckb")},
			Audience: map[string]int{"commenter": 500}}, false,
			map[string]string{SignalArabicKurdish: Weak, SignalEngagesIraqi: Weak}},
		{"many monitored accounts following it", Inputs{Audience: map[string]int{"following": 10}}, true,
			map[string]string{SignalFollowedByIraqi: Strong}},
		{"mention and reply are one signal", Inputs{Audience: map[string]int{"mention": 1, "reply": 1}}, false,
			map[string]string{SignalMentionedByIraqi: Medium}},
		{"shown in Iraq and mentioned", Inputs{ShownInIraq: 1, Audience: map[string]int{"reply": 1}}, true,
			map[string]string{SignalShownInIraq: Medium, SignalMentionedByIraqi: Medium}},
		{"nothing", Inputs{Account: AccountText{Bio: "hello"}, Posts: []PostFacts{post("US", "hi", "en")}}, false, map[string]string{}},
	} {
		r := Score(cfg, m, c.in)
		if r.Pass != c.pass {
			t.Errorf("%s: pass = %v (score %.2f), want %v", c.name, r.Pass, r.Score, c.pass)
		}
		if got := signals(r); len(got) != len(c.want) {
			t.Errorf("%s: signals %v, want %v", c.name, got, c.want)
		} else {
			for k, v := range c.want {
				if got[k] != v {
					t.Errorf("%s: signals %v, want %v", c.name, got, c.want)
				}
			}
		}
	}
	// The evidence never carries the account's text, only what matched.
	r := Score(cfg, m, Inputs{Account: AccountText{Bio: "my private words, Baghdad"}})
	if len(r.Evidence) != 1 || !slices.Equal(r.Evidence[0].Terms, []string{"baghdad"}) {
		t.Errorf("evidence %+v", r.Evidence)
	}
}
