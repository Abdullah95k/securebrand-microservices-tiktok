package discprobe

import (
	_ "embed"
	"fmt"
	"strings"
	"unicode"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

//go:embed operations.yaml
var operationsYAML []byte

// Operations.
const (
	OpSearch          = "search_general"
	OpSearchUser      = "search_user"
	OpChallengeDetail = "challenge_detail"
	OpChallengeList   = "challenge_list"
	OpMusicDetail     = "music_detail"
	OpMusicList       = "music_list"
	OpRecommend       = "recommend_list"
	OpRelated         = "related_list"
	OpUserList        = "user_list"
	OpReposts         = "repost_list"
	// The crawler's own creator list (the entry check's and T34's read).
	OpCreatorList = "creator_list"
	// Not registry calls.
	OpBootstrap = "bootstrap"
	OpPage      = "page"      // a source's page by plain HTTP (tag, music)
	OpPostPage  = "post_page" // T34
	OpShortLink = "short_link"
	OpProfile   = "profile_page"
)

// Seed is the probe's registry: its discovery calls with the crawler's
// client params, and the crawler's creator list.
func Seed() (registry.Seed, error) {
	crawler, err := ttclient.Seed()
	if err != nil {
		return registry.Seed{}, err
	}
	s, err := registry.ParseSeed(operationsYAML)
	if err != nil {
		return registry.Seed{}, fmt.Errorf("discprobe: operations: %w", err)
	}
	s.ClientParams = crawler.ClientParams
	for _, q := range crawler.Queries {
		if q.Name == OpCreatorList {
			s.Queries = append(s.Queries, q)
		}
	}
	return s, nil
}

// query returns an operation's seed variant.
func query(s registry.Seed, name string) (registry.Query, bool) {
	for _, q := range s.Queries {
		if q.Name == name {
			return q, true
		}
	}
	return registry.Query{}, false
}

// candidates are an operation's variants to try: the seed's, then its
// signed or unsigned twin.
func candidates(s registry.Seed, name string) []registry.Query {
	q, ok := query(s, name)
	if !ok {
		return nil
	}
	return []registry.Query{q, ttclient.Twin(q)}
}

// PickKeywords chooses n enabled keywords of the Iraq list, spread over
// its groups and languages: each round takes one term from every group,
// the languages turning from group to group and round to round (the first
// group's first term in Arabic, the language most Iraqi posts use:
// har-findings-search §3), until n are chosen.
func PickKeywords(seed []discovery.SeedKeyword, n int) []Keyword {
	type group struct {
		name  string
		langs map[string][]string
	}
	var groups []*group
	byName := map[string]*group{}
	for _, k := range seed {
		if !k.Enabled {
			continue
		}
		g := byName[k.Group]
		if g == nil {
			g = &group{name: k.Group, langs: map[string][]string{}}
			byName[k.Group] = g
			groups = append(groups, g)
		}
		g.langs[k.Language] = append(g.langs[k.Language], k.Term)
	}
	order := []string{"ar", "en", "ckb", "kmr"}
	var out []Keyword
	taken := map[string]bool{}
	for round := 0; len(out) < n; round++ {
		added := false
		for gi, g := range groups {
			for i := range order {
				lang := order[(round+gi+i)%len(order)]
				terms := g.langs[lang]
				pick := ""
				for _, t := range terms {
					if !taken[discovery.Normalize(t)] {
						pick = t
						break
					}
				}
				if pick == "" {
					continue
				}
				taken[discovery.Normalize(pick)] = true
				out = append(out, Keyword{Term: pick, Language: lang, Group: g.name})
				added = true
				break
			}
			if len(out) == n {
				return out
			}
		}
		if !added {
			break
		}
	}
	return out
}

// hashtagOf makes a hashtag of a keyword: one word, no spaces (TikTok's
// hashtags have none), lowercased; "" when it is several words.
func hashtagOf(term string) string {
	if strings.IndexFunc(term, unicode.IsSpace) >= 0 {
		return ""
	}
	return strings.ToLower(term)
}
