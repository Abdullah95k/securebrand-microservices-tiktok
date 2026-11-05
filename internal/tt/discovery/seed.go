package discovery

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
)

// SeedKeyword is one keyword of a seed file.
type SeedKeyword struct {
	Term     string
	Language string
	Group    string
	Enabled  bool
}

// seedFile is configs/tt/discovery/*.yaml: groups of terms by language,
// `off` ones loaded disabled.
type seedFile struct {
	Groups []struct {
		Name  string              `yaml:"name"`
		Terms map[string][]string `yaml:"terms"`
		Off   map[string][]string `yaml:"off"`
	} `yaml:"groups"`
}

// ParseSeed reads a seed file. A spelling that folds to one already listed
// in the same language (Normalize) is kept once, enabled if any listing of
// it is.
func ParseSeed(b []byte) ([]SeedKeyword, error) {
	var f seedFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("discovery: seed: %w", err)
	}
	var out []SeedKeyword
	seen := map[[2]string]int{}
	add := func(group, lang, term string, enabled bool) error {
		if !Languages[lang] {
			return fmt.Errorf("discovery: seed group %q: unknown language %q (en, ar, ckb or kmr)", group, lang)
		}
		n := Normalize(term)
		if n == "" {
			return fmt.Errorf("discovery: seed group %q: empty %s term", group, lang)
		}
		k := [2]string{n, lang}
		if i, ok := seen[k]; ok {
			out[i].Enabled = out[i].Enabled || enabled
			return nil
		}
		seen[k] = len(out)
		out = append(out, SeedKeyword{Term: term, Language: lang, Group: group, Enabled: enabled})
		return nil
	}
	for _, g := range f.Groups {
		if g.Name == "" {
			return nil, fmt.Errorf("discovery: seed group without a name")
		}
		for _, set := range []struct {
			terms   map[string][]string
			enabled bool
		}{{g.Terms, true}, {g.Off, false}} {
			langs := make([]string, 0, len(set.terms))
			for l := range set.terms {
				langs = append(langs, l)
			}
			sort.Strings(langs)
			for _, l := range langs {
				for _, t := range set.terms[l] {
					if err := add(g.Name, l, t, set.enabled); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return out, nil
}

// IraqSeed is the embedded Iraq seed (configs/tt/discovery/iraq.yaml).
func IraqSeed() ([]SeedKeyword, error) {
	b, err := configs.FS.ReadFile("tt/discovery/iraq.yaml")
	if err != nil {
		return nil, err
	}
	return ParseSeed(b)
}

// ParseKeywordCSV reads term,language[,group[,enabled]] rows (fbs's fbctl
// import format); a first row whose second column is not a language is a
// header.
func ParseKeywordCSV(r io.Reader) ([]SeedKeyword, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []SeedKeyword
	for i, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("line %d: want term,language[,group[,enabled]]", i+1)
		}
		lang := strings.TrimSpace(row[1])
		if !Languages[lang] {
			if i == 0 {
				continue // header
			}
			return nil, fmt.Errorf("line %d: language %q must be en, ar, ckb or kmr", i+1, lang)
		}
		k := SeedKeyword{Term: strings.TrimSpace(row[0]), Language: lang, Enabled: true}
		if len(row) > 2 {
			k.Group = strings.TrimSpace(row[2])
		}
		if len(row) > 3 && strings.TrimSpace(row[3]) != "" {
			if k.Enabled, err = strconv.ParseBool(strings.TrimSpace(row[3])); err != nil {
				return nil, fmt.Errorf("line %d: enabled %q: %w", i+1, row[3], err)
			}
		}
		out = append(out, k)
	}
	if len(out) == 0 {
		return nil, errors.New("no keywords in the file")
	}
	return out, nil
}
