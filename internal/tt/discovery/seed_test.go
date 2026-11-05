package discovery

import (
	"strings"
	"testing"
)

// The Iraq seed covers the country, the Kurdistan Region and all 19
// governorates, in English and Arabic everywhere, with Kurdish where
// Kurdish media name them, and dozens of cities and towns.
func TestIraqSeed(t *testing.T) {
	kws, err := IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	governorates := []string{"baghdad", "basra", "nineveh", "erbil", "sulaymaniyah", "duhok", "halabja", "kirkuk", "diyala",
		"salah_al_din", "anbar", "babil", "karbala", "najaf", "qadisiyah", "muthanna", "dhi_qar", "maysan", "wasit"}
	byGroup := map[string]map[string]int{}
	enabled := map[string]int{}
	for _, k := range kws {
		if byGroup[k.Group] == nil {
			byGroup[k.Group] = map[string]int{}
		}
		if k.Enabled {
			byGroup[k.Group][k.Language]++
			enabled[k.Language]++
		}
	}
	for _, g := range append([]string{"iraq", "kurdistan_region"}, governorates...) {
		if byGroup[g]["en"] == 0 || byGroup[g]["ar"] == 0 {
			t.Errorf("group %s lacks an enabled English or Arabic keyword: %v", g, byGroup[g])
		}
	}
	for _, g := range []string{"iraq", "kurdistan_region", "erbil", "sulaymaniyah", "duhok", "halabja", "kirkuk"} {
		if byGroup[g]["ckb"] == 0 || byGroup[g]["kmr"] == 0 {
			t.Errorf("group %s lacks a Sorani or Kurmanji keyword: %v", g, byGroup[g])
		}
	}
	if len(byGroup) != 21 {
		t.Errorf("%d groups, want the country, the region and 19 governorates", len(byGroup))
	}
	// Dozens of cities and towns beyond the governorate names.
	if enabled["en"] < 100 || enabled["ar"] < 80 || enabled["ckb"] < 40 || enabled["kmr"] < 30 {
		t.Errorf("enabled keywords by language: %v", enabled)
	}
	for _, k := range kws {
		if k.Language == "ckb" || k.Language == "ar" {
			if strings.ContainsAny(k.Term, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
				t.Errorf("%s keyword %q is in Latin script", k.Language, k.Term)
			}
		}
		if (k.Language == "en" || k.Language == "kmr") && strings.ContainsFunc(k.Term, func(r rune) bool { return r >= 0x0600 && r <= 0x06FF }) {
			t.Errorf("%s keyword %q is in Arabic script", k.Language, k.Term)
		}
	}
}

// Words that are also everyday words or names are loaded disabled.
func TestSeedOffTermsLoadDisabled(t *testing.T) {
	kws, err := IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"Hit": false, "حديثة": false, "القائم": false, "Mosul": true, "الموصل": true}
	for _, k := range kws {
		if e, ok := want[k.Term]; ok {
			if k.Enabled != e {
				t.Errorf("%q enabled=%v, want %v", k.Term, k.Enabled, e)
			}
			delete(want, k.Term)
		}
	}
	if len(want) > 0 {
		t.Errorf("missing from the seed: %v", want)
	}
}

func TestParseSeed(t *testing.T) {
	kws, err := ParseSeed([]byte(`groups:
  - name: g
    terms:
      en: [Erbil, ERBIL, Al-Qaim]
      kmr: [Hewlêr]
    off:
      en: [Al Qaim, Hit]
`))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, k := range kws {
		got[k.Language+":"+k.Term] = k.Enabled
	}
	want := map[string]bool{"en:Erbil": true, "en:Al-Qaim": true, "kmr:Hewlêr": true, "en:Hit": false}
	if len(got) != len(want) {
		t.Fatalf("%v, want %v", got, want)
	}
	for k, v := range want {
		if e, ok := got[k]; !ok || e != v {
			t.Errorf("%s: %v %v, want %v", k, e, ok, v)
		}
	}
	for _, bad := range []string{"groups: [{name: g, terms: {fr: [Paris]}}]", "groups: [{terms: {en: [x]}}]", "groups: [{name: g, terms: {en: ['  ']}}]"} {
		if _, err := ParseSeed([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
