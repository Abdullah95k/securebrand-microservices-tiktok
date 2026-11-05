package discovery

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		// Latin: case, Kurmanji accents, apostrophes, hyphens, spaces.
		{"  Erbil ", "erbil"},
		{"Hewlêr", "hewler"},
		{"Silêmanî", "silemani"},
		{"Kerkûk", "kerkuk"},
		{"Şengal", "sengal"},
		{"Çemçemal", "cemcemal"},
		{"Ba'quba", "baquba"},
		{"Al-Qaim", "al qaim"},
		{"Tal   Afar", "tal afar"},
		{"İraq", "iraq"},
		// Arabic: alef forms, taa marbuta, alef maqsura, harakat, tatweel.
		{"أربيل", "اربیل"},
		{"إربيل", "اربیل"},
		{"البصرة", "البصره"},
		{"نينوى", "نینوی"},
		{"بَغْدَاد", "بغداد"},
		{"بغـــداد", "بغداد"},
		{"كركوك", "کرکوک"},
		{"سامراء", "سامراء"},
		// Sorani: ە and ZWNJ typing, Kurdish yeh and kaf, letters kept.
		{"هەولێر", "ههولێر"},
		{"هه\u200cولێر", "ههولێر"},
		{"کەرکووک", "کهرکووک"},
		{"دهۆک", "دهۆک"},
		{"سلێمانی", "سلێمانی"},
		// Digits.
		{"٢٠٢٦", "2026"},
		{"۲۰۲۶", "2026"},
	}
	for _, c := range cases {
		if got := Normalize(c.in); got != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Spellings of one keyword typed on different keyboards fold together;
// different Kurdish letters do not.
func TestNormalizeFoldsKeyboardVariants(t *testing.T) {
	same := [][2]string{
		{"كركوك", "کرکوک"},       // Arabic and Persian/Kurdish kaf
		{"سليمانية", "سلیمانیة"}, // Arabic and Kurdish yeh
		{"هەڵەبجە", "هه\u200cڵه\u200cبجه"},
		{"Duhok", "DUHOK"},
	}
	for _, p := range same {
		if Normalize(p[0]) != Normalize(p[1]) {
			t.Errorf("%q and %q fold apart: %q, %q", p[0], p[1], Normalize(p[0]), Normalize(p[1]))
		}
	}
	if Normalize("ێ") == Normalize("ی") || Normalize("ڵ") == Normalize("ل") || Normalize("ۆ") == Normalize("و") {
		t.Error("Kurdish letters of their own are folded into others")
	}
}

func TestMentions(t *testing.T) {
	if !Mentions("انفجار في وبغداد اليوم", Normalize("بغداد")) {
		t.Error("an Arabic term with a prefix attached does not match")
	}
	if !Mentions("Live from HEWLÊR tonight", Normalize("Hewler")) {
		t.Error("case and accents block a match")
	}
	if !Mentions("ئاهەنگ لە هه\u200cولێر", Normalize("هەولێر")) {
		t.Error("a ZWNJ-typed Sorani spelling does not match")
	}
	if Mentions("Basra", Normalize("Baghdad")) || Mentions("anything", "") {
		t.Error("unrelated text matches")
	}
}
