// Package discovery is tks's account discovery (docs/tiktok/discovery-plan.md):
// the keyword list (fbs's Iraq seed and its spelling fold, copied from fbs's
// internal/discovery at 4816736), and the store of the keywords. The entry
// check, the dormant list and the Iraq score (M10c) and the sources that
// find accounts (M11 to M13) build on it.
package discovery

import (
	"strings"
	"unicode"
)

// Normalize folds a keyword or a post's text for matching and for spotting
// two spellings of one keyword. The same fold applies to every language, so
// a term matches whichever script variant a post uses:
//
//   - Arabic script: harakat, the superscript alef, tatweel and zero-width
//     joiners are dropped (Sorani is often typed with a ZWNJ inside a word:
//     هه‌ولێر); أ إ آ ٱ fold to ا; ة and Sorani ە fold to ه (older Sorani
//     text writes ە as ه); ي ى and Persian/Kurdish ی fold to ی, and ك to
//     ک, the forms Kurdish uses, so Arabic and Kurdish keyboards agree.
//     Kurdish letters that are letters of their own (ێ ۆ ڵ ڕ ڤ گ چ پ ژ) are
//     kept: folding them would merge different words. Arabic-Indic digits
//     become ASCII.
//   - Latin script: lower case; Kurmanji and other accents are folded (ê→e,
//     î→i, û→u, ç→c, ş→s, ğ→g, ı→i, and the common Western accents);
//     apostrophes are dropped (Ba'quba, Baquba); hyphens and underscores
//     become spaces (Al-Qaim, Al Qaim).
//   - Whitespace is collapsed and trimmed.
//
// The Arabic article ال is not stripped: البصرة and بصرة are kept as
// separate keywords when both are wanted.
func Normalize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		r = fold(r)
		switch {
		case r == drop:
			continue
		case unicode.IsSpace(r):
			space = b.Len() > 0
			continue
		}
		if space {
			b.WriteByte(' ')
			space = false
		}
		b.WriteRune(r)
	}
	return b.String()
}

// drop marks a rune Normalize removes.
const drop = rune(-1)

func fold(r rune) rune {
	switch {
	case r >= 0x064B && r <= 0x065F, r == 0x0670, r == 0x0640, // harakat, superscript alef, tatweel
		r == 0x200C, r == 0x200D, r == 0x200E, r == 0x200F, r == 0xFEFF: // ZWNJ, ZWJ, marks, BOM
		return drop
	case r == '\'', r == '’', r == 'ʼ', r == '‘', r == '`':
		return drop
	case r == '-', r == '_', r == 0x2010, r == 0x2011, r == 0x2013:
		return ' '
	case r >= 0x0660 && r <= 0x0669: // Arabic-Indic digits
		return '0' + (r - 0x0660)
	case r >= 0x06F0 && r <= 0x06F9: // Extended Arabic-Indic digits
		return '0' + (r - 0x06F0)
	}
	switch r {
	case 'أ', 'إ', 'آ', 'ٱ':
		return 'ا'
	case 'ة', 'ە':
		return 'ه'
	case 'ي', 'ى':
		return 'ی'
	case 'ك':
		return 'ک'
	}
	if f, ok := latin[r]; ok {
		return f
	}
	return unicode.ToLower(r)
}

// latin folds accented Latin letters to their base letter.
var latin = map[rune]rune{
	'ê': 'e', 'Ê': 'e', 'é': 'e', 'É': 'e', 'è': 'e', 'È': 'e', 'ë': 'e', 'Ë': 'e',
	'î': 'i', 'Î': 'i', 'í': 'i', 'Í': 'i', 'ì': 'i', 'ï': 'i', 'ı': 'i', 'İ': 'i',
	'û': 'u', 'Û': 'u', 'ú': 'u', 'Ú': 'u', 'ù': 'u', 'ü': 'u', 'Ü': 'u',
	'â': 'a', 'Â': 'a', 'á': 'a', 'Á': 'a', 'à': 'a', 'ä': 'a', 'Ä': 'a',
	'ô': 'o', 'Ô': 'o', 'ó': 'o', 'Ó': 'o', 'ò': 'o', 'ö': 'o', 'Ö': 'o',
	'ç': 'c', 'Ç': 'c', 'ş': 's', 'Ş': 's', 'ğ': 'g', 'Ğ': 'g',
}

// Mentions reports whether text mentions the normalized term: the term
// appears in the folded text. A match inside a longer word counts, so an
// Arabic term with a prefix attached (وبغداد, لبغداد) still matches.
func Mentions(text, normalizedTerm string) bool {
	if normalizedTerm == "" {
		return false
	}
	return strings.Contains(Normalize(text), normalizedTerm)
}

// Languages are the keyword languages: English, Arabic, Sorani Kurdish
// (Arabic script) and Kurmanji Kurdish (Latin script).
var Languages = map[string]bool{"en": true, "ar": true, "ckb": true, "kmr": true}
