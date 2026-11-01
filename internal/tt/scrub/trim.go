package scrub

// trim keeps at most n of a list's items or a page's comments, chosen so a
// small fixture still covers every shape the parsers read. It keeps the
// answer's order.
func trim(v any, n int) {
	if n <= 0 {
		return
	}
	root, ok := v.(map[string]any)
	if !ok {
		return
	}
	for _, k := range []string{"itemList", "comments"} {
		if xs, ok := root[k].([]any); ok && len(xs) > n {
			root[k] = pick(xs, n, features(k))
		}
	}
}

// features returns, for a list key, the tests of the shapes worth keeping,
// most wanted first.
func features(key string) []func(map[string]any) bool {
	has := func(k string) func(map[string]any) bool {
		return func(m map[string]any) bool { return nonEmpty(m[k]) }
	}
	is := func(k string) func(map[string]any) bool {
		return func(m map[string]any) bool { b, _ := m[k].(bool); return b }
	}
	if key == "comments" {
		return []func(map[string]any) bool{has("reply_comment"), has("image_list"), is("author_pin"),
			is("is_author_digged"), mentions("text_extra", "user_id"), has("label_list")}
	}
	return []func(map[string]any) bool{is("isPinnedItem"), has("imagePost"), is("isAd"), mentions("textExtra", "userId"),
		has("challenges"), has("anchors"), has("warnInfo"), has("aigcLabelType"), has("playlistId")}
}

// mentions tests for an entry of list key that names a person by field.
func mentions(key, field string) func(map[string]any) bool {
	return func(m map[string]any) bool {
		xs, _ := m[key].([]any)
		for _, x := range xs {
			if e, ok := x.(map[string]any); ok && nonEmpty(e[field]) {
				return true
			}
		}
		return false
	}
}

func nonEmpty(v any) bool {
	switch v := v.(type) {
	case nil:
		return false
	case string:
		return v != "" && v != "0"
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	}
	return true
}

// pick keeps the first item of each feature, then the first items, up to
// n, in their original order.
func pick(xs []any, n int, feats []func(map[string]any) bool) []any {
	keep := make([]bool, len(xs))
	kept := 0
	for _, f := range feats {
		for i, x := range xs {
			if m, ok := x.(map[string]any); ok && !keep[i] && kept < n && f(m) {
				keep[i] = true
				kept++
				break
			}
		}
	}
	for i := range xs {
		if kept >= n {
			break
		}
		if !keep[i] {
			keep[i] = true
			kept++
		}
	}
	out := make([]any, 0, n)
	for i, x := range xs {
		if keep[i] {
			out = append(out, x)
		}
	}
	return out
}
