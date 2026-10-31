// Package walk finds entities in GraphQL and embedded-JSON payloads without
// fixed paths: a walk collects every object a kind function recognises, and
// fields are read from several candidate paths. Parsers built on it survive
// the frequent A/B shape changes Facebook and Instagram ship; a Report lists
// the required fields that could not be found, so the caller can classify a
// response as schema drift.
package walk

import (
	"github.com/tidwall/gjson"
)

// Node is a collected entity together with its nearest collected ancestor.
type Node struct {
	// Kind is what the kind function named the object (with Collect, its
	// __typename).
	Kind   string
	V      gjson.Result
	Parent *Node
	key    string // kind|id under the parent's key
}

// Ancestor returns the nearest ancestor of one of the kinds, or nil.
func (n *Node) Ancestor(kinds ...string) *Node {
	for p := n.Parent; p != nil; p = p.Parent {
		for _, k := range kinds {
			if p.Kind == k {
				return p
			}
		}
	}
	return nil
}

// KindFunc names the kind of an object a walk should collect, or returns ""
// to pass it by. Objects without a __typename (most of Instagram's) are
// recognised by their fields.
type KindFunc func(obj gjson.Result) string

// Typename collects objects whose __typename is one of want.
func Typename(want ...string) KindFunc {
	w := make(map[string]bool, len(want))
	for _, t := range want {
		w[t] = true
	}
	return func(obj gjson.Result) string {
		if tn := obj.Get("__typename"); tn.Type == gjson.String && w[tn.Str] {
			return tn.Str
		}
		return ""
	}
}

// Collect walks every part once and returns the objects whose __typename is
// in want, in document order. It is CollectBy with Typename(want...).
func Collect(parts []gjson.Result, want ...string) []*Node {
	return CollectBy(parts, Typename(want...))
}

// CollectBy walks every part once and returns the objects kind names, in
// document order. An object repeated with the same kind and id at the same
// place (under the same collected ancestors, as when parts repeat it) is
// returned once. The same object quoted elsewhere, such as a shared post
// inside the story that shares it, is returned again: the first copy must
// not hide the one in its own place. Objects without an id are never
// merged, but they are placed by kind alone, so two id-less siblings of one
// kind share the place of their children.
func CollectBy(parts []gjson.Result, kind KindFunc) []*Node {
	c := &collector{kind: kind, seen: map[string]bool{}}
	for _, p := range parts {
		c.walk(p, nil, 0)
	}
	return c.out
}

type collector struct {
	kind KindFunc
	seen map[string]bool
	out  []*Node
}

const maxWalkDepth = 200

func (c *collector) walk(v gjson.Result, parent *Node, depth int) {
	if depth > maxWalkDepth {
		return
	}
	switch {
	case v.IsArray():
		v.ForEach(func(_, x gjson.Result) bool {
			c.walk(x, parent, depth+1)
			return true
		})
	case v.IsObject():
		cur := parent
		if k := c.kind(v); k != "" {
			key := k + "|" + v.Get("id").String()
			if parent != nil {
				key = parent.key + "/" + key
			}
			if v.Get("id").Exists() {
				if c.seen[key] {
					return
				}
				c.seen[key] = true
			}
			n := &Node{Kind: k, V: v, Parent: parent, key: key}
			c.out = append(c.out, n)
			cur = n
		}
		v.ForEach(func(k, x gjson.Result) bool {
			if x.IsObject() || x.IsArray() {
				c.walk(x, cur, depth+1)
			}
			return true
		})
	}
}

// First returns the first candidate path that exists and is not null.
func First(v gjson.Result, paths ...string) gjson.Result {
	for _, p := range paths {
		r := v.Get(p)
		if r.Exists() && r.Type != gjson.Null {
			return r
		}
	}
	return gjson.Result{}
}

// Find does a depth-first search for key under v and returns the first
// non-null value. It does not descend into nested objects whose __typename
// is in skip (for example a shared Story inside a Story).
func Find(v gjson.Result, key string, skip ...string) gjson.Result {
	return FindWhere(v, key, nil, skip...)
}

// FindWhere is Find restricted to values accepted by ok.
func FindWhere(v gjson.Result, key string, ok func(gjson.Result) bool, skip ...string) gjson.Result {
	var out gjson.Result
	var rec func(x gjson.Result, root bool, depth int) bool
	rec = func(x gjson.Result, root bool, depth int) bool {
		if depth > maxWalkDepth {
			return false
		}
		if x.IsObject() {
			if !root {
				if tn := x.Get("__typename").String(); tn != "" {
					for _, s := range skip {
						if tn == s {
							return false
						}
					}
				}
			}
			if r := x.Get(gjsonEscape(key)); r.Exists() && r.Type != gjson.Null && (ok == nil || ok(r)) {
				out = r
				return true
			}
		}
		found := false
		if x.IsObject() || x.IsArray() {
			x.ForEach(func(_, y gjson.Result) bool {
				if y.IsObject() || y.IsArray() {
					if rec(y, false, depth+1) {
						found = true
						return false
					}
				}
				return true
			})
		}
		return found
	}
	rec(v, true, 0)
	return out
}

// FindAll returns every non-null value stored under key below v, skipping
// nested objects with a typename in skip.
func FindAll(v gjson.Result, key string, skip ...string) []gjson.Result {
	var out []gjson.Result
	var rec func(x gjson.Result, root bool, depth int)
	rec = func(x gjson.Result, root bool, depth int) {
		if depth > maxWalkDepth {
			return
		}
		if x.IsObject() {
			if !root {
				if tn := x.Get("__typename").String(); tn != "" {
					for _, s := range skip {
						if tn == s {
							return
						}
					}
				}
			}
			if r := x.Get(gjsonEscape(key)); r.Exists() && r.Type != gjson.Null {
				out = append(out, r)
			}
		}
		if x.IsObject() || x.IsArray() {
			x.ForEach(func(_, y gjson.Result) bool {
				if y.IsObject() || y.IsArray() {
					rec(y, false, depth+1)
				}
				return true
			})
		}
	}
	rec(v, true, 0)
	return out
}

func gjsonEscape(k string) string {
	// Keys used here are plain identifiers; escape the gjson metacharacters
	// anyway so a key like "a.b" is matched literally.
	out := make([]byte, 0, len(k))
	for i := 0; i < len(k); i++ {
		switch k[i] {
		case '.', '*', '?', '|', '#', '@', '\\':
			out = append(out, '\\')
		}
		out = append(out, k[i])
	}
	return string(out)
}

// PageInfo is a connection's pagination state.
type PageInfo struct {
	HasNext   bool
	EndCursor string
	Found     bool
}

// FindPageInfo returns the page_info of the connection named one of keys
// (e.g. "timeline_list_feed_units", "comments", "replies_connection"),
// choosing the shallowest occurrence that is not inside an entity of the
// skip typenames. Deferred page_info parts ({"data":{"page_info":...}}) are
// also accepted.
func FindPageInfo(parts []gjson.Result, keys []string, skip ...string) PageInfo {
	best := PageInfo{}
	bestDepth := 1 << 30
	consider := func(pi gjson.Result, depth int) {
		if !pi.IsObject() || depth >= bestDepth {
			return
		}
		best = PageInfo{HasNext: pi.Get("has_next_page").Bool(), EndCursor: pi.Get("end_cursor").String(), Found: true}
		bestDepth = depth
	}
	var rec func(x gjson.Result, depth int)
	rec = func(x gjson.Result, depth int) {
		if depth > maxWalkDepth || depth >= bestDepth {
			return
		}
		if x.IsObject() {
			if tn := x.Get("__typename").String(); tn != "" {
				for _, s := range skip {
					if tn == s {
						return
					}
				}
			}
			for _, k := range keys {
				if conn := x.Get(gjsonEscape(k)); conn.IsObject() {
					consider(conn.Get("page_info"), depth+1)
				}
			}
		}
		if x.IsObject() || x.IsArray() {
			x.ForEach(func(_, y gjson.Result) bool {
				if y.IsObject() || y.IsArray() {
					rec(y, depth+1)
				}
				return true
			})
		}
	}
	for _, p := range parts {
		if pi := p.Get("data.page_info"); pi.IsObject() {
			// Deferred page_info part: authoritative for streamed connections.
			lbl := p.Get("label").String()
			for _, k := range keys {
				if lbl == "" || containsFold(lbl, k) {
					consider(pi, 0)
					break
				}
			}
		}
		rec(p, 0)
	}
	return best
}

func containsFold(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a|0x20 != b|0x20 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
