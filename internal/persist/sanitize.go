package persist

import (
	"bytes"
	"encoding/json"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Postgres text and jsonb can't hold a NUL character, which is valid
// UTF-8 and can come in Facebook text. One would make every batch it is in
// fail for good, so it is removed from every string before writing.

// stripNUL removes NUL characters from every string of m, nested messages,
// lists and maps included.
func stripNUL(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				switch fd.Kind() {
				case protoreflect.StringKind:
					if s := l.Get(i).String(); strings.ContainsRune(s, 0) {
						l.Set(i, protoreflect.ValueOfString(strings.ReplaceAll(s, "\x00", "")))
					}
				case protoreflect.MessageKind, protoreflect.GroupKind:
					stripNUL(l.Get(i).Message())
				}
			}
		case fd.IsMap():
			mp := v.Map()
			type kv struct{ k, v string }
			var fix []kv
			mp.Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				switch fd.MapValue().Kind() {
				case protoreflect.StringKind:
					if s := mv.String(); strings.ContainsRune(s, 0) {
						fix = append(fix, kv{k.String(), strings.ReplaceAll(s, "\x00", "")})
					}
				case protoreflect.MessageKind:
					stripNUL(mv.Message())
				}
				return true
			})
			for _, f := range fix {
				mp.Set(protoreflect.ValueOfString(f.k).MapKey(), protoreflect.ValueOfString(f.v))
			}
		case fd.Kind() == protoreflect.StringKind:
			if s := v.String(); strings.ContainsRune(s, 0) {
				m.Set(fd, protoreflect.ValueOfString(strings.ReplaceAll(s, "\x00", "")))
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			stripNUL(v.Message())
		}
		return true
	})
}

// jsonbSafe returns JSON without \u0000 escapes, which jsonb rejects.
func jsonbSafe(b []byte) []byte {
	if !bytes.Contains(b, []byte(`\u0000`)) {
		return b
	}
	// Numbers stay exact (UseNumber): float64 would round 64-bit ids.
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if d.Decode(&v) != nil {
		return nil
	}
	out, err := json.Marshal(dropNUL(v))
	if err != nil {
		return nil
	}
	return out
}

func dropNUL(v any) any {
	switch x := v.(type) {
	case string:
		return strings.ReplaceAll(x, "\x00", "")
	case []any:
		for i := range x {
			x[i] = dropNUL(x[i])
		}
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[strings.ReplaceAll(k, "\x00", "")] = dropNUL(e)
		}
		return out
	}
	return v
}
