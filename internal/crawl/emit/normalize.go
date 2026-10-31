package emit

import (
	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// hashable returns a copy of msg with volatile values removed: signed CDN
// URLs carry per-request expiry/signature parameters that would otherwise
// change the content hash on every scrape.
func hashable(msg proto.Message) proto.Message {
	c := proto.Clone(msg)
	stripURLs(c.ProtoReflect())
	return c
}

func stripURLs(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if fd.Kind() == protoreflect.MessageKind {
					stripURLs(l.Get(i).Message())
				} else if fd.Kind() == protoreflect.StringKind {
					l.Set(i, protoreflect.ValueOfString(stableURL(l.Get(i).String())))
				}
			}
		case fd.IsMap():
			// Maps here hold counts and contact details: nothing volatile.
		case fd.Kind() == protoreflect.MessageKind:
			stripURLs(v.Message())
		case fd.Kind() == protoreflect.StringKind:
			if s := v.String(); strings.Contains(s, "oe=") || strings.Contains(s, "_nc_") {
				m.Set(fd, protoreflect.ValueOfString(stableURL(s)))
			}
		}
		return true
	})
}

func stableURL(s string) string {
	if !strings.Contains(s, "?") {
		return s
	}
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	q := u.Query()
	if q.Get("oe") == "" && !strings.Contains(u.RawQuery, "_nc_") {
		return s
	}
	u.RawQuery = ""
	return u.String()
}
