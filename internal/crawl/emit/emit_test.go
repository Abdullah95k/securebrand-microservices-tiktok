package emit

import (
	"bytes"
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
)

func TestHashIgnoresSignedURLParams(t *testing.T) {
	e := &Emitter{HMACKey: []byte("k")}
	mk := func(sig string) *fbv1.Comment {
		return &fbv1.Comment{CommentId: 1, PostId: 2, Text: "hi",
			Author:      &fbv1.Author{Id: "9", AvatarUrl: "https://scontent.xx.fbcdn.net/a.jpg?oe=" + sig + "&oh=x" + sig},
			Attachments: []*fbv1.Attachment{{Url: "https://scontent.xx.fbcdn.net/p.jpg?oe=" + sig}}}
	}
	a, b := mk("6A000000"), mk("6B000000")
	e.stamp(Source{}, "comment:1", a, func(o *fbv1.Observation) { a.Obs = o })
	e.stamp(Source{}, "comment:1", b, func(o *fbv1.Observation) { b.Obs = o })
	if !bytes.Equal(a.Obs.ContentHash, b.Obs.ContentHash) || a.Obs.EventId != b.Obs.EventId {
		t.Fatal("re-signed URLs must not change the content hash")
	}
	if a.Author.AvatarUrl == b.Author.AvatarUrl {
		t.Fatal("the published message must keep the real URL")
	}
	c := mk("6A000000")
	c.Text = "edited"
	e.stamp(Source{}, "comment:1", c, func(o *fbv1.Observation) { c.Obs = o })
	if bytes.Equal(a.Obs.ContentHash, c.Obs.ContentHash) {
		t.Fatal("text changes must change the hash")
	}
}

func TestPrivatize(t *testing.T) {
	key := []byte("secret")
	s := gdpr.NewSuppression(nil)
	s.Add(gdpr.AuthorKey(key, "erased"), 0)
	e := &Emitter{HMACKey: key, Suppression: s}

	c := &fbv1.Comment{Text: "secret text", Author: &fbv1.Author{Id: "erased", Name: "X"},
		Attachments: []*fbv1.Attachment{{Url: "u"}}}
	if e.Privatize(c) || !c.Redacted || c.Text != "" || c.Author.Name != "" || len(c.Author.AuthorKey) == 0 || c.Attachments != nil {
		t.Fatalf("suppressed author not redacted: %+v", c)
	}
	e.Pseudonymize = true
	d := &fbv1.Comment{Text: "ok", Author: &fbv1.Author{Id: "someone", Name: "Y", Url: "u"}}
	if !e.Privatize(d) || d.Author.Id != "" || d.Author.Name != "" || len(d.Author.AuthorKey) != 32 || d.Text != "ok" {
		t.Fatalf("pseudonymisation wrong: %+v", d)
	}
}
