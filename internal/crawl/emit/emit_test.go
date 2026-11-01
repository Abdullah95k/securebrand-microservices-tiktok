package emit

import (
	"bytes"
	"context"
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
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

// Pseudonymisation also clears the people a comment or post tags, which a
// reply carries for the person it answers.
func TestPseudonymizeScrubsMentions(t *testing.T) {
	mention := func() []*fbv1.TextRange {
		return []*fbv1.TextRange{{EntityType: "User", EntityId: "100001", Name: "Some One", Url: "u"}, {EntityType: "Hashtag", Name: "#x"}}
	}
	for _, pseudo := range []bool{false, true} {
		e := &Emitter{HMACKey: []byte("k"), Pseudonymize: pseudo}
		c := &fbv1.Comment{Text: "Some One hi", TextRanges: mention()}
		e.Privatize(c)
		authorless := &fbv1.Comment{TextRanges: mention()}
		e.Privatize(authorless)
		for _, rs := range [][]*fbv1.TextRange{c.TextRanges, authorless.TextRanges} {
			if scrubbed := rs[0].EntityId == "" && rs[0].Name == ""; scrubbed != pseudo || rs[1].Name != "#x" {
				t.Errorf("pseudonymize %v: %+v", pseudo, rs)
			}
		}
	}
}

func TestPostMentionsArePseudonymized(t *testing.T) {
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // nothing is sent; only the message matters
	for _, pseudo := range []bool{false, true} {
		e := &Emitter{HMACKey: []byte("k"), Pseudonymize: pseudo}
		p := &fbv1.Post{PostId: 1, TextRanges: []*fbv1.TextRange{{EntityType: "User", EntityId: "100001", Name: "Some One"}}}
		_ = e.Post(ctx, prod.NewBatch(), Source{}, p)
		if scrubbed := p.TextRanges[0].EntityId == ""; scrubbed != pseudo {
			t.Errorf("pseudonymize %v: %+v", pseudo, p.TextRanges[0])
		}
	}
}

// Without pseudonymisation, the mentions of an erased author are still
// scrubbed; other people's stay.
func TestErasedAuthorMentionsAreScrubbed(t *testing.T) {
	key := []byte("secret")
	s := gdpr.NewSuppression(nil)
	s.Add(gdpr.AuthorKey(key, "100001"), 0)
	e := &Emitter{HMACKey: key, Suppression: s}
	mentions := func() []*fbv1.TextRange {
		return []*fbv1.TextRange{{EntityType: "User", EntityId: "100001", Name: "Erased"}, {EntityType: "User", EntityId: "100002", Name: "Kept"}}
	}
	c := &fbv1.Comment{Text: "Erased Kept", TextRanges: mentions(), Author: &fbv1.Author{Id: "100003"}}
	e.Privatize(c)
	p := &fbv1.Post{TextRanges: mentions()}
	e.scrubMentions(p.TextRanges)
	for _, rs := range [][]*fbv1.TextRange{c.TextRanges, p.TextRanges} {
		if rs[0].EntityId != "" || rs[0].Name != "" || rs[1].EntityId != "100002" || rs[1].Name != "Kept" {
			t.Errorf("mentions: %+v", rs)
		}
	}
}

// What a task already running when its page was erased finds is not
// published: comments, posts and media are dropped (a comment privatized on
// its own is redacted).
func TestErasedPageIsNotPublished(t *testing.T) {
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // nothing is sent; only what reaches the batch matters
	s := gdpr.NewSuppression(nil)
	s.Add(nil, 9)
	e := &Emitter{HMACKey: []byte("k"), Suppression: s}
	c := &fbv1.Comment{CommentId: 1, PageId: 9, Text: "hi", Author: &fbv1.Author{Id: "100001", Name: "A"},
		TextRanges: []*fbv1.TextRange{{EntityType: "User", EntityId: "100002", Name: "B"}}}
	if e.Privatize(c) || !c.Redacted || c.Text != "" || c.Author.Id != "" || c.TextRanges != nil {
		t.Fatalf("comment of an erased page: %+v", c)
	}
	published := &fbv1.Comment{CommentId: 4, PageId: 9, Text: "hi"}
	if keep, err := e.Comment(ctx, prod.NewBatch(), Source{}, published); keep || err != nil || published.Obs != nil {
		t.Fatalf("comment of an erased page was stamped for publishing: %v", err)
	}
	p := &fbv1.Post{PostId: 2, PageId: 9}
	if err := e.Post(ctx, prod.NewBatch(), Source{}, p); err != nil || p.Obs != nil {
		t.Fatalf("post of an erased page was stamped for publishing: %v", err)
	}
	m := &fbv1.Media{MediaId: "m", PageId: 9}
	if err := e.Media(ctx, prod.NewBatch(), Source{}, m); err != nil || m.Obs != nil {
		t.Fatalf("media of an erased page was stamped for publishing: %v", err)
	}
	other := &fbv1.Comment{CommentId: 3, PageId: 8, Text: "kept", Author: &fbv1.Author{Id: "100003"}}
	if !e.Privatize(other) || other.Text != "kept" {
		t.Fatal("another page's comment was redacted")
	}
}
