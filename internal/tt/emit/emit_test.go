package emit

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
)

var secret = []byte("test-secret")

// tks's key is domain-separated from fbs's on the same id and secret.
func TestAuthorKey(t *testing.T) {
	k := AuthorKey(secret, 42)
	if len(k) != 32 || bytes.Equal(k, gdpr.AuthorKey(secret, "42")) || !bytes.Equal(k, AuthorKey(secret, 42)) {
		t.Fatalf("key %x", k)
	}
	if AuthorKey(secret, 0) != nil {
		t.Fatal("a key for no id")
	}
}

// The same content read twice, with new signed links, expiries and capture
// times, has the same hash and event id; a changed count does not.
func TestStampStable(t *testing.T) {
	e := &Emitter{HMACKey: secret}
	stampOf := func(m proto.Message, key string) *commonv1.Observation {
		var o *commonv1.Observation
		e.stamp(Source{JobID: "j"}, key, m, func(x *commonv1.Observation) { o = x })
		return o
	}
	media := func(sig string, exp int64) *ttv1.Media {
		return &ttv1.Media{MediaId: "m", SourceUrl: "https://p16-sign.example/cover.jpg?x-expires=" + sig + "&x-signature=" + sig,
			SourceExpiresAt: timestamppb.New(timestamppb.Now().AsTime().Add(1e9 * 3600 * 48)), Width: int32(exp)}
	}
	a, b := stampOf(media("1", 1), "media:m"), stampOf(media("2", 1), "media:m")
	if !bytes.Equal(a.ContentHash, b.ContentHash) || a.EventId != b.EventId || a.Platform != commonv1.Platform_PLATFORM_TIKTOK {
		t.Errorf("signed links changed the hash: %x %x", a.ContentHash, b.ContentHash)
	}
	if c := stampOf(media("1", 2), "media:m"); bytes.Equal(a.ContentHash, c.ContentHash) {
		t.Error("a changed width kept the hash")
	}
	m1 := &ttv1.PostMetrics{PostId: 1, CapturedAt: timestamppb.Now(), Counts: &ttv1.PostCounts{Likes: proto.Int64(3)}}
	m2 := &ttv1.PostMetrics{PostId: 1, CapturedAt: timestamppb.New(m1.CapturedAt.AsTime().Add(1e9)), Counts: &ttv1.PostCounts{Likes: proto.Int64(3)}}
	if x, y := stampOf(m1, "pm:1"), stampOf(m2, "pm:1"); x.EventId != y.EventId {
		t.Error("the capture time changed an unchanged sample's event")
	}
	m2.Counts.Likes = proto.Int64(4)
	if x, y := stampOf(m1, "pm:1"), stampOf(m2, "pm:1"); x.EventId == y.EventId {
		t.Error("a changed count kept the event")
	}
}

func person(id int64) *ttv1.Person {
	return &ttv1.Person{UserId: proto.Int64(id), SecUid: "sec", Username: "name", Nickname: "nick", AvatarUrl: "https://a/x.jpg"}
}

// Pseudonymising leaves people only their author key (and TikTok's age
// band, which does not identify them): comment authors and mentioned users
// alike. Without it everything is kept, the age band included.
func TestPrivatize(t *testing.T) {
	e := &Emitter{HMACKey: secret, Pseudonymize: true}
	author := person(5)
	author.PredictedAgeGroup = "band"
	c := &ttv1.Comment{CommentId: 1, UserId: 9, Text: "hi @name", Author: author,
		TextRanges: []*ttv1.TextRange{{Offset: 3, Length: 5, Type: "mention", Person: person(6)}}}
	e.Privatize(c)
	if !proto.Equal(c.Author, &ttv1.Person{AuthorKey: AuthorKey(secret, 5), PredictedAgeGroup: "band"}) ||
		!proto.Equal(c.TextRanges[0].Person, &ttv1.Person{AuthorKey: AuthorKey(secret, 6)}) {
		t.Errorf("people kept identifying fields: %v %v", c.Author, c.TextRanges[0].Person)
	}
	if c.Text == "" {
		t.Error("the text went")
	}

	e = &Emitter{HMACKey: secret}
	author = person(5)
	author.PredictedAgeGroup = "band"
	c = &ttv1.Comment{CommentId: 2, UserId: 9, Text: "kept", Author: author,
		TextRanges: []*ttv1.TextRange{{Type: "mention", Person: person(7)}}}
	e.Privatize(c)
	if c.Text != "kept" || c.Redacted || c.Author.GetUsername() != "name" || c.Author.PredictedAgeGroup != "band" ||
		!bytes.Equal(c.Author.AuthorKey, AuthorKey(secret, 5)) || c.TextRanges[0].Person.GetUsername() != "name" {
		t.Errorf("a comment lost data: %v", c)
	}
}
