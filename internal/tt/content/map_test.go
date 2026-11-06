package content

import (
	"bytes"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// Synthetic records only: no real account, name or text.
var t0 = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func obs(at time.Time) *commonv1.Observation {
	return &commonv1.Observation{ObservedAt: timestamppb.New(at), JobIds: []string{"00000000-0000-0000-0000-000000000001"},
		QueryName: "post_list", DocId: "creator", ParserVersion: 3, Mode: commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, ExitCountry: "IQ"}
}

func pageAccount(at time.Time) *ttv1.Account {
	return &ttv1.Account{Obs: obs(at), UserId: 6900000000000000001, SecUid: "MS4wfake", Username: "fake.user", Nickname: "Fake User",
		Bio: "synthetic bio", BioLink: proto.String("https://example.org"), BioLinkRisk: proto.Int32(0), Verified: proto.Bool(false),
		Private: proto.Bool(false), Organization: proto.Bool(true), Source: ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE,
		CreatedAt: timestamppb.New(t0.Add(-1000 * time.Hour)), FollowingVisibility: proto.Int32(1),
		Counts: &ttv1.AccountCounts{Followers: proto.Int64(12345), Videos: proto.Int64(10), Hearts: proto.Int64(99), Exact: true}}
}

func post(at time.Time, caption string, plays int64) *ttv1.Post {
	return &ttv1.Post{Obs: obs(at), PostId: 7400000000000000001, UserId: 6900000000000000001, Kind: ttv1.PostKind_POST_KIND_VIDEO,
		CreatedAt: timestamppb.New(t0.Add(-2 * time.Hour)), Caption: caption, TextLanguage: "un",
		Hashtags:   []*ttv1.Hashtag{{Id: 1, Name: "Fake"}, {Id: 1, Name: "fake"}},
		TextRanges: []*ttv1.TextRange{{Offset: 0, Length: 5, Type: "hashtag", Name: "fake"}, {Offset: 6, Length: 5, Type: "mention", Name: "other", Person: &ttv1.Person{UserId: proto.Int64(6900000000000000002), Username: "other"}}},
		DurationMs: 15000, Width: 720, Height: 1280, CoverMediaId: "11111111-1111-5111-8111-111111111111",
		MusicId: proto.Int64(7300000000000000001), Pinned: proto.Bool(false), Category: proto.Int32(112), PlaylistId: "7500000000000000001",
		Labels: &ttv1.PostLabels{AigcLabelType: proto.Int32(2), Warnings: []string{"w"}}, DiscoveredVia: ttv1.DiscoveredVia_DISCOVERED_VIA_LIST,
		Counts: &ttv1.PostCounts{Plays: proto.Int64(plays), Likes: proto.Int64(9999), Comments: proto.Int64(3)}}
}

func comment(at time.Time, depth int32, parent, replyTo *int64) *ttv1.Comment {
	return &ttv1.Comment{Obs: obs(at), CommentId: 7600000000000000001 + int64(depth), PostId: 7400000000000000001, UserId: 6900000000000000001,
		PostCreatedAt: timestamppb.New(t0), Depth: depth, ParentCommentId: parent, ReplyToCommentId: replyTo,
		Author: &ttv1.Person{UserId: proto.Int64(6900000000000000001), SecUid: "MS4wfake", Username: "fake.user", PredictedAgeGroup: "band"},
		Text:   "synthetic", CreatedAt: timestamppb.New(t0), LikeCount: proto.Int64(10001), ReplyCount: proto.Int64(2),
		StickPosition: 1, Labels: []string{"Creator"}}
}

// Every event tks maps passes the contract's conformance checks, in
// production mode.
func TestMappedEventsConform(t *testing.T) {
	root := int64(7600000000000000001)
	reply := int64(7600000000000000002)
	msgs := []proto.Message{
		pageAccount(t0),
		&ttv1.Account{Obs: obs(t0), UserId: 6900000000000000003, Status: ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE},
		post(t0, "#fake @other", 15000),
		&ttv1.Post{Obs: obs(t0), PostId: 7400000000000000002, UserId: 6900000000000000001, Status: ttv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusCode: proto.Int64(10204)},
		&ttv1.PostMetrics{Obs: obs(t0), PostId: 7400000000000000001, UserId: 6900000000000000001, Counts: &ttv1.PostCounts{Plays: proto.Int64(5)}},
		comment(t0, 0, nil, nil),
		comment(t0, 1, &root, nil),
		comment(t0, 1, &root, &reply),
		&ttv1.Comment{Obs: obs(t0), CommentId: 7600000000000000009, PostId: 7400000000000000001, Depth: 1, ParentCommentId: &root, Status: ttv1.CommentStatus_COMMENT_STATUS_DELETED},
		&ttv1.Music{Obs: obs(t0), MusicId: 7300000000000000001, Title: "sound", AuthorName: "artist", DurationMs: 15000, CoverMediaId: "22222222-2222-5222-8222-222222222222"},
		&ttv1.Media{Obs: obs(t0), MediaId: "33333333-3333-5333-8333-333333333333", Kind: ttv1.MediaKind_MEDIA_KIND_VIDEO, PostId: proto.Int64(7400000000000000001), UserId: 6900000000000000001, Bitrate: 1000},
		&ttv1.Media{Obs: obs(t0), MediaId: "44444444-4444-5444-8444-444444444444", Kind: ttv1.MediaKind_MEDIA_KIND_AVATAR, UserId: 6900000000000000001},
	}
	evs := Map(msgs, map[int64]*Owner{6900000000000000001: {Username: "fake.user"}})
	kinds := map[string]int{}
	for _, e := range evs {
		e.Edited(t0)
		e.Seal()
		if err := Check(e.Msg, conformance.Options{Production: true}); err != nil {
			t.Errorf("%s %s: %v", e.Topic, e.Gate, err)
		}
		kinds[e.Topic]++
	}
	want := map[string]int{topicAccount: 2, topicAccountSnapshot: 1, topicPost: 2, topicMetrics: 4, topicComment: 4, topicAudio: 1, topicMedia: 2}
	for k, n := range want {
		if kinds[k] != n {
			t.Errorf("%s: %d events, want %d", k, kinds[k], n)
		}
	}
}

func TestPostMapping(t *testing.T) {
	evs := Post(post(t0, "#fake @other", 15000), &Owner{Username: "fake.user", SecUID: "MS4wfake"})
	m := evs[0].Msg.(*contentv1.Post)
	if m.Url != "https://www.tiktok.com/@fake.user/video/7400000000000000001" || m.Type != contentv1.PostType_POST_TYPE_SHORT_VIDEO {
		t.Fatalf("url %q type %v", m.Url, m.Type)
	}
	if len(m.Hashtags) != 1 || m.Hashtags[0] != "fake" || len(m.Mentions) != 1 || m.Mentions[0].Id != "tt:account:6900000000000000002" {
		t.Fatalf("hashtags %v mentions %v", m.Hashtags, m.Mentions)
	}
	if m.Counts.Views.GetExactness() != contentv1.CountExactness_COUNT_EXACTNESS_ROUNDED ||
		m.Counts.Likes.GetExactness() != contentv1.CountExactness_COUNT_EXACTNESS_EXACT {
		t.Fatalf("exactness: views %v likes %v", m.Counts.Views, m.Counts.Likes)
	}
	if m.Text.PlatformLanguage != "" || m.Category != "112" || m.Playlists[0].PlaylistId != "tt:playlist:7500000000000000001" {
		t.Fatalf("language %q category %q playlists %v", m.Text.PlatformLanguage, m.Category, m.Playlists)
	}
	if m.Extras.Fields["tt.aigc_label_type"].GetNumberValue() != 2 || m.Extras.Fields["tt.discovered_via"].GetStringValue() != "list" {
		t.Fatalf("extras %v", m.Extras)
	}
	// Counts are not content; the caption is.
	again := Post(post(t0.Add(time.Hour), "#fake @other", 20000), &Owner{Username: "fake.user", SecUID: "MS4wfake"})[0]
	if !bytes.Equal(again.Hash, evs[0].Hash) {
		t.Fatal("a change of counts alone changed the content hash")
	}
	edited := Post(post(t0.Add(time.Hour), "#fake @other edited", 15000), &Owner{Username: "fake.user", SecUID: "MS4wfake"})[0]
	if bytes.Equal(edited.Hash, evs[0].Hash) || bytes.Equal(edited.TextHash, evs[0].TextHash) {
		t.Fatal("a caption change kept the hashes")
	}
}

// The event id is the same for a redelivered record, and differs for a
// later observation of the same content.
func TestEventID(t *testing.T) {
	a, b := Post(post(t0, "x", 1), nil)[0], Post(post(t0, "x", 1), nil)[0]
	later := Post(post(t0.Add(time.Second), "x", 1), nil)[0]
	for _, e := range []*Event{a, b, later} {
		e.Seal()
	}
	id := func(e *Event) string { return e.Msg.(*contentv1.Post).Envelope.EventId }
	if id(a) != id(b) || id(a) == id(later) || len(id(a)) != 64 {
		t.Fatalf("event ids %s %s %s", id(a), id(b), id(later))
	}
	if v := a.Msg.(*contentv1.Post).Envelope.Version; v != t0.UnixMicro() {
		t.Fatalf("version %d", v)
	}
}

func TestCommentThreading(t *testing.T) {
	root, reply := int64(7600000000000000001), int64(7600000000000000002)
	cases := []struct {
		c              *ttv1.Comment
		parent, rootID string
		depth          int32
	}{
		{comment(t0, 0, nil, nil), "", "", 0},
		{comment(t0, 1, &root, nil), "tt:comment:7600000000000000001", "tt:comment:7600000000000000001", 1},
		{comment(t0, 1, &root, &root), "tt:comment:7600000000000000001", "tt:comment:7600000000000000001", 1},
		{comment(t0, 1, &root, &reply), "tt:comment:7600000000000000002", "tt:comment:7600000000000000001", 2},
	}
	for i, tc := range cases {
		m := Comment(tc.c)[0].Msg.(*contentv1.Comment)
		if m.ParentCommentId != tc.parent || m.RootCommentId != tc.rootID || m.Depth != tc.depth {
			t.Errorf("case %d: parent %q root %q depth %d", i, m.ParentCommentId, m.RootCommentId, m.Depth)
		}
	}
	m := Comment(comment(t0, 0, nil, nil))[0].Msg.(*contentv1.Comment)
	if !m.Flags.GetByPostOwner() || m.Extras.Fields["tt.predicted_age_group"].GetStringValue() != "band" {
		t.Fatalf("flags %v extras %v", m.Flags, m.Extras)
	}
	if e := Comment(comment(t0, 0, nil, nil))[0]; e.Key != "tt:post:7400000000000000001" {
		t.Fatalf("comment key %q, want the post", e.Key)
	}
}

// A list's author maps to nothing (rounded counts, no bio); the page's
// account does, with exact counts and the organization type.
func TestAccountSources(t *testing.T) {
	list := pageAccount(t0)
	list.Source = ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST
	if evs := Account(list); len(evs) != 0 {
		t.Fatalf("list author mapped to %d events", len(evs))
	}
	evs := Account(pageAccount(t0))
	m := evs[0].Msg.(*contentv1.Account)
	if m.Type != contentv1.AccountType_ACCOUNT_TYPE_ORGANIZATION || m.AltIds["sec_uid"] != "MS4wfake" ||
		m.Counts.Followers.GetExactness() != contentv1.CountExactness_COUNT_EXACTNESS_EXACT || m.Links[0].Source != contentv1.LinkSource_LINK_SOURCE_BIO {
		t.Fatalf("account %v", m)
	}
	if m.Envelope.Provenance.ExitCountry != "IQ" || m.Scope != contentv1.CollectionScope_COLLECTION_SCOPE_UNSPECIFIED {
		t.Fatalf("exit %q scope %v", m.Envelope.Provenance.ExitCountry, m.Scope)
	}
	b := pageAccount(t0.Add(time.Hour))
	b.Counts.Followers = proto.Int64(20000)
	if !bytes.Equal(Account(b)[0].Hash, evs[0].Hash) || bytes.Equal(Account(b)[1].Hash, evs[1].Hash) {
		t.Fatal("counts must change the snapshot's hash, not the account's")
	}
}
