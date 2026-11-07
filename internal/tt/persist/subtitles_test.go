package persist

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

func subs(at time.Time, tracks ...*ttv1.Subtitle) *ttv1.PostSubtitles {
	return &ttv1.PostSubtitles{Obs: obs(at, "s"), PostId: 7400000000000000001, UserId: 7001,
		PostCreatedAt: timestamppb.New(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)), Tracks: tracks}
}

func track(lang, src, body, skipped string) *ttv1.Subtitle {
	s := ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR
	if src == "MT" {
		s = ttv1.SubtitleSource_SUBTITLE_SOURCE_MT
	}
	return &ttv1.Subtitle{Info: &ttv1.SubtitleInfo{Language: lang, SourceName: src, Source: s, Version: "1", Format: "webvtt",
		SizeBytes: int64(len(body))}, Body: body, Skipped: skipped}
}

// One row per post, language and source; rewritten only when the file
// changed and the read is not older; a later fetch without a body keeps
// the stored one; a post's title is stored with it.
func TestSubtitlesStored(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	q := `SELECT coalesce(body, '') || '|' || coalesce(skipped, '') || '|' || source FROM post_subtitles
		WHERE post_id = 7400000000000000001 AND language = $1 AND source_name = $2`
	if err := w.Subtitles(ctx, []*ttv1.PostSubtitles{subs(t0, track("ara-SA", "ASR", "WEBVTT a", ""), track("eng-US", "MT", "WEBVTT b", ""))}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, q, "ara-SA", "ASR"); got != "WEBVTT a||asr" {
		t.Errorf("ara-SA: %q", got)
	}
	if got := one[string](t, w.DB, q, "eng-US", "MT"); got != "WEBVTT b||mt" {
		t.Errorf("eng-US: %q", got)
	}
	// An older read does not overwrite; a newer one does; one too large
	// keeps the text stored.
	if err := w.Subtitles(ctx, []*ttv1.PostSubtitles{
		subs(t0.Add(-time.Hour), track("ara-SA", "ASR", "WEBVTT old", "")),
		subs(t0.Add(time.Hour), track("eng-US", "MT", "", "too_large")),
	}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, q, "ara-SA", "ASR"); got != "WEBVTT a||asr" {
		t.Errorf("an older read overwrote: %q", got)
	}
	if got := one[string](t, w.DB, q, "eng-US", "MT"); got != "WEBVTT b||mt" {
		t.Errorf("a fetch without its text dropped the stored one: %q", got)
	}
	if err := w.Subtitles(ctx, []*ttv1.PostSubtitles{subs(t0.Add(2*time.Hour), track("ara-SA", "ASR", "WEBVTT new", ""))}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, q, "ara-SA", "ASR"); got != "WEBVTT new||asr" {
		t.Errorf("a newer read: %q", got)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM post_subtitles`); n != 2 {
		t.Errorf("%d rows, want 2", n)
	}

	p := post(7400000000000000002, t0, 1, nil)
	p.Title = "synthetic title"
	if err := w.Posts(ctx, []*ttv1.Post{p}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT title FROM posts WHERE post_id = $1`, p.PostId); got != p.Title {
		t.Errorf("title %q", got)
	}
}
