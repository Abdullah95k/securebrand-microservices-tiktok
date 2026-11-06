package stage

import (
	"context"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

func subtitleRecords(h *harness, job string) map[int64]*ttv1.PostSubtitles {
	out := map[int64]*ttv1.PostSubtitles{}
	for _, s := range records(h, topics.Subtitle, job, func() *ttv1.PostSubtitles { return &ttv1.PostSubtitles{} }) {
		out[s.PostId] = s
	}
	return out
}

// A backfill fetches every video's subtitle files and publishes them on
// tt.subtitle.v1 as fakett serves them, with the tracks, the photo posts'
// titles and the AI text on the posts; tracks tks has stored are not
// fetched again; a refused file fails no task and is fetched on the next
// read; a file over max_bytes is recorded without its text (M17).
func TestSubtitles(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	acc := w.Accounts()[0]
	sc := task.ScopeFromRef("u:" + acc.Username)
	st := h.check("subs", commonv1.CrawlMode_CRAWL_MODE_FULL, sc)
	if st.Counters["tasks_failed"] != 0 {
		t.Fatalf("counters %v", st.Counters)
	}
	got, ps := subtitleRecords(h, "subs"), posts(h, "subs")
	want, files := 0, 0
	for _, p := range acc.Posts {
		pub := ps[p.ID]
		if pub.GetTitle() != p.Title || pub.GetLabels().GetAiDescription() != p.AIDescription || len(pub.GetSubtitles()) != len(p.Subtitles) {
			t.Errorf("post %d: title %q, AI text %q, %d tracks; want %q, %q, %d", p.ID, pub.GetTitle(),
				pub.GetLabels().GetAiDescription(), len(pub.GetSubtitles()), p.Title, p.AIDescription, len(p.Subtitles))
		}
		if len(p.Subtitles) == 0 {
			if got[p.ID] != nil {
				t.Errorf("post %d: subtitles published for a post without tracks", p.ID)
			}
			continue
		}
		want++
		files += len(p.Subtitles)
		rec := got[p.ID]
		if rec == nil || len(rec.Tracks) != len(p.Subtitles) || rec.UserId != acc.UID || !rec.PostCreatedAt.AsTime().Equal(p.Created) {
			t.Fatalf("post %d: %v, want %d tracks; counters %v", p.ID, rec, len(p.Subtitles), st.Counters)
		}
		for _, tr := range p.Subtitles {
			ok := false
			for _, g := range rec.Tracks {
				ok = ok || (g.Info.Language == tr.Language && g.Info.SourceName == tr.Source && g.Body == tr.VTT)
			}
			if !ok {
				t.Errorf("post %d: track %s/%s missing or different", p.ID, tr.Language, tr.Source)
			}
		}
		if rec.Tracks[0].Info.Source != ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR {
			t.Errorf("post %d: speech-to-text not first", p.ID)
		}
	}
	if want == 0 || len(got) != want || st.Counters["subtitles_fetched"] != int64(files) {
		t.Fatalf("%d posts' subtitles published, want %d; counters %v (%d files)", len(got), want, st.Counters, files)
	}
	if n := h.env.Fake.SubtitleDownloads.Load(); n != int64(files) {
		t.Errorf("fakett served %d files, want %d", n, files)
	}

	// Stored tracks are not fetched again (the persister's row stands in).
	ctx := context.Background()
	for id, rec := range got {
		for _, tr := range rec.Tracks {
			if _, err := h.d.DB.Exec(ctx, `INSERT INTO post_subtitles (post_id, language, source_name, source, user_id,
				post_created_at, body, content_hash, first_seen_at, last_seen_at) VALUES ($1, $2, $3, 'asr', $4, $5, $6, '\x00', now(), now())`,
				id, tr.Info.Language, tr.Info.SourceName, rec.UserId, rec.PostCreatedAt.AsTime(), tr.Body); err != nil {
				t.Fatal(err)
			}
		}
	}
	served := h.env.Fake.SubtitleDownloads.Load()
	st = h.check("again", commonv1.CrawlMode_CRAWL_MODE_FULL, sc)
	if len(subtitleRecords(h, "again")) != 0 || h.env.Fake.SubtitleDownloads.Load() != served || st.Counters["subtitles_fetched"] != 0 {
		t.Errorf("stored tracks fetched again: counters %v", st.Counters)
	}

	// A new post: its tracks only. Refused, then served.
	w.Advance(time.Hour)
	np := w.AddPost(acc, 0)
	h.env.Fake.SetFaults(fakett.Faults{SubtitlesRefused: true})
	st = h.check("refused", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, sc)
	if st.Counters["tasks_failed"] != 0 || st.Counters["subtitles_failed"] != int64(len(np.Subtitles)) || len(subtitleRecords(h, "refused")) != 0 {
		t.Errorf("refused files: counters %v", st.Counters)
	}
	h.env.Fake.SetFaults(fakett.Faults{})
	st = h.check("served", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, sc)
	if rec := subtitleRecords(h, "served"); len(rec) != 1 || len(rec[np.ID].GetTracks()) != len(np.Subtitles) {
		t.Errorf("after the refusal: %v, counters %v", rec, st.Counters)
	}

	// Too large: recorded without its text.
	h.d.Cfg.TT.Subtitles.MaxBytes = 10
	st = h.check("large", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, sc)
	rec := subtitleRecords(h, "large")[np.ID]
	if rec == nil || st.Counters["subtitles_too_large"] != int64(len(np.Subtitles)) {
		t.Fatalf("too large: %v, counters %v", rec, st.Counters)
	}
	for _, tr := range rec.Tracks {
		if tr.Body != "" || tr.Skipped != "too_large" {
			t.Errorf("too large track %s: body %d bytes, skipped %q", tr.Info.Language, len(tr.Body), tr.Skipped)
		}
	}

	// Off: nothing fetched.
	h.d.Cfg.TT.Subtitles.Include = false
	served = h.env.Fake.SubtitleDownloads.Load()
	h.check("off", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, sc)
	if len(subtitleRecords(h, "off")) != 0 || h.env.Fake.SubtitleDownloads.Load() != served {
		t.Error("subtitles fetched with tt.subtitles.include off")
	}
}

func TestWantedTracks(t *testing.T) {
	tr := func(lang string, s ttv1.SubtitleSource) parse.SubtitleTrack {
		return parse.SubtitleTrack{Info: &ttv1.SubtitleInfo{Language: lang, Source: s}}
	}
	mt, asr := ttv1.SubtitleSource_SUBTITLE_SOURCE_MT, ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR
	got := wantedTracks([]parse.SubtitleTrack{tr("eng-US", mt), tr("fra-FR", mt), tr("ara-SA", asr)}, 2)
	var langs []string
	for _, g := range got {
		langs = append(langs, g.Info.Language)
	}
	if strings.Join(langs, ",") != "ara-SA,eng-US" {
		t.Errorf("wanted %v, want speech-to-text first, then TikTok's order, capped at 2", langs)
	}
	if !isWebVTT([]byte("\xef\xbb\xbfWEBVTT\n\n")) || isWebVTT(nil) || isWebVTT([]byte("<html>")) {
		t.Error("isWebVTT")
	}
}
