package content

import (
	"testing"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// Synthetic WebVTT, shaped like TikTok's (a header, numbered cues).
const vtt = "\ufeffWEBVTT\r\n\r\nNOTE a comment\r\n\r\n1\r\n00:00:00.000 --> 00:00:02.000\r\n<c.x>first</c> line &amp; more\r\n" +
	"second\u200f line\r\n\r\n00:00:02.000 --> 00:00:04.000 align:start\r\n<00:00:02.500>third\r\n"

func TestVTTText(t *testing.T) {
	if got, want := VTTText(vtt), "first line & more\nsecond line\nthird"; got != want {
		t.Errorf("VTTText = %q, want %q", got, want)
	}
	if VTTText("") != "" || VTTText("WEBVTT\n\n") != "" {
		t.Error("an empty file has text")
	}
}

func TestSubtitleLanguage(t *testing.T) {
	for in, want := range map[string]string{"ara-SA": "ar-SA", "eng-US": "en-US", "ckb-IQ": "ckb-IQ", "ar": "ar", "un": "", "": "", "kur_iq": "ku-IQ"} {
		if got := SubtitleLanguage(in); got != want {
			t.Errorf("SubtitleLanguage(%q) = %q, want %q", in, got, want)
		}
	}
}

func subtitle(lang string, src ttv1.SubtitleSource, body string) *ttv1.Subtitle {
	return &ttv1.Subtitle{Info: &ttv1.SubtitleInfo{Language: lang, SourceName: src.String(), Source: src}, Body: body}
}

// A post carries its title, AI text, caption languages and stored
// captions; a read's subtitles are a partial post event with the captions
// only, on a gate stream of their own; both conform.
func TestCaptionsMapping(t *testing.T) {
	asr, mt := ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR, ttv1.SubtitleSource_SUBTITLE_SOURCE_MT
	stored := []*ttv1.Subtitle{subtitle("eng-US", mt, vtt), subtitle("ara-SA", asr, vtt), subtitle("fra-FR", mt, "")}
	p := post(t0, "#fake @other", 15000)
	p.Labels.AiDescription = "synthetic AI note"
	p.Subtitles = []*ttv1.SubtitleInfo{{Language: "ara-SA", Source: asr}, {Language: "eng-US", Source: mt}, {Language: "ara-SA", Source: mt}}
	msgs := []proto.Message{p,
		&ttv1.PostSubtitles{Obs: obs(t0), PostId: p.PostId, UserId: p.UserId, PostCreatedAt: p.CreatedAt, Tracks: stored[:1]},
		&ttv1.Post{Obs: obs(t0), PostId: 7400000000000000003, UserId: p.UserId, Kind: ttv1.PostKind_POST_KIND_PHOTO,
			CreatedAt: timestamppb.New(t0), Caption: "photo", Title: "synthetic title", TextLanguage: "ar"},
	}
	evs := Map(msgs, nil, map[int64][]*ttv1.Subtitle{p.PostId: stored})
	if len(evs) != 3 {
		t.Fatalf("%d events, want 3", len(evs))
	}
	for _, e := range evs {
		e.Seal()
		if err := Check(e.Msg, conformance.Options{Production: true}); err != nil {
			t.Errorf("%s: %v", e.Gate, err)
		}
	}
	full, part, photo := evs[0].Msg.(*contentv1.Post), evs[1].Msg.(*contentv1.Post), evs[2].Msg.(*contentv1.Post)
	if l := full.Labels.GetAiLabelText(); l != "synthetic AI note" {
		t.Errorf("ai_label_text %q", l)
	}
	if got := full.Video.GetCaptionLanguages(); len(got) != 2 || got[0] != "ar-SA" || got[1] != "en-US" {
		t.Errorf("caption_languages %v", got)
	}
	want := []*contentv1.CaptionTrack{
		{Language: "ar-SA", Text: "first line & more\nsecond line\nthird", Source: contentv1.CaptionSource_CAPTION_SOURCE_PLATFORM_ASR},
		{Language: "en-US", Text: "first line & more\nsecond line\nthird", Source: contentv1.CaptionSource_CAPTION_SOURCE_MACHINE_TRANSLATION},
	}
	for name, got := range map[string][]*contentv1.CaptionTrack{"full": full.Captions, "partial": part.Captions} {
		if len(got) != len(want) {
			t.Fatalf("%s: %d captions, want %d (the empty track left out)", name, len(got), len(want))
		}
		for i := range want {
			if !proto.Equal(got[i], want[i]) {
				t.Errorf("%s caption %d: %v, want %v", name, i, got[i], want[i])
			}
		}
	}
	if !part.Envelope.Partial || part.Text != nil || part.Counts != nil || evs[1].Gate != "captions/"+full.Envelope.Id {
		t.Errorf("captions event: partial %v, gate %q", part.Envelope.Partial, evs[1].Gate)
	}
	if photo.Title.GetText() != "synthetic title" || photo.Title.GetPlatformLanguage() != "ar" || photo.Captions != nil {
		t.Errorf("photo post: title %v, captions %v", photo.Title, photo.Captions)
	}
	// Without stored tracks, the read's own; none with text: no event.
	if e := PostCaptions(msgs[1].(*ttv1.PostSubtitles), nil); e == nil || len(e.Msg.(*contentv1.Post).Captions) != 1 {
		t.Error("the read's tracks are not used when none is stored")
	}
	if PostCaptions(&ttv1.PostSubtitles{Obs: obs(t0), PostId: 1, UserId: 2, Tracks: stored[2:]}, nil) != nil {
		t.Error("an event for tracks without text")
	}
}
