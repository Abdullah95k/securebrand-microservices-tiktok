package content

import (
	"cmp"
	"context"
	"html"
	"slices"
	"strings"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// iso639 maps the three-letter language codes TikTok's subtitle tracks
// carry (LanguageCodeName: ara-SA, eng-US) to BCP 47's two-letter ones,
// where one exists; any other code is kept as it is (BCP 47 takes
// three-letter codes too: ckb).
var iso639 = map[string]string{
	"ara": "ar", "eng": "en", "kur": "ku", "tur": "tr", "fas": "fa", "per": "fa", "fra": "fr", "fre": "fr", "deu": "de",
	"ger": "de", "spa": "es", "rus": "ru", "hin": "hi", "urd": "ur", "ind": "id", "por": "pt", "ita": "it", "jpn": "ja",
	"kor": "ko", "zho": "zh", "chi": "zh", "cmn": "zh", "vie": "vi", "tha": "th", "heb": "he", "nld": "nl", "dut": "nl",
	"pol": "pl", "ukr": "uk", "ben": "bn", "msa": "ms", "may": "ms", "swe": "sv", "ron": "ro", "rum": "ro", "ell": "el",
	"gre": "el", "aze": "az", "kaz": "kk", "uzb": "uz", "tgl": "tl", "fil": "fil",
}

// SubtitleLanguage is a track's language as BCP 47 ("ara-SA" is "ar-SA");
// empty for none.
func SubtitleLanguage(code string) string {
	lang, region, _ := strings.Cut(strings.ReplaceAll(strings.TrimSpace(code), "_", "-"), "-")
	lang = strings.ToLower(lang)
	if lang == "" || lang == "un" || lang == "und" {
		return ""
	}
	if two, ok := iso639[lang]; ok {
		lang = two
	}
	if region != "" {
		return lang + "-" + strings.ToUpper(region)
	}
	return lang
}

func captionSource(s ttv1.SubtitleSource) contentv1.CaptionSource {
	switch s {
	case ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR:
		return contentv1.CaptionSource_CAPTION_SOURCE_PLATFORM_ASR
	case ttv1.SubtitleSource_SUBTITLE_SOURCE_MT:
		return contentv1.CaptionSource_CAPTION_SOURCE_MACHINE_TRANSLATION
	}
	return contentv1.CaptionSource_CAPTION_SOURCE_UNSPECIFIED
}

// VTTText is a WebVTT file's text without its timings: each cue's lines,
// tags and entities removed, one line per cue line. The contract's
// CaptionTrack.text is the text; tks keeps the file (tt.post_subtitles).
func VTTText(vtt string) string {
	vtt = strings.TrimPrefix(strings.ReplaceAll(vtt, "\r\n", "\n"), "\ufeff")
	var out []string
	for i, block := range strings.Split(vtt, "\n\n") {
		lines := strings.Split(strings.Trim(block, "\n"), "\n")
		if i == 0 && strings.HasPrefix(lines[0], "WEBVTT") {
			continue // the header block
		}
		if f := lines[0]; strings.HasPrefix(f, "NOTE") || f == "STYLE" || strings.HasPrefix(f, "REGION") {
			continue
		}
		cue := false
		for _, l := range lines {
			if !cue {
				// a cue's identifier comes before its timing line
				cue = strings.Contains(l, "-->")
				continue
			}
			if t := strings.TrimSpace(html.UnescapeString(stripTags(l))); t != "" {
				out = append(out, strings.Map(dropMarks, t))
			}
		}
	}
	return strings.Join(out, "\n")
}

// stripTags removes WebVTT's cue tags (<c.x>, <v Name>, <00:00:01.000>).
func stripTags(s string) string {
	var b strings.Builder
	in := false
	for _, r := range s {
		switch {
		case r == '<':
			in = true
		case r == '>' && in:
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// dropMarks drops the direction marks WebVTT's &lrm; and &rlm; leave.
func dropMarks(r rune) rune {
	if r == '\u200e' || r == '\u200f' {
		return -1
	}
	return r
}

// captionTracks maps a post's stored or fetched tracks to the contract's,
// speech-to-text first, then by language; tracks without text are left
// out.
func captionTracks(ss []*ttv1.Subtitle) []*contentv1.CaptionTrack {
	type tr struct {
		asr bool
		c   *contentv1.CaptionTrack
	}
	var out []tr
	for _, s := range ss {
		text := VTTText(s.GetBody())
		if text == "" {
			continue
		}
		i := s.GetInfo()
		out = append(out, tr{asr: i.GetSource() == ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR,
			c: &contentv1.CaptionTrack{Language: SubtitleLanguage(i.GetLanguage()), Text: text, Source: captionSource(i.GetSource())}})
	}
	slices.SortStableFunc(out, func(a, b tr) int {
		if a.asr != b.asr {
			if a.asr {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.c.Language, b.c.Language)
	})
	cs := make([]*contentv1.CaptionTrack, len(out))
	for i, t := range out {
		cs[i] = t.c
	}
	return cs
}

// PostCaptions maps a read's subtitle files to a partial content.post.v1
// event that carries only Post.captions: every track of the post with text
// (stored, when the persister gives them; else the read's). It goes through
// the gate on a stream of its own ("captions/<post id>"), so it goes out
// when the post's captions change, and does not disturb the post's content
// stream.
func PostCaptions(s *ttv1.PostSubtitles, stored []*ttv1.Subtitle) *Event {
	if s.GetPostId() == 0 || s.GetUserId() == 0 {
		return nil
	}
	tracks := stored
	if len(tracks) == 0 {
		tracks = s.Tracks
	}
	cs := captionTracks(tracks)
	if len(cs) == 0 {
		return nil
	}
	env := envelope("post", "post", s.PostId, s.Obs, true)
	m := &contentv1.Post{Envelope: env, AccountId: idOf("account", s.UserId), Captions: cs}
	e := newEvent(m, env, topicPost, nil, false)
	e.Gate = "captions/" + env.Id
	return e
}

// storedSubtitles reads the stored tracks with text of the batch's posts
// and of the posts whose subtitles it carries (written before the
// contract's events are mapped).
func (e *Emitter) storedSubtitles(ctx context.Context, msgs []proto.Message) (map[int64][]*ttv1.Subtitle, error) {
	var ids []int64
	for _, m := range msgs {
		switch m := m.(type) {
		case *ttv1.Post:
			if m.Status != ttv1.PostStatus_POST_STATUS_UNAVAILABLE && m.Status != ttv1.PostStatus_POST_STATUS_PRIVATE &&
				m.Kind == ttv1.PostKind_POST_KIND_VIDEO {
				ids = append(ids, m.PostId)
			}
		case *ttv1.PostSubtitles:
			ids = append(ids, m.PostId)
		}
	}
	if len(ids) == 0 || e.DB == nil {
		return nil, nil
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	rows, err := e.DB.Query(ctx, `SELECT post_id, language, source_name, source, body FROM post_subtitles
		WHERE post_id = ANY($1) AND body IS NOT NULL`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]*ttv1.Subtitle{}
	for rows.Next() {
		var id int64
		var lang, name, src, body string
		if err := rows.Scan(&id, &lang, &name, &src, &body); err != nil {
			return nil, err
		}
		s := ttv1.SubtitleSource_SUBTITLE_SOURCE_UNSPECIFIED
		switch src {
		case "asr":
			s = ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR
		case "mt":
			s = ttv1.SubtitleSource_SUBTITLE_SOURCE_MT
		}
		out[id] = append(out[id], &ttv1.Subtitle{Info: &ttv1.SubtitleInfo{Language: lang, SourceName: name, Source: s}, Body: body})
	}
	return out, rows.Err()
}
