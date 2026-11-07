package stage

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"slices"

	"google.golang.org/protobuf/proto"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/media"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Subtitle fetch results (tks_subtitle_fetches_total, and the job's
// subtitles_* counters).
const (
	subFetched  = "fetched"
	subFailed   = "failed"
	subExpired  = "expired"
	subTooLarge = "too_large"
)

// skippedTooLarge is Subtitle.skipped for a file over tt.subtitles.max_bytes.
const skippedTooLarge = "too_large"

// subtitleKey names a track: its language and source, as served.
func subtitleKey(language, source string) string { return language + "|" + source }

// storedSubtitles returns the tracks tt.post_subtitles holds for posts, by
// post: what a read need not fetch again. Without a database nothing is
// stored.
func (d *Deps) storedSubtitles(ctx context.Context, posts []int64) (map[int64]map[string]bool, error) {
	out := map[int64]map[string]bool{}
	if d.DB == nil || len(posts) == 0 {
		return out, nil
	}
	rows, err := d.DB.Query(ctx, `SELECT post_id, language, source_name FROM post_subtitles WHERE post_id = ANY($1)`, posts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var lang, src string
		if err := rows.Scan(&id, &lang, &src); err != nil {
			return nil, err
		}
		if out[id] == nil {
			out[id] = map[string]bool{}
		}
		out[id][subtitleKey(lang, src)] = true
	}
	return out, rows.Err()
}

// subtitles fetches the subtitle files of a page's videos with a track
// tks has not stored (tt.subtitles; M17) and publishes them on
// tt.subtitle.v1, one record per post with all its tracks (speech-to-text
// first, up to max_tracks), so a post's record is always whole. The links
// expire (about two days after the read), so the files are fetched now, by
// the identity that read them: its exit, its TLS fingerprint and, to the
// cookie domains only, its tt_chain_token, as a video file needs (T18; T42:
// whether a subtitle file needs it is not measured). A file that fails is
// not recorded, so a later read of the post fetches it again; one over
// max_bytes is recorded without its text. A failure here never fails the
// task: the posts are already published.
func (d *Deps) subtitles(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, src emit.Source, id *session.Identity,
	posts []parse.PostBundle, c map[string]int64) error {
	cfg := d.Cfg.TT.Subtitles
	if !cfg.Include || id == nil || id.Doer == nil {
		return nil
	}
	var ids []int64
	for _, pb := range posts {
		if len(pb.Subtitles) > 0 {
			ids = append(ids, pb.Post.PostId)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	have, err := d.storedSubtitles(ctx, ids)
	if err != nil {
		return err
	}
	f := fetcherOf(id)
	now := d.now()
	for _, pb := range posts {
		tracks := wantedTracks(pb.Subtitles, cfg.MaxTracks)
		if len(tracks) == 0 {
			continue
		}
		stored := have[pb.Post.PostId]
		missing := false
		for _, tr := range tracks {
			if !stored[subtitleKey(tr.Info.Language, tr.Info.SourceName)] {
				missing = true
			}
		}
		if !missing {
			continue
		}
		rec := &ttv1.PostSubtitles{PostId: pb.Post.PostId, UserId: pb.Post.UserId, PostCreatedAt: pb.Post.CreatedAt}
		for _, tr := range tracks {
			if !tr.Expires.IsZero() && !tr.Expires.After(now) {
				d.subtitleResult(c, subExpired)
				continue
			}
			body, res := d.fetchSubtitle(ctx, t, id, f, tr.URL, c)
			d.subtitleResult(c, res)
			sub := &ttv1.Subtitle{Info: proto.Clone(tr.Info).(*ttv1.SubtitleInfo)}
			switch res {
			case subFetched:
				sub.Body = string(body)
			case subTooLarge:
				sub.Skipped = skippedTooLarge
			default:
				continue
			}
			rec.Tracks = append(rec.Tracks, sub)
		}
		if err := d.Emit.Subtitles(ctx, b, src, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *Deps) subtitleResult(c map[string]int64, res string) {
	c["subtitles_"+res]++
	metrics.SubtitleFetches.WithLabelValues(res).Inc()
}

// wantedTracks orders a post's tracks speech-to-text first (the original
// speech; the translations are made from it), keeping TikTok's order
// otherwise, and caps them.
func wantedTracks(ts []parse.SubtitleTrack, limit int) []parse.SubtitleTrack {
	out := slices.Clone(ts)
	slices.SortStableFunc(out, func(a, b parse.SubtitleTrack) int {
		ra, rb := a.Info.Source != ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR, b.Info.Source != ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR
		switch {
		case ra == rb:
			return 0
		case !ra:
			return -1
		}
		return 1
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// fetchSubtitle gets one file the way a browser playing the video does:
// the CDN's link with media headers, the cookie only to the cookie domains.
// A file is good when it answers 200 with a WebVTT body (an empty answer is
// how TikTok refuses, T4).
func (d *Deps) fetchSubtitle(ctx context.Context, t *tkstaskv1.Task, id *session.Identity, f fetcher, raw string, c map[string]int64) ([]byte, string) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, subFailed
	}
	ctx, cancel := context.WithTimeout(ctx, d.Cfg.TT.Subtitles.Timeout)
	defer cancel()
	hdr := [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", f.userAgent}, {"accept", "*/*"}, {"origin", "https://www.tiktok.com"}, {"referer", "https://www.tiktok.com/"},
		{"sec-fetch-site", "cross-site"}, {"sec-fetch-mode", "cors"}, {"sec-fetch-dest", "empty"},
		{"accept-language", d.Cfg.TT.AcceptLanguage},
	}
	if f.chainToken != "" && media.InDomains(u.Hostname(), d.Cfg.TT.Media.CookieDomains) {
		hdr = append(hdr, [2]string{"cookie", "tt_chain_token=" + f.chainToken})
	}
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: raw, Header: hdr})
	if resp != nil {
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		// A CDN file, not one of TikTok's answers: it counts for the
		// traffic budget, not for AIMD (as a transport class does).
		if d.Client != nil && d.Client.Outcomes != nil {
			d.Client.Outcomes.Observe(detect.Transport, topics.LaneName(t.Lane), resp.BytesIn+resp.BytesOut)
		}
	}
	switch {
	case errors.Is(err, transport.ErrBodyTooLarge):
		return nil, subTooLarge
	case err != nil:
		return nil, subFailed
	case resp.Status != 200:
		return nil, subFailed
	case int64(len(resp.Body)) > d.Cfg.TT.Subtitles.MaxBytes:
		return nil, subTooLarge
	case !isWebVTT(resp.Body):
		return nil, subFailed
	}
	return resp.Body, subFetched
}

// isWebVTT reports whether b starts like a WebVTT file ("WEBVTT", after an
// optional byte order mark).
func isWebVTT(b []byte) bool {
	return bytes.HasPrefix(bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")), []byte("WEBVTT"))
}
