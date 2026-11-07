package persist

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// subtitleSource is a track's source as post_subtitles.source stores it.
func subtitleSource(s ttv1.SubtitleSource) string {
	switch s {
	case ttv1.SubtitleSource_SUBTITLE_SOURCE_ASR:
		return "asr"
	case ttv1.SubtitleSource_SUBTITLE_SOURCE_MT:
		return "mt"
	}
	return "unspecified"
}

// subtitleHash is a track's content: what a later fetch must change for
// its row to be rewritten.
func subtitleHash(s *ttv1.Subtitle) []byte {
	i := s.GetInfo()
	h := sha256.Sum256([]byte(strings.Join([]string{i.GetVersion(), i.GetFormat(), i.GetLanguageId(),
		strconv.FormatInt(i.GetSizeBytes(), 10), s.GetSkipped(), s.GetBody()}, "\x00")))
	return h[:]
}

// Subtitles upserts videos' subtitle tracks (M17), one row per post,
// language and source. A row is rewritten only when its content changed and
// the read is not older than the stored one; a fetch that came back without
// a body (too large) keeps a body stored before.
func (w *Writer) Subtitles(ctx context.Context, ss []*ttv1.PostSubtitles) error {
	type row struct {
		s  *ttv1.Subtitle
		p  *ttv1.PostSubtitles
		at time.Time
	}
	by := map[string]row{}
	var order []string
	for _, p := range ss {
		if p.PostId == 0 || p.UserId == 0 || p.PostCreatedAt == nil {
			metrics.PersistRows.WithLabelValues("subtitle", "rejected").Add(float64(len(p.Tracks)))
			continue
		}
		at := observed(p.Obs)
		for _, s := range p.Tracks {
			i := s.GetInfo()
			if i.GetLanguage() == "" {
				metrics.PersistRows.WithLabelValues("subtitle", "rejected").Inc()
				continue
			}
			k := strconv.FormatInt(p.PostId, 10) + "|" + i.Language + "|" + i.SourceName
			if prev, ok := by[k]; ok && prev.at.After(at) {
				continue
			} else if !ok {
				order = append(order, k)
			}
			by[k] = row{s: s, p: p, at: at}
		}
	}
	if len(order) == 0 {
		return nil
	}
	start := time.Now()
	var n int64
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		n = 0
		b := &pgx.Batch{}
		for _, k := range order {
			r := by[k]
			i := r.s.GetInfo()
			b.Queue(`INSERT INTO post_subtitles AS t (post_id, language, source_name, source, user_id, post_created_at, language_id,
					version, format, size_bytes, body, skipped, content_hash, first_seen_at, last_seen_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)
				ON CONFLICT (post_id, language, source_name) DO UPDATE SET source = EXCLUDED.source,
					language_id = EXCLUDED.language_id, version = EXCLUDED.version, format = EXCLUDED.format,
					size_bytes = EXCLUDED.size_bytes, body = COALESCE(EXCLUDED.body, t.body),
					skipped = CASE WHEN EXCLUDED.body IS NULL AND t.body IS NOT NULL THEN t.skipped ELSE EXCLUDED.skipped END,
					content_hash = EXCLUDED.content_hash, last_seen_at = EXCLUDED.last_seen_at, updated_at = now()
				WHERE t.last_seen_at <= EXCLUDED.last_seen_at AND t.content_hash IS DISTINCT FROM EXCLUDED.content_hash`,
				r.p.PostId, i.Language, i.SourceName, subtitleSource(i.Source), r.p.UserId, r.p.PostCreatedAt.AsTime(),
				nilIfEmpty(i.LanguageId), nilIfEmpty(i.Version), nilIfEmpty(i.Format), nilIfZero(i.SizeBytes),
				nilIfEmpty(r.s.Body), nilIfEmpty(r.s.Skipped), subtitleHash(r.s), r.at)
		}
		res := tx.SendBatch(ctx, b)
		for range order {
			tag, err := res.Exec()
			if err != nil {
				_ = res.Close()
				return err
			}
			n += tag.RowsAffected()
		}
		return res.Close()
	})
	if err != nil {
		return fmt.Errorf("persist: subtitles: %w", err)
	}
	written("subtitle", n, start)
	metrics.PersistRows.WithLabelValues("subtitle", "unchanged").Add(float64(int64(len(order)) - n))
	return nil
}
