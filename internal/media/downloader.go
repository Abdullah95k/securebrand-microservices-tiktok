// Package media implements the temporary media pipeline: download from the
// CDN into object storage, announce on fb.media.ready.v1, and delete once
// the analysis service acknowledges (or the TTL passes).
package media

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Downloader consumes fbs.task.media.
type Downloader struct {
	DB         *pgxpool.Pool
	S3         *minio.Client
	Bucket     string
	Producer   *kafka.Producer
	HTTP       *http.Client // direct egress, never the residential proxies
	TTL        time.Duration
	MaxBytes   int64
	Presign    bool
	PresignTTL time.Duration
}

// NewHTTPClient returns a client for CDN downloads.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			Proxy:               nil, // never use HTTPS_PROXY: media is fetched over datacenter egress
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// ObjectKey is the deterministic storage key of a media item.
func ObjectKey(m *fbv1.Media, contentType string) string {
	ext := ".bin"
	if exts, _ := mime.ExtensionsByType(contentType); len(exts) > 0 {
		ext = exts[0]
		if contentType == "image/jpeg" {
			ext = ".jpg"
		}
	} else if e := path.Ext(strings.SplitN(m.SourceUrl, "?", 2)[0]); len(e) > 1 && len(e) <= 5 {
		ext = e
	}
	return "media/" + m.MediaId[:2] + "/" + m.MediaId + ext
}

var errExpired = errors.New("source url expired")

// Handle implements kafka.Handler.
func (d *Downloader) Handle(ctx context.Context, rec *kgo.Record) error {
	var t taskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		slog.Error("media: undecodable task", "err", err)
		return nil
	}
	m := t.GetMedia().GetMedia()
	if m == nil || m.MediaId == "" {
		return nil
	}
	kind := strings.ToLower(strings.TrimPrefix(m.Kind.String(), "MEDIA_KIND_"))
	var status string
	err := d.DB.QueryRow(ctx, `SELECT status FROM media WHERE media_id = $1`, m.MediaId).Scan(&status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	switch status {
	case "ready", "deleted", "expired":
		// Already handed to the analysis service once: do not re-download
		// the same media on every re-scrape.
		metrics.MediaOutcome.WithLabelValues(kind, "skipped").Inc()
		return nil
	}
	if exp := m.SourceExpiresAt; exp != nil && exp.AsTime().Before(time.Now()) {
		return d.markFailed(ctx, m, "expired", "source url expired before download")
	}
	start := time.Now()
	obj, err := d.download(ctx, m)
	switch {
	case errors.Is(err, errExpired):
		metrics.MediaOutcome.WithLabelValues(kind, "expired").Inc()
		return d.markFailed(ctx, m, "expired", err.Error())
	case err != nil:
		var perm permanentError
		if errors.As(err, &perm) {
			metrics.MediaOutcome.WithLabelValues(kind, "failed").Inc()
			return d.markFailed(ctx, m, "failed", err.Error())
		}
		metrics.MediaOutcome.WithLabelValues(kind, "retry").Inc()
		return err // consumer retries with backoff
	}
	now := time.Now().UTC()
	deleteAfter := now.Add(d.TTL)
	_, err = d.DB.Exec(ctx, `
		INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, owner_id, post_id, page_id, comment_id, source_url,
			width, height, duration_ms, mime, alt_text, status, s3_bucket, s3_key, size_bytes, sha256, attempts,
			delete_after, stored_at, ready_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'ready',$16,$17,$18,$19,1,$20,$21,$21)
		ON CONFLICT (media_id) DO UPDATE SET status = 'ready', s3_bucket = EXCLUDED.s3_bucket, s3_key = EXCLUDED.s3_key,
			size_bytes = EXCLUDED.size_bytes, sha256 = EXCLUDED.sha256, mime = EXCLUDED.mime, attempts = media.attempts + 1,
			delete_after = EXCLUDED.delete_after, stored_at = EXCLUDED.stored_at, ready_at = EXCLUDED.ready_at, last_error = NULL`,
		m.MediaId, m.FbMediaId, m.Variant, kind, m.OwnerType, nz(m.OwnerId), nz(m.PostId), nz(m.PageId), m.CommentId, m.SourceUrl,
		m.Width, m.Height, m.DurationMs, obj.ContentType, m.AltText, obj.Bucket, obj.Key, obj.Size, obj.Sha256, deleteAfter, now)
	if err != nil {
		return err
	}
	ready := &fbv1.MediaReady{
		EventId: m.MediaId + ":ready:1", MediaId: m.MediaId, FbMediaId: m.FbMediaId, Kind: m.Kind, Variant: m.Variant,
		PageId: m.PageId, PostId: m.PostId, CommentId: m.CommentId, Object: obj, Width: m.Width, Height: m.Height,
		DurationMs: m.DurationMs, AltText: m.AltText, DiscoveredAt: m.GetObs().GetObservedAt(), StoredAt: timestamppb.New(now),
		DeleteAfter: timestamppb.New(deleteAfter), Attempt: 1,
	}
	if t.JobId != "" {
		ready.JobIds = []string{t.JobId}
	}
	if d.Presign {
		if u, err := d.S3.PresignedGetObject(ctx, obj.Bucket, obj.Key, d.PresignTTL, nil); err == nil {
			ready.PresignedUrl = u.String()
		}
	}
	b := d.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaReady, m.MediaId, ready); err != nil {
		return err
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	metrics.MediaOutcome.WithLabelValues(kind, "ready").Inc()
	if m.GetObs().GetObservedAt() != nil {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(time.Since(m.Obs.ObservedAt.AsTime()).Seconds())
	} else {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	}
	return nil
}

type permanentError struct{ error }

func (d *Downloader) download(ctx context.Context, m *fbv1.Media) (*fbv1.S3Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.SourceUrl, nil)
	if err != nil {
		return nil, permanentError{err}
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8")
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone:
		return nil, errExpired
	case resp.StatusCode == http.StatusNotFound:
		return nil, permanentError{fmt.Errorf("http %d", resp.StatusCode)}
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	if d.MaxBytes > 0 && resp.ContentLength > d.MaxBytes {
		return nil, permanentError{fmt.Errorf("object too large: %d bytes", resp.ContentLength)}
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	ct = strings.TrimSpace(strings.SplitN(ct, ";", 2)[0])
	key := ObjectKey(m, ct)
	h := sha256.New()
	var src io.Reader = resp.Body
	if d.MaxBytes > 0 {
		src = io.LimitReader(resp.Body, d.MaxBytes+1)
	}
	body := io.TeeReader(src, h)
	info, err := d.S3.PutObject(ctx, d.Bucket, key, body, resp.ContentLength, minio.PutObjectOptions{
		ContentType: ct, PartSize: 8 << 20,
		UserMetadata: map[string]string{"media-id": m.MediaId, "fb-media-id": m.FbMediaId},
	})
	if err != nil {
		return nil, fmt.Errorf("s3 put: %w", err)
	}
	if d.MaxBytes > 0 && info.Size > d.MaxBytes {
		_ = d.S3.RemoveObject(ctx, d.Bucket, key, minio.RemoveObjectOptions{})
		return nil, permanentError{fmt.Errorf("object too large: %d bytes", info.Size)}
	}
	return &fbv1.S3Object{Bucket: d.Bucket, Key: key, Size: info.Size, Sha256: h.Sum(nil), ContentType: ct}, nil
}

func (d *Downloader) markFailed(ctx context.Context, m *fbv1.Media, status, reason string) error {
	kind := strings.ToLower(strings.TrimPrefix(m.Kind.String(), "MEDIA_KIND_"))
	_, err := d.DB.Exec(ctx, `
		INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, owner_id, post_id, page_id, comment_id, source_url,
			status, attempts, last_error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,$12)
		ON CONFLICT (media_id) DO UPDATE SET status = EXCLUDED.status, attempts = media.attempts + 1, last_error = EXCLUDED.last_error
		WHERE media.status NOT IN ('ready','deleted')`,
		m.MediaId, m.FbMediaId, m.Variant, kind, m.OwnerType, nz(m.OwnerId), nz(m.PostId), nz(m.PageId), m.CommentId, m.SourceUrl,
		status, reason)
	return err
}

func nz(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}
