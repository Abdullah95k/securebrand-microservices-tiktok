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
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
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
	if m == nil || uuid.Validate(m.MediaId) != nil {
		return nil // nothing that can be stored
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
	case "stored":
		// Stored, but its ready event was not confirmed (stopped or failed
		// in between): announce it now, without downloading it again
		// (unless erased since: announce checks).
		var obj fbv1.S3Object
		var storedAt, deleteAfter time.Time
		if err := d.DB.QueryRow(ctx, `SELECT s3_bucket, s3_key, coalesce(size_bytes, 0), coalesce(sha256, ''::bytea), coalesce(mime, ''),
			stored_at, delete_after FROM media WHERE media_id = $1`, m.MediaId).
			Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.Sha256, &obj.ContentType, &storedAt, &deleteAfter); err != nil {
			return err
		}
		return d.announce(ctx, &t, m, kind, &obj, storedAt, deleteAfter, time.Now())
	}
	// Media of an erased page or author is never downloaded (checked again
	// under the erasure lock when stored).
	if sup, err := gdpr.Lookup(ctx, d.DB, [][]byte{m.AuthorKey}, []int64{m.PageId}); err != nil {
		return err
	} else if sup.PageSuppressed(m.PageId) || sup.AuthorSuppressed(m.AuthorKey) {
		metrics.MediaOutcome.WithLabelValues(kind, "suppressed").Inc()
		return nil
	}
	// A URL that expired before the download is marked source_expired, not
	// expired (which says the media was handed over and its TTL ran out):
	// a later scrape's task brings a fresh URL, and is downloaded.
	if exp := m.SourceExpiresAt; exp != nil && exp.AsTime().Before(time.Now()) {
		return d.markFailed(ctx, m, "source_expired", "source url expired before download")
	}
	start := time.Now()
	obj, err := d.download(ctx, m)
	switch {
	case errors.Is(err, errExpired):
		metrics.MediaOutcome.WithLabelValues(kind, "expired").Inc()
		return d.markFailed(ctx, m, "source_expired", failureReason(err, m))
	case err != nil:
		var perm permanentError
		if errors.As(err, &perm) {
			metrics.MediaOutcome.WithLabelValues(kind, "failed").Inc()
			return d.markFailed(ctx, m, "failed", failureReason(err, m))
		}
		metrics.MediaOutcome.WithLabelValues(kind, "retry").Inc()
		return err // consumer retries with backoff
	}
	now := time.Now().UTC()
	deleteAfter := now.Add(d.TTL)
	var suppressed, taken, shared bool
	err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		sup, err := gdpr.Guard(ctx, tx, [][]byte{m.AuthorKey}, []int64{m.PageId})
		if err != nil {
			return err
		}
		if suppressed = sup.PageSuppressed(m.PageId) || sup.AuthorSuppressed(m.AuthorKey); suppressed {
			return nil
		}
		// Another copy of the task (two jobs on one page) may have stored
		// or handed over the media meanwhile: that row stands, and this
		// copy announces nothing (it wrote the same object key).
		tag, err := tx.Exec(ctx, `
		INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, owner_id, post_id, page_id, comment_id, source_url,
			width, height, duration_ms, mime, alt_text, status, s3_bucket, s3_key, size_bytes, sha256, attempts,
			delete_after, stored_at, ready_at, author_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'stored',$16,$17,$18,$19,1,$20,$21,NULL,$22)
		ON CONFLICT (media_id) DO UPDATE SET status = 'stored', s3_bucket = EXCLUDED.s3_bucket, s3_key = EXCLUDED.s3_key,
			size_bytes = EXCLUDED.size_bytes, sha256 = EXCLUDED.sha256, mime = EXCLUDED.mime, attempts = media.attempts + 1,
			delete_after = EXCLUDED.delete_after, stored_at = EXCLUDED.stored_at, last_error = NULL,
			author_key = COALESCE(EXCLUDED.author_key, media.author_key)
		WHERE media.status NOT IN ('stored','ready','deleted','expired')`,
			m.MediaId, m.FbMediaId, m.Variant, kind, m.OwnerType, nz(m.OwnerId), nz(m.PostId), nz(m.PageId), m.CommentId, m.SourceUrl,
			m.Width, m.Height, m.DurationMs, obj.ContentType, m.AltText, obj.Bucket, obj.Key, obj.Size, obj.Sha256, deleteAfter, now,
			m.AuthorKey)
		if err != nil || tag.RowsAffected() > 0 {
			return err
		}
		// Taken: the object this copy uploaded is the row's own only if
		// the row is stored or ready under the same key (same media id and
		// type); otherwise it is left over.
		taken = true
		var status, key string
		if err := tx.QueryRow(ctx, `SELECT status, coalesce(s3_key, '') FROM media WHERE media_id = $1`, m.MediaId).
			Scan(&status, &key); err != nil {
			return err
		}
		shared = (status == "stored" || status == "ready") && key == obj.Key
		return nil
	})
	if err != nil {
		return err
	}
	if taken {
		if !shared {
			if err := d.S3.RemoveObject(ctx, obj.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
		metrics.MediaOutcome.WithLabelValues(kind, "skipped").Inc()
		return nil
	}
	if suppressed {
		// Erased while it downloaded: the object must not stay.
		if err := d.S3.RemoveObject(ctx, obj.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		metrics.MediaOutcome.WithLabelValues(kind, "suppressed").Inc()
		return nil
	}
	return d.announce(ctx, &t, m, kind, obj, now, deleteAfter, start)
}

// announce publishes the media's ready event, then marks it ready: a
// failure in between leaves it stored, and a redelivery announces it again
// (the event id is stable, so consumers see it once).
func (d *Downloader) announce(ctx context.Context, t *taskv1.Task, m *fbv1.Media, kind string, obj *fbv1.S3Object,
	storedAt, deleteAfter, start time.Time) error {
	// Erased since it was stored: nothing is announced (the erasure deletes
	// the object and the row).
	if sup, err := gdpr.Lookup(ctx, d.DB, [][]byte{m.AuthorKey}, []int64{m.PageId}); err != nil {
		return err
	} else if sup.PageSuppressed(m.PageId) || sup.AuthorSuppressed(m.AuthorKey) {
		metrics.MediaOutcome.WithLabelValues(kind, "suppressed").Inc()
		return nil
	}
	ready := &fbv1.MediaReady{
		EventId: m.MediaId + ":ready:1", MediaId: m.MediaId, FbMediaId: m.FbMediaId, Kind: m.Kind, Variant: m.Variant,
		PageId: m.PageId, PostId: m.PostId, CommentId: m.CommentId, Object: obj, Width: m.Width, Height: m.Height,
		DurationMs: m.DurationMs, AltText: m.AltText, DiscoveredAt: m.GetObs().GetObservedAt(), StoredAt: timestamppb.New(storedAt),
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
	if _, err := d.DB.Exec(ctx, `UPDATE media SET status = 'ready', ready_at = now() WHERE media_id = $1 AND status = 'stored'`,
		m.MediaId); err != nil {
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
		// The error names the URL, signature included (a credential for
		// the CDN object), and the consumer logs it on every retry.
		var ue *url.Error
		if errors.As(err, &ue) {
			ue.URL = bareURL(ue.URL)
		}
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

// markFailed records a download that failed for good, under the erasure
// lock: the row of a page or author erased meanwhile is not written back.
func (d *Downloader) markFailed(ctx context.Context, m *fbv1.Media, status, reason string) error {
	kind := strings.ToLower(strings.TrimPrefix(m.Kind.String(), "MEDIA_KIND_"))
	return pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		sup, err := gdpr.Guard(ctx, tx, [][]byte{m.AuthorKey}, []int64{m.PageId})
		if err != nil || sup.PageSuppressed(m.PageId) || sup.AuthorSuppressed(m.AuthorKey) {
			return err
		}
		_, err = tx.Exec(ctx, `
		INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, owner_id, post_id, page_id, comment_id, source_url,
			status, attempts, last_error, author_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,$12,$13)
		ON CONFLICT (media_id) DO UPDATE SET status = EXCLUDED.status, attempts = media.attempts + 1, last_error = EXCLUDED.last_error
		WHERE media.status NOT IN ('stored','ready','deleted','expired')`,
			m.MediaId, m.FbMediaId, m.Variant, kind, m.OwnerType, nz(m.OwnerId), nz(m.PostId), nz(m.PageId), m.CommentId, m.SourceUrl,
			status, reason, m.AuthorKey)
		return err
	})
}

// failureReason is err as stored in last_error, with the media's signed
// URL cut to its path: the signature is a credential for the CDN object.
func failureReason(err error, m *fbv1.Media) string {
	msg := err.Error()
	if m.SourceUrl == "" {
		return msg
	}
	return strings.ReplaceAll(msg, m.SourceUrl, bareURL(m.SourceUrl))
}

// bareURL is u without its query and fragment (a CDN URL's signature).
func bareURL(u string) string {
	if p, err := url.Parse(u); err == nil {
		p.RawQuery, p.Fragment = "", ""
		return p.String()
	}
	if i := strings.IndexAny(u, "?#"); i >= 0 {
		return u[:i]
	}
	return u
}

func nz(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}
