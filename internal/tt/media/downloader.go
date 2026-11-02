// Package media downloads TikTok's media files into object storage and hands
// them over on tt.media.ready.v1 (build brief D5; docs/tiktok ADR 0007 §7,
// §15). It is internal/media's downloader on tks's envelope (tks.task.v1),
// records (tt.v1, common.v1) and schema tt, with two differences:
//   - **Nothing is deleted.** tks sets no TTL, installs no bucket lifecycle
//     rule and runs no janitor: another service deletes the files (owner,
//     2026-11-02) and says so on tt.media.deleted.v1, which the Ledger
//     records, as it records the consumers' acknowledgements.
//   - **A video file is fetched with a cookie.** TikTok's video hosts refuse a
//     video without the tt_chain_token cookie of a TikTok page (T18: C2 run
//     3); the task carries the reading identity's, and the downloader sends
//     it to the cookie domains only, as a browser does.
//
// Downloads go over the service's own egress, never the crawl proxies, as in
// fbs. Whether TikTok's video hosts accept a token read on another exit is
// T33, for Phase 0.
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

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Downloader consumes tks.task.media.
type Downloader struct {
	// DB is a pool pinned to schema tt (ttstore.Open).
	DB       *pgxpool.Pool
	S3       *minio.Client
	Bucket   string
	Producer *kafka.Producer
	// HTTP is the client downloads use (NewHTTPClient: direct egress).
	HTTP *http.Client
	// CookieDomains are the hosts, with their subdomains, that get a video
	// task's tt_chain_token (tt.media.cookie_domains).
	CookieDomains []string
	MaxBytes      int64
	Presign       bool
	PresignTTL    time.Duration
	Now           func() time.Time
}

// NewHTTPClient returns a client for TikTok's media hosts: direct egress,
// never HTTPS_PROXY or the crawl proxies.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			Proxy:               nil,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func (d *Downloader) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

// defaultUserAgent is sent when a task names none (a comment image).
const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"

// Kind is a media kind's name as schema tt stores it (cover, video, ...).
func Kind(k ttv1.MediaKind) string {
	return strings.ToLower(strings.TrimPrefix(k.String(), "MEDIA_KIND_"))
}

// NeutralKind maps TikTok's kind to common.v1's.
func NeutralKind(k ttv1.MediaKind) commonv1.MediaKind {
	switch k {
	case ttv1.MediaKind_MEDIA_KIND_COVER, ttv1.MediaKind_MEDIA_KIND_ORIGIN_COVER, ttv1.MediaKind_MEDIA_KIND_DYNAMIC_COVER:
		return commonv1.MediaKind_MEDIA_KIND_COVER
	case ttv1.MediaKind_MEDIA_KIND_VIDEO:
		return commonv1.MediaKind_MEDIA_KIND_VIDEO
	case ttv1.MediaKind_MEDIA_KIND_PHOTO:
		return commonv1.MediaKind_MEDIA_KIND_PHOTO
	case ttv1.MediaKind_MEDIA_KIND_AVATAR:
		return commonv1.MediaKind_MEDIA_KIND_AVATAR
	case ttv1.MediaKind_MEDIA_KIND_MUSIC_COVER:
		return commonv1.MediaKind_MEDIA_KIND_AUDIO_COVER
	case ttv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE:
		return commonv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE
	}
	return commonv1.MediaKind_MEDIA_KIND_UNSPECIFIED
}

// ObjectKey is the deterministic storage key of a media file, fbs's layout.
func ObjectKey(m *ttv1.Media, contentType string) string {
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

// errRefused is a 403 or 410 from the media host: the link expired, or (a
// video before its link's expiry) the host wanted a cookie it did not get.
var errRefused = errors.New("source refused")

type permanentError struct{ error }

// Handle implements kafka.Handler.
func (d *Downloader) Handle(ctx context.Context, rec *kgo.Record) error {
	var t tkstaskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		slog.Error("media: undecodable task", "err", err)
		return nil
	}
	sc := t.GetMedia()
	m := sc.GetMedia()
	if m == nil || uuid.Validate(m.MediaId) != nil {
		return nil // nothing that can be stored
	}
	kind := Kind(m.Kind)
	var status string
	err := d.DB.QueryRow(ctx, `SELECT status FROM media WHERE media_id = $1`, m.MediaId).Scan(&status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	switch status {
	case "ready", "deleted", "expired":
		// Handed over once: a re-read of the post does not download the
		// same media again, and what the deleting service removed stays
		// removed.
		metrics.MediaOutcome.WithLabelValues(kind, "skipped").Inc()
		return nil
	case "stored":
		// Stored, but its ready event was not confirmed: announce it now,
		// without downloading it again.
		obj := &commonv1.S3Object{}
		var storedAt time.Time
		if err := d.DB.QueryRow(ctx, `SELECT s3_bucket, s3_key, coalesce(size_bytes, 0), coalesce(sha256, ''::bytea),
			coalesce(mime, ''), stored_at FROM media WHERE media_id = $1`, m.MediaId).
			Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.Sha256, &obj.ContentType, &storedAt); err != nil {
			return err
		}
		return d.announce(ctx, &t, m, kind, obj, storedAt, d.now())
	}
	// A link that expired before the download is marked source_expired: the
	// next read of the post brings a fresh one (and a fresh cookie), which
	// is downloaded.
	if exp := m.SourceExpiresAt; exp != nil && exp.AsTime().Before(d.now()) {
		metrics.MediaOutcome.WithLabelValues(kind, "expired").Inc()
		return d.markFailed(ctx, m, "source_expired", "source url expired before download")
	}
	start := time.Now()
	obj, err := d.download(ctx, sc)
	switch {
	case errors.Is(err, errRefused):
		outcome := "expired"
		if exp := m.SourceExpiresAt; exp == nil || exp.AsTime().After(d.now()) {
			// Refused before its link expired: for a video, most likely the
			// cookie (T18, T33). Counted apart so it shows.
			outcome = "refused"
		}
		metrics.MediaOutcome.WithLabelValues(kind, outcome).Inc()
		return d.markFailed(ctx, m, "source_expired", failureReason(err, m))
	case err != nil:
		var perm permanentError
		if errors.As(err, &perm) {
			metrics.MediaOutcome.WithLabelValues(kind, "failed").Inc()
			return d.markFailed(ctx, m, "failed", failureReason(err, m))
		}
		metrics.MediaOutcome.WithLabelValues(kind, "retry").Inc()
		return err // the consumer retries with backoff
	}
	now := d.now()
	var taken, shared bool
	err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		// Another copy of the task (two jobs read the same post) may have
		// stored or handed over the media meanwhile: that row stands, and
		// this copy announces nothing.
		tag, err := tx.Exec(ctx, `
		INSERT INTO media (media_id, source_media_id, kind, variant, user_id, post_id, music_id, comment_id, position, author_key,
			source_url, source_expires_at, width, height, duration_ms, bitrate, mime, post_created_at, status, s3_bucket, s3_key,
			size_bytes, sha256, attempts, discovered_at, stored_at)
		VALUES ($1::uuid,$1::text,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'stored',$18,$19,$20,$21,1,$22,$23)
		ON CONFLICT (media_id) DO UPDATE SET status = 'stored', s3_bucket = EXCLUDED.s3_bucket, s3_key = EXCLUDED.s3_key,
			size_bytes = EXCLUDED.size_bytes, sha256 = EXCLUDED.sha256, mime = EXCLUDED.mime, attempts = media.attempts + 1,
			stored_at = EXCLUDED.stored_at, last_error = NULL, author_key = COALESCE(EXCLUDED.author_key, media.author_key)
		WHERE media.status NOT IN ('stored','ready','deleted','expired')`,
			m.MediaId, kind, m.Variant, nz(m.UserId), m.PostId, m.MusicId, m.CommentId, m.Position, nilBytes(m.AuthorKey),
			m.SourceUrl, ts(m.SourceExpiresAt), nz32(m.Width), nz32(m.Height), nz(m.DurationMs), nz(m.Bitrate), obj.ContentType,
			ts(m.PostCreatedAt), obj.Bucket, obj.Key, obj.Size, obj.Sha256, discovered(m, now), now)
		if err != nil || tag.RowsAffected() > 0 {
			return err
		}
		// Taken: the object this copy uploaded is the row's own only if the
		// row is stored or ready under the same key.
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
			// Left over: no row names it. Removing tks's own stray upload
			// is not deleting a handed-over file.
			if err := d.S3.RemoveObject(ctx, obj.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
		metrics.MediaOutcome.WithLabelValues(kind, "skipped").Inc()
		return nil
	}
	return d.announce(ctx, &t, m, kind, obj, now, start)
}

// announce publishes the media's ready event, then marks it ready: a failure
// in between leaves it stored, and a redelivery announces it again (the
// event id is stable, so consumers see it once). The event has no
// delete_after: tks keeps the file until the deleting service removes it.
func (d *Downloader) announce(ctx context.Context, t *tkstaskv1.Task, m *ttv1.Media, kind string, obj *commonv1.S3Object,
	storedAt, start time.Time) error {
	ready := Ready(m.MediaId, 1, obj)
	ready.Kind, ready.PlatformKind, ready.Variant = NeutralKind(m.Kind), kind, m.Variant
	ready.AccountId, ready.PostId, ready.CommentId = m.UserId, m.PostId, m.CommentId
	ready.Width, ready.Height, ready.DurationMs = m.Width, m.Height, m.DurationMs
	ready.DiscoveredAt, ready.StoredAt = m.GetObs().GetObservedAt(), timestamppb.New(storedAt)
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
	if _, err := d.DB.Exec(ctx, `UPDATE media SET status = 'ready', ready_at = $2 WHERE media_id = $1 AND status = 'stored'`,
		m.MediaId, d.now()); err != nil {
		return err
	}
	metrics.MediaOutcome.WithLabelValues(kind, "ready").Inc()
	if o := m.GetObs().GetObservedAt(); o != nil {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(d.now().Sub(o.AsTime()).Seconds())
	} else {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	}
	return nil
}

// Ready is a ready event's skeleton: its id says which announcement of the
// file it is.
func Ready(mediaID string, attempt uint32, obj *commonv1.S3Object) *commonv1.MediaReady {
	return &commonv1.MediaReady{EventId: fmt.Sprintf("%s:ready:%d", mediaID, attempt), MediaId: mediaID,
		Platform: commonv1.Platform_PLATFORM_TIKTOK, SourceMediaId: mediaID, Object: obj, Attempt: attempt}
}

// download fetches the file and stores it. A video's request carries the
// task's cookie for the cookie domains, on redirects too; Chrome's media
// headers go with it, as the probe sent them (C2 run 3).
func (d *Downloader) download(ctx context.Context, sc *tkstaskv1.MediaScope) (*commonv1.S3Object, error) {
	m := sc.GetMedia()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.SourceUrl, nil)
	if err != nil {
		return nil, permanentError{err}
	}
	video := m.Kind == ttv1.MediaKind_MEDIA_KIND_VIDEO
	ua := sc.GetUserAgent()
	if ua == "" {
		ua = defaultUserAgent
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Referer", "https://www.tiktok.com/")
	req.Header.Set("Sec-Fetch-Site", "same-site")
	req.Header.Set("Sec-Fetch-Mode", "no-cors")
	if video {
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Encoding", "identity;q=1, *;q=0")
		req.Header.Set("Range", "bytes=0-")
		req.Header.Set("Sec-Fetch-Dest", "video")
	} else {
		req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
		req.Header.Set("Sec-Fetch-Dest", "image")
	}
	token := ""
	if video {
		token = sc.GetChainToken()
	}
	d.setCookie(req, token)
	c := *d.HTTP
	c.CheckRedirect = func(r *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		d.setCookie(r, token)
		return nil
	}
	resp, err := c.Do(req)
	if err != nil {
		// The error names the URL, signature included (a credential for the
		// object), and the consumer logs it on every retry.
		var ue *url.Error
		if errors.As(err, &ue) {
			ue.URL = bareURL(ue.URL)
		}
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone:
		return nil, fmt.Errorf("%w: http %d", errRefused, resp.StatusCode)
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
	info, err := d.S3.PutObject(ctx, d.Bucket, key, io.TeeReader(src, h), resp.ContentLength, minio.PutObjectOptions{
		ContentType: ct, PartSize: 8 << 20, UserMetadata: map[string]string{"media-id": m.MediaId, "platform": "tiktok"},
	})
	if err != nil {
		return nil, fmt.Errorf("s3 put: %w", err)
	}
	if d.MaxBytes > 0 && info.Size > d.MaxBytes {
		_ = d.S3.RemoveObject(ctx, d.Bucket, key, minio.RemoveObjectOptions{})
		return nil, permanentError{fmt.Errorf("object too large: %d bytes", info.Size)}
	}
	return &commonv1.S3Object{Bucket: d.Bucket, Key: key, Size: info.Size, Sha256: h.Sum(nil), ContentType: ct}, nil
}

// setCookie sends token as tt_chain_token to the cookie domains only, and
// nothing else as a cookie anywhere.
func (d *Downloader) setCookie(r *http.Request, token string) {
	r.Header.Del("Cookie")
	if token != "" && InDomains(r.URL.Hostname(), d.CookieDomains) {
		r.Header.Set("Cookie", "tt_chain_token="+token)
	}
}

// InDomains reports whether host is one of domains or a subdomain of one.
func InDomains(host string, domains []string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, d := range domains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// markFailed records a download that failed for good, or a link to read
// again.
func (d *Downloader) markFailed(ctx context.Context, m *ttv1.Media, status, reason string) error {
	_, err := d.DB.Exec(ctx, `
		INSERT INTO media (media_id, source_media_id, kind, variant, user_id, post_id, music_id, comment_id, position, author_key,
			source_url, source_expires_at, post_created_at, status, attempts, last_error, discovered_at)
		VALUES ($1::uuid,$1::text,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1,$14,$15)
		ON CONFLICT (media_id) DO UPDATE SET status = EXCLUDED.status, attempts = media.attempts + 1, last_error = EXCLUDED.last_error
		WHERE media.status NOT IN ('stored','ready','deleted','expired')`,
		m.MediaId, Kind(m.Kind), m.Variant, nz(m.UserId), m.PostId, m.MusicId, m.CommentId, m.Position, nilBytes(m.AuthorKey),
		m.SourceUrl, ts(m.SourceExpiresAt), ts(m.PostCreatedAt), status, reason, discovered(m, d.now()))
	return err
}

// failureReason is err as stored in last_error, with the signed link cut to
// its path: the signature is a credential for the object.
func failureReason(err error, m *ttv1.Media) string {
	msg := err.Error()
	if m.SourceUrl == "" {
		return msg
	}
	return strings.ReplaceAll(msg, m.SourceUrl, bareURL(m.SourceUrl))
}

// bareURL is u without its query and fragment (a CDN link's signature).
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

func discovered(m *ttv1.Media, now time.Time) time.Time {
	if o := m.GetObs().GetObservedAt(); o != nil {
		return o.AsTime()
	}
	return now
}

func ts(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime()
	return &v
}

func nz(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nz32(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}

func nilBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// WarnExpiry logs a warning for each rule of the bucket that expires
// objects: tks keeps its files until the deleting service removes them
// (owner, 2026-11-02), so such a rule deletes what tks hands over. It
// changes nothing; stores without lifecycle support say nothing.
func WarnExpiry(ctx context.Context, s3 *minio.Client, bucket string) int {
	cfg, err := s3.GetBucketLifecycle(ctx, bucket)
	if err != nil || cfg == nil {
		return 0
	}
	n := 0
	for _, r := range cfg.Rules {
		if r.Status == "Enabled" && !r.Expiration.IsNull() {
			n++
			slog.Warn("media: the bucket has a rule that deletes objects; tks sets none and keeps its files",
				"bucket", bucket, "rule", r.ID, "prefix", r.RuleFilter.Prefix)
		}
	}
	return n
}
