package app

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	ttmedia "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/media"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Media download consumer groups, apart from the copied Facebook pipeline's
// (tks-media-downloader, tks-media-janitor).
const (
	MediaDownloaderGroup = "tks-tt-media-downloader"
	MediaLedgerGroup     = "tks-tt-media-ledger"
)

// MediaOptions are what tks-media-downloader's caller provides.
type MediaOptions struct {
	// DB is a pool pinned to schema tt (ttstore.Open).
	DB *pgxpool.Pool
	S3 *minio.Client
	// HTTP is the downloads' client; nil uses ttmedia.NewHTTPClient.
	HTTP *http.Client
	Now  func() time.Time
	// DrainTimeout bounds how long in-flight downloads run after shutdown
	// starts (default 60 s).
	DrainTimeout time.Duration
}

// Media is tks-media-downloader: the downloader on tks.task.media and the
// ledger of acknowledgements and deletions. It deletes nothing and sets no
// TTL (owner, 2026-11-02; docs/tiktok ADR 0007 §15).
type Media struct {
	Downloader *ttmedia.Downloader
	Ledger     *ttmedia.Ledger
	Producer   *kafka.Producer
	Components []run.Func
}

// NewMedia builds tks-media-downloader's components. Close the producer
// once they have stopped.
func NewMedia(ctx context.Context, cfg *config.Config, o MediaOptions) (*Media, error) {
	kcfg, err := connect.Kafka(cfg, "-media")
	if err != nil {
		return nil, err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return nil, err
	}
	ttmedia.WarnExpiry(ctx, o.S3, cfg.S3.MediaBucket)
	hc := o.HTTP
	if hc == nil {
		hc = ttmedia.NewHTTPClient()
	}
	drain := o.DrainTimeout
	if drain <= 0 {
		drain = 60 * time.Second
	}
	m := &Media{Producer: producer,
		Downloader: &ttmedia.Downloader{DB: o.DB, S3: o.S3, Bucket: cfg.S3.MediaBucket, Producer: producer, HTTP: hc,
			CookieDomains: cfg.TT.Media.CookieDomains, MaxBytes: cfg.Media.MaxBytes, Presign: cfg.Media.Presign,
			PresignTTL: cfg.Media.PresignTTL, Now: o.Now},
		Ledger: &ttmedia.Ledger{DB: o.DB, Producer: producer, MaxRetryableFailures: cfg.Media.MaxRetryableFailures, Now: o.Now},
	}
	down, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: MediaDownloaderGroup,
		Topics: []string{topics.TaskMedia}, MaxInflight: cfg.Media.Concurrency, DrainTimeout: drain}, m.Downloader.Handle)
	if err != nil {
		producer.Close()
		return nil, err
	}
	ledger, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: MediaLedgerGroup,
		Topics: ttmedia.LedgerTopics, MaxInflight: 32, DrainTimeout: 20 * time.Second}, m.Ledger.Handle)
	if err != nil {
		producer.Close()
		return nil, err
	}
	m.Components = []run.Func{down.Run, ledger.Run}
	return m, nil
}
