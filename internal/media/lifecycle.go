// Package media installs the raw bucket's lifecycle rule. tks's media
// downloads are internal/tt/media's; deleting downloaded files is another
// service's job (D5), so tks sets no media TTL.
package media

import (
	"context"
	"log/slog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

// RawLifecycle expires every object of the raw bucket (raw/ and
// raw-failures/: TikTok responses, with comment text and authors) after
// days. Stores without lifecycle support log and continue.
func RawLifecycle(ctx context.Context, s3 *minio.Client, bucket string, days int) {
	if err := s3.SetBucketLifecycle(ctx, bucket, rawLifecycleConfig(days)); err != nil {
		slog.Warn("media: bucket lifecycle not applied (store may not support it)", "bucket", bucket, "err", err)
	}
}

// rawLifecycleConfig expires the whole raw bucket after days.
func rawLifecycleConfig(days int) *lifecycle.Configuration {
	return &lifecycle.Configuration{Rules: []lifecycle.Rule{{
		ID:         "tks-raw-retention",
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: ""},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(days)},
	}}}
}
