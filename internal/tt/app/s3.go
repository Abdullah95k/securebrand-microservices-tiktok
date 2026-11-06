package app

import (
	"context"

	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/s3x"
)

// OpenS3 connects to the object store; with s3.create_buckets it also
// makes bucket if it is missing, as in testing. In production the platform
// owns the buckets, so tks only writes into them.
func OpenS3(ctx context.Context, cfg *config.Config, bucket string) (*minio.Client, error) {
	s := cfg.S3
	if !s.CreateBuckets {
		return s3x.Connect(s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL)
	}
	return s3x.Open(ctx, s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL, bucket)
}
