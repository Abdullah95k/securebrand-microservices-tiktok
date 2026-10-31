// Package s3x opens an S3-compatible client (AWS S3, MinIO, SeaweedFS, ...).
package s3x

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Open returns a client and ensures the buckets exist.
func Open(ctx context.Context, endpoint, access, secret, region string, useSSL bool, buckets ...string) (*minio.Client, error) {
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: useSSL,
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	for _, b := range buckets {
		if b == "" {
			continue
		}
		ok, err := c.BucketExists(ctx, b)
		if err != nil {
			return nil, fmt.Errorf("s3: bucket %s: %w", b, err)
		}
		if !ok {
			if err := c.MakeBucket(ctx, b, minio.MakeBucketOptions{Region: region}); err != nil {
				return nil, fmt.Errorf("s3: make bucket %s: %w", b, err)
			}
		}
	}
	return c, nil
}
