// Package s3x opens an S3-compatible client (AWS S3, MinIO, SeaweedFS, ...).
package s3x

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Connect returns a client without touching any bucket: where the
// platform owns the buckets, tks neither checks nor makes them. An
// endpoint given as a URL ("https://fra1.example.com") sets TLS from its
// scheme.
func Connect(endpoint, access, secret, region string, useSSL bool) (*minio.Client, error) {
	endpoint, useSSL = Endpoint(endpoint, useSSL)
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: useSSL,
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	return c, nil
}

// Endpoint splits an endpoint given as a URL into its host and whether it
// uses TLS; a bare host keeps useSSL.
func Endpoint(endpoint string, useSSL bool) (string, bool) {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http") {
		return u.Host, u.Scheme == "https"
	}
	return strings.TrimSuffix(endpoint, "/"), useSSL
}

// Open returns a client and ensures the buckets exist.
func Open(ctx context.Context, endpoint, access, secret, region string, useSSL bool, buckets ...string) (*minio.Client, error) {
	c, err := Connect(endpoint, access, secret, region, useSSL)
	if err != nil {
		return nil, err
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
