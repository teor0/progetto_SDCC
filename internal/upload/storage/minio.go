// Package storage wraps the MinIO client used to persist photo bytes.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

type Storage struct {
	client *minio.Client
	bucket string
}

// NewStorage connects to MinIO and ensures the target bucket exists, so the
// service can start cold against a freshly provisioned MinIO container.
func NewStorage(ctx context.Context, cfg Config) (*Storage, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("creating bucket %q: %w", cfg.Bucket, err)
		}
	}

	return &Storage{client: client, bucket: cfg.Bucket}, nil
}

// Upload uploads photo bytes under objectKey and returns the ETag MinIO assigns.
func (s *Storage) Upload(ctx context.Context, objectKey string, contentType string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	info, err := s.client.PutObject(ctx, s.bucket, objectKey, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", fmt.Errorf("putting object %q: %w", objectKey, err)
	}
	return info.ETag, nil
}

// PresignedURL returns a temporary GET URL for a stored photo, so clients
// (or Notification Service, when building a link) fetch bytes directly
// from MinIO instead of proxying them back through Upload Service.
func (s *Storage) PresignedURL(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	//Build a plain URL — swap for a presigned URL if the bucket is private.
	//url := fmt.Sprintf("http://%s/%s/%s", s.client.EndpointURL().Host, s.bucket, objectKey)
	url, err := s.client.PresignedGetObject(ctx, s.bucket, objectKey, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("presigning %q: %w", objectKey, err)
	}
	return url.String(), nil
}
