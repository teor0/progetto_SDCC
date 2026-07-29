package storage

import (
	"bytes"
	"context"
	"fmt"

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

const publicReadPolicy = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::%s/*"]
    }
  ]
}`

// Uploader is the subset of Storage's behavior that Server depends on.
// Declaring it lets tests substitute a GoMock-generated mock instead of a
// real MinIO connection, without Storage itself needing to change.
type Uploader interface {
	Upload(ctx context.Context, objectKey string, contentType string, data []byte) (string, error)
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

	policy := fmt.Sprintf(publicReadPolicy, cfg.Bucket)

	if err := client.SetBucketPolicy(ctx, cfg.Bucket, policy); err != nil {
		return nil, fmt.Errorf("setting bucket policy: %w", err)
	}

	return &Storage{client: client, bucket: cfg.Bucket}, nil
}

// Upload uploads photo bytes under objectKey and returns its plain URL.
func (s *Storage) Upload(ctx context.Context, objectKey string, contentType string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, s.bucket, objectKey, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", fmt.Errorf("putting object %q: %w", objectKey, err)
	}
	endpoint := s.client.EndpointURL()
	url := fmt.Sprintf("%s/%s/%s", endpoint.String(), s.bucket, objectKey)
	return url, nil
}
