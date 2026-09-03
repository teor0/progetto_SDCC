package upload

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	GRPCPort string

	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool

	RabbitMQURL      string
	RabbitMQExchange string

	GalleryServiceAddress string
	GalleryCallTimeout    time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		GRPCPort:              os.Getenv("UPLOAD_GRPC_PORT"),
		MinIOEndpoint:         os.Getenv("MINIO_ENDPOINT"),
		MinIOAccessKey:        os.Getenv("MINIO_ACCESS_KEY"),
		MinIOSecretKey:        os.Getenv("MINIO_SECRET_KEY"),
		MinIOBucket:           os.Getenv("MINIO_BUCKET"),
		MinIOUseSSL:           os.Getenv("MINIO_USE_SSL") == "true",
		RabbitMQURL:           os.Getenv("RABBITMQ_URL"),
		RabbitMQExchange:      os.Getenv("RABBITMQ_EXCHANGE"),
		GalleryServiceAddress: os.Getenv("GALLERY_SERVICE_ADDRESS"),
		GalleryCallTimeout:    2 * time.Second,
	}

	if cfg.MinIOAccessKey == "" || cfg.MinIOSecretKey == "" {
		return Config{}, fmt.Errorf("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")
	}

	return cfg, nil
}
