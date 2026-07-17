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
		GRPCPort:              getEnv("GRPC_PORT", "50052"),
		MinIOEndpoint:         getEnv("MINIO_ENDPOINT", "minio:9000"),
		MinIOAccessKey:        os.Getenv("MINIO_ACCESS_KEY"),
		MinIOSecretKey:        os.Getenv("MINIO_SECRET_KEY"),
		MinIOBucket:           getEnv("MINIO_BUCKET", "photos"),
		MinIOUseSSL:           getEnv("MINIO_USE_SSL", "false") == "true",
		RabbitMQURL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"),
		RabbitMQExchange:      getEnv("RABBITMQ_EXCHANGE", "gallery.events"),
		GalleryServiceAddress: getEnv("GALLERY_SERVICE_ADDRESS", "gallery-service:50051"),
		GalleryCallTimeout:    2 * time.Second,
	}

	if cfg.MinIOAccessKey == "" || cfg.MinIOSecretKey == "" {
		return Config{}, fmt.Errorf("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
