package main

import (
	"context"
	"log"
	"net"
	"os"

	gallerypb "photogallery/gen/gallery"
	uploadpb "photogallery/gen/upload"
	"photogallery/internal/auth"
	"photogallery/internal/upload"
	"photogallery/internal/upload/api"
	"photogallery/internal/upload/events"
	"photogallery/internal/upload/storage"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatalln("JWT_SECRET environment variable is required")
	}

	cfg, err := upload.Load()
	if err != nil {
		log.Fatalln("Failed to load config:", err)
	}

	ctx := context.Background()

	minioStorage, err := storage.NewStorage(ctx, storage.Config{
		Endpoint:  cfg.MinIOEndpoint,
		AccessKey: cfg.MinIOAccessKey,
		SecretKey: cfg.MinIOSecretKey,
		Bucket:    cfg.MinIOBucket,
		UseSSL:    cfg.MinIOUseSSL,
	})
	if err != nil {
		log.Fatalln("Failed to connect to MinIO:", err)
	}

	publisher, err := events.NewPublisher()
	if err != nil {
		log.Fatalln("Failed to create RabbitMQ publisher:", err)
	}
	defer publisher.Close()

	galleryConn, err := grpc.NewClient(
		cfg.GalleryServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		auth.ServiceCredentials(jwtSecret),
	)

	if err != nil {
		log.Fatalln("Failed to dial Gallery Service:", err)
	}
	defer galleryConn.Close()
	galleryClient := gallerypb.NewGalleryServiceClient(galleryConn)

	// In-memory only: does not survive a restart and is not shared across
	// replicas. Fine for a single-replica deployment; swap for a
	// Postgres-backed Repository before running more than one.
	//repo := upload.NewInMemoryRepository()

	db, err := upload.NewDB()
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalln("Failed to get underlying sql.DB:", err)
	}
	defer sqlDB.Close()

	repo := upload.NewPostgresRepository(db)

	srv := api.NewServer(minioStorage, publisher, galleryClient, repo)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcauth.UnaryServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
		grpc.ChainStreamInterceptor(
			grpcauth.StreamServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
	)

	uploadpb.RegisterUploadServiceServer(s, srv)

	log.Println("Serving gRPC on 0.0.0.0:" + cfg.GRPCPort)
	log.Fatalln(s.Serve(lis))
}
