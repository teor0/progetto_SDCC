package main

import (
	"context"
	"log"
	"net/http"
	"os"
	gallerypb "photogallery/gen/gallery"
	userpb "photogallery/gen/user"
	"photogallery/internal/clients"
	"photogallery/internal/handlers"

	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// corsMiddleware is the function that embeds CORS authorization
func corsMiddleware(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set(
			"Access-Control-Allow-Headers",
			"Content-Type, Authorization",
		)

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on environment variables")
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	uploadAddr := os.Getenv("UPLOAD_SERVICE_ADDRESS")
	if uploadAddr == "" {
		log.Fatal("UPLOAD_SERVICE_ADDRESS environment variable is required")
	}

	minioPublicURL := os.Getenv("MINIO_PUBLIC_URL")
	if minioPublicURL == "" {
		log.Fatal("MINIO_PUBLIC_URL environment variable is required")
	}

	minioBucket := os.Getenv("MINIO_BUCKET")
	if minioBucket == "" {
		log.Fatal("MINIO_BUCKET environment variable is required")
	}

	notificationAddr := os.Getenv("NOTIFICATION_SERVICE_ADDRESS")
	if notificationAddr == "" {
		log.Fatal("NOTIFICATION_SERVICE_ADDRESS environment variable is required")
	}

	userAddr := os.Getenv("USER_SERVICE_ADDRESS")
	if userAddr == "" {
		log.Fatal("USER_SERVICE_ADDRESS environment variable is required")
	}
	galleryAddr := os.Getenv("GALLERY_SERVICE_ADDRESS")
	if galleryAddr == "" {
		log.Fatal("GALLERY_SERVICE_ADDRESS environment variable is required")
	}

	uploadConn, err := grpc.NewClient(
		uploadAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	)
	if err != nil {
		log.Fatalf("failed to connect to UploadService: %v", err)
	}
	defer uploadConn.Close()
	uploadClient := clients.NewUploadClient(uploadConn)

	notificationConn, err := grpc.NewClient(
		notificationAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	)
	if err != nil {
		log.Fatalf("failed to connect to NotificationService: %v", err)
	}
	defer notificationConn.Close()
	notificationClient := clients.NewNotificationClient(notificationConn)

	router := gin.Default()

	uploadHandler := handlers.NewUploadHandler(uploadClient, minioPublicURL, minioBucket)
	notificationHandler := handlers.NewNotificationHandler(notificationClient)

	// add to router the paths a specific handler is in charge
	router.POST("/api/uploads", uploadHandler.UploadPhoto)
	router.GET("/api/galleries/:galleryId/uploads", uploadHandler.ListUploads)
	router.GET("/api/notifications/stream", notificationHandler.Stream)

	userConn, err := grpc.NewClient(userAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to UserService: %v", err)
	}
	defer userConn.Close()

	galleryConn, err := grpc.NewClient(galleryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to GalleryService: %v", err)
	}
	defer galleryConn.Close()

	ctx := context.Background()
	grpcGatewayMux := runtime.NewServeMux()

	if err := userpb.RegisterUserServiceHandler(ctx, grpcGatewayMux, userConn); err != nil {
		log.Fatalf("failed to register UserService gateway: %v", err)
	}
	if err := gallerypb.RegisterGalleryServiceHandler(ctx, grpcGatewayMux, galleryConn); err != nil {
		log.Fatalf("failed to register GalleryService gateway: %v", err)
	}

	root := http.NewServeMux()
	root.Handle("/photogallery/", grpcGatewayMux) // User + Gallery REST routes
	root.Handle("/api/", router)                  // Upload + Notification (Gin)

	log.Printf("API Gateway listening on :%s", httpPort)
	frontendOrigin := os.Getenv("FRONTEND_ORIGIN")
	handler := corsMiddleware(frontendOrigin, root)

	log.Fatal(http.ListenAndServe(":"+httpPort, handler))
}
