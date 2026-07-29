package main

import (
	"log"
	"os"
	"photogallery/internal/clients"
	"photogallery/internal/handlers"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	notificationAddr := os.Getenv("NOTIFICATION_SERVICE_ADDRESS")
	if notificationAddr == "" {
		log.Fatal("NOTIFICATION_SERVICE_ADDRESS environment variable is required")
	}

	uploadConn, err := grpc.NewClient(
		uploadAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to UploadService: %v", err)
	}
	defer uploadConn.Close()
	uploadClient := clients.NewUploadClient(uploadConn)

	notificationConn, err := grpc.NewClient(
		notificationAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to NotificationService: %v", err)
	}
	defer notificationConn.Close()
	notificationClient := clients.NewNotificationClient(notificationConn)

	router := gin.Default()

	uploadHandler := handlers.NewUploadHandler(uploadClient)
	notificationHandler := handlers.NewNotificationHandler(notificationClient)

	router.POST("/api/uploads", uploadHandler.UploadPhoto)
	router.GET("/api/notifications/stream", notificationHandler.Stream)

	log.Printf("API Gateway listening on :%s", httpPort)
	log.Fatal(router.Run(":" + httpPort))
}
