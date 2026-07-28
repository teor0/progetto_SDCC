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
		log.Fatal("UPLOAD_SERVICE_ADDR environment variable is required")
	}

	conn, err := grpc.NewClient(
		"localhost:8092",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to UploadService: %v", err)
	}
	defer conn.Close()

	uploadClient := clients.NewUploadClient(conn)

	router := gin.Default()

	uploadHandler := handlers.NewUploadHandler(uploadClient)

	router.POST(
		"/api/uploads",
		uploadHandler.UploadPhoto,
	)

	log.Printf("API Gateway listening on :%s", httpPort)
	log.Fatal(router.Run(":" + httpPort))
}
