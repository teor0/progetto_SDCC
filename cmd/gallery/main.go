package main

import (
	"log"
	"net"
	"os"
	"photogallery/internal/gallery"
	"photogallery/internal/gallery/api"

	gallerypb "photogallery/gen/gallery"
	"photogallery/internal/auth"
	"photogallery/internal/gallery/command"
	"photogallery/internal/gallery/query"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatalln("JWT_SECRET environment variable is required")
	}

	grpcPort := os.Getenv("GALLERY_GRPC_PORT")

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}

	db, err := gallery.NewDB()
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalln("Failed to get underlying sql.DB:", err)
	}
	defer sqlDB.Close()

	amqpURL := os.Getenv("RABBITMQ_URL")
	publisher, err := command.NewRabbitMQPublisher(amqpURL)
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer publisher.Close()

	cmdRepo := command.NewCommandRepository(db)
	qryRepo := query.NewQueryRepository(db)

	cmdSvc := command.NewCommandService(cmdRepo, publisher)
	qrySvc := query.NewQueryService(qryRepo)
	srv := api.NewServer(cmdSvc, qrySvc, jwtSecret)

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcauth.UnaryServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
		grpc.ChainStreamInterceptor(
			grpcauth.StreamServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
	)

	gallerypb.RegisterGalleryServiceServer(s, srv)

	log.Println("Serving gRPC on 0.0.0.0:" + grpcPort)
	log.Fatalln(s.Serve(lis)) // blocks here — keeps the process alive
}
