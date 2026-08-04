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

	// config del server per gallery service
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}

	db, err := gallery.NewDB()
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
	sqlDB, err := db.DB()
	defer sqlDB.Close()

	amqpURL := os.Getenv("RABBITMQ_URL") // e.g. amqp://guest:guest@rabbitmq:5672/
	publisher, err := command.NewRabbitMQPublisher(amqpURL)
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer publisher.Close()

	cmdRepo := command.NewCommandRepository(db)
	qryRepo := query.NewQueryRepository(db)

	cmdSvc := command.NewCommandService(cmdRepo, publisher) // swap command.NoopPublisher{} with publisher
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

	// Attach the service to the server
	gallerypb.RegisterGalleryServiceServer(s, srv)
	// Serve gRPC server
	log.Println("Serving gRPC on 0.0.0.0:" + grpcPort)
	go func() {
		log.Fatalln(s.Serve(lis))
	}()
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
}
