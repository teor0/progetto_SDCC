package main

import (
	"log"
	"net"
	"os"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/user/api"
	"photogallery/internal/user/repository"

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
	grpcPort := os.Getenv("USER_GRPC_PORT")
	//config of the server of User service
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}
	db, err := repository.NewDB()
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
	defer db.Close()
	srv := api.NewServer(db, jwtSecret)
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcauth.UnaryServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
		grpc.ChainStreamInterceptor(
			grpcauth.StreamServerInterceptor(auth.AuthFunc(jwtSecret)),
		),
	)

	// Attach the service to the server
	userpb.RegisterUserServiceServer(s, srv)
	// Serve gRPC server
	log.Println("Serving gRPC on 0.0.0.0:" + grpcPort)
	log.Fatalln(s.Serve(lis)) // blocks here — keeps the process alive
}
