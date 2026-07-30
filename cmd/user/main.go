package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/user/api"
	"photogallery/internal/user/repository"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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
	grpcPort := os.Getenv("USER_GRPC_PORT")
	gatewayPort := os.Getenv("USER_GATEWAY_PORT")
	//config del server per user service
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
	go func() {
		log.Fatalln(s.Serve(lis))
	}()
	if err != nil {
		log.Fatalln("Failed to serve server:", err)
	}

	// Create a client connection to the gRPC server we just started
	// This is where the gRPC-Gateway proxies the requests
	conn, err := grpc.NewClient(
		"0.0.0.0:"+grpcPort,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalln("Failed to dial server:", err)
	}

	//
	gatewaymux := runtime.NewServeMux()
	// Register UserService
	err = userpb.RegisterUserServiceHandler(context.Background(), gatewaymux, conn)
	if err != nil {
		log.Fatalln("Failed to register gateway:", err)
	}

	//open the gatewayserver that handle all the clients request!
	gwServer := &http.Server{
		Addr:    ":" + gatewayPort,
		Handler: gatewaymux,
	}

	log.Println("Serving gRPC-Gateway on http://0.0.0.0:" + gatewayPort)
	log.Fatalln(gwServer.ListenAndServe())
}
