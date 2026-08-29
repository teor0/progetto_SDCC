package main

import (
	"context"
	"log"
	"net"
	"os"

	gallerypb "photogallery/gen/gallery"
	notificationpb "photogallery/gen/notification"
	"photogallery/internal/auth"
	"photogallery/internal/notification"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// exchangeName must match the topic exchange every producer (Gallery
// Service's command.RabbitMQPublisher, Upload Service's events.Publisher)
// declares and publishes domain events to.
const exchangeName = "gallery.events"

// queueName is durable and explicitly named rather than an auto-generated
// exclusive queue, so that: (a) events published while this service is
// down are still delivered on restart instead of being dropped, and
// (b) if this is ever scaled to more than one replica, instances compete
// for the same queue (work-queue fan-out) instead of each getting its own
// duplicate copy of every event.
const queueName = "notification.events"
const bindingKey = "gallery.#"

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatalln("JWT_SECRET environment variable is required")
	}

	grpcPort := os.Getenv("NOTIFICATION_GRPC_PORT")
	if grpcPort == "" {
		log.Fatalln("NOTIFICATION_GRPC_PORT environment variable is required")
	}

	galleryAddr := os.Getenv("GALLERY_SERVICE_ADDRESS")
	if galleryAddr == "" {
		log.Fatalln("GALLERY_SERVICE_ADDRESS environment variable is required")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatalln("REDIS_ADDR environment variable is required")
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	registry := notification.New()
	broadcaster := notification.NewBroadcaster(rdb, registry)

	broadcastCtx, cancelBroadcast := context.WithCancel(context.Background())
	defer cancelBroadcast()
	go broadcaster.Run(broadcastCtx)

	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		log.Fatalln("RABBITMQ_URL environment variable is required")
	}

	galleryConn, err := grpc.NewClient(
		galleryAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		auth.ServiceCredentials(jwtSecret),
	)
	if err != nil {
		log.Fatalln("Failed to dial Gallery Service:", err)
	}
	defer galleryConn.Close()
	galleryClient := gallerypb.NewGalleryServiceClient(galleryConn)

	// --- RabbitMQ consumer ---------------------------------------------------
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open RabbitMQ channel:", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	if err := ch.QueueBind(q.Name, bindingKey, exchangeName, false, nil); err != nil {
		log.Fatalln("Failed to bind queue:", err)
	}

	// Fair dispatch: still meaningful per replica even though it's no longer
	// sharing the queue with siblings -- bounds how many in-flight unacked
	// deliveries this one replica takes on at once, protecting it from being
	// overwhelmed by a burst.
	if err := ch.Qos(10, 0, false); err != nil {
		log.Fatalln("Failed to set QoS:", err)
	}

	deliveries, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		false, // auto-ack -- Consumer.handle acks/nacks explicitly
		true,  // exclusive -- matches the queue itself
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to register RabbitMQ consumer:", err)
	}

	consumer := notification.NewConsumer(broadcaster, galleryClient)

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()
	go consumer.Consume(consumeCtx, deliveries)

	srv := notification.NewServer(registry, galleryClient)

	lis, err := net.Listen("tcp", ":"+grpcPort)
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

	notificationpb.RegisterNotificationServiceServer(s, srv)

	log.Println("Serving gRPC on 0.0.0.0:" + grpcPort)
	log.Fatalln(s.Serve(lis))
}
