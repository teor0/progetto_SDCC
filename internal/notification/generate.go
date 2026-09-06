package notification

//go:generate mockgen -destination=./mocks/mock_gallery_client.go -package=mocks photogallery/gen/gallery GalleryServiceClient
//go:generate mockgen -destination=./mocks/mock_notification_stream.go -package=mocks photogallery/gen/notification NotificationService_SubscribeServer
//go:generate mockgen -source=./consumer.go -destination=./mocks/mock_notifier.go -package=mocks
