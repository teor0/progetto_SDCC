package notification

//go:generate mockgen -destination=./mocks/mock_gallery_client.go -package=mocks photogallery/gen/gallery GalleryServiceClient
//go:generate mockgen -destination=./mocks/mock_notification_stream.go -package=mocks photogallery/gen/notification NotificationService_SubscribeServer
