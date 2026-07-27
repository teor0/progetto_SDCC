package mocks

//go:generate mockgen -destination=gallery_client_mock.go -package=mocks photogallery/gen/gallery GalleryServiceClient
//go:generate mockgen -source=../api/grpc.go -destination=upload_stream_mock.go -package=mocks
//go:generate mockgen -destination=storage_mock.go -package=mocks photogallery/internal/upload/storage Uploader
//go:generate mockgen -destination=events_mock.go -package=mocks photogallery/internal/upload/events Notifier
//go:generate mockgen -destination=repository_mock.go -package=mocks photogallery/internal/upload Repository
