package clients

import (
	uploadpb "photogallery/gen/upload"

	"google.golang.org/grpc"
)

type UploadClient struct {
	//possible general use
	Upload uploadpb.UploadServiceClient
	//Gallery      gallerypb.GalleryServiceClient
	//User         userpb.UserServiceClient
}

func NewUploadClient(conn *grpc.ClientConn) *UploadClient {
	return &UploadClient{
		Upload: uploadpb.NewUploadServiceClient(conn),
		//Gallery:      gallerypb.NewGalleryServiceClient(conn),
		//User:         userpb.NewUserServiceClient(conn),
	}
}
