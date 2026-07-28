package clients

import (
	uploadpb "photogallery/gen/upload"

	"google.golang.org/grpc"
)

type UploadClient struct {
	Client uploadpb.UploadServiceClient
	//possible general use
	//Upload uploadpb.UploadServiceClient
	//Gallery gallerypb.GalleryServiceClient
	//User userpb.UserServiceClient
}

func NewUploadClient(conn *grpc.ClientConn) *UploadClient {
	return &UploadClient{
		Client: uploadpb.NewUploadServiceClient(conn),
	}
}
