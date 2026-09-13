package clients

import (
	uploadpb "photogallery/gen/upload"

	"google.golang.org/grpc"
)

type UploadClient struct {
	Upload uploadpb.UploadServiceClient
}

func NewUploadClient(conn *grpc.ClientConn) *UploadClient {
	return &UploadClient{
		Upload: uploadpb.NewUploadServiceClient(conn),
	}
}
