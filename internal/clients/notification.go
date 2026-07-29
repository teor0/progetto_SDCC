package clients

import (
	notificationpb "photogallery/gen/notification"

	"google.golang.org/grpc"
)

type NotificationClient struct {
	Client notificationpb.NotificationServiceClient
}

func NewNotificationClient(conn *grpc.ClientConn) *NotificationClient {
	return &NotificationClient{
		Client: notificationpb.NewNotificationServiceClient(conn),
	}
}
