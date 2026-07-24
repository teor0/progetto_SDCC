package testing

import (
	"context"
	"errors"
	"photogallery/internal/notification/internal/notification/mocks"
	mocknotification "photogallery/internal/notification/mocks"
	"testing"

	notificationpb "photogallery/gen/notification"
	"photogallery/internal/notification"

	"go.uber.org/mock/gomock"
)

func TestRegistryNotify(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream1 := mocknotification.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)
	stream2 := mocknotification.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	registry.Subscribe("gallery-1", "alice", stream1)
	registry.Subscribe("gallery-1", "bob", stream2)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: "gallery-1",
		PhotoUrl:  "http://example.com/photo.jpg",
	}

	stream1.EXPECT().
		Send(n).
		Return(nil)

	stream2.EXPECT().
		Send(n).
		Return(nil)

	registry.Notify(context.Background(), "gallery-1", n)
}

func TestRegistryNotify_RemovesDisconnectedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mocknotification.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	registry.Subscribe("gallery-1", "alice", stream)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: "gallery-1",
		Message:   "hello",
	}

	stream.EXPECT().
		Send(n).
		Return(errors.New("stream closed"))

	registry.Notify(context.Background(), "gallery-1", n)

	// No expectations here. If Send() is called again,
	// gomock will fail the test.
	registry.Notify(context.Background(), "gallery-1", n)
}

func TestRegistryUnsubscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mocks.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	registry.Subscribe("gallery-1", "alice", stream)
	registry.Unsubscribe("gallery-1", "alice")

	n := &notificationpb.Notification{
		GalleryId: "gallery-1",
	}

	// Since alice was unsubscribed, Notify should never call Send.
	registry.Notify(context.Background(), "gallery-1", n)
}

func TestRegistryRemoveClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mocks.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	registry.Subscribe("gallery-1", "alice", stream)
	registry.Subscribe("gallery-2", "alice", stream)

	registry.RemoveClient("alice")

	registry.Notify(context.Background(), "gallery-1", &notificationpb.Notification{
		GalleryId: "gallery-1",
	})

	registry.Notify(context.Background(), "gallery-2", &notificationpb.Notification{
		GalleryId: "gallery-2",
	})
}
