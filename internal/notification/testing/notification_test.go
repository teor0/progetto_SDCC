package testing

import (
	"context"
	"errors"
	mock "photogallery/internal/notification/mocks"
	"testing"

	notificationpb "photogallery/gen/notification"
	"photogallery/internal/notification"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

func TestRegistryNotify(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream1 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)
	stream2 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID1 := uuid.New()
	userID2 := uuid.New()

	registry.Subscribe(galleryID, userID1, stream1)
	registry.Subscribe(galleryID, userID2, stream2)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: galleryID.String(),
		PhotoUrl:  "http://example.com/photo.jpg",
	}

	stream1.EXPECT().
		Send(n).
		Return(nil)

	stream2.EXPECT().
		Send(n).
		Return(nil)

	registry.Notify(context.Background(), galleryID, n)
}

func TestRegistryNotify_RemovesDisconnectedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()
	registry.Subscribe(galleryID, userID, stream)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: galleryID.String(),
		Message:   "hello",
	}

	stream.EXPECT().
		Send(n).
		Return(errors.New("stream closed"))

	registry.Notify(context.Background(), galleryID, n)

	// No expectations here. If Send() is called again,
	// gomock will fail the test.
	registry.Notify(context.Background(), galleryID, n)
}

func TestRegistryUnsubscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()
	registry.Subscribe(galleryID, userID, stream)
	registry.Unsubscribe(galleryID, userID)

	n := &notificationpb.Notification{
		GalleryId: galleryID.String(),
		Message:   "hello",
	}

	// Since alice was unsubscribed, Notify should never call Send.
	registry.Notify(context.Background(), galleryID, n)
}

func TestRegistryRemoveClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID1 := uuid.New()
	galleryID2 := uuid.New()
	userID := uuid.New()

	registry.Subscribe(galleryID1, userID, stream)
	registry.Subscribe(galleryID2, userID, stream)

	registry.RemoveClient(userID)

	registry.Notify(context.Background(), galleryID1, &notificationpb.Notification{
		GalleryId: galleryID1.String(),
	})

	registry.Notify(context.Background(), galleryID2, &notificationpb.Notification{
		GalleryId: galleryID2.String(),
	})
}
