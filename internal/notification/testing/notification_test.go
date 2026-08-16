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

	// CreateClient registers the stream and hands back a connection ID --
	// Subscribe then operates on that ID, not on userID/stream directly.
	conn1 := registry.CreateClient(userID1, stream1)
	conn2 := registry.CreateClient(userID2, stream2)

	registry.Subscribe(conn1, galleryID)
	registry.Subscribe(conn2, galleryID)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: galleryID.String(),
		PhotoUrl:  "http://example.com/photo.jpg",
	}

	stream1.EXPECT().Send(n).Return(nil)
	stream2.EXPECT().Send(n).Return(nil)

	registry.Notify(context.Background(), galleryID, n)
}

func TestRegistryNotify_RemovesDisconnectedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	conn := registry.CreateClient(userID, stream)
	registry.Subscribe(conn, galleryID)

	n := &notificationpb.Notification{
		Id:        "notif-1",
		GalleryId: galleryID.String(),
		Message:   "hello",
	}

	stream.EXPECT().Send(n).Return(errors.New("stream closed"))

	registry.Notify(context.Background(), galleryID, n)

	// No expectations here -- Notify should have called RemoveClient
	// internally after the failed Send, so a second Notify must not
	// invoke Send again. If Send is called, gomock fails the test.
	registry.Notify(context.Background(), galleryID, n)
}

func TestRegistryUnsubscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	stream := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	conn := registry.CreateClient(userID, stream)
	registry.Subscribe(conn, galleryID)
	registry.Unsubscribe(galleryID, userID)

	n := &notificationpb.Notification{
		GalleryId: galleryID.String(),
		Message:   "hello",
	}

	// Notify should never call Send -- this connection was unsubscribed.
	registry.Notify(context.Background(), galleryID, n)
}

// TestRegistryUnsubscribe_AllConnectionsForUser covers the multi-tab case
// Unsubscribe's own doc comment promises ("removes ALL connections
// belonging to userID from galleryID") but the original single-stream
// tests had no way to express, since there was never more than one
// connection per user to begin with.
func TestRegistryUnsubscribe_AllConnectionsForUser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	streamTab1 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)
	streamTab2 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	connTab1 := registry.CreateClient(userID, streamTab1)
	connTab2 := registry.CreateClient(userID, streamTab2)

	registry.Subscribe(connTab1, galleryID)
	registry.Subscribe(connTab2, galleryID)

	registry.Unsubscribe(galleryID, userID)

	n := &notificationpb.Notification{GalleryId: galleryID.String()}

	// Neither tab should receive it. If Unsubscribe only cleared one
	// connection instead of iterating every connection for the user,
	// one of these mocks would get an unexpected Send call and fail.
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

	conn := registry.CreateClient(userID, stream)
	registry.Subscribe(conn, galleryID1)
	registry.Subscribe(conn, galleryID2)

	registry.RemoveClient(conn)

	// No expectations set on stream -- if Send is called for either
	// gallery after removal, gomock fails the test.
	registry.Notify(context.Background(), galleryID1, &notificationpb.Notification{
		GalleryId: galleryID1.String(),
	})
	registry.Notify(context.Background(), galleryID2, &notificationpb.Notification{
		GalleryId: galleryID2.String(),
	})
}

// TestRegistryRemoveClient_DoesNotAffectOtherConnectionsOfSameUser is the
// other half of the multi-tab contract: closing one tab (one connection)
// must not disconnect a user's other open tabs. RemoveClient takes a
// connectionID specifically so this is expressible -- the old
// RemoveClient(userID) signature could only remove everything for a user
// at once, which is the wrong granularity for "a browser tab closed."
func TestRegistryRemoveClient_DoesNotAffectOtherConnectionsOfSameUser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	streamTab1 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)
	streamTab2 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	connTab1 := registry.CreateClient(userID, streamTab1)
	connTab2 := registry.CreateClient(userID, streamTab2)

	registry.Subscribe(connTab1, galleryID)
	registry.Subscribe(connTab2, galleryID)

	registry.RemoveClient(connTab1)

	n := &notificationpb.Notification{GalleryId: galleryID.String()}
	streamTab2.EXPECT().Send(n).Return(nil)

	registry.Notify(context.Background(), galleryID, n)
}

// TestAddGalleryForClient_RegistersEveryConnectionOfUser covers the
// join-mid-session path Consumer.handleMemberAdded relies on: a user with
// multiple open tabs joins a new gallery, and every one of their existing
// connections should start receiving events for it -- not just whichever
// connection happened to be created most recently.
func TestAddGalleryForClient_RegistersEveryConnectionOfUser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := notification.New()

	streamTab1 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)
	streamTab2 := mock.NewMockNotificationService_SubscribeServer[*notificationpb.Notification](ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	registry.CreateClient(userID, streamTab1)
	registry.CreateClient(userID, streamTab2)

	// Neither connection has subscribed to galleryID yet -- this simulates
	// the user joining a gallery after both tabs are already connected.
	registry.AddGalleryForClient(galleryID, userID)

	n := &notificationpb.Notification{GalleryId: galleryID.String()}
	streamTab1.EXPECT().Send(n).Return(nil)
	streamTab2.EXPECT().Send(n).Return(nil)

	registry.Notify(context.Background(), galleryID, n)
}
