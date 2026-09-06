package testing

import (
	"context"
	"encoding/json"
	"errors"
	mock "photogallery/internal/notification/mocks"
	"testing"
	"time"

	notificationpb "photogallery/gen/notification"
	"photogallery/internal/notification"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
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

	// Notify should have called RemoveClient internally after the failed Send,
	// so a second Notify must not invoke Send again.
	// If Send is called, gomock fails the test.
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

	// Notify should never call Send, this connection was unsubscribed.
	registry.Notify(context.Background(), galleryID, n)
}

// TestRegistryUnsubscribe_AllConnectionsForUser covers
// the case which removes ALL connections belonging to userID from galleryID
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

	registry.Notify(context.Background(), galleryID1, &notificationpb.Notification{
		GalleryId: galleryID1.String(),
	})
	registry.Notify(context.Background(), galleryID2, &notificationpb.Notification{
		GalleryId: galleryID2.String(),
	})
}

// TestAddGalleryForClient_RegistersEveryConnectionOfUser covers the
// join-mid-session
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

// --- Consumer / GalleryClosed ---

// wireEnvelope mirrors the private "envelope" shape Consumer parses
// (internal/notification/consumer.go) -- duplicated here deliberately so
// this test exercises the actual wire contract (JSON field names) rather
// than reaching into an unexported type from another package.
type wireEnvelope struct {
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// fakeAcknowledger stands in for the real AMQP channel so
// amqp.Delivery.Ack/Nack (called internally by Consumer.handle) have
// something non-nil to call, and so the test can assert which one fired.
type fakeAcknowledger struct {
	acked  bool
	nacked bool
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.acked = true
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	f.nacked = true
	return nil
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

// galleryClosedDelivery builds a single amqp.Delivery carrying a
// "GalleryClosed" event, with payload built from the given key/value
// pairs (so the missing-gallery_id test can omit it).
func galleryClosedDelivery(t *testing.T, ack *fakeAcknowledger, payload map[string]string) amqp.Delivery {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	body, err := json.Marshal(wireEnvelope{
		EventType: "GalleryClosed",
		Timestamp: time.Now().UTC(),
		Payload:   payloadBytes,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	return amqp.Delivery{Acknowledger: ack, Body: body}
}

// TestConsumer_GalleryClosed_PublishesNotification exercises the full
// path a real "gallery.closed" RabbitMQ delivery takes: Consumer.Consume
// decodes the envelope, builds a NOTIFICATION_TYPE_GALLERY_CLOSED
// Notification, and hands it to the broadcaster for fan-out -- then acks
// the delivery.
func TestConsumer_GalleryClosed_PublishesNotification(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	broadcaster := mock.NewMocknotifier(ctrl)
	galleryClient := mock.NewMockGalleryServiceClient(ctrl)

	galleryID := uuid.New()

	var captured *notificationpb.Notification
	broadcaster.EXPECT().
		PublishNotification(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, n *notificationpb.Notification) error {
			captured = n
			return nil
		})

	consumer := notification.NewConsumer(broadcaster, galleryClient)

	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- galleryClosedDelivery(t, ack, map[string]string{
		"gallery_id":   galleryID.String(),
		"gallery_name": "Summer Trip",
	})
	close(deliveries)

	consumer.Consume(context.Background(), deliveries)

	if captured == nil {
		t.Fatal("expected a notification to be published, got none")
	}
	if captured.Type != notificationpb.NotificationType_NOTIFICATION_TYPE_GALLERY_CLOSED {
		t.Fatalf("expected type GALLERY_CLOSED, got %s", captured.Type)
	}
	if captured.GalleryId != galleryID.String() {
		t.Fatalf("expected gallery_id %s, got %s", galleryID, captured.GalleryId)
	}
	if captured.GalleryName != "Summer Trip" {
		t.Fatalf("expected gallery_name %q, got %q", "Summer Trip", captured.GalleryName)
	}
	if captured.Message == "" {
		t.Fatal("expected a non-empty message")
	}
	if !ack.acked {
		t.Fatal("expected the delivery to be Acked")
	}
	if ack.nacked {
		t.Fatal("an Acked delivery should not also be Nacked")
	}
}

// TestConsumer_GalleryClosed_MissingGalleryID_NacksAndDrops confirms a
// malformed event (missing gallery_id) never reaches the broadcaster and
// is Nacked rather than Acked.
func TestConsumer_GalleryClosed_MissingGalleryID_NacksAndDrops(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	broadcaster := mock.NewMocknotifier(ctrl)
	galleryClient := mock.NewMockGalleryServiceClient(ctrl)

	broadcaster.EXPECT().PublishNotification(gomock.Any(), gomock.Any()).Times(0)

	consumer := notification.NewConsumer(broadcaster, galleryClient)

	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- galleryClosedDelivery(t, ack, map[string]string{
		"gallery_name": "Summer Trip", // gallery_id deliberately omitted
	})
	close(deliveries)

	consumer.Consume(context.Background(), deliveries)

	if !ack.nacked {
		t.Fatal("expected the malformed delivery to be Nacked")
	}
	if ack.acked {
		t.Fatal("a Nacked delivery should not also be Acked")
	}
}
