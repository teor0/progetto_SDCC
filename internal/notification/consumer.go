package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	gallerypb "photogallery/gen/gallery"
	notificationpb "photogallery/gen/notification"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// notifier is the subset of Broadcaster's function Consumer depends on.
type notifier interface {
	PublishNotification(ctx context.Context, n *notificationpb.Notification) error
	PublishMemberAdded(ctx context.Context, galleryID, userID uuid.UUID) error
	PublishMemberRemoved(ctx context.Context, galleryID, userID uuid.UUID) error
}

type Consumer struct {
	broadcaster   notifier
	galleryClient gallerypb.GalleryServiceClient
	mu            sync.RWMutex
	galleryNames  map[uuid.UUID]string
}

func NewConsumer(b notifier, galleryClient gallerypb.GalleryServiceClient) *Consumer {
	return &Consumer{
		broadcaster:   b,
		galleryClient: galleryClient,
		galleryNames:  make(map[uuid.UUID]string),
	}
}

func (c *Consumer) galleryName(ctx context.Context, galleryID uuid.UUID) string {
	c.mu.RLock()
	name, ok := c.galleryNames[galleryID]
	c.mu.RUnlock()
	if ok {
		return name
	}

	resp, err := c.galleryClient.GetGallery(ctx, &gallerypb.GetGalleryRequest{
		GalleryId: galleryID.String(),
	})
	if err != nil {
		log.Printf("notification: failed to resolve gallery name for %s: %v", galleryID, err)
		return ""
	}

	c.mu.Lock()
	c.galleryNames[galleryID] = resp.Name
	c.mu.Unlock()

	return resp.Name
}

// envelope mirrors the wire shape of events.envelope
type envelope struct {
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// photoUploadedPayload matches events.UploadEvent's JSON shape
type photoUploadedPayload struct {
	PhotoID     string    `json:"photo_id"`
	GalleryID   uuid.UUID `json:"gallery_id"`
	UploaderID  uuid.UUID `json:"uploader_id"`
	GalleryName string    `json:"gallery_name"`
	PhotoURL    string    `json:"photo_url"`
}

type moderatorAlertPayload struct {
	GalleryID   uuid.UUID `json:"gallery_id"`
	GalleryName string    `json:"gallery_name"`
	Message     string    `json:"message"`
}

type galleryClosedPayload struct {
	GalleryID   uuid.UUID `json:"gallery_id"`
	GalleryName string    `json:"gallery_name"`
}

func (c *Consumer) Consume(ctx context.Context, deliveries <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-deliveries:
			if !ok {
				return
			}
			c.handle(ctx, msg)
		}
	}
}

// handle decodes one delivery. MemberAdded/MemberRemoved update an already-connected
// client's live subscriptions directly (no user-facing
// notification involved). Everything else that produces a notification
// goes through buildNotification; event types that are valid but don't
// produce one are acked and ignored.
func (c *Consumer) handle(ctx context.Context, msg amqp.Delivery) {
	var env envelope
	if err := json.Unmarshal(msg.Body, &env); err != nil {
		log.Printf("notification: malformed envelope (routing_key=%s): %v", msg.RoutingKey, err)
		msg.Nack(false, false) // not requeued: a malformed message will never parse
		return
	}

	notif, err := c.buildNotification(ctx, env)
	if err != nil {
		log.Printf("notification: dropping event_type=%s (routing_key=%s): %v", env.EventType, msg.RoutingKey, err)
		msg.Nack(false, false)
		return
	}
	if notif == nil {
		msg.Ack(false)
		return
	}

	if err := c.broadcaster.PublishNotification(ctx, notif); err != nil {
		log.Printf("notification: failed to publish notification for fan-out: %v", err)
		msg.Nack(false, false)
		return
	}
	msg.Ack(false)
}

// buildNotification returns (nil, nil) for event types that are valid but
// don't produce a user-facing notification.
func (c *Consumer) buildNotification(ctx context.Context, env envelope) (*notificationpb.Notification, error) {
	switch env.EventType {
	case "PhotoUploaded":
		var p photoUploadedPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return nil, fmt.Errorf("unmarshal PhotoUploaded payload: %w", err)
		}
		if p.GalleryID == uuid.Nil {
			return nil, fmt.Errorf("PhotoUploaded payload missing gallery_id")
		}
		return &notificationpb.Notification{
			Id:          uuid.NewString(),
			Type:        notificationpb.NotificationType_NOTIFICATION_TYPE_PHOTO_UPLOADED,
			GalleryId:   p.GalleryID.String(),
			GalleryName: c.galleryName(ctx, p.GalleryID),
			PhotoId:     p.PhotoID,
			UploaderId:  p.UploaderID.String(),
			PhotoUrl:    p.PhotoURL,
			Message:     "A new photo was uploaded ",
			OccurredAt:  timestamppb.New(env.Timestamp),
		}, nil

	case "ModeratorAlert":
		var p moderatorAlertPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return nil, fmt.Errorf("unmarshal ModeratorAlert payload: %w", err)
		}
		if p.GalleryID == uuid.Nil {
			return nil, fmt.Errorf("ModeratorAlert payload missing gallery_id")
		}
		return &notificationpb.Notification{
			Id:          uuid.NewString(),
			Type:        notificationpb.NotificationType_NOTIFICATION_TYPE_MODERATOR_ALERT,
			GalleryId:   p.GalleryID.String(),
			GalleryName: c.galleryName(ctx, p.GalleryID),
			Message:     p.Message,
			OccurredAt:  timestamppb.New(env.Timestamp),
		}, nil

	case "GalleryClosed":
		var p galleryClosedPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return nil, fmt.Errorf("unmarshal GalleryClosed payload: %w", err)
		}
		if p.GalleryID == uuid.Nil {
			return nil, fmt.Errorf("GalleryClosed payload missing gallery_id")
		}
		// always prefer the event cause it's 'fresh'
		name := p.GalleryName
		if name == "" {
			name = c.galleryName(ctx, p.GalleryID)
		}
		return &notificationpb.Notification{
			Id:          uuid.NewString(),
			Type:        notificationpb.NotificationType_NOTIFICATION_TYPE_GALLERY_CLOSED,
			GalleryId:   p.GalleryID.String(),
			GalleryName: name,
			Message:     "This gallery has been closed by its moderator.",
			OccurredAt:  timestamppb.New(env.Timestamp),
		}, nil

	default:
		// an event type not recognized
		log.Printf("notification: ignoring unrecognized event_type %q", env.EventType)
		return nil, nil
	}
}
