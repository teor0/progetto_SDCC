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

type Consumer struct {
	registry      *Registry
	galleryClient gallerypb.GalleryServiceClient

	// galleryNames caches gallery_id -> name so a burst of uploads to the
	// same gallery doesn't mean a GetGallery round trip per event. It's
	// unbounded and never invalidated: gallery names change rarely (there's
	// no rename RPC today), and a briefly-stale cached name is a far
	// smaller problem than adding a synchronous cross-service call to the
	// hot path of every single notification. Revisit if renaming becomes
	// a real feature.
	mu           sync.RWMutex
	galleryNames map[uuid.UUID]string
}

func NewConsumer(r *Registry, galleryClient gallerypb.GalleryServiceClient) *Consumer {
	return &Consumer{
		registry:      r,
		galleryClient: galleryClient,
		galleryNames:  make(map[uuid.UUID]string),
	}
}

// galleryName resolves a gallery's display name for inclusion in a
// notification, via GetGallery -- a public (unauthenticated) RPC, so no
// token needs to be attached to ctx here. Returns "" on failure rather
// than erroring the whole notification: a notification with a blank
// gallery name is still useful; dropping it entirely because a name
// lookup failed would not be.
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

// envelope mirrors the wire shape both Gallery Service (command.Envelope)
// and Upload Service (events.envelope) publish. Every producer wraps its
// event in this shape, so Consume only ever has one format to parse --
// EventType decides how Payload gets interpreted from there.
type envelope struct {
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// photoUploadedPayload matches events.UploadEvent's JSON shape (Upload
// Service, internal/upload/events/publisher.go).
type photoUploadedPayload struct {
	PhotoID     string    `json:"photo_id"`
	GalleryID   uuid.UUID `json:"gallery_id"`
	UploaderID  uuid.UUID `json:"uploader_id"`
	GalleryName string    `json:"gallery_name"`
	PhotoURL    string    `json:"photo_url"`
}

// moderatorAlertPayload matches the map Gallery Service's
// CommandService.SendModeratorAlert publishes (internal/gallery/command/service.go).
type moderatorAlertPayload struct {
	GalleryID   uuid.UUID `json:"gallery_id"`
	GalleryName string    `json:"gallery_name"`
	Message     string    `json:"message"`
}

// memberChangedPayload matches the map Gallery Service's AddMember and
// RemoveMember publish (internal/gallery/command/service.go: "MemberAdded"
// / "MemberRemoved" events).
type memberChangedPayload struct {
	GalleryID uuid.UUID `json:"gallery_id"`
	UserID    uuid.UUID `json:"user_id"`
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

	switch env.EventType {
	case "MemberAdded":
		if err := c.handleMemberAdded(env); err != nil {
			log.Printf("notification: dropping MemberAdded (routing_key=%s): %v", msg.RoutingKey, err)
			msg.Nack(false, false)
			return
		}
		msg.Ack(false)
		return

	case "MemberRemoved":
		if err := c.handleMemberRemoved(env); err != nil {
			log.Printf("notification: dropping MemberRemoved (routing_key=%s): %v", msg.RoutingKey, err)
			msg.Nack(false, false)
			return
		}
		msg.Ack(false)
		return
	}

	notif, err := c.buildNotification(ctx, env)
	if err != nil {
		log.Printf("notification: dropping event_type=%s (routing_key=%s): %v",
			env.EventType, msg.RoutingKey, err)
		msg.Nack(false, false)
		return
	}
	if notif == nil {
		// Recognized as "not our concern" (or explicitly ignored) -- not an
		// error, just nothing to deliver.
		msg.Ack(false)
		return
	}

	galleryID, err := uuid.Parse(notif.GalleryId)
	if err != nil {
		msg.Nack(false, false)
		return
	}

	c.registry.Notify(ctx, galleryID, notif)
	msg.Ack(false)
}

// handleMemberAdded registers an already-connected client for a gallery it
// just joined, without requiring a reconnect. A no-op if that user isn't
// currently connected (Registry.AddGalleryForClient handles that).
func (c *Consumer) handleMemberAdded(env envelope) error {
	var p memberChangedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("unmarshal MemberAdded payload: %w", err)
	}
	if p.GalleryID == uuid.Nil || p.UserID == uuid.Nil {
		return fmt.Errorf("MemberAdded payload missing gallery_id or user_id")
	}
	c.registry.AddGalleryForClient(p.GalleryID, p.UserID)
	return nil
}

// handleMemberRemoved stops fanning out that gallery's events to the given
// user immediately, rather than waiting for their next failed Send to lazily
// prune them. A no-op if that user isn't currently subscribed to it.
func (c *Consumer) handleMemberRemoved(env envelope) error {
	var p memberChangedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("unmarshal MemberRemoved payload: %w", err)
	}
	if p.GalleryID == uuid.Nil || p.UserID == uuid.Nil {
		return fmt.Errorf("MemberRemoved payload missing gallery_id or user_id")
	}
	c.registry.Unsubscribe(p.GalleryID, p.UserID)
	return nil
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
			Message:     "A new photo was uploaded to this gallery.",
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

	case "GalleryCreated", "GalleryClosed":
		// Valid Gallery Service domain events, but no user-facing
		// notification is defined for them yet. (MemberAdded/MemberRemoved
		// are intercepted in handle() before reaching here -- they update
		// the registry directly rather than producing a notification.)
		return nil, nil

	default:
		// An event type not recognized -- possibly a newer producer
		// publishing something this build predates. Treat as
		// forward-compatible no-op rather than a hard error: a malformed message
		// (bad JSON) is a real bug worth Nack-ing and logging loudly; an
		// unrecognized-but-well-formed event type is just "not our concern
		// yet" and shouldn't be treated the same way.
		log.Printf("notification: ignoring unrecognized event_type %q", env.EventType)
		return nil, nil
	}
}
