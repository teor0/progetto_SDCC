package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	notificationpb "photogallery/gen/notification"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const broadcastChannel = "notification-fanout"

// broadcastEnvelope is what Notification Service replicas exchange over
// Redis Pub/Sub. Two kinds:
//
//   - "notify": a fully-built Notification to check against this
//     replica's own local Registry and deliver on a match.
//   - "member_added" / "member_removed": a membership change to apply to
//     this replica's own local Registry bookkeeping, so a user's live
//     connection stays in sync with their actual memberships regardless
//     of which replica happens to be holding that connection.
//
// Every replica publishes and subscribes to the SAME channel and applies
// every message to its own local state -- there's no per-replica
// targeting/directory. At this project's scale, broadcast-and-filter-
// locally is simpler and easier to reason about than maintaining a live
// directory of which replica holds which connection.
type broadcastEnvelope struct {
	Kind           string `json:"kind"`
	NotificationPB []byte `json:"notification_pb,omitempty"` // proto.Marshal'd Notification, for kind=="notify"
	GalleryID      string `json:"gallery_id,omitempty"`
	UserID         string `json:"user_id,omitempty"`
}

// Broadcaster is the fan-out layer between whichever replica drains a
// RabbitMQ message and every replica's local Registry. Consumer is the
// only thing that calls the Publish* methods; every replica's Run loop
// subscribes and applies -- including the replica that published, so
// there's exactly one code path for "apply to local Registry," not a
// separate one for "the replica that happened to drain this."
type Broadcaster struct {
	rdb      *redis.Client
	registry *Registry
}

func NewBroadcaster(rdb *redis.Client, registry *Registry) *Broadcaster {
	return &Broadcaster{rdb: rdb, registry: registry}
}

func (b *Broadcaster) PublishNotification(ctx context.Context, n *notificationpb.Notification) error {
	raw, err := proto.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}
	return b.publish(ctx, broadcastEnvelope{Kind: "notify", NotificationPB: raw})
}

func (b *Broadcaster) PublishMemberAdded(ctx context.Context, galleryID, userID uuid.UUID) error {
	return b.publish(ctx, broadcastEnvelope{Kind: "member_added", GalleryID: galleryID.String(), UserID: userID.String()})
}

func (b *Broadcaster) PublishMemberRemoved(ctx context.Context, galleryID, userID uuid.UUID) error {
	return b.publish(ctx, broadcastEnvelope{Kind: "member_removed", GalleryID: galleryID.String(), UserID: userID.String()})
}

func (b *Broadcaster) publish(ctx context.Context, env broadcastEnvelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	return b.rdb.Publish(ctx, broadcastChannel, body).Err()
}

// Run subscribes to the broadcast channel and applies every envelope --
// including ones this same replica published -- to the local Registry.
// Blocks until ctx is cancelled; run it in a goroutine.
func (b *Broadcaster) Run(ctx context.Context) {
	sub := b.rdb.Subscribe(ctx, broadcastChannel)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			b.apply(ctx, msg.Payload)
		}
	}
}

func (b *Broadcaster) apply(ctx context.Context, payload string) {
	var env broadcastEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		log.Printf("broadcaster: malformed envelope: %v", err)
		return
	}

	switch env.Kind {
	case "notify":
		var n notificationpb.Notification
		if err := proto.Unmarshal(env.NotificationPB, &n); err != nil {
			log.Printf("broadcaster: malformed notification payload: %v", err)
			return
		}
		galleryID, err := uuid.Parse(n.GetGalleryId())
		if err != nil {
			log.Printf("broadcaster: invalid gallery_id in notification: %v", err)
			return
		}
		b.registry.Notify(ctx, galleryID, &n)

	case "member_added", "member_removed":
		galleryID, err1 := uuid.Parse(env.GalleryID)
		userID, err2 := uuid.Parse(env.UserID)
		if err1 != nil || err2 != nil {
			log.Printf("broadcaster: invalid ids in %s envelope", env.Kind)
			return
		}
		if env.Kind == "member_added" {
			b.registry.AddGalleryForClient(galleryID, userID)
		} else {
			b.registry.Unsubscribe(galleryID, userID)
		}

	default:
		log.Printf("broadcaster: ignoring unknown envelope kind %q", env.Kind)
	}
}
