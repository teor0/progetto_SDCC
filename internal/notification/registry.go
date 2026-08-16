package notification

import (
	"context"
	"log"
	notificationpb "photogallery/gen/notification"
	"sync"

	"github.com/google/uuid"
)

// Client represents one connected, streaming subscriber.
//
// A Client represents a CONNECTION, not a user.
// A single user can therefore have multiple Clients, for example:
//
//	User A
//	├── browser tab 1 -> Client A1
//	└── browser tab 2 -> Client A2
//
// galleries tracks which galleries this particular connection
// is currently registered for.
type Client struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Stream    notificationpb.NotificationService_SubscribeServer
	galleries map[uuid.UUID]struct{}
}

// Registry indexes connected clients three ways:
//
//   - galleries: gallery_id -> connection_id -> Client
//     Used for notification fan-out.
//
//   - clients: user_id -> connection_id -> Client
//     Used to find all active connections belonging to a user.
//
//   - connections: connection_id -> Client
//     Used for O(1) connection lookup during cleanup.
//
// A user can have multiple simultaneous connections.
type Registry struct {
	mu sync.RWMutex

	galleries   map[uuid.UUID]map[uuid.UUID]*Client
	clients     map[uuid.UUID]map[uuid.UUID]*Client
	connections map[uuid.UUID]*Client
}

func New() *Registry {
	return &Registry{
		galleries:   make(map[uuid.UUID]map[uuid.UUID]*Client),
		clients:     make(map[uuid.UUID]map[uuid.UUID]*Client),
		connections: make(map[uuid.UUID]*Client),
	}
}

// CreateClient creates and registers one connection.
//
// This must be called exactly once for each Subscribe RPC / stream.
func (r *Registry) CreateClient(userID uuid.UUID, stream notificationpb.NotificationService_SubscribeServer) uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()

	connectionID := uuid.New()

	client := &Client{
		ID:        connectionID,
		UserID:    userID,
		Stream:    stream,
		galleries: make(map[uuid.UUID]struct{}),
	}

	r.connections[connectionID] = client

	if _, ok := r.clients[userID]; !ok {
		r.clients[userID] = make(map[uuid.UUID]*Client)
	}

	r.clients[userID][connectionID] = client

	return connectionID
}

// Subscribe registers an existing connection for a gallery.
//
// The connection must have been created with CreateClient.
//
// Calling this multiple times for different galleries adds those
// galleries to the SAME connection.
func (r *Registry) Subscribe(connectionID uuid.UUID, galleryID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, ok := r.connections[connectionID]
	if !ok {
		return
	}

	if _, ok := client.galleries[galleryID]; ok {
		return
	}

	client.galleries[galleryID] = struct{}{}

	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] =
			make(map[uuid.UUID]*Client)
	}

	r.galleries[galleryID][connectionID] = client
}

// AddGalleryForClient registers ALL active connections belonging to a user
// for a newly joined gallery.
//
// This is important when the same user has multiple browser tabs:
//
//	User A
//	├── connection A1
//	└── connection A2
//
// If User A joins Gallery X, both connections become subscribers.
func (r *Registry) AddGalleryForClient(galleryID uuid.UUID, userID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	connections, ok := r.clients[userID]
	if !ok {
		return
	}

	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] =
			make(map[uuid.UUID]*Client)
	}

	for connectionID, client := range connections {
		client.galleries[galleryID] = struct{}{}

		r.galleries[galleryID][connectionID] = client
	}
}

// Unsubscribe removes ALL connections belonging to userID from galleryID.
//
// The connections themselves remain alive and remain subscribed to their
// other galleries.
//
// This is used when the user leaves a gallery.
func (r *Registry) Unsubscribe(galleryID uuid.UUID, userID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	connections, ok := r.clients[userID]
	if !ok {
		return
	}

	for connectionID, client := range connections {
		if _, subscribed :=
			client.galleries[galleryID]; !subscribed {
			continue
		}

		delete(client.galleries, galleryID)

		if galleryConnections, ok :=
			r.galleries[galleryID]; ok {

			delete(
				galleryConnections,
				connectionID,
			)

			if len(galleryConnections) == 0 {
				delete(r.galleries, galleryID)
			}
		}
	}
}

// RemoveClient completely removes ONE connection.
//
// It does NOT remove the other connections belonging to the same user.
//
// This is what should be called when a browser tab / streaming RPC
// disconnects.
func (r *Registry) RemoveClient(connectionID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, ok := r.connections[connectionID]
	if !ok {
		return
	}

	for galleryID := range client.galleries {
		if galleryConnections, ok :=
			r.galleries[galleryID]; ok {

			delete(
				galleryConnections,
				connectionID,
			)

			if len(galleryConnections) == 0 {
				delete(r.galleries, galleryID)
			}
		}
	}

	if userConnections, ok :=
		r.clients[client.UserID]; ok {

		delete(
			userConnections,
			connectionID,
		)

		if len(userConnections) == 0 {
			delete(r.clients, client.UserID)
		}
	}

	delete(r.connections, connectionID)
}

// Notify sends a notification to every active connection registered
// for the gallery.
//
// We copy the clients while holding the read lock and release the lock
// before performing network operations.
func (r *Registry) Notify(ctx context.Context, galleryID uuid.UUID, n *notificationpb.Notification) {
	r.mu.RLock()

	galleryConnections, ok :=
		r.galleries[galleryID]

	if !ok {
		r.mu.RUnlock()
		return
	}

	clients := make(
		[]*Client,
		0,
		len(galleryConnections),
	)

	for _, client := range galleryConnections {
		clients = append(clients, client)
	}

	r.mu.RUnlock()

	for _, client := range clients {
		if err := client.Stream.Send(n); err != nil {
			log.Printf(
				"failed sending notification to user %s on connection %s: %v",
				client.UserID,
				client.ID,
				err,
			)

			r.RemoveClient(client.ID)
		}
	}
}
