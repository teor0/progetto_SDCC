package notification

import (
	"context"
	"log"
	notificationpb "photogallery/gen/notification"
	"sync"

	"github.com/google/uuid"
)

// Client represents one connected, streaming subscriber. galleries tracks
// which galleries this client is currently registered for, so RemoveClient
// can clean up in O(number of this client's galleries) instead of scanning
// every gallery in the registry.
type Client struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Stream    notificationpb.NotificationService_SubscribeServer
	galleries map[uuid.UUID]struct{}
}

// Registry indexes connected clients two ways: by gallery (for fan-out on
// Notify) and by user (for O(1) lookup/cleanup, and for adding an
// already-connected client to a new gallery without needing their stream
// handed to us again).
type Registry struct {
	mu          sync.RWMutex
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

// Subscribe registers userID's stream for galleryID. Safe to call multiple
// times for the same user across different galleries (that's the normal
// case: Subscribe's handler calls this once per gallery membership at
// connect time) -- subsequent calls for the same user reuse the existing
// Client entry and just add another gallery to it, updating Stream in case
// this is actually a reconnect under the same userID.
func (r *Registry) Subscribe(galleryID uuid.UUID, userID uuid.UUID, stream notificationpb.NotificationService_SubscribeServer) uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()

	connectionID := uuid.New()

	client := &Client{
		ID:        connectionID,
		UserID:    userID,
		Stream:    stream,
		galleries: make(map[uuid.UUID]struct{}),
	}

	// Register connection under the user.
	if _, ok := r.clients[userID]; !ok {
		r.clients[userID] = make(map[uuid.UUID]*Client)
	}

	r.clients[userID][connectionID] = client

	// Register connection under the gallery.
	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] = make(map[uuid.UUID]*Client)
	}

	r.galleries[galleryID][connectionID] = client

	client.galleries[galleryID] = struct{}{}

	return connectionID
}

// AddGalleryForClient registers an already-connected client for a gallery
// it wasn't subscribed to at connect time. It's a no-op if userID isn't
// currently connected. This is what closes the "joined a gallery mid-
// session" gap: a Consumer handling a MemberAdded event can call this
// instead of requiring the user to reconnect before they start receiving
// that gallery's notifications.
func (r *Registry) AddGalleryForClient(
	galleryID uuid.UUID,
	userID uuid.UUID,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	connections, ok := r.clients[userID]
	if !ok {
		return
	}

	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] = make(map[uuid.UUID]*Client)
	}

	for connectionID, client := range connections {
		client.galleries[galleryID] = struct{}{}

		r.galleries[galleryID][connectionID] = client
	}
}

// Unsubscribe removes userID from galleryID only -- the client stays
// connected and registered for whatever other galleries it has. Use
// RemoveClient for a full disconnect.
func (r *Registry) Unsubscribe(galleryID uuid.UUID, connectionID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unsubscribeLocked(galleryID, connectionID)
}

func (r *Registry) unsubscribeLocked(galleryID uuid.UUID, connectionID uuid.UUID) {
	connections, ok := r.galleries[galleryID]
	if !ok {
		return
	}

	client, ok := connections[connectionID]
	if !ok {
		return
	}

	delete(connections, connectionID)

	if len(connections) == 0 {
		delete(r.galleries, galleryID)
	}

	delete(client.galleries, galleryID)

	// If this connection isn't subscribed to any other
	// galleries, remove it from the user's connections.
	if len(client.galleries) == 0 {
		if userConnections, ok := r.clients[client.UserID]; ok {
			delete(userConnections, connectionID)

			if len(userConnections) == 0 {
				delete(r.clients, client.UserID)
			}
		}
	}
}

// RemoveClient fully disconnects userID: removes it from every gallery
// it was registered for and drops the client entry itself. O(number of
// galleries this client was in), not O(every gallery in the registry).
func (r *Registry) RemoveClient(connectionID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var client *Client

	// Find the connection.
	//
	// We can avoid this search if Subscribe's caller
	// retains the Client, but for now this keeps the
	// Registry API simple.
	for _, connections := range r.clients {
		if c, ok := connections[connectionID]; ok {
			client = c
			break
		}
	}

	if client == nil {
		return
	}

	for galleryID := range client.galleries {
		r.unsubscribeLocked(
			galleryID,
			connectionID,
		)
	}

	if userConnections, ok := r.clients[client.UserID]; ok {
		delete(userConnections, connectionID)

		if len(userConnections) == 0 {
			delete(r.clients, client.UserID)
		}
	}
}

func (r *Registry) Notify(ctx context.Context, galleryID uuid.UUID, n *notificationpb.Notification) {
	r.mu.RLock()

	connections, ok := r.galleries[galleryID]
	if !ok {
		r.mu.RUnlock()
		return
	}

	clients := make([]*Client, 0, len(connections))

	for _, client := range connections {
		clients = append(clients, client)
	}

	r.mu.RUnlock()

	// Never hold the mutex while sending over gRPC.
	for _, client := range clients {
		if err := client.Stream.Send(n); err != nil {
			log.Printf(
				"failed sending notification to user %s connection %s: %v",
				client.UserID,
				client.ID,
				err,
			)

			r.Unsubscribe(
				galleryID,
				client.ID,
			)
		}
	}
}
