package notification

import (
	"context"
	"log"
	notificationpb "photogallery/gen/notification"
	"sync"
)

// Client represents one connected, streaming subscriber. galleries tracks
// which galleries this client is currently registered for, so RemoveClient
// can clean up in O(number of this client's galleries) instead of scanning
// every gallery in the registry.
type Client struct {
	UserID    string
	Stream    notificationpb.NotificationService_SubscribeServer
	galleries map[string]struct{}
}

// Registry indexes connected clients two ways: by gallery (for fan-out on
// Notify) and by user (for O(1) lookup/cleanup, and for adding an
// already-connected client to a new gallery without needing their stream
// handed to us again).
type Registry struct {
	mu sync.RWMutex

	galleries map[string]map[string]*Client // gallery_id -> user_id -> Client
	clients   map[string]*Client            // user_id -> Client
}

func New() *Registry {
	return &Registry{
		galleries: make(map[string]map[string]*Client),
		clients:   make(map[string]*Client),
	}
}

// Subscribe registers userID's stream for galleryID. Safe to call multiple
// times for the same user across different galleries (that's the normal
// case: Subscribe's handler calls this once per gallery membership at
// connect time) -- subsequent calls for the same user reuse the existing
// Client entry and just add another gallery to it, updating Stream in case
// this is actually a reconnect under the same userID.
func (r *Registry) Subscribe(galleryID string, userID string, stream notificationpb.NotificationService_SubscribeServer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, ok := r.clients[userID]
	if !ok {
		client = &Client{
			UserID:    userID,
			galleries: make(map[string]struct{}),
		}
		r.clients[userID] = client
	}
	client.Stream = stream // covers reconnects: always point at the current stream
	client.galleries[galleryID] = struct{}{}

	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] = make(map[string]*Client)
	}
	r.galleries[galleryID][userID] = client
}

// AddGalleryForClient registers an already-connected client for a gallery
// it wasn't subscribed to at connect time. It's a no-op if userID isn't
// currently connected. This is what closes the "joined a gallery mid-
// session" gap: a Consumer handling a MemberAdded event can call this
// instead of requiring the user to reconnect before they start receiving
// that gallery's notifications.
func (r *Registry) AddGalleryForClient(galleryID string, userID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, ok := r.clients[userID]
	if !ok {
		return
	}
	client.galleries[galleryID] = struct{}{}

	if _, ok := r.galleries[galleryID]; !ok {
		r.galleries[galleryID] = make(map[string]*Client)
	}
	r.galleries[galleryID][userID] = client
}

// Unsubscribe removes userID from galleryID only -- the client stays
// connected and registered for whatever other galleries it has. Use
// RemoveClient for a full disconnect.
func (r *Registry) Unsubscribe(galleryID string, userID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unsubscribeLocked(galleryID, userID)
}

func (r *Registry) unsubscribeLocked(galleryID string, userID string) {
	if users, ok := r.galleries[galleryID]; ok {
		delete(users, userID)
		if len(users) == 0 {
			delete(r.galleries, galleryID)
		}
	}
	if client, ok := r.clients[userID]; ok {
		delete(client.galleries, galleryID)
	}
}

// RemoveClient fully disconnects userID: removes it from every gallery
// it was registered for and drops the client entry itself. O(number of
// galleries this client was in), not O(every gallery in the registry).
func (r *Registry) RemoveClient(userID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, ok := r.clients[userID]
	if !ok {
		return
	}
	for galleryID := range client.galleries {
		r.unsubscribeLocked(galleryID, userID)
	}
	delete(r.clients, userID)
}

func (r *Registry) Notify(ctx context.Context, galleryID string, n *notificationpb.Notification) {
	// Copy clients while holding the read lock.
	r.mu.RLock()

	users, ok := r.galleries[galleryID]
	if !ok {
		r.mu.RUnlock()
		return
	}

	clients := make([]*Client, 0, len(users))
	for _, c := range users {
		clients = append(clients, c)
	}

	r.mu.RUnlock()

	// Never hold the mutex while sending over gRPC.
	for _, client := range clients {
		if err := client.Stream.Send(n); err != nil {
			log.Printf("failed sending notification to %s: %v", client.UserID, err)

			// Stream is probably dead.
			r.Unsubscribe(galleryID, client.UserID)
		}
	}
}
