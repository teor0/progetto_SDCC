package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	notificationpb "photogallery/gen/notification"
	"photogallery/internal/clients"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"
)

type NotificationHandler struct {
	notificationClient *clients.NotificationClient
}

func NewNotificationHandler(notificationClient *clients.NotificationClient) *NotificationHandler {
	return &NotificationHandler{notificationClient: notificationClient}
}

// notificationDTO is the JSON shape pushed to browser clients. Kept
// separate from notificationpb.Notification rather than reusing protobuf's
// generated JSON tags, so the public wire format isn't coupled to whatever
// the .proto happens to look like internally.
type notificationDTO struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	PhotoID     string `json:"photoId,omitempty"`
	GalleryID   string `json:"galleryId"`
	GalleryName string `json:"galleryName,omitempty"`
	UploaderID  string `json:"uploaderId,omitempty"`
	Message     string `json:"message,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	OccurredAt  string `json:"occurredAt,omitempty"`
}

// Stream opens a Server-Sent Events connection and forwards every
// Notification the caller is subscribed to -- one "notification" SSE event
// per message -- until the browser disconnects or NotificationService closes
// the underlying stream. SSE is enough here rather than a full WebSocket
// bridge because delivery is one-directional (server -> browser); Subscribe
// itself has no client-to-server messages to forward back.
func (h *NotificationHandler) Stream(c *gin.Context) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing Authorization header"})
		return
	}

	// Forward the browser's JWT so NotificationService's auth interceptor
	// can resolve the caller's identity -- Subscribe() reads it via
	// auth.FromContext to know which galleries to fan out to.
	ctx := metadata.NewOutgoingContext(
		c.Request.Context(),
		metadata.Pairs("authorization", authHeader),
	)

	stream, err := h.notificationClient.Client.Subscribe(ctx, &notificationpb.SubscribeRequest{})
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // disable nginx response buffering, if present

	c.Stream(func(w io.Writer) bool {
		n, err := stream.Recv()
		if err != nil {
			// c.Request.Context() cancellation (browser disconnected) is the
			// expected way this loop ends -- don't try to write an SSE event
			// on a connection that's already gone.
			if c.Request.Context().Err() != nil {
				return false
			}
			if !errors.Is(err, io.EOF) {
				c.SSEvent("error", err.Error())
			}
			return false
		}

		dto := notificationDTO{
			ID:          n.GetId(),
			Type:        n.GetType().String(),
			PhotoID:     n.GetPhotoId(),
			GalleryID:   n.GetGalleryId(),
			GalleryName: n.GetGalleryName(),
			UploaderID:  n.GetUploaderId(),
			Message:     n.GetMessage(),
			PhotoURL:    n.GetPhotoUrl(),
		}
		if occurredAt := n.GetOccurredAt(); occurredAt != nil {
			dto.OccurredAt = occurredAt.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		body, err := json.Marshal(dto)
		if err != nil {
			return false
		}

		c.SSEvent("notification", string(body))
		return true
	})
}
