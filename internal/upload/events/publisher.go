// Package events publishes upload notifications to RabbitMQ for the
// Notification Service to fan out to gallery members.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"photogallery/internal/upload"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchange       = "gallery.events"
	routingPhoto   = "gallery.photo_uploaded" // matches Gallery Service's dot-separated convention
	url            = "amqp://guest:guest@localhost:5672/"
	maxFailures    = 3
	publishTimeout = 30 * time.Second
)

// envelope mirrors the wire shape Gallery Service's command.Envelope uses
// (internal/gallery/command/events.go). It's intentionally a separate,
// duplicated type rather than an import of that package: services should
// stay independently buildable/deployable, and this struct is small enough
// that keeping the JSON *shape* in sync is simpler than sharing the Go type
// across service boundaries. If a third producer needs this, promote it to
// a small shared package instead of duplicating a third time.
type envelope struct {
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// UploadEvent is the payload published whenever a photo finishes
// uploading and is durably stored in MinIO.
type UploadEvent struct {
	PhotoID     string    `json:"photo_id"`
	GalleryID   string    `json:"gallery_id"`
	UploaderID  string    `json:"uploader_id"`
	StorageKey  string    `json:"storage_key"`
	PhotoURL    string    `json:"photo_url"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	MemberIDs   []string  `json:"member_ids"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

// Notifier is the subset of Publisher's behavior that Server depends on.
type Notifier interface {
	PublishPhoto(ctx context.Context, event *UploadEvent)
}

type Publisher struct {
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	breaker *upload.CircuitBreaker
}

func NewPublisher() (*Publisher, error) {
	p := &Publisher{
		breaker: upload.NewCircuitBreaker(maxFailures, publishTimeout),
	}
	if err := p.connect(); err != nil {
		// Non-fatal: we can still serve uploads, just without notifications.
		log.Printf("Publisher: initial RabbitMQ connection failed (%v) — events will be dropped until recovery", err)
	}
	return p, nil
}

// connect (re)establishes the AMQP connection and channel.
// Must be called with p.mu held or before the publisher is shared.
func (p *Publisher) connect() error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		exchange, "topic",
		true, false, false, false, nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("exchange declare: %w", err)
	}

	p.mu.Lock()
	oldConn := p.conn
	oldCh := p.channel
	p.conn = conn
	p.channel = ch
	p.mu.Unlock()

	if oldCh != nil {
		_ = oldCh.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}

	return nil
}

func (p *Publisher) Close() error {
	if err := p.channel.Close(); err != nil {
		return err
	}
	return p.conn.Close()
}

func (p *Publisher) PublishPhoto(ctx context.Context, event *UploadEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("Publisher: marshal error: %v", err)
		return
	}

	env := envelope{
		EventType: "PhotoUploaded",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		log.Printf("Publisher: marshal envelope error: %v", err)
		return
	}

	err = p.breaker.Call(func() error {
		return p.publish(ctx, routingPhoto, body)
	})

	switch err {
	case nil:
		log.Printf("Publisher: published photo.uploaded gallery=%s photo=%s cb=%s",
			event.GalleryID, event.PhotoID, p.breaker.State())
	case upload.ErrCircuitOpen:
		log.Printf("Publisher: circuit OPEN — dropping photo.uploaded event gallery=%s photo=%s",
			event.GalleryID, event.PhotoID)
	default:
		log.Printf("Publisher: publish failed (%v) cb=%s", err, p.breaker.State())
	}
}

// publish sends a single message to the topic exchange.
// It attempts one reconnect if the channel is closed.
func (p *Publisher) publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.tryPublish(ctx, routingKey, body)
	if err != nil {
		// Channel may have been closed — attempt one reconnect.
		log.Printf("Publisher: publish error (%v) — reconnecting", err)
		if reconnErr := p.connect(); reconnErr != nil {
			return fmt.Errorf("reconnect: %w", reconnErr)
		}
		return p.tryPublish(ctx, routingKey, body)
	}
	return nil
}

func (p *Publisher) tryPublish(ctx context.Context, routingKey string, body []byte) error {
	if p.channel == nil {
		return fmt.Errorf("channel is nil")
	}
	return p.channel.PublishWithContext(ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}
