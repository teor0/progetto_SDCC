package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeName is the topic exchange all gallery domain events are published to.
// The Notification Service binds queues to this exchange with routing keys
// matching the event types it cares about (e.g. "gallery.moderator_alert").
const ExchangeName = "gallery.events"

// Envelope wraps every published event with a consistent shape so consumers
// can deserialize the type first, then the payload, without guessing.
type Envelope struct {
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// RabbitMQPublisher implements EventPublisher by publishing JSON-encoded
// events to a topic exchange.
type RabbitMQPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewRabbitMQPublisher dials RabbitMQ, opens a channel, and declares the
// topic exchange (idempotent — safe to call on every service startup).
func NewRabbitMQPublisher(amqpURL string) (*RabbitMQPublisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	err = ch.ExchangeDeclare(
		ExchangeName, //name
		"topic",
		true,  // durable — survives broker restart
		false, // auto-deleted
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	return &RabbitMQPublisher{conn: conn, channel: ch}, nil
}

// Publish encodes payload as JSON and publishes it to ExchangeName with a
// routing key derived from eventType. Publish failures are logged and
// returned as errors — callers in service.go currently ignore the error
// (`_ = s.publisher.Publish(...)`), which is a deliberate choice: a
// notification failing to fire shouldn't roll back a successful gallery
// mutation.
func (p *RabbitMQPublisher) Publish(ctx context.Context, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	env := Envelope{
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		Payload:   body,
	}
	envBody, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	routingKey := routingKeyFor(eventType)

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.channel.PublishWithContext(publishCtx,
		ExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         envBody,
		},
	)
	if err != nil {
		log.Printf("publish event %s failed: %v", eventType, err)
		return fmt.Errorf("publish %s: %w", eventType, err)
	}
	return nil
}

// Close releases the channel and connection.
func (p *RabbitMQPublisher) Close() error {
	if err := p.channel.Close(); err != nil {
		return err
	}
	return p.conn.Close()
}

// routingKeyFor maps Go-style event type names to dot-separated routing keys.
func routingKeyFor(eventType string) string {
	switch eventType {
	case "GalleryCreated":
		return "gallery.created"
	case "GalleryClosed":
		return "gallery.closed"
	case "MemberAdded":
		return "gallery.member_added"
	case "GalleryDeleted":
		return "gallery.deleted"
	case "MemberRemoved":
		return "gallery.member_removed"
	case "ModeratorAlert":
		return "gallery.moderator_alert"
	default:
		return "gallery.unknown"
	}
}
