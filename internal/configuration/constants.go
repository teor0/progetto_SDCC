package configuration

import "time"

// ExchangeName is the topic exchange all gallery domain events are published to.
// The Notification Service binds queues to this exchange with routing keys
// matching the event types it cares about (e.g. "gallery.moderator_alert").
const (
	ExchangeName   = "gallery.events"
	RoutingPhoto   = "gallery.photo_uploaded"
	MaxFailures    = 3
	PublishTimeout = 30 * time.Second
	// QueueName is durable and explicitly named rather than an auto-generated
	// exclusive queue, so that: (a) events published while this service is
	// down are still delivered on restart instead of being dropped, and
	// (b) if this is ever scaled to more than one replica, instances compete
	// for the same queue (work-queue fan-out) instead of each getting its own
	// duplicate copy of every event.
	QueueName  = "notification.events"
	BindingKey = "gallery.#"
	TokenTTL   = 1 * time.Hour
)
