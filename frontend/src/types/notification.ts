export type NotificationType =
    | "NOTIFICATION_TYPE_UNSPECIFIED"
    | "NOTIFICATION_TYPE_PHOTO_UPLOADED"
    | "NOTIFICATION_TYPE_MODERATOR_ALERT";

// Matches internal/handlers/notification.go's notificationDTO. galleryName
// is never actually populated server-side (Consumer.buildNotification
// doesn't set it) and photoUrl points at MinIO's internal Docker hostname,
// not a browser-reachable one -- this UI deliberately ignores both rather
// than rendering something that's always empty or always broken.
export type NotificationEvent = {
    id: string;
    type: NotificationType;
    photoId?: string;
    galleryId: string;
    galleryName?: string;
    uploaderId?: string;
    message?: string;
    photoUrl?: string;
    occurredAt?: string;
};