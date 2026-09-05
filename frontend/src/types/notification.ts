export type NotificationType =
    | "NOTIFICATION_TYPE_UNSPECIFIED"
    | "NOTIFICATION_TYPE_PHOTO_UPLOADED"
    | "NOTIFICATION_TYPE_MODERATOR_ALERT"
    | "NOTIFICATION_TYPE_GALLERY_CLOSED";


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