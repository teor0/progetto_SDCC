import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { notificationStore } from "../stores/NotificationStore";
import { startNotificationStream, stopNotificationStream } from "../actions/notificationActions";
import { authStore } from "../stores/AuthStore";
import type { NotificationEvent } from "../types/notification";

function describe(notification: NotificationEvent): string {
    switch (notification.type) {
        case "NOTIFICATION_TYPE_PHOTO_UPLOADED":
            return notification.message || "A new photo was uploaded.";
        case "NOTIFICATION_TYPE_MODERATOR_ALERT":
            return notification.message || "Moderator alert.";
        default:
            return notification.message || "New activity.";
    }
}

export default function NotificationFeed() {
    const [state, setState] = useState(notificationStore.getState());

    useEffect(() => {
        const unsubscribe = notificationStore.subscribe(() => {
            setState(notificationStore.getState());
        });

        const token = authStore.getState().token;
        if (token) {
            startNotificationStream(token);
        }

        return () => {
            unsubscribe();
            stopNotificationStream();
        };
    }, []);

    return (
        <aside className="notification-feed">
            <h4>
                Notifications <span className={`status-dot status-${state.status}`} title={state.status} />
            </h4>

            {state.notifications.length === 0 && <p>No notifications yet.</p>}

            <ul>
                {state.notifications.map((n) => (
                    <li key={n.id}>
                        <Link to={`/galleries/${n.galleryId}`}>{describe(n)}</Link>
                        {n.occurredAt && (
                            <time dateTime={n.occurredAt}>
                                {" "}
                                {new Date(n.occurredAt).toLocaleTimeString()}
                            </time>
                        )}
                    </li>
                ))}
            </ul>
        </aside>
    );
}