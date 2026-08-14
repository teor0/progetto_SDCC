import { dispatcher } from "../stores/Dispatcher";
import { subscribeToNotifications, type StreamStatus } from "../services/notificationStream";
import type { NotificationEvent } from "../types/notification";

let unsubscribe: (() => void) | null = null;

// Guards against double-connecting if the feed mounts twice (e.g. React
// StrictMode's dev-only double-invoke of effects).
export function startNotificationStream(token: string): void {
    if (unsubscribe) {
        return;
    }

    unsubscribe = subscribeToNotifications(token, {
        onNotification: (notification: NotificationEvent) => {
            dispatcher.dispatch({ type: "NOTIFICATION_RECEIVED", payload: notification });
        },
        onStatusChange: (status: StreamStatus) => {
            dispatcher.dispatch({ type: "NOTIFICATION_STATUS", payload: status });
        },
    });
}

export function stopNotificationStream(): void {
    if (unsubscribe) {
        unsubscribe();
        unsubscribe = null;
    }
}