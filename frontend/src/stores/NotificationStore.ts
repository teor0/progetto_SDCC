import { dispatcher, type Action } from "./Dispatcher";
import type { NotificationEvent } from "../types/notification";
import type { StreamStatus } from "../services/notificationStream";

const MAX_NOTIFICATIONS = 30;

type NotificationState = {
    notifications: NotificationEvent[];
    status: StreamStatus | "idle";
};

class NotificationStore {
    private state: NotificationState = { notifications: [], status: "idle" };
    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): NotificationState {
        return this.state;
    }

    subscribe(listener: () => void): () => void {
        this.listeners.push(listener);
        return () => {
            this.listeners = this.listeners.filter((l) => l !== listener);
        };
    }

    private emitChange(): void {
        for (const listener of this.listeners) listener();
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "NOTIFICATION_RECEIVED": {
                const notification = action.payload as NotificationEvent;
                // Most recent first, capped so a long session doesn't grow
                // this list -- and the DOM it renders -- without bound.
                const notifications = [notification, ...this.state.notifications].slice(
                    0,
                    MAX_NOTIFICATIONS
                );
                this.state = { ...this.state, notifications };
                this.emitChange();
                break;
            }

            case "NOTIFICATION_STATUS":
                this.state = { ...this.state, status: action.payload as StreamStatus };
                this.emitChange();
                break;
        }
    }
}

export const notificationStore = new NotificationStore();