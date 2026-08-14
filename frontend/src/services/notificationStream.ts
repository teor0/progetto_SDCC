import type { NotificationEvent } from "../types/notification";

const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

export type StreamStatus = "connecting" | "open" | "closed" | "error";

type Handlers = {
    onNotification: (event: NotificationEvent) => void;
    onStatusChange: (status: StreamStatus) => void;
};

// Reads the SSE stream manually via fetch's ReadableStream rather than
// EventSource, since EventSource can't attach an Authorization header and
// this endpoint requires one. Reconnects with exponential backoff per the
// API's own doc comment ("Clients should reconnect with exponential
// back-off on stream termination").
export function subscribeToNotifications(token: string, handlers: Handlers): () => void {
    const controller = new AbortController();
    let stopped = false;
    let backoff = INITIAL_BACKOFF_MS;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    async function connect() {
        if (stopped) return;

        handlers.onStatusChange("connecting");

        try {
            const response = await fetch(`${API_BASE_URL}/api/notifications/stream`, {
                headers: { Authorization: `Bearer ${token}` },
                signal: controller.signal,
            });

            if (!response.ok || !response.body) {
                throw new Error(`stream request failed with status ${response.status}`);
            }

            handlers.onStatusChange("open");
            backoff = INITIAL_BACKOFF_MS; // reset after a successful connect

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";

            while (!stopped) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });

                // SSE events are separated by a blank line.
                let boundary = buffer.indexOf("\n\n");
                while (boundary !== -1) {
                    handleRawEvent(buffer.slice(0, boundary), handlers);
                    buffer = buffer.slice(boundary + 2);
                    boundary = buffer.indexOf("\n\n");
                }
            }
        } catch {
            if (stopped || controller.signal.aborted) {
                return;
            }
            handlers.onStatusChange("error");
        }

        if (stopped) return;

        handlers.onStatusChange("closed");
        reconnectTimer = setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }

    connect();

    return () => {
        stopped = true;
        controller.abort();
        if (reconnectTimer) clearTimeout(reconnectTimer);
    };
}

function handleRawEvent(raw: string, handlers: Handlers) {
    let eventName = "message";
    const dataLines: string[] = [];

    for (const line of raw.split("\n")) {
        if (line.startsWith("event:")) {
            eventName = line.slice("event:".length).trim();
        } else if (line.startsWith("data:")) {
            dataLines.push(line.slice("data:".length).trim());
        }
    }

    if (eventName !== "notification" || dataLines.length === 0) {
        return;
    }

    try {
        handlers.onNotification(JSON.parse(dataLines.join("\n")) as NotificationEvent);
    } catch {
        // Malformed payload -- drop it rather than crash the stream.
    }
}