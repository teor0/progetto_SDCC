import { dispatcher } from "../stores/Dispatcher";

const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

class ApiClient {
    private readonly baseUrl: string;
    private loggingOut = false; // guards against duplicate dispatch/redirect on concurrent 401s

    constructor(baseUrl: string) {
        this.baseUrl = baseUrl;
    }

    async request<T>(
        path: string,
        options: RequestInit = {}
    ): Promise<T> {
        const token = localStorage.getItem("access_token");

        const headers = new Headers(options.headers);

        // FormData bodies need the browser to set Content-Type itself, with
        // the multipart boundary it generates -- setting it manually here
        // would strip that boundary and break parsing server-side.
        const isFormData = options.body instanceof FormData;
        if (!isFormData) {
            headers.set("Content-Type", "application/json");
        }

        if (token) {
            headers.set("Authorization", `Bearer ${token}`);
        }

        const response = await fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers,
        });

        if (response.status === 401 && token) {
            this.handleSessionExpired();
            throw new Error("Session expired. Please log in again.");
        }

        if (!response.ok) {
            let message = `Request failed with status ${response.status}`;
            try {
                const body = await response.json();
                if (body.message) message = body.message;
                else if (body.error) message = body.error;
            } catch {
                // Response wasn't JSON.
            }
            throw new Error(message);
        }

        if (response.status === 204) return undefined as T;

        const text = await response.text();
        return text ? (JSON.parse(text) as T) : (undefined as T);
    }
    

    postForm<T>(path: string, formData: FormData): Promise<T> {
        return this.request<T>(path, {
            method: "POST",
            body: formData,
        });
    }

    private handleSessionExpired(): void {
        if (this.loggingOut) {
            return;
        }
        this.loggingOut = true;

        dispatcher.dispatch({ type: "AUTH_LOGOUT" });

        // Hard redirect rather than client-side navigation: this module
        // sits outside the React tree, and a full reload guarantees every
        // page resets to a clean, logged-out state regardless of whether
        // the current view happens to be subscribed to AuthStore.
        window.location.href = "/login";
    }

    get<T>(path: string): Promise<T> {
        return this.request<T>(path);
    }

    post<T>(path: string, body?: unknown): Promise<T> {
        return this.request<T>(path, {
            method: "POST",
            body: body !== undefined ? JSON.stringify(body) : undefined,
        });
    }

    delete<T>(path: string): Promise<T> {
        return this.request<T>(path, {
            method: "DELETE",
        });
    }
}

export const api = new ApiClient(API_BASE_URL);