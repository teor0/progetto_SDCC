const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

class ApiClient {
    private readonly baseUrl: string;

    constructor(baseUrl: string) {
        this.baseUrl = baseUrl;
    }

    async request<T>(
        path: string,
        options: RequestInit = {}
    ): Promise<T> {
        const token = localStorage.getItem("access_token");

        const headers = new Headers(options.headers);

        headers.set("Content-Type", "application/json");

        if (token) {
            headers.set("Authorization", `Bearer ${token}`);
        }

        const response = await fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers,
        });

        if (!response.ok) {
            let message = `Request failed with status ${response.status}`;

            try {
                const body = await response.json();

                if (body.message) {
                    message = body.message;
                } else if (body.error) {
                    message = body.error;
                }
            } catch {
                // Response wasn't JSON.
            }

            throw new Error(message);
        }

        if (response.status === 204) {
            return undefined as T;
        }

        const text = await response.text();

        if (!text) {
            return undefined as T;
        }

        return JSON.parse(text) as T;
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