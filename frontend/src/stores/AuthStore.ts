import { dispatcher, type Action } from "./Dispatcher";
import type { AuthState } from "../types/auth";

const TOKEN_KEY = "access_token";
const EXPIRY_KEY = "access_token_expires_at";

class AuthStore {

    private state: AuthState = {
        token: localStorage.getItem(TOKEN_KEY),
        expiresAt: this.loadExpiry(),
        userId: null,
        loading: false,
        error: null,
    };

    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): AuthState {
        return this.state;
    }

    isAuthenticated(): boolean {
        if (!this.state.token || !this.state.expiresAt) {
            return false;
        }

        return Date.now() < this.state.expiresAt;
    }

    subscribe(listener: () => void): () => void {
        this.listeners.push(listener);

        return () => {
            this.listeners = this.listeners.filter(
                (registered) => registered !== listener
            );
        };
    }

    private loadExpiry(): number | null {
        const value = localStorage.getItem(EXPIRY_KEY);

        if (!value) {
            return null;
        }

        const expiry = Number(value);

        return Number.isFinite(expiry) ? expiry : null;
    }

    private emitChange(): void {
        for (const listener of this.listeners) {
            listener();
        }
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "AUTH_LOGIN_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;
            case "AUTH_LOGIN_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string,
                };

                this.emitChange();
                break;


            case "AUTH_LOGIN_SUCCESS": {
                const payload = action.payload as {
                    token: string;
                    expiresIn: number;
                };

                const expiresAt =
                    Date.now() + payload.expiresIn * 1000;

                localStorage.setItem(
                    TOKEN_KEY,
                    payload.token
                );

                localStorage.setItem(
                    EXPIRY_KEY,
                    String(expiresAt)
                );

                this.state = {
                    token: payload.token,
                    expiresAt,
                    userId: null,
                    loading: false,
                    error: null,
                };

                this.emitChange();
                break;
            }

            case "AUTH_USER_LOADED":
                this.state = {
                    ...this.state,
                    userId: action.payload as string,
                };

                this.emitChange();
                break;

            case "AUTH_LOGOUT":
                localStorage.removeItem(TOKEN_KEY);
                localStorage.removeItem(EXPIRY_KEY);
                this.state = {
                    token: null,
                    expiresAt: null,
                    userId: null,
                    loading: false,
                    error: null,
                };

                this.emitChange();
                break;
        }
    }
}

export const authStore = new AuthStore();