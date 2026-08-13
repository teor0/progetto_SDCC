import { jwtDecode, type JwtPayload } from "jwt-decode";
import { dispatcher, type Action } from "./Dispatcher";
import type { AuthState, Role } from "../types/auth";

const TOKEN_KEY = "access_token";
const EXPIRY_KEY = "access_token_expires_at";

type TokenClaims = JwtPayload & {
    user_id: string;
    role: Role;
};

class AuthStore {
    private state: AuthState = this.loadInitialState();

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

    private loadInitialState(): AuthState {
        const token = localStorage.getItem(TOKEN_KEY);

        if (!token) {
            return this.emptyState();
        }

        try {
            const claims = jwtDecode<TokenClaims>(token);

            if (!claims.exp || !claims.user_id || !claims.role) {
                this.clearStoredAuth();
                return this.emptyState();
            }

            const expiresAt = claims.exp * 1000;

            if (Date.now() >= expiresAt) {
                this.clearStoredAuth();
                return this.emptyState();
            }

            localStorage.setItem(
                EXPIRY_KEY,
                String(expiresAt)
            );

            return {
                token,
                expiresAt,
                userId: claims.user_id,
                role: claims.role,
                loading: false,
                error: null,
            };
        } catch {
            this.clearStoredAuth();
            return this.emptyState();
        }
    }

    private emptyState(): AuthState {
        return {
            token: null,
            expiresAt: null,
            userId: null,
            role: null,
            loading: false,
            error: null,
        };
    }

    private clearStoredAuth(): void {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(EXPIRY_KEY);
    }

    private emitChange(): void {
        for (const listener of this.listeners) {
            listener();
        }
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "AUTH_REGISTER_START":
            case "AUTH_LOGIN_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;

            case "AUTH_REGISTER_FAILURE":
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

                try {
                    const claims =
                        jwtDecode<TokenClaims>(
                            payload.token
                        );

                    if (
                        !claims.exp ||
                        !claims.user_id ||
                        !claims.role
                    ) {
                        throw new Error(
                            "Invalid authentication token"
                        );
                    }

                    const expiresAt =
                        claims.exp * 1000;

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
                        userId: claims.user_id,
                        role: claims.role,
                        loading: false,
                        error: null,
                    };

                    this.emitChange();
                } catch (error) {
                    this.clearStoredAuth();

                    this.state = {
                        ...this.emptyState(),
                        error:
                            error instanceof Error
                                ? error.message
                                : "Invalid authentication token",
                    };

                    this.emitChange();
                }

                break;
            }

            case "AUTH_LOGOUT":
                this.clearStoredAuth();

                this.state = this.emptyState();

                this.emitChange();
                break;
        }
    }
}

export const authStore = new AuthStore();