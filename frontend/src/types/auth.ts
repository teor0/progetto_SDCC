export type TokenResponse = {
    token: string;
    expiresIn: number;
};

export type AuthState = {
    token: string | null;
    expiresAt: number | null;
    userId: string | null;
    loading: boolean;
    error: string | null;
};