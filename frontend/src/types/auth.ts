export type Role =
    | "ROLE_USER"
    | "ROLE_MODERATOR";

export type RegisterRequest = {
    email: string;
    password: string;
    role: Role;
};
export type TokenResponse = {
    token: string;
    expiresIn: number;
};

export type AuthState = {
    token: string | null;
    expiresAt: number | null;
    userId: string | null;
    role: Role | null;
    loading: boolean;
    error: string | null;
};

export type JwtClaims = {
    user_id: string;
    role: Role;
    exp: number;
};