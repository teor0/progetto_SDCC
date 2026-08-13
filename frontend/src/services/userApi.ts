import { api } from "./api";
import type {
    RegisterRequest,
    TokenResponse,
} from "../types/auth";

export type LoginRequest = {
    email: string;
    password: string;
};

export const userApi = {
    async login(
        request: LoginRequest
    ): Promise<TokenResponse> {
        return api.post<TokenResponse>(
            "/photogallery/auth/login",
            request
        );
    },

    async register(
        request: RegisterRequest
    ): Promise<TokenResponse> {
        return api.post<TokenResponse>(
            "/photogallery/auth/register",
            request
        );
    },

    async getCurrentUser(): Promise<{
        userId: string;
    }> {
        return api.get(
            "/photogallery/auth/me"
        );
    },
};