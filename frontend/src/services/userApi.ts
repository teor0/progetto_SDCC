import { api } from "./api";
import type { TokenResponse } from "../types/auth";

export type LoginRequest = {
    email: string;
    password: string;
};

export type RegisterRequest = {
    email: string;
    password: string;
    role: number;
};

export type InfoResponse = {
    userId: string;
};

export const userApi = {
    async login(request: LoginRequest): Promise<TokenResponse> {
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

    async getCurrentUser(): Promise<InfoResponse> {
        return api.get<InfoResponse>(
            "/photogallery/auth/me"
        );
    },
};