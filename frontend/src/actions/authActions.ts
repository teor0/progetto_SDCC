import { dispatcher } from "../stores/Dispatcher";
import { userApi } from "../services/userApi";
import type { Role } from "../types/auth";

export async function login(email: string, password: string): Promise<void> {
    dispatcher.dispatch({
        type: "AUTH_LOGIN_START",
    });

    try {
        const response = await userApi.login({
            email,
            password,
        });

        dispatcher.dispatch({
            type: "AUTH_LOGIN_SUCCESS",
            payload: response,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "AUTH_LOGIN_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Login failed",
        });
    }
}

export async function register(email: string, password: string, role: Role): Promise<void> {
    dispatcher.dispatch({
        type: "AUTH_REGISTER_START",
    });

    try {
        const response = await userApi.register({
            email,
            password,
            role,
        });

        dispatcher.dispatch({
            type: "AUTH_LOGIN_SUCCESS",
            payload: response,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "AUTH_REGISTER_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Registration failed",
        });
    }
}

export function logout(): void {
    dispatcher.dispatch({
        type: "AUTH_LOGOUT",
    });
}