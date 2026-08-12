import { dispatcher } from "../stores/Dispatcher";
import { userApi } from "../services/userApi";

export async function login(
    email: string,
    password: string
): Promise<void> {
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

        // Ask the backend who we are.
        const user = await userApi.getCurrentUser();

        dispatcher.dispatch({
            type: "AUTH_USER_LOADED",
            payload: user.userId,
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

export function logout(): void {
    dispatcher.dispatch({
        type: "AUTH_LOGOUT",
    });
}