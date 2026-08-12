import { useEffect, useState } from "react";
import { login } from "../actions/authActions";
import { authStore } from "../stores/AuthStore";

export default function LoginPage() {
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");

    const [auth, setAuth] = useState(
        authStore.getState()
    );

    useEffect(() => {
        return authStore.subscribe(() => {
            setAuth(authStore.getState());
        });
    }, []);

    async function handleSubmit(
        event: React.FormEvent
    ) {
        event.preventDefault();

        await login(email, password);
    }

    return (
        <main>
            <h1>PhotoGallery</h1>

            <h2>Login</h2>

            <form onSubmit={handleSubmit}>
                <div>
                    <label htmlFor="email">
                        Email
                    </label>

                    <input
                        id="email"
                        type="email"
                        value={email}
                        onChange={(event) =>
                            setEmail(event.target.value)
                        }
                        required
                    />
                </div>

                <div>
                    <label htmlFor="password">
                        Password
                    </label>

                    <input
                        id="password"
                        type="password"
                        value={password}
                        onChange={(event) =>
                            setPassword(event.target.value)
                        }
                        required
                    />
                </div>

                {auth.error && (
                    <p>
                        {auth.error}
                    </p>
                )}

                <button
                    type="submit"
                    disabled={auth.loading}
                >
                    {auth.loading ? "Logging in..." : "Login"}
                </button>
            </form>
        </main>
    );
}