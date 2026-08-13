import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { register } from "../actions/authActions";
import { authStore } from "../stores/AuthStore";
import type { Role } from "../types/auth";

export default function RegisterPage() {
    const navigate = useNavigate();

    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");

    const [role, setRole] =
        useState<Role>("ROLE_USER");

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

        await register(
            email,
            password,
            role
        );

        if (authStore.isAuthenticated()) {
            navigate("/galleries");
        }
    }

    return (
        <main>
            <h1>PhotoGallery</h1>

            <h2>Create account</h2>

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

                <fieldset>
                    <legend>Account type</legend>

                    <label>
                        <input
                            type="radio"
                            name="role"
                            value="ROLE_USER"
                            checked={role === "ROLE_USER"}
                            onChange={() =>
                                setRole("ROLE_USER")
                            }
                        />

                        User
                    </label>

                    <label>
                        <input
                            type="radio"
                            name="role"
                            value="ROLE_MODERATOR"
                            checked={role === "ROLE_MODERATOR"}
                            onChange={() =>
                                setRole("ROLE_MODERATOR")
                            }
                        />

                        Moderator
                    </label>
                </fieldset>

                {auth.error && (
                    <p>{auth.error}</p>
                )}

                <button
                    type="submit"
                    disabled={auth.loading}
                >
                    {auth.loading
                        ? "Creating account..."
                        : "Register"}
                </button>
            </form>

            <p>
                Already have an account?{" "}
                <Link to="/login">
                    Login
                </Link>
            </p>
        </main>
    );
}