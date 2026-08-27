import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { register } from "../actions/authActions";
import { authStore } from "../stores/AuthStore";
import type { Role } from "../types/auth";
import "./Auth.css";

export default function RegisterPage() {
    const navigate = useNavigate();

    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");

    const [role, setRole] =
        useState<Role>("ROLE_USER");

    const [auth, setAuth] = useState(
        authStore.getState()
    );
    const { loading, error } = auth;

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
        <main className="auth-page">
            <section className="auth-card">

                <header className="auth-header">
                    <div className="auth-logo">
                        P
                    </div>

                    <h1 className="auth-title">
                        Create your account
                    </h1>

                    <p className="auth-subtitle">
                        Join PhotoGallery and start sharing
                    </p>
                </header>

                <form
                    className="auth-form"
                    onSubmit={handleSubmit}
                >
                    <div className="auth-field">
                        <label htmlFor="email">
                            Email
                        </label>

                        <input
                            id="email"
                            type="email"
                            value={email}
                            onChange={(e) =>
                                setEmail(e.target.value)
                            }
                            placeholder="you@example.com"
                            required
                        />
                    </div>

                    <div className="auth-field">
                        <label htmlFor="password">
                            Password
                        </label>

                        <input
                            id="password"
                            type="password"
                            value={password}
                            onChange={(e) =>
                                setPassword(e.target.value)
                            }
                            placeholder="••••••••"
                            required
                        />
                    </div>

                    <div className="auth-role">
                        <label>
                            Account type
                        </label>

                        <div className="auth-role-options">

                            <button
                                type="button"
                                className={`auth-role-option ${
                                    role === "ROLE_USER"
                                        ? "selected"
                                        : ""
                                }`}
                                onClick={() =>
                                    setRole("ROLE_USER")
                                }
                            >
                                <strong>
                                    User
                                </strong>

                                <span>
                                    Join galleries and
                                    upload photos
                                </span>
                            </button>

                            <button
                                type="button"
                                className={`auth-role-option ${
                                    role === "ROLE_MODERATOR"
                                        ? "selected"
                                        : ""
                                }`}
                                onClick={() =>
                                    setRole(
                                        "ROLE_MODERATOR"
                                    )
                                }
                            >
                                <strong>
                                    Moderator
                                </strong>

                                <span>
                                    Create and manage
                                    galleries
                                </span>
                            </button>

                        </div>
                    </div>

                    {error && (
                        <div className="auth-error">
                            {error}
                        </div>
                    )}

                    <button
                        className="auth-button"
                        type="submit"
                        disabled={loading}
                    >
                        {loading
                            ? "Creating account..."
                            : "Create account"}
                    </button>
                </form>

                <footer className="auth-footer">
                    Already have an account?{" "}
                    <Link to="/login">
                        Sign in
                    </Link>
                </footer>

            </section>
        </main>
    );
}