import { useEffect, useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { login } from "../actions/authActions";
import { authStore } from "../stores/AuthStore";
import "./Auth.css";

export default function LoginPage() {
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const navigate = useNavigate();
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

        await login(email, password);

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
                        Welcome back
                    </h1>

                    <p className="auth-subtitle">
                        Sign in to your PhotoGallery account
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
                            ? "Signing in..."
                            : "Sign in"}
                    </button>
                </form>

                <footer className="auth-footer">
                    Don't have an account?{" "}
                    <Link to="/register">
                        Create one
                    </Link>
                </footer>

            </section>
        </main>
    );
}