import { useEffect, useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { galleryStore } from "../stores/GalleryStore";
import {
    loadGalleries,
    createGallery,
    joinGallery,
    leaveGallery,
    deleteGallery,
} from "../actions/galleryActions";
import { authStore } from "../stores/AuthStore";
import { logout } from "../actions/authActions.ts";
import type { Gallery } from "../types/gallery";
import "./Galleries.css";

export default function GalleriesPage() {
    const [state, setState] = useState(galleryStore.getState());
    const navigate = useNavigate();
    const auth = authStore.getState();
    const [newGalleryName, setNewGalleryName] = useState("");
    const [newGalleryDescription, setNewGalleryDescription] = useState("");

    useEffect(() => {
        const unsubscribe = galleryStore.subscribe(() => {
            setState(galleryStore.getState());
        });

        loadGalleries();
        const intervalId = setInterval(loadGalleries, 15000);

        return () => {
            unsubscribe();
            clearInterval(intervalId);
        };
    }, []);

    function handleCreateGallery() {
        const name = newGalleryName.trim();
        const description = newGalleryDescription.trim();

        if (!name) {
            return;
        }

        createGallery(name, description);
        setNewGalleryName("");
        setNewGalleryDescription("");
    }

    function handleDelete(gallery: Gallery) {
        if (window.confirm(`Delete "${gallery.name}"? This cannot be undone from the app.`)) {
            deleteGallery(gallery.id);
        }
    }

    function handleLogout() {
        logout();
        navigate("/login");
    }

    return (
        <main className="galleries-page">
            <div className="galleries-shell">
                <header className="galleries-header">
                    <div>
                        <p className="galleries-eyebrow">Photo Gallery</p>
                        <h1>Galleries</h1>
                        <p className="galleries-subtitle">
                            Browse your galleries, join new ones, and share photos with other members.
                        </p>
                    </div>

                    <button className="gallery-button gallery-button-secondary" onClick={handleLogout}>
                        Logout
                    </button>
                </header>

                {auth.role === "ROLE_MODERATOR" && (
                    <section className="gallery-panel create-gallery-panel">
                        <div className="section-heading">
                            <div>
                                <h2>Create a gallery</h2>
                                <p>Start a new space for members to join and share photos.</p>
                            </div>
                        </div>

                        <div className="create-gallery-form">
                            <input
                                type="text"
                                placeholder="Gallery name"
                                value={newGalleryName}
                                onChange={(event) => setNewGalleryName(event.target.value)}
                            />
                            <textarea
                                placeholder="Gallery description"
                                value={newGalleryDescription}
                                onChange={(event) => setNewGalleryDescription(event.target.value)}
                            />
                            <button className="gallery-button" onClick={handleCreateGallery}>
                                Create Gallery
                            </button>
                        </div>
                    </section>
                )}

                {state.error && <p className="gallery-message gallery-error">Error: {state.error}</p>}
                {state.loading && <p className="gallery-message">Loading galleries...</p>}

                <section className="gallery-section">
                    <div className="section-heading">
                        <div>
                            <h2>My Galleries</h2>
                            <p>Galleries you are currently a member of.</p>
                        </div>
                    </div>

                    {!state.loading && state.myGalleries.length === 0 && (
                        <div className="gallery-empty-state">
                            You haven't joined any galleries yet.
                        </div>
                    )}

                    <div className="gallery-grid">
                        {state.myGalleries.map((gallery) => (
                            <GalleryCard
                                key={gallery.id}
                                gallery={gallery}
                                actionLabel="Leave"
                                onAction={() => leaveGallery(gallery.id)}
                                canDelete={auth.userId === gallery.moderatorId}
                                onDelete={() => handleDelete(gallery)}
                            />
                        ))}
                    </div>
                </section>

                <section className="gallery-section">
                    <div className="section-heading">
                        <div>
                            <h2>Available Galleries</h2>
                            <p>Open galleries you can join.</p>
                        </div>
                    </div>

                    {!state.loading && state.availableGalleries.length === 0 && (
                        <div className="gallery-empty-state">
                            No other open galleries right now.
                        </div>
                    )}

                    <div className="gallery-grid">
                        {state.availableGalleries.map((gallery) => (
                            <GalleryCard
                                key={gallery.id}
                                gallery={gallery}
                                actionLabel="Join"
                                onAction={() => joinGallery(gallery.id)}
                                canDelete={auth.userId === gallery.moderatorId}
                                onDelete={() => handleDelete(gallery)}
                            />
                        ))}
                    </div>
                </section>
            </div>
        </main>
    );
}

function GalleryCard({
                         gallery,
                         actionLabel,
                         onAction,
                         canDelete,
                         onDelete,
                     }: {
    gallery: Gallery;
    actionLabel: string;
    onAction: () => void;
    canDelete: boolean;
    onDelete: () => void;
}) {
    return (
        <article className="gallery-card">
            <div className="gallery-card-content">
                <h3>
                    <Link to={`/galleries/${gallery.id}`}>{gallery.name}</Link>
                </h3>
                <p>{gallery.description || "No description provided."}</p>
            </div>

            <div className="gallery-card-actions">
                <button className="gallery-button gallery-button-secondary" onClick={onAction}>
                    {actionLabel}
                </button>
                {canDelete && (
                    <button className="gallery-button gallery-button-danger" onClick={onDelete}>
                        Delete
                    </button>
                )}
            </div>
        </article>
    );
}
