import { useEffect, useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { galleryStore } from "../stores/GalleryStore";
import {
    loadGalleries,
    createGallery,
    joinGallery,
    leaveGallery,
} from "../actions/galleryActions";
import { authStore } from "../stores/AuthStore";
import { logout } from "../actions/authActions.ts";
import type { Gallery } from "../types/gallery";

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

        // Refresh periodically to catch membership changes made by other
        // users/tabs -- there's no push channel for gallery membership today
        // (the notification stream only carries photo-upload/moderator-alert
        // events, not join/leave), so polling is the pragmatic fallback.
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

    function handleLogout() {
        logout();
        navigate("/login");
    }

    return (
        <main>
            <h1>Galleries</h1>
            <button onClick={handleLogout}>Logout</button>

            {auth.role === "ROLE_MODERATOR" && (
                <section>
                    <input
                        type="text"
                        placeholder="Gallery name"
                        value={newGalleryName}
                        onChange={(event) => setNewGalleryName(event.target.value)}
                    />
                    <textarea
                        placeholder="Gallery description"
                        value={newGalleryDescription}
                        onChange={(event) =>
                            setNewGalleryDescription(event.target.value)
                        }
                    />
                    <button onClick={handleCreateGallery}>Create Gallery</button>
                </section>
            )}

            {state.error && <p>Error: {state.error}</p>}
            {state.loading && <p>Loading galleries...</p>}

            <section>
                <h2>My Galleries</h2>

                {!state.loading && state.myGalleries.length === 0 && (
                    <p>You haven't joined any galleries yet.</p>
                )}

                {state.myGalleries.map((gallery) => (
                    <GalleryCard
                        key={gallery.id}
                        gallery={gallery}
                        actionLabel="Leave"
                        onAction={() => leaveGallery(gallery.id)}
                    />
                ))}
            </section>

            <section>
                <h2>Available Galleries</h2>

                {!state.loading && state.availableGalleries.length === 0 && (
                    <p>No other open galleries right now.</p>
                )}

                {state.availableGalleries.map((gallery) => (
                    <GalleryCard
                        key={gallery.id}
                        gallery={gallery}
                        actionLabel="Join"
                        onAction={() => joinGallery(gallery.id)}
                    />
                ))}
            </section>
        </main>
    );
}

function GalleryCard({
                         gallery,
                         actionLabel,
                         onAction,
                     }: {
    gallery: Gallery;
    actionLabel: string;
    onAction: () => void;
}) {
    return (
        <article>
            <h3>
                <Link to={`/galleries/${gallery.id}`}>{gallery.name}</Link>
            </h3>
            <p>{gallery.description}</p>
            <button onClick={onAction}>{actionLabel}</button>
        </article>
    );
}