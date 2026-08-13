import { useEffect, useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { galleryStore } from "../stores/GalleryStore";
import {
    loadGalleries,
    createGallery,
    leaveGallery,
} from "../actions/galleryActions";
import { authStore } from "../stores/AuthStore";
import {logout} from "../actions/authActions.ts";

export default function GalleriesPage() {
    //all these const are components of the pages
    const [state, setState] = useState(galleryStore.getState());
    const navigate = useNavigate();
    const auth = authStore.getState();
    const [newGalleryName, setNewGalleryName] = useState("");
    const [newGalleryDescription, setNewGalleryDescription] = useState("");

    useEffect(() => {
        const unsubscribe = galleryStore.subscribe(() => {
            setState(galleryStore.getState());
        });

        loadGalleries(true);

        return unsubscribe;
    }, []);

    //handle function connected to action
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

    //function that is also the name in html
    function handleLogout() {
        logout();
        navigate("/login");
    }

    return (
        <main>
            <h1>My Galleries</h1>
            <button onClick={handleLogout}>
                Logout
            </button>

            {auth.role === "ROLE_MODERATOR" &&
                (
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
                    <button onClick={handleCreateGallery}>
                    Create Gallery
                    </button>
            </section>
                )}

            {state.loading && <p>Loading galleries...</p>}

            {state.error && (
                <p>
                    Error: {state.error}
                </p>
            )}

            {!state.loading && state.galleries.length === 0 && (
                <p>No galleries found.</p>
            )}

            <section>
                {state.galleries.map((gallery) => (
                    <article key={gallery.id}>
                        <h2>
                            <Link to={`/galleries/${gallery.id}`}>
                                {gallery.name}
                            </Link>
                        </h2>

                        <p>ID: {gallery.id}</p>

                        <button
                            onClick={() => leaveGallery(gallery.id)}
                        >
                            Remove
                        </button>
                    </article>
                ))}
            </section>
        </main>
    );
}