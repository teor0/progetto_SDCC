import { useEffect, useState } from "react";
import { galleryStore } from "../stores/GalleryStore";
import {
    loadGalleries,
    createGallery,
    removeGallery,
} from "../actions/galleryActions";

export default function GalleriesPage() {
    const [state, setState] = useState(galleryStore.getState());
    const [newGalleryName, setNewGalleryName] = useState("");

    useEffect(() => {
        const unsubscribe = galleryStore.subscribe(() => {
            setState(galleryStore.getState());
        });

        loadGalleries();

        return unsubscribe;
    }, []);

    function handleCreateGallery() {
        const name = newGalleryName.trim();

        if (!name) {
            return;
        }

        createGallery(name);
        setNewGalleryName("");
    }

    return (
        <main>
            <h1>My Galleries</h1>

            <section>
                <input
                    type="text"
                    placeholder="Gallery name"
                    value={newGalleryName}
                    onChange={(event) => setNewGalleryName(event.target.value)}
                />

                <button onClick={handleCreateGallery}>
                    Create Gallery
                </button>
            </section>

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
                        <h2>{gallery.name}</h2>

                        <p>ID: {gallery.id}</p>

                        <button
                            onClick={() => removeGallery(gallery.id)}
                        >
                            Remove
                        </button>
                    </article>
                ))}
            </section>
        </main>
    );
}