import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { loadGallery } from "../actions/galleryDetailsActions";
import { galleryDetailsStore } from "../stores/GalleryDetailsStore";
import UploadForm from "../components/UploadForm";
import PhotoGrid from "../components/PhotoGrid";
import ModeratorAlertForm from "../components/ModeratorAlertForm";

export default function GalleryDetailsPage() {
    const { galleryId } = useParams<{ galleryId: string; }>();

    const [state, setState] = useState(galleryDetailsStore.getState());

    useEffect(() => {
        const unsubscribe = galleryDetailsStore.subscribe(() => {
            setState(galleryDetailsStore.getState());
        });

        if (galleryId) {
            loadGallery(galleryId);
        }

        return unsubscribe;
    }, [galleryId]);

    if (!galleryId) {
        return <p>Invalid gallery ID.</p>;
    }

    if (state.loading) {
        return <p>Loading gallery...</p>;
    }

    if (state.error) {
        return (
            <main>
                <Link to="/galleries">
                    ← Back to galleries
                </Link>

                <p>Error: {state.error}</p>
            </main>
        );
    }

    if (!state.gallery) {
        return (
            <main>
                <Link to="/galleries">
                    ← Back to galleries
                </Link>

                <p>Gallery not found.</p>
            </main>
        );
    }

    const { gallery } = state;

    return (
        <main>
            <Link to="/galleries">
                ← Back to galleries
            </Link>

            <h1>{gallery.name}</h1>
            <ModeratorAlertForm galleryId={galleryId} galleryModeratorId={gallery.moderatorId} />
            <p>{gallery.description}</p>

            <p>
                Status: {gallery.status}
            </p>

            <p>
                Created:{" "}
                {new Date(gallery.createdAt).toLocaleString()}
            </p>
            {gallery.status === "GALLERY_STATUS_OPEN" ? (
                <><UploadForm galleryId={galleryId}/><PhotoGrid galleryId={galleryId}/></>
            ) : (
                <p>This gallery is closed. Uploads are disabled.</p>
            )}
        </main>
    );
}