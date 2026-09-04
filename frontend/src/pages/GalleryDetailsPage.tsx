import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { loadGallery } from "../actions/galleryDetailsActions";
import { galleryDetailsStore } from "../stores/GalleryDetailsStore";
import UploadForm from "../components/UploadForm";
import PhotoGrid from "../components/PhotoGrid";
import ModeratorAlertForm from "../components/ModeratorAlertForm";
import "./GalleryDetails.css";

export default function GalleryDetailsPage() {
    const { galleryId } = useParams<{ galleryId: string }>();
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
        return <p className="details-message">Invalid gallery ID.</p>;
    }

    if (state.loading) {
        return <p className="details-message">Loading gallery...</p>;
    }

    if (state.error) {
        return (
            <main className="gallery-details-page">
                <div className="gallery-details-shell">
                    <Link className="back-link" to="/galleries">← Back to galleries</Link>
                    <p className="details-message details-error">Error: {state.error}</p>
                </div>
            </main>
        );
    }

    if (!state.gallery) {
        return (
            <main className="gallery-details-page">
                <div className="gallery-details-shell">
                    <Link className="back-link" to="/galleries">← Back to galleries</Link>
                    <p className="details-message">Gallery not found.</p>
                </div>
            </main>
        );
    }

    const { gallery } = state;
    const isOpen = gallery.status === "GALLERY_STATUS_OPEN";

    return (
        <main className="gallery-details-page">
            <div className="gallery-details-shell">
                <Link className="back-link" to="/galleries">← Back to galleries</Link>

                <section className="gallery-hero">
                    <div className="gallery-hero-topline">
                        <span className={`gallery-status ${isOpen ? "open" : "closed"}`}>
                            {isOpen ? "Open" : "Closed"}
                        </span>
                        <span className="gallery-created">
                            Created {new Date(gallery.createdAt).toLocaleString()}
                        </span>
                    </div>

                    <h1>{gallery.name}</h1>
                    <p className="gallery-description">
                        {gallery.description || "No description provided."}
                    </p>
                </section>

                <div className="gallery-details-content">
                    <ModeratorAlertForm
                        galleryId={galleryId}
                        galleryModeratorId={gallery.moderatorId}
                    />

                    {isOpen ? (
                        <>
                            <section className="details-panel">
                                <div className="details-section-heading">
                                    <h2>Upload a photo</h2>
                                    <p>Add a new photo to this gallery.</p>
                                </div>
                                <UploadForm galleryId={galleryId} />
                            </section>

                            <section className="details-panel photo-section">
                                <div className="details-section-heading">
                                    <h2>Photos</h2>
                                    <p>Photos shared by gallery members.</p>
                                </div>
                                <PhotoGrid galleryId={galleryId} />
                            </section>
                        </>
                    ) : (
                        <div className="details-message">
                            This gallery is closed. Uploads are disabled.
                        </div>
                    )}
                </div>
            </div>
        </main>
    );
}
