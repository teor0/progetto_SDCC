import { useEffect, useState } from "react";
import { photoStore } from "../stores/PhotoStore";
import { loadUploads } from "../actions/uploadActions";
import './PhotoGrid.css';

type PhotoGridProps = {
    galleryId: string;
};

export default function PhotoGrid({ galleryId }: PhotoGridProps) {
    const [state, setState] = useState(photoStore.getState());

    useEffect(() => {
        const unsubscribe = photoStore.subscribe(() => {
            setState(photoStore.getState());
        });

        loadUploads(galleryId);

        return unsubscribe;
    }, [galleryId]);

    if (state.loading) {
        return <p>Loading photos...</p>;
    }

    if (state.error) {
        return <p>Error loading photos: {state.error}</p>;
    }

    if (state.photos.length === 0) {
        return <p>No photos yet.</p>;
    }

    return (
        <div className="photo-grid">
            {state.photos.map((photo) => (
                <img
                    key={photo.photoId}
                    src={photo.url}
                    alt="loading photo..."
                    loading="lazy"
                />
            ))}
        </div>
    );
}