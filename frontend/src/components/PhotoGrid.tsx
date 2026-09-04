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
                <a
                    key={photo.photoId}
                    href={photo.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="photo-link"
                >
                    <img
                        src={photo.url}
                        alt="Gallery photo"
                        loading="lazy"
                    />
                </a>
            ))}
        </div>
    );
}