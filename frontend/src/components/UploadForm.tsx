import { useEffect, useRef, useState } from "react";
import { uploadStore } from "../stores/UploadStore";
import { uploadPhoto } from "../actions/uploadActions";

type UploadFormProps = {
    galleryId: string;
    disabled?: boolean;
};

export default function UploadForm({ galleryId, disabled }: UploadFormProps) {
    const [state, setState] = useState(uploadStore.getState());
    const [selectedFile, setSelectedFile] = useState<File | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        return uploadStore.subscribe(() => {
            setState(uploadStore.getState());
        });
    }, []);

    // Clear the file input after a successful upload so the form is ready
    // for the next photo instead of showing a stale selected filename.
    useEffect(() => {
        if (state.lastPhotoId && !state.uploading) {
            setSelectedFile(null);
            if (fileInputRef.current) {
                fileInputRef.current.value = "";
            }
        }
    }, [state.lastPhotoId, state.uploading]);

    function handleFileChange(event: React.ChangeEvent<HTMLInputElement>) {
        setSelectedFile(event.target.files?.[0] ?? null);
    }

    function handleUpload() {
        if (!selectedFile) {
            return;
        }
        uploadPhoto(galleryId, selectedFile);
    }

    return (
        <section>
            <h3>Upload a photo</h3>

            <input
                ref={fileInputRef}
                type="file"
                accept="image/*"
                onChange={handleFileChange}
                disabled={disabled || state.uploading}
            />

            <button
                onClick={handleUpload}
                disabled={disabled || state.uploading || !selectedFile}
            >
                {state.uploading ? "Uploading..." : "Upload"}
            </button>

            {state.error && <p>Error: {state.error}</p>}

            {state.lastPhotoId && !state.uploading && (
                <p>Uploaded successfully (photo id: {state.lastPhotoId})</p>
            )}
        </section>
    );
}