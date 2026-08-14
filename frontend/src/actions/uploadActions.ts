import { dispatcher } from "../stores/Dispatcher";
import { uploadApi } from "../services/uploadApi";

export async function loadUploads(galleryId: string): Promise<void> {
    dispatcher.dispatch({ type: "PHOTOS_LOAD_START" });

    try {
        const response = await uploadApi.listUploads(galleryId);
        dispatcher.dispatch({ type: "PHOTOS_LOAD_SUCCESS", payload: response.uploads });
    } catch (error) {
        dispatcher.dispatch({
            type: "PHOTOS_LOAD_FAILURE",
            payload: error instanceof Error ? error.message : "Failed to load photos",
        });
    }
}

export async function uploadPhoto(galleryId: string, file: File): Promise<void> {
    dispatcher.dispatch({ type: "UPLOAD_START" });

    try {
        const response = await uploadApi.uploadPhoto(galleryId, file);
        dispatcher.dispatch({ type: "UPLOAD_SUCCESS", payload: response });
        // Refresh the grid so the new photo appears without a manual reload.
        await loadUploads(galleryId);
    } catch (error) {
        dispatcher.dispatch({
            type: "UPLOAD_FAILURE",
            payload: error instanceof Error ? error.message : "Failed to upload photo",
        });
    }
}