import { dispatcher } from "../stores/Dispatcher";
import { galleryApi } from "../services/galleryApi";

export async function loadGallery(
    galleryId: string
): Promise<void> {
    dispatcher.dispatch({
        type: "GALLERY_DETAILS_LOAD_START",
    });

    try {
        const gallery = await galleryApi.getGallery(galleryId);

        dispatcher.dispatch({
            type: "GALLERY_DETAILS_LOAD_SUCCESS",
            payload: gallery,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_DETAILS_LOAD_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to load gallery",
        });
    }
}