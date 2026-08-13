import { dispatcher } from "../stores/Dispatcher";
import { galleryApi } from "../services/galleryApi";

export async function loadGalleries(myGalleries = false): Promise<void> {
    dispatcher.dispatch({
        type: "GALLERIES_LOAD_START",
    });

    try {
        const response = await galleryApi.listGalleries(
            myGalleries
        );

        dispatcher.dispatch({
            type: "GALLERIES_LOAD_SUCCESS",
            payload: response.galleries,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERIES_LOAD_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to load galleries",
        });
    }
}

export async function createGallery(name: string, description: string): Promise<void> {
    dispatcher.dispatch({
        type: "GALLERY_CREATE_START",
    });

    try {
        const gallery = await galleryApi.createGallery({
            name,
            description,
        });

        dispatcher.dispatch({
            type: "GALLERY_CREATED",
            payload: gallery,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_CREATE_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to create gallery",
        });
    }
}

export async function joinGallery(galleryId: string): Promise<void> {
    try {
        await galleryApi.joinGallery(galleryId);

        dispatcher.dispatch({
            type: "GALLERY_JOINED",
            payload: galleryId,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_JOIN_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to join gallery",
        });
    }
}

export async function leaveGallery(galleryId: string): Promise<void> {
    try {
        await galleryApi.leaveGallery(galleryId);

        dispatcher.dispatch({
            type: "GALLERY_LEFT",
            payload: galleryId,
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_LEAVE_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to leave gallery",
        });
    }
}