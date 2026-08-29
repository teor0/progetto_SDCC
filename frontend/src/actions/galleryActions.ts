import { dispatcher } from "../stores/Dispatcher";
import { galleryApi } from "../services/galleryApi";

export async function loadGalleries(): Promise<void> {
    dispatcher.dispatch({ type: "GALLERIES_LOAD_START" });

    try {
        const [mine, all] = await Promise.all([
            galleryApi.listGalleries(true),
            galleryApi.listGalleries(false),
        ]);

        const myIds = new Set(mine.galleries.map((g) => g.id));
        const available = all.galleries.filter((g) => !myIds.has(g.id));

        dispatcher.dispatch({
            type: "GALLERIES_LOAD_SUCCESS",
            payload: {
                myGalleries: mine.galleries,
                availableGalleries: available,
            },
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
    dispatcher.dispatch({ type: "GALLERY_CREATE_START" });

    try {
        await galleryApi.createGallery({ name, description });
        // Re-sync from the server instead of splicing the new gallery into
        // local state by hand -- the list always reflects what the backend
        // actually has, not what this client assumes just happened.
        await loadGalleries();
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
        await loadGalleries();
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

export async function deleteGallery(galleryId: string): Promise<void> {
    try {
        await galleryApi.deleteGallery(galleryId);
        await loadGalleries();
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_DELETE_FAILURE",
            payload: error instanceof Error ? error.message : "Failed to delete gallery",
        });
    }
}

export async function leaveGallery(galleryId: string): Promise<void> {
    try {
        await galleryApi.leaveGallery(galleryId);
        await loadGalleries();
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