import { dispatcher } from "../stores/Dispatcher";
import { galleryApi } from "../services/galleryApi";

// Page sizes are deliberately different: a user's own memberships are
// usually a short list, so we ask for enough in one request that "Load
// more" is rarely needed there. The browsable "available" list is
// expected to grow without bound, so it stays small and paginated.
const MY_GALLERIES_PAGE_SIZE = 50;
const AVAILABLE_GALLERIES_PAGE_SIZE = 12;

export async function loadGalleries(): Promise<void> {
    dispatcher.dispatch({ type: "GALLERIES_LOAD_START" });

    try {
        const [mine, all] = await Promise.all([
            galleryApi.listGalleries(true, MY_GALLERIES_PAGE_SIZE),
            galleryApi.listGalleries(false, AVAILABLE_GALLERIES_PAGE_SIZE),
        ]);

        const myIds = new Set(mine.galleries.map((g) => g.id));
        const available = all.galleries.filter((g) => !myIds.has(g.id));

        dispatcher.dispatch({
            type: "GALLERIES_LOAD_SUCCESS",
            payload: {
                myGalleries: mine.galleries,
                availableGalleries: available,
                myGalleriesNextPageToken: mine.nextPageToken || null,
                availableGalleriesNextPageToken: all.nextPageToken || null,
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

export async function loadMoreMyGalleries(pageToken: string): Promise<void> {
    dispatcher.dispatch({ type: "GALLERIES_LOAD_MORE_MY_START" });

    try {
        const page = await galleryApi.listGalleries(true, MY_GALLERIES_PAGE_SIZE, pageToken);

        dispatcher.dispatch({
            type: "GALLERIES_LOAD_MORE_MY_SUCCESS",
            payload: {
                galleries: page.galleries,
                nextPageToken: page.nextPageToken || null,
            },
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERIES_LOAD_MORE_MY_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to load more galleries",
        });
    }
}

export async function loadMoreAvailableGalleries(pageToken: string): Promise<void> {
    dispatcher.dispatch({ type: "GALLERIES_LOAD_MORE_AVAILABLE_START" });

    try {
        const page = await galleryApi.listGalleries(false, AVAILABLE_GALLERIES_PAGE_SIZE, pageToken);

        dispatcher.dispatch({
            type: "GALLERIES_LOAD_MORE_AVAILABLE_SUCCESS",
            payload: {
                galleries: page.galleries,
                nextPageToken: page.nextPageToken || null,
            },
        });
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERIES_LOAD_MORE_AVAILABLE_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to load more galleries",
        });
    }
}

export async function createGallery(name: string, description: string): Promise<void> {
    dispatcher.dispatch({ type: "GALLERY_CREATE_START" });

    try {
        await galleryApi.createGallery({ name, description });
        // Re-sync from the server instead of splicing the new gallery into
        // local state by hand -- the list always reflects what the backend
        // actually has, not what this client assumes just happened. This
        // also resets pagination back to page 1 for both lists, which is
        // the simplest correct behavior after a mutation.
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

export async function closeGallery(galleryId: string): Promise<void> {
    try {
        await galleryApi.closeGallery(galleryId);
        await loadGalleries();
    } catch (error) {
        dispatcher.dispatch({
            type: "GALLERY_CLOSE_FAILURE",
            payload:
                error instanceof Error
                    ? error.message
                    : "Failed to close gallery",
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