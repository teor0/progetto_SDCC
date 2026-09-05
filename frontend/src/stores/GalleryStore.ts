import { dispatcher, type Action } from "./Dispatcher";
import type { Gallery } from "../types/gallery";

export type GalleryState = {
    myGalleries: Gallery[];
    availableGalleries: Gallery[];
    // null means "no more pages". Set from each response's nextPageToken.
    myGalleriesNextPageToken: string | null;
    availableGalleriesNextPageToken: string | null;
    loadingMoreMy: boolean;
    loadingMoreAvailable: boolean;
    loading: boolean;
    error: string | null;
};

class GalleryStore {
    private state: GalleryState = {
        myGalleries: [],
        availableGalleries: [],
        myGalleriesNextPageToken: null,
        availableGalleriesNextPageToken: null,
        loadingMoreMy: false,
        loadingMoreAvailable: false,
        loading: false,
        error: null,
    };

    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): GalleryState {
        return this.state;
    }

    subscribe(listener: () => void): () => void {
        this.listeners.push(listener);

        return () => {
            this.listeners = this.listeners.filter(
                (registered) => registered !== listener
            );
        };
    }

    private emitChange(): void {
        for (const listener of this.listeners) {
            listener();
        }
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "GALLERY_CREATE_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };
                this.emitChange();
                break;

            case "GALLERY_CREATE_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string,
                };
                this.emitChange();
                break;

            case "GALLERY_CLOSE_FAILURE":
                this.state = { ...this.state, error: action.payload as string };
                this.emitChange();
                break;

            case "GALLERIES_LOAD_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERIES_LOAD_SUCCESS": {
                const payload = action.payload as {
                    myGalleries: Gallery[];
                    availableGalleries: Gallery[];
                    myGalleriesNextPageToken: string | null;
                    availableGalleriesNextPageToken: string | null;
                };

                this.state = {
                    myGalleries: payload.myGalleries,
                    availableGalleries: payload.availableGalleries,
                    myGalleriesNextPageToken: payload.myGalleriesNextPageToken,
                    availableGalleriesNextPageToken: payload.availableGalleriesNextPageToken,
                    loadingMoreMy: false,
                    loadingMoreAvailable: false,
                    loading: false,
                    error: null,
                };

                this.emitChange();
                break;
            }

            case "GALLERIES_LOAD_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string,
                };

                this.emitChange();
                break;

            case "GALLERIES_LOAD_MORE_MY_START":
                this.state = { ...this.state, loadingMoreMy: true, error: null };
                this.emitChange();
                break;

            case "GALLERIES_LOAD_MORE_MY_SUCCESS": {
                const payload = action.payload as {
                    galleries: Gallery[];
                    nextPageToken: string | null;
                };
                const newMyIds = new Set(payload.galleries.map((g) => g.id));

                this.state = {
                    ...this.state,
                    myGalleries: [...this.state.myGalleries, ...payload.galleries],
                    // Safety net: if a gallery just paged into "my galleries"
                    // is still sitting in "available" from an earlier page,
                    // drop it from there too.
                    availableGalleries: this.state.availableGalleries.filter(
                        (g) => !newMyIds.has(g.id)
                    ),
                    myGalleriesNextPageToken: payload.nextPageToken,
                    loadingMoreMy: false,
                };
                this.emitChange();
                break;
            }

            case "GALLERIES_LOAD_MORE_MY_FAILURE":
                this.state = { ...this.state, loadingMoreMy: false, error: action.payload as string };
                this.emitChange();
                break;

            case "GALLERIES_LOAD_MORE_AVAILABLE_START":
                this.state = { ...this.state, loadingMoreAvailable: true, error: null };
                this.emitChange();
                break;

            case "GALLERIES_LOAD_MORE_AVAILABLE_SUCCESS": {
                const payload = action.payload as {
                    galleries: Gallery[];
                    nextPageToken: string | null;
                };
                // Filter against the memberships we currently know about --
                // this list is fetched independently of "my galleries", so a
                // gallery the user already belongs to can still show up in a
                // raw "all galleries" page.
                const myIds = new Set(this.state.myGalleries.map((g) => g.id));
                const newAvailable = payload.galleries.filter((g) => !myIds.has(g.id));

                this.state = {
                    ...this.state,
                    availableGalleries: [...this.state.availableGalleries, ...newAvailable],
                    availableGalleriesNextPageToken: payload.nextPageToken,
                    loadingMoreAvailable: false,
                };
                this.emitChange();
                break;
            }

            case "GALLERIES_LOAD_MORE_AVAILABLE_FAILURE":
                this.state = { ...this.state, loadingMoreAvailable: false, error: action.payload as string };
                this.emitChange();
                break;

            case "GALLERY_JOINED": {
                const gallery =
                    action.payload as Gallery;

                this.state = {
                    ...this.state,

                    // Remove it from available galleries
                    availableGalleries:
                        this.state.availableGalleries.filter(
                            (g) => g.id !== gallery.id
                        ),

                    // Add it to my galleries
                    myGalleries: [
                        ...this.state.myGalleries,
                        gallery,
                    ],

                    error: null,
                };

                this.emitChange();
                break;
            }

            case "GALLERY_LEFT": {
                const gallery =
                    action.payload as Gallery;

                this.state = {
                    ...this.state,

                    // Remove it from my galleries
                    myGalleries:
                        this.state.myGalleries.filter(
                            (g) => g.id !== gallery.id
                        ),

                    // Put it back into available galleries
                    availableGalleries: [
                        ...this.state.availableGalleries,
                        gallery,
                    ],

                    error: null,
                };

                this.emitChange();
                break;
            }

            case "GALLERY_JOIN_FAILURE":
                this.state = {
                    ...this.state,
                    error: action.payload as string,
                };

                this.emitChange();
                break;

            case "GALLERY_LEAVE_FAILURE":
                this.state = {
                    ...this.state,
                    error: action.payload as string,
                };

                this.emitChange();
                break;

            case "GALLERY_DELETED": {
                const galleryId =
                    action.payload as string;

                this.state = {
                    ...this.state,

                    myGalleries:
                        this.state.myGalleries.filter(
                            (gallery) =>
                                gallery.id !== galleryId
                        ),

                    availableGalleries:
                        this.state.availableGalleries.filter(
                            (gallery) =>
                                gallery.id !== galleryId
                        ),

                    error: null,
                };

                this.emitChange();
                break;
            }

            case "GALLERY_DELETE_FAILURE":
                this.state = { ...this.state, loading: false, error: action.payload as string };
                this.emitChange();
                break;
        }
    }
}

export const galleryStore = new GalleryStore();