import { dispatcher, type Action } from "./Dispatcher";
import type { Gallery } from "../types/gallery";

export type GalleryState = {
    myGalleries: Gallery[];
    availableGalleries: Gallery[];
    loading: boolean;
    error: string | null;
};

class GalleryStore {
    private state: GalleryState = {
        myGalleries: [],
        availableGalleries: [],
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
                this.state = { ...this.state,
                    loading: true,
                    error: null
                };
                this.emitChange();
                break;

            case "GALLERY_CREATE_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string
                };
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
                };

                this.state = {
                    myGalleries: payload.myGalleries,
                    availableGalleries:
                    payload.availableGalleries,
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