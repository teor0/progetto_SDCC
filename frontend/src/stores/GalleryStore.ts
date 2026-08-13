import { dispatcher, type Action } from "./Dispatcher";
import type { Gallery } from "../types/gallery";

type GalleryState = {
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
            this.listeners = this.listeners.filter((l) => l !== listener);
        };
    }

    private emitChange(): void {
        for (const listener of this.listeners) {
            listener();
        }
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "GALLERIES_LOAD_START":
            case "GALLERY_CREATE_START":
                this.state = { ...this.state, loading: true, error: null };
                this.emitChange();
                break;

            case "GALLERIES_LOAD_SUCCESS": {
                const { myGalleries, availableGalleries } = action.payload as {
                    myGalleries: Gallery[];
                    availableGalleries: Gallery[];
                };
                this.state = {
                    myGalleries,
                    availableGalleries,
                    loading: false,
                    error: null,
                };
                this.emitChange();
                break;
            }

            case "GALLERIES_LOAD_FAILURE":
            case "GALLERY_CREATE_FAILURE":
            case "GALLERY_JOIN_FAILURE":
            case "GALLERY_LEAVE_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string,
                };
                this.emitChange();
                break;
        }
    }
}

export const galleryStore = new GalleryStore();