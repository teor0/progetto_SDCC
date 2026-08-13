import { dispatcher, type Action } from "./Dispatcher";
import type { Gallery } from "../types/gallery";

type GalleryDetailsState = {
    gallery: Gallery | null;
    loading: boolean;
    error: string | null;
};

class GalleryDetailsStore {
    private state: GalleryDetailsState = {
        gallery: null,
        loading: false,
        error: null,
    };

    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): GalleryDetailsState {
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
            case "GALLERY_DETAILS_LOAD_START":
                this.state = {
                    gallery: null,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERY_DETAILS_LOAD_SUCCESS":
                this.state = {
                    gallery: action.payload as Gallery,
                    loading: false,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERY_DETAILS_LOAD_FAILURE":
                this.state = {
                    gallery: null,
                    loading: false,
                    error: action.payload as string,
                };

                this.emitChange();
                break;
        }
    }
}

export const galleryDetailsStore =
    new GalleryDetailsStore();