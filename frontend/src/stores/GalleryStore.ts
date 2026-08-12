import { dispatcher, type Action } from "./Dispatcher";
import type { Gallery } from "../types/gallery";

type GalleryState = {
    galleries: Gallery[];
    loading: boolean;
    error: string | null;
};

class GalleryStore {
    private state: GalleryState = {
        galleries: [],
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
            case "GALLERIES_LOAD_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERIES_LOAD_SUCCESS":
                this.state = {
                    galleries: action.payload as Gallery[],
                    loading: false,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERIES_LOAD_FAILURE":
                this.state = {
                    ...this.state,
                    loading: false,
                    error: action.payload as string,
                };

                this.emitChange();
                break;

            case "GALLERY_CREATED":
                this.state = {
                    ...this.state,
                    galleries: [
                        ...this.state.galleries,
                        action.payload as Gallery,
                    ],
                };

                this.emitChange();
                break;

            case "GALLERY_REMOVED":
                this.state = {
                    ...this.state,
                    galleries: this.state.galleries.filter(
                        (gallery) => gallery.id !== action.payload
                    ),
                };

                this.emitChange();
                break;
            case "GALLERY_CREATE_START":
                this.state = {
                    ...this.state,
                    loading: true,
                    error: null,
                };

                this.emitChange();
                break;

            case "GALLERY_CREATED":
                this.state = {
                    ...this.state,
                    galleries: [
                        ...this.state.galleries,
                        action.payload as Gallery,
                    ],
                    loading: false,
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

            case "GALLERY_JOINED":
                this.emitChange();
                break;

            case "GALLERY_LEFT":
                this.state = {
                    ...this.state,
                    galleries: this.state.galleries.filter(
                        (gallery) => gallery.id !== action.payload
                    ),
                };

                this.emitChange();
                break;
        }
    }
}

export const galleryStore = new GalleryStore();