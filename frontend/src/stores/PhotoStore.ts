import { dispatcher, type Action } from "./Dispatcher";
import type { PhotoUpload } from "../types/upload";

type PhotoState = {
    photos: PhotoUpload[];
    loading: boolean;
    error: string | null;
};

class PhotoStore {
    private state: PhotoState = { photos: [], loading: false, error: null };
    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): PhotoState {
        return this.state;
    }

    subscribe(listener: () => void): () => void {
        this.listeners.push(listener);
        return () => {
            this.listeners = this.listeners.filter((l) => l !== listener);
        };
    }

    private emitChange(): void {
        for (const listener of this.listeners) listener();
    }

    private handleAction(action: Action): void {
        switch (action.type) {
            case "PHOTOS_LOAD_START":
                this.state = { ...this.state, loading: true, error: null };
                this.emitChange();
                break;

            case "PHOTOS_LOAD_SUCCESS":
                this.state = {
                    photos: action.payload as PhotoUpload[],
                    loading: false,
                    error: null,
                };
                this.emitChange();
                break;

            case "PHOTOS_LOAD_FAILURE":
                this.state = { ...this.state, loading: false, error: action.payload as string };
                this.emitChange();
                break;
        }
    }
}

export const photoStore = new PhotoStore();