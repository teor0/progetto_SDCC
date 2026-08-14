import { dispatcher, type Action } from "./Dispatcher";
import type { UploadPhotoResponse } from "../types/upload";

type UploadState = {
    uploading: boolean;
    error: string | null;
    lastPhotoId: string | null;
};

class UploadStore {
    private state: UploadState = {
        uploading: false,
        error: null,
        lastPhotoId: null,
    };

    private listeners: (() => void)[] = [];

    constructor() {
        dispatcher.register(this.handleAction.bind(this));
    }

    getState(): UploadState {
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
            case "UPLOAD_START":
                this.state = { uploading: true, error: null, lastPhotoId: null };
                this.emitChange();
                break;

            case "UPLOAD_SUCCESS": {
                const { photoId } = action.payload as UploadPhotoResponse;
                this.state = { uploading: false, error: null, lastPhotoId: photoId };
                this.emitChange();
                break;
            }

            case "UPLOAD_FAILURE":
                this.state = {
                    uploading: false,
                    error: action.payload as string,
                    lastPhotoId: null,
                };
                this.emitChange();
                break;
        }
    }
}

export const uploadStore = new UploadStore();