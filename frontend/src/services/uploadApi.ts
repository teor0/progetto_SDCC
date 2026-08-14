import { api } from "./api";
import type { UploadPhotoResponse, ListUploadsResponse } from "../types/upload";

export const uploadApi = {
    async uploadPhoto(galleryId: string, file: File): Promise<UploadPhotoResponse> {
        const formData = new FormData();
        formData.append("photo", file);
        formData.append("galleryId", galleryId);
        return api.postForm<UploadPhotoResponse>("/api/uploads", formData);
    },

    async listUploads(galleryId: string): Promise<ListUploadsResponse> {
        return api.get<ListUploadsResponse>(`/api/galleries/${galleryId}/uploads`);
    },
};