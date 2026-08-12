import { api } from "./api";
import type {
    CreateGalleryRequest,
    Gallery,
    ListGalleriesResponse,
} from "../types/gallery";

export const galleryApi = {
    async listGalleries(
        myGalleries = false
    ): Promise<ListGalleriesResponse> {
        const params = new URLSearchParams();

        if (myGalleries) {
            params.set("my_galleries", "true");
        }

        const query = params.toString();

        return api.get<ListGalleriesResponse>(
            `/photogallery/galleries${query ? `?${query}` : ""}`
        );
    },

    async getGallery(galleryId: string): Promise<Gallery> {
        return api.get<Gallery>(
            `/photogallery/galleries/${galleryId}`
        );
    },

    async createGallery(
        request: CreateGalleryRequest
    ): Promise<Gallery> {
        return api.post<Gallery>(
            "/photogallery/galleries",
            request
        );
    },

    async joinGallery(galleryId: string): Promise<void> {
        await api.post<void>(
            `/photogallery/galleries/${galleryId}/members`
        );
    },

    async leaveGallery(galleryId: string): Promise<void> {
        await api.delete<void>(
            `/photogallery/galleries/${galleryId}/members`
        );
    },

    async closeGallery(galleryId: string): Promise<void> {
        await api.post<void>(
            `/photogallery/galleries/${galleryId}/close`,
            {
                galleryId,
            }
        );
    },
};