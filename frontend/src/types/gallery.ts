export type GalleryStatus =
    | "GALLERY_STATUS_UNSPECIFIED"
    | "GALLERY_STATUS_OPEN"
    | "GALLERY_STATUS_CLOSED";

export type Gallery = {
    id: string;
    name: string;
    description: string;
    status: GalleryStatus;
    moderatorId: string;
    createdAt: string;
    updatedAt: string;
};

export type ListGalleriesResponse = {
    galleries: Gallery[];
    nextPageToken: string;
};

export type CreateGalleryRequest = {
    name: string;
    description: string;
};