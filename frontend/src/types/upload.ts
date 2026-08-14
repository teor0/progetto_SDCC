export type UploadPhotoResponse = {
    photoId: string;
};

export type PhotoUpload = {
    photoId: string;
    galleryId: string;
    uploaderUserId: string;
    sizeBytes: number;
    status: string;
    uploadedAt: string;
    url: string;
};

export type ListUploadsResponse = {
    uploads: PhotoUpload[];
    nextPageToken: string;
};