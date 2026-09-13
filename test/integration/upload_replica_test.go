package integration

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type photoUploadDTO struct {
	PhotoID    string `json:"photoId"`
	GalleryID  string `json:"galleryId"`
	UploaderID string `json:"uploaderUserId"`
	SizeBytes  int64  `json:"sizeBytes"`
	Status     string `json:"status"`
	UploadedAt string `json:"uploadedAt"`
	URL        string `json:"url"`
}

type galleryUploadsResponse struct {
	Uploads       []photoUploadDTO `json:"uploads"`
	NextPageToken string           `json:"nextPageToken"`
}

func listGalleryUploads(t *testing.T, token, galleryID string) galleryUploadsResponse {
	t.Helper()
	var out galleryUploadsResponse
	resp := doJSON(t, http.MethodGet, "/api/galleries/"+galleryID+"/uploads", token, nil, &out)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return out
}

func containsPhotoID(uploads []photoUploadDTO, photoID string) bool {
	for _, u := range uploads {
		if u.PhotoID == photoID {
			return true
		}
	}
	return false
}

func TestUploadPhoto_VisibleAcrossReplicas(t *testing.T) {
	const numPhotos = 20

	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Cross-Replica Upload Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	photoIDs := make([]string, numPhotos)
	for i := range numPhotos {
		var uploaded uploadResponse
		resp := uploadPhoto(t, memberToken, gallery.ID)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		decodeJSONBody(t, resp, &uploaded)
		require.NotEmpty(t, uploaded.PhotoID)
		photoIDs[i] = uploaded.PhotoID
	}

	const numListChecks = 10
	for check := range numListChecks {
		result := listGalleryUploads(t, memberToken, gallery.ID)

		for _, photoID := range photoIDs {
			require.True(t, containsPhotoID(result.Uploads, photoID),
				"check %d/%d: photo %s missing from ListUploads -- likely served by a replica "+
					"that never processed its own write (the exact failure mode PostgresRepository was meant to fix)",
				check+1, numListChecks, photoID)
		}
	}
}
