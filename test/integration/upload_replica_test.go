package integration

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// photoUploadDTO mirrors internal/handlers/upload.go's uploadSummaryDTO --
// the JSON shape returned by GET /api/galleries/{galleryId}/uploads.
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

// TestUploadPhoto_VisibleAcrossReplicas exercises the exact scenario that
// motivated swapping InMemoryRepository for PostgresRepository: with
// Upload Service scaled to multiple replicas and the gateway's connection
// to it load-balanced across them, a write handled by one replica must
// still be visible to a read handled by a DIFFERENT replica. With the old
// in-memory map, ListUploads would only ever see uploads that happened to
// land on the SAME replica instance as the query -- silently and
// non-deterministically incomplete, the same failure shape as the
// pre-fix notification fan-out bug, just for different state.
//
// Prerequisites:
//  1. cmd/gateway/main.go's uploadConn needs round-robin load balancing:
//     grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`)
//     Without this, the gateway's persistent connection to upload-service
//     sticks to whichever replica it resolved first (pick_first), and
//     this test would pass even against the OLD broken repository, since
//     every request -- write and read alike -- would land on that one
//     replica regardless of how many exist.
//  2. upload-service must NOT have a static host port mapping (e.g.
//     "8083:8083") while scaled -- Docker Compose can't bind the same
//     host port from more than one replica, so --scale fails outright
//     for the 2nd+ replica if it's there. This test talks to
//     upload-service only through the gateway, which reaches replicas
//     over the internal Docker network by service name (no host port
//     needed), so it isn't affected -- but upload_flow_test.go's
//     direct-gRPC tests (which dial upload-service's own published port)
//     CANNOT be run against a scaled upload-service without separately
//     reworking how they locate a specific replica.
//
// Run:
//
//	docker compose up -d --build --scale upload-service=3
//	GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration \
//	  ./test/integration/... -run TestUploadPhoto_VisibleAcrossReplicas -v
func TestUploadPhoto_VisibleAcrossReplicas(t *testing.T) {
	const numPhotos = 20

	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Cross-Replica Upload Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	// Upload several photos. With round-robin in place, these are likely
	// spread across different upload-service replicas -- each one only
	// visible via that same replica's own state if the repository were
	// still in-memory.
	photoIDs := make([]string, numPhotos)
	for i := 0; i < numPhotos; i++ {
		var uploaded uploadResponse
		resp := uploadPhoto(t, memberToken, gallery.ID)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		decodeJSONBody(t, resp, &uploaded)
		require.NotEmpty(t, uploaded.PhotoID)
		photoIDs[i] = uploaded.PhotoID
	}

	// Query the gallery's uploads repeatedly. With round-robin, each call
	// independently has a chance to land on a different replica than the
	// one that served any given write above -- doing this several times,
	// not just once, meaningfully increases the odds of actually
	// exercising a replica mismatch instead of relying on a single
	// lucky/unlucky roll of the load balancer.
	const numListChecks = 10
	for check := 0; check < numListChecks; check++ {
		result := listGalleryUploads(t, memberToken, gallery.ID)

		for _, photoID := range photoIDs {
			require.True(t, containsPhotoID(result.Uploads, photoID),
				"check %d/%d: photo %s missing from ListUploads -- likely served by a replica "+
					"that never processed its own write (the exact failure mode PostgresRepository was meant to fix)",
				check+1, numListChecks, photoID)
		}
	}
}
