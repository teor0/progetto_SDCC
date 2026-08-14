// Run with:
//
//	go test -tags=integration ./test/integration/... -v
//
// Point at a non-local stack (AWS) with:
//
//		UPLOAD_GRPC_URL=<PUBLIC_IPV4>:8083 GATEWAY_URL=http://<PUBLIC_IPV4>:8080 \
//	    go test -tags=integration ./test/integration/... -v
//
// Upload Service gRPC port must be reachable for this test to work!
// This test doesn't uses mock so YOU NEED TO CLEANUP THE TEST RESULTS AFTER
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"testing"

	uploadpb "photogallery/gen/upload"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// decodeJSONBody reads and JSON-decodes an *http.Response body that hasn't
// already been consumed. uploadPhoto returns the raw response (rather than
// decoding inline like doJSON does) because callers need to branch on
// status code first -- a failed upload's error body has a different shape
// ({"error": "..."}) than a successful one, so decoding eagerly with a
// single fixed target type isn't the right shape here.
func decodeJSONBody(t *testing.T, resp *http.Response, out any) {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body, "expected a JSON response body, got an empty one")
	require.NoError(t, json.Unmarshal(body, out), "response body: %s", body)
}

// uploadServiceClient dials Upload Service's gRPC port directly.
//
// This bypasses the API Gateway on purpose: GetUploadStatus and ListUploads
// have no google.api.http annotations in upload.proto and no route wired
// up in internal/handlers/upload.go, so there is currently no HTTP path to
// them at all.
func uploadServiceClient(t *testing.T) uploadpb.UploadServiceClient {
	t.Helper()

	addr := os.Getenv("UPLOAD_GRPC_URL")
	if addr == "" {
		addr = "localhost:8083"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dialing upload-service at %s -- is its gRPC port published to the host?", addr)
	t.Cleanup(func() { conn.Close() })

	return uploadpb.NewUploadServiceClient(conn)
}

func authContext(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "bearer "+token)
}

func createTestGallery(t *testing.T, moderatorToken, name string) galleryResponse {
	t.Helper()
	var gallery galleryResponse
	resp := doJSON(t, http.MethodPost, "/photogallery/galleries", moderatorToken, map[string]string{
		"name":        name,
		"description": "created for upload integration test",
	}, &gallery)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return gallery
}

func joinTestGallery(t *testing.T, token, galleryID string) {
	t.Helper()
	resp := doJSON(t, http.MethodPost, "/photogallery/galleries/"+galleryID+"/members", token, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// uploadResponse mirrors the (intentionally minimal) JSON shape
// internal/handlers/upload.go actually returns on success -- just photoId,
// not the gallery_id/status/url the underlying gRPC response carries.
type uploadResponse struct {
	PhotoID string `json:"photoId"`
}

// uploadPhoto builds a multipart request matching what
// internal/handlers/upload.go expects.
func uploadPhoto(t *testing.T, token, galleryID string) *http.Response {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	fw, err := w.CreateFormFile("photo", "test.jpg")
	require.NoError(t, err)
	_, err = fw.Write([]byte("fake-image-bytes-for-integration-test"))
	require.NoError(t, err)

	require.NoError(t, w.WriteField("galleryId", galleryID))
	require.NoError(t, w.Close())

	req, err := http.NewRequest(http.MethodPost, baseURL()+"/api/uploads", &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })

	return resp
}

// TestUploadPhoto_MemberCanUploadAndRetrieveStatus exercises the full
// write path for a legitimate member: gateway -> Upload Service -> a real
// MinIO PutObject -> Upload Service's own IsMember call to Gallery Service
// (through its circuit breaker) -> a persisted record queryable back out.
// None of this is mocked -- if MinIO credentials are wrong, if the gallery
// circuit breaker is misconfigured, or if the two services can't reach
// each other on the Docker network, this fails for real reasons a unit
// test's mocked Uploader/GalleryServiceClient can't surface.
func TestUploadPhoto_MemberCanUploadAndRetrieveStatus(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Upload Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	// --- upload as the now-confirmed member ---
	var uploaded uploadResponse
	resp := uploadPhoto(t, memberToken, gallery.ID)
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload should succeed for a gallery member")
	decodeJSONBody(t, resp, &uploaded)
	require.NotEmpty(t, uploaded.PhotoID)

	// --- verify against Upload Service directly
	client := uploadServiceClient(t)

	status, err := client.GetUploadStatus(authContext(memberToken), &uploadpb.GetUploadStatusRequest{
		PhotoId: uploaded.PhotoID,
	})
	require.NoError(t, err)
	require.Equal(t, gallery.ID, status.GalleryId)
	require.Equal(t, uploadpb.UploadStatus_UPLOAD_STATUS_COMPLETED, status.Status,
		"status should be COMPLETED, meaning both the MinIO write and the record save succeeded")

	list, err := client.ListUploads(authContext(memberToken), &uploadpb.ListUploadsRequest{
		GalleryId: gallery.ID,
	})
	require.NoError(t, err)

	found := false
	for _, u := range list.Uploads {
		if u.PhotoId == uploaded.PhotoID {
			found = true
			require.NotEmpty(t, u.StorageKey, "expected a real MinIO object key, not an empty one")
			break
		}
	}
	require.True(t, found, "expected uploaded photo %s in ListUploads(gallery=%s), got %+v",
		uploaded.PhotoID, gallery.ID, list.Uploads)
}

// TestUploadPhoto_RejectsNonMember reuses the moderator's own known
// non-membership (CreateGallery doesn't auto-join its creator -- see
// gallery_flow_test.go) as a convenient, already-guaranteed non-member to
// upload as. This exercises the real IsMember round trip to Gallery
// Service actually returning false, not a mock configured to return false.
//
// Note: the current handler collapses every upload failure (InvalidArgument
// for "not a member" included) to a flat HTTP 500 with no distinguishing
// error code -- so this only asserts "did not succeed", not a specific
// status. If internal/handlers/upload.go is later fixed to map gRPC codes
// to proper HTTP statuses, tighten this to require.Equal(t,
// http.StatusBadRequest, resp.StatusCode) instead.
func TestUploadPhoto_RejectsNonMember(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Non-Member Upload Test Gallery")

	resp := uploadPhoto(t, moderatorToken, gallery.ID)
	require.NotEqual(t, http.StatusOK, resp.StatusCode,
		"moderator has not joined their own gallery and should be rejected as a non-member")
}
