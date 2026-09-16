//go:build integration

// Run with:
//
//	go test -tags=integration ./test/integration/... -v
//
// Point at a non-local stack (AWS) with:
//
//	GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration ./test/integration/... -v
//
// This test doesn't uses mock so YOU NEED TO CLEANUP THE TEST RESULTS AFTER
package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeJSONBody(t *testing.T, resp *http.Response, out any) {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body, "expected a JSON response body, got an empty one")
	require.NoError(t, json.Unmarshal(body, out), "response body: %s", body)
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

// uploadResponse mirrors the JSON shape
type uploadResponse struct {
	PhotoID string `json:"photoId"`
}

// uploadPhoto builds a multipart request
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

func TestUploadPhoto_RejectsNonMember(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Non-Member Upload Test Gallery")

	resp := uploadPhoto(t, moderatorToken, gallery.ID)
	require.NotEqual(t, http.StatusOK, resp.StatusCode,
		"moderator has not joined their own gallery and should be rejected as a non-member")
}
