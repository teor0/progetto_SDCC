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
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func baseURL() string {
	if v := os.Getenv("GATEWAY_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

type tokenResponse struct {
	Token     string `json:"token"`
	ExpiresIn string `json:"expiresIn"`
}

type galleryResponse struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
	ModeratorID string `json:"moderatorId"`
}

type listGalleriesResponse struct {
	Galleries     []galleryResponse `json:"galleries"`
	NextPageToken string            `json:"nextPageToken"`
}

// doJSON performs an HTTP request against the gateway and decodes a JSON response
func doJSON(t *testing.T, method, path, token string, body any, out any) *http.Response {
	t.Helper()

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, baseURL()+path, reqBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })

	if out != nil {
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		if len(respBody) > 0 {
			require.NoError(t, json.Unmarshal(respBody, out), "response body: %s", respBody)
		}
	}

	return resp
}

// registerUser creates a fresh user with a unique email
func registerUser(t *testing.T, role string) (email string, token string) {
	t.Helper()

	email = fmt.Sprintf("itest-%d@example.com", time.Now().UnixNano())
	var tr tokenResponse
	resp := doJSON(t, http.MethodPost, "/photogallery/auth/register", "", map[string]string{
		"email":    email,
		"password": "password123",
		"role":     role,
	}, &tr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "register failed")
	require.NotEmpty(t, tr.Token)

	return email, tr.Token
}

func containsGalleryID(galleries []galleryResponse, id string) bool {
	for _, g := range galleries {
		if g.ID == id {
			return true
		}
	}
	return false
}

// TestGalleryJoinFlow exercises the full path this app is built around: a
// moderator creates a gallery, a separate user joins it, and membership is
// then correctly reflected back through ListGalleries(my_galleries=true).
func TestGalleryJoinFlow(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	var gallery galleryResponse
	resp := doJSON(t, http.MethodPost, "/photogallery/galleries", moderatorToken, map[string]string{
		"name":        "Integration Test Gallery",
		"description": "created by TestGalleryJoinFlow",
	}, &gallery)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, gallery.ID)
	require.Equal(t, "GALLERY_STATUS_OPEN", gallery.Status)

	resp = doJSON(t, http.MethodPost, "/photogallery/galleries/"+gallery.ID+"/members", memberToken, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var memberGalleries listGalleriesResponse
	resp = doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", memberToken, nil, &memberGalleries)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, containsGalleryID(memberGalleries.Galleries, gallery.ID),
		"expected joined gallery %s in member's my_galleries list, got %+v", gallery.ID, memberGalleries.Galleries)

	var moderatorGalleries listGalleriesResponse
	resp = doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", moderatorToken, nil, &moderatorGalleries)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, containsGalleryID(moderatorGalleries.Galleries, gallery.ID),
		"moderator was auto-joined on create -- if intentional, update this test to match the new behavior")
}

func TestListGalleries_RequiresAuthForMyGalleries(t *testing.T) {
	resp := doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", "", nil, nil)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestListGalleries_PublicWithoutAuth(t *testing.T) {
	var out listGalleriesResponse
	resp := doJSON(t, http.MethodGet, "/photogallery/galleries", "", nil, &out)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func deleteGallery(t *testing.T, token, galleryID string) *http.Response {
	t.Helper()
	return doJSON(t, http.MethodDelete, "/photogallery/galleries/"+galleryID, token, nil, nil)
}

// TestDeleteGallery_ModeratorCanDeleteOwnGallery test a gallery's own moderator can delete it
func TestDeleteGallery_ModeratorCanDeleteOwnGallery(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Delete Test Gallery")

	resp := deleteGallery(t, moderatorToken, gallery.ID)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	getResp := doJSON(t, http.MethodGet, "/photogallery/galleries/"+gallery.ID, "", nil, nil)
	require.Equal(t, http.StatusNotFound, getResp.StatusCode)

	var all listGalleriesResponse
	listResp := doJSON(t, http.MethodGet, "/photogallery/galleries", "", nil, &all)
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	require.False(t, containsGalleryID(all.Galleries, gallery.ID),
		"deleted gallery %s should not appear in ListGalleries(all), got %+v", gallery.ID, all.Galleries)
}

func TestDeleteGallery_RemovesMemberAccess(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Delete Cascade Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	var before listGalleriesResponse
	resp := doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", memberToken, nil, &before)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, containsGalleryID(before.Galleries, gallery.ID),
		"sanity check: member should see the gallery before deletion")

	resp = deleteGallery(t, moderatorToken, gallery.ID)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var after listGalleriesResponse
	resp = doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", memberToken, nil, &after)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, containsGalleryID(after.Galleries, gallery.ID),
		"deleted gallery should no longer appear in the member's my_galleries list")
}

// TestDeleteGallery_RejectsNonModerator confirms a regular member of the gallery cannot delete it, and that

func TestDeleteGallery_RejectsNonModerator(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Non-Moderator Delete Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	resp := deleteGallery(t, memberToken, gallery.ID)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	getResp := doJSON(t, http.MethodGet, "/photogallery/galleries/"+gallery.ID, "", nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode,
		"gallery should still exist")
}

func TestDeleteGallery_RejectsWrongGalleryModerator(t *testing.T) {
	_, ownerToken := registerUser(t, "ROLE_MODERATOR")
	_, otherModeratorToken := registerUser(t, "ROLE_MODERATOR")

	gallery := createTestGallery(t, ownerToken, "Wrong Moderator Delete Test Gallery")

	resp := deleteGallery(t, otherModeratorToken, gallery.ID)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	getResp := doJSON(t, http.MethodGet, "/photogallery/galleries/"+gallery.ID, "", nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
}
