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

// baseURL points at the API Gateway. Override with GATEWAY_URL when running
// against a deployed instance instead of a local docker compose stack.
func baseURL() string {
	if v := os.Getenv("GATEWAY_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

// These mirror the gateway's JSON wire shape (protojson, camelCase field
// names) rather than importing the internal proto packages directly --
// keeping this test a true black-box client of the HTTP surface, the same
// contract the frontend and any other real client depends on.
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

// doJSON performs an HTTP request against the gateway and decodes a JSON
// response into out (if non-nil). token, if non-empty, is sent as a bearer
// token -- exactly what a real browser client would do.
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

// registerUser creates a fresh user with a unique email (so the test suite
// can be re-run against a persistent database without hitting AlreadyExists)
// and returns a ready-to-use bearer token.
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

	// moderator never explicitly joined their own gallery
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

// TestDeleteGallery_ModeratorCanDeleteOwnGallery confirms the main path: a
// gallery's own moderator can delete it, and it's then genuinely gone --
// GetGallery returns NotFound, and it drops out of the public listing.
//
// On its own this test isn't proof the authorization fix landed -- it
// would have passed even against the original code, which had no
// ownership (or any auth) check at all. TestDeleteGallery_RejectsNonModerator
// and TestDeleteGallery_RejectsWrongGalleryModerator below are the ones
// that actually pin that fix down; this one just confirms the happy path
// still works correctly alongside it.
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

// TestDeleteGallery_RemovesMemberAccess is the black-box proxy for "the
// underlying Member row was actually cleaned up". repository.go's
// DeleteGallery now does an Unscoped hard delete specifically so the
// ON DELETE CASCADE constraint on Gallery.Members fires -- this test
// can't query Postgres directly without breaking the black-box pattern
// every other file in this package follows, so instead it checks the
// only externally observable consequence: a joined member's own
// my_galleries view no longer includes the deleted gallery. If cascade
// deletion ever regressed back to a soft delete, this specific assertion
// wouldn't actually catch it (my_galleries already filters via a query
// that only looks at non-deleted galleries either way) -- it's really
// confirming "the member-facing contract still holds after delete", not
// "the Member row is physically gone". Treat it as a smoke test for the
// cascade, not a strict proof.
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

// TestDeleteGallery_RejectsNonModerator confirms a regular member of the
// gallery -- not just an unrelated stranger -- cannot delete it, and that
// the gallery is genuinely untouched afterward.
func TestDeleteGallery_RejectsNonModerator(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Non-Moderator Delete Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	resp := deleteGallery(t, memberToken, gallery.ID)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	getResp := doJSON(t, http.MethodGet, "/photogallery/galleries/"+gallery.ID, "", nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode,
		"gallery should still exist -- a rejected delete must not have any effect")
}

// TestDeleteGallery_RejectsWrongGalleryModerator confirms being
// ROLE_MODERATOR isn't sufficient on its own -- same ownership pattern as
// CloseGallery/SendModeratorAlert: only this specific gallery's own
// moderator may delete it. This is the test that would have caught the
// original bug most directly: before the fix, this moderator (who has no
// relationship to the gallery at all) could delete it just by knowing its
// ID.
func TestDeleteGallery_RejectsWrongGalleryModerator(t *testing.T) {
	_, ownerToken := registerUser(t, "ROLE_MODERATOR")
	_, otherModeratorToken := registerUser(t, "ROLE_MODERATOR")

	gallery := createTestGallery(t, ownerToken, "Wrong Moderator Delete Test Gallery")

	resp := deleteGallery(t, otherModeratorToken, gallery.ID)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	getResp := doJSON(t, http.MethodGet, "/photogallery/galleries/"+gallery.ID, "", nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
}

// TestDeleteGallery_SecondDeleteReturnsNotFound confirms deleting an
// already-deleted gallery fails cleanly as NotFound rather than a generic
// Internal error -- CommandService.DeleteGallery's GetGallery lookup
// should fail before ever attempting a second repository delete.
func TestDeleteGallery_SecondDeleteReturnsNotFound(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Double Delete Test Gallery")

	resp := deleteGallery(t, moderatorToken, gallery.ID)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = deleteGallery(t, moderatorToken, gallery.ID)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
