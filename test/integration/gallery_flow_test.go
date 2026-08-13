//go:build integration

// Package integration holds tests that run against a live, fully deployed
// stack (docker compose up, or your EC2 instance) rather than against
// mocked repositories/clients. They're gated behind the "integration"
// build tag so `go test ./...` doesn't try to run them without a running
// backend to talk to.
//
// Run with:
//
//	go test -tags=integration ./test/integration/... -v
//
// Point at a non-local stack (AWS) with:
//
//	GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration ./test/integration/... -v
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
// ExpiresIn is a string, not a number: protojson's canonical JSON mapping
// marshals int64/uint64 fields as JSON strings (not numbers) to avoid
// precision loss, since JSON numbers are IEEE 754 doubles and can't safely
// represent the full int64 range. This isn't a quirk of your gateway --
// it's the protobuf-to-JSON spec grpc-gateway follows.
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
//
// This is the exact flow that was silently broken until ListGalleries was
// fixed to delegate to ListGalleriesByMember instead of filtering on a
// Members association that ListGalleries's own query never loaded. A
// mocked unit test of QueryService can't catch that class of bug -- the
// mock repository returns whatever the test tells it to, regardless of
// whether the real GORM query behind it actually populates the field being
// filtered on.
func TestGalleryJoinFlow(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	// --- moderator creates a gallery ---
	var gallery galleryResponse
	resp := doJSON(t, http.MethodPost, "/photogallery/galleries", moderatorToken, map[string]string{
		"name":        "Integration Test Gallery",
		"description": "created by TestGalleryJoinFlow",
	}, &gallery)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, gallery.ID)
	require.Equal(t, "GALLERY_STATUS_OPEN", gallery.Status)

	// --- member joins it ---
	resp = doJSON(t, http.MethodPost, "/photogallery/galleries/"+gallery.ID+"/members", memberToken, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// --- member's "my galleries" should now include it ---
	var memberGalleries listGalleriesResponse
	resp = doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", memberToken, nil, &memberGalleries)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, containsGalleryID(memberGalleries.Galleries, gallery.ID),
		"expected joined gallery %s in member's my_galleries list, got %+v", gallery.ID, memberGalleries.Galleries)

	// --- moderator never explicitly joined their own gallery. This is
	// known current backend behavior (CreateGallery does not insert a
	// Member row for the creator) -- asserting it here documents that
	// behavior rather than silently relying on it elsewhere. If you later
	// add auto-join-on-create, flip this assertion to True.
	var moderatorGalleries listGalleriesResponse
	resp = doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", moderatorToken, nil, &moderatorGalleries)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, containsGalleryID(moderatorGalleries.Galleries, gallery.ID),
		"moderator was auto-joined on create -- if intentional, update this test to match the new behavior")
}

// TestListGalleries_RequiresAuthForMyGalleries locks in the AuthFuncOverride
// fix: my_galleries=true without a token must fail as an auth error (401),
// not as an internal error caused by the handler trying to read claims
// that a "public method" auth override never attached to the context.
func TestListGalleries_RequiresAuthForMyGalleries(t *testing.T) {
	resp := doJSON(t, http.MethodGet, "/photogallery/galleries?my_galleries=true", "", nil, nil)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestListGalleries_PublicWithoutAuth confirms the other half of that same
// fix: ListGalleries must still work anonymously (my_galleries omitted),
// since GetGallery/ListGalleries are meant to support unauthenticated
// browsing, not just authenticated "my galleries" queries.
func TestListGalleries_PublicWithoutAuth(t *testing.T) {
	var out listGalleriesResponse
	resp := doJSON(t, http.MethodGet, "/photogallery/galleries", "", nil, &out)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
