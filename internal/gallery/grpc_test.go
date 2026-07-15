package gallery

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"photogallery/internal/auth"
	"photogallery/internal/gallery/api"
	"photogallery/internal/gallery/mocks"
	"photogallery/internal/gallery/models"

	gallerypb "photogallery/gen/gallery"
	userpb "photogallery/gen/user"
)

const jwtSecret = "test-secret"

func newTestServer(t *testing.T) (*api.Server, *mocks.MockCommandRunner, *mocks.MockQueryRunner) {
	t.Helper()
	ctrl := gomock.NewController(t)
	cmd := mocks.NewMockCommandRunner(ctrl)
	qry := mocks.NewMockQueryRunner(ctrl)
	return api.NewServer(cmd, qry, jwtSecret), cmd, qry
}

func moderatorCtx(userID string) context.Context {
	return auth.NewContext(context.Background(), &auth.Claims{
		UserID: userID,
		Role:   userpb.Role_ROLE_MODERATOR.String(),
	})
}

func memberCtx(userID string) context.Context {
	return auth.NewContext(context.Background(), &auth.Claims{
		UserID: userID,
		Role:   userpb.Role_ROLE_USER.String(),
	})
}

func requireGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "error is not a gRPC status error: %v", err)
	assert.Equal(t, want, st.Code())
}

func TestServer_CreateGallery(t *testing.T) {
	t.Run("moderator succeeds and maps the response", func(t *testing.T) {
		srv, cmd, _ := newTestServer(t)
		now := time.Now()
		g := &models.Gallery{
			ID:          uuid.New(),
			Name:        "Test gallery",
			Description: "this is a test",
			Status:      models.GalleryOpen,
			ModeratorID: "mod-1",
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		cmd.EXPECT().
			CreateGallery(gomock.Any(), "Test gallery", "this is a test", "mod-1").
			Return(g, nil)

		resp, err := srv.CreateGallery(moderatorCtx("mod-1"), &gallerypb.CreateGalleryRequest{
			Name:        "Test gallery",
			Description: "this is a test",
		})

		require.NoError(t, err)
		assert.Equal(t, g.ID.String(), resp.Id)
		assert.Equal(t, gallerypb.GalleryStatus_GALLERY_STATUS_OPEN, resp.Status)
		assert.Equal(t, "mod-1", resp.ModeratorId)
	})

	t.Run("non-moderator rejected before hitting the command layer", func(t *testing.T) {
		srv, cmd, _ := newTestServer(t)
		cmd.EXPECT().CreateGallery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		_, err := srv.CreateGallery(memberCtx("user-1"), &gallerypb.CreateGalleryRequest{Name: "x"})

		requireGRPCCode(t, err, codes.PermissionDenied)
	})

	t.Run("command layer error propagates unchanged", func(t *testing.T) {
		srv, cmd, _ := newTestServer(t)
		cmd.EXPECT().
			CreateGallery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, status.Error(codes.InvalidArgument, "gallery name is required"))

		_, err := srv.CreateGallery(moderatorCtx("mod-1"), &gallerypb.CreateGalleryRequest{Name: ""})

		requireGRPCCode(t, err, codes.InvalidArgument)
	})
}

func TestServer_CloseGallery(t *testing.T) {
	t.Run("invalid gallery id rejected before hitting the command layer", func(t *testing.T) {
		srv, cmd, _ := newTestServer(t)
		cmd.EXPECT().CloseGallery(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		_, err := srv.CloseGallery(moderatorCtx("mod-1"), &gallerypb.CloseGalleryRequest{GalleryId: "not-a-uuid"})

		requireGRPCCode(t, err, codes.InvalidArgument)
	})

	t.Run("success", func(t *testing.T) {
		srv, cmd, _ := newTestServer(t)
		id := uuid.New()
		cmd.EXPECT().CloseGallery(gomock.Any(), id, "mod-1").Return(nil)

		resp, err := srv.CloseGallery(moderatorCtx("mod-1"), &gallerypb.CloseGalleryRequest{GalleryId: id.String()})

		require.NoError(t, err)
		assert.True(t, resp.Success)
	})
}

func TestServer_ListGalleries(t *testing.T) {
	t.Run("my_galleries=true requires an authenticated caller", func(t *testing.T) {
		srv, _, qry := newTestServer(t)
		qry.EXPECT().ListGalleries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		// No claims in context at all.
		_, err := srv.ListGalleries(context.Background(), &gallerypb.ListGalleriesRequest{MyGalleries: true})

		requireGRPCCode(t, err, codes.Unauthenticated)
	})

	t.Run("maps results and pagination token", func(t *testing.T) {
		srv, _, qry := newTestServer(t)
		g1 := models.Gallery{ID: uuid.New(), Name: "A", Status: models.GalleryOpen}
		g2 := models.Gallery{ID: uuid.New(), Name: "B", Status: models.GalleryClosed}

		qry.EXPECT().
			ListGalleries(gomock.Any(), true, "user-1", 10, "cursor-1").
			Return([]models.Gallery{g1, g2}, "cursor-2", nil)

		resp, err := srv.ListGalleries(memberCtx("user-1"), &gallerypb.ListGalleriesRequest{
			MyGalleries: true,
			PageSize:    10,
			PageToken:   "cursor-1",
		})

		require.NoError(t, err)
		require.Len(t, resp.Galleries, 2)
		assert.Equal(t, "cursor-2", resp.NextPageToken)
		assert.Equal(t, gallerypb.GalleryStatus_GALLERY_STATUS_CLOSED, resp.Galleries[1].Status)
	})
}

func TestServer_AuthFuncOverride(t *testing.T) {
	t.Run("public methods bypass auth entirely", func(t *testing.T) {
		srv, _, _ := newTestServer(t)
		ctx := context.Background() // no metadata at all

		outCtx, err := srv.AuthFuncOverride(ctx, "/proto.GalleryService/GetGallery")

		require.NoError(t, err)
		assert.Equal(t, ctx, outCtx)
	})

	t.Run("non-public method with no token is rejected", func(t *testing.T) {
		srv, _, _ := newTestServer(t)
		ctx := context.Background()

		_, err := srv.AuthFuncOverride(ctx, "/proto.GalleryService/CreateGallery")

		requireGRPCCode(t, err, codes.Unauthenticated)
	})

	t.Run("non-public method with a valid token authenticates", func(t *testing.T) {
		srv, _, _ := newTestServer(t)

		token, err := auth.SignToken(jwtSecret, "mod-1", userpb.Role_ROLE_MODERATOR.String())
		require.NoError(t, err)

		md := metadata.Pairs("authorization", "bearer "+token)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		outCtx, err := srv.AuthFuncOverride(ctx, "/proto.GalleryService/CreateGallery")
		require.NoError(t, err)

		claims, err := auth.FromContext(outCtx)
		require.NoError(t, err)
		assert.Equal(t, "mod-1", claims.UserID)
		assert.Equal(t, userpb.Role_ROLE_MODERATOR.String(), claims.Role)
	})

	t.Run("non-public method with an expired token is rejected", func(t *testing.T) {
		srv, _, _ := newTestServer(t)

		// Sign with a secret the server won't accept, to simulate an invalid token
		// without waiting on a real TTL expiry.
		token, err := auth.SignToken("wrong-secret", "mod-1", userpb.Role_ROLE_MODERATOR.String())
		require.NoError(t, err)

		md := metadata.Pairs("authorization", "bearer "+token)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		_, err = srv.AuthFuncOverride(ctx, "/proto.GalleryService/CreateGallery")

		requireGRPCCode(t, err, codes.Unauthenticated)
	})
}
