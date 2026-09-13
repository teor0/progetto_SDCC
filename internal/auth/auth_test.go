package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestServiceAuthInterceptor_AttachesValidBearerToken(t *testing.T) {
	interceptor := serviceAuthInterceptor("test-secret")

	var capturedCtx context.Context
	fakeInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		capturedCtx = ctx
		return nil
	}

	err := interceptor(context.Background(), "/proto.GalleryService/IsMember", nil, nil, nil, fakeInvoker)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(capturedCtx)
	require.True(t, ok)

	authHeader := md.Get("authorization")
	require.Len(t, authHeader, 1)
	require.True(t, strings.HasPrefix(authHeader[0], "bearer "))

	token := strings.TrimPrefix(authHeader[0], "bearer ")
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(tk *jwt.Token) (any, error) {
		return []byte("test-secret"), nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)

	claims := parsed.Claims.(*Claims)
	require.Equal(t, ServiceRole, claims.Role)
}

func TestServiceAuthInterceptor_PropagatesInvokerError(t *testing.T) {
	interceptor := serviceAuthInterceptor("test-secret")

	wantErr := context.DeadlineExceeded
	fakeInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return wantErr
	}

	err := interceptor(context.Background(), "/proto.GalleryService/IsMember", nil, nil, nil, fakeInvoker)
	require.ErrorIs(t, err, wantErr)
}
