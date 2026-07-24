package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TokenTTL = 1 * time.Hour

// ClaimsKey is the context key under which *UserClaims is stored after
// successful authentication. Use FromContext to retrieve it.
type claimsKey struct{}

// claims is the JWT payload structure, matching what UserService signs.
type Claims struct {
	UserID uuid.UUID `json:"user_id"`
	Role   string    `json:"role"`
	jwt.RegisteredClaims
}

// SignToken creates and signs a JWT for the given user.
func SignToken(secret string, userID uuid.UUID, role string) (string, error) {
	c := Claims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(TokenTTL)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	return signed, nil
}

// AuthFunc returns a grpcauth.AuthFunc that:
//  1. Extracts the Bearer token from the incoming gRPC metadata.
//  2. Validates and parses it using jwtSecret.
//  3. Stores the resulting *userpb.UserClaims in the context so handlers
//     can retrieve it via auth.FromContext(ctx).
func AuthFunc(jwtSecret string) grpcauth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		tokenStr, err := grpcauth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "missing or malformed Authorization header")
		}

		parsed, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, status.Errorf(codes.Unauthenticated, "unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(jwtSecret), nil
		})
		if err != nil || !parsed.Valid {
			return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
		}

		c, ok := parsed.Claims.(*Claims)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "malformed token claims")
		}

		return context.WithValue(ctx, claimsKey{}, c), nil
	}
}

// FromContext retrieves the claims injected by AuthFunc.
func FromContext(ctx context.Context) (*Claims, error) {
	c, ok := ctx.Value(claimsKey{}).(*Claims)
	if !ok || c == nil {
		return nil, status.Error(codes.Unauthenticated, "no auth claims in context")
	}
	return c, nil
}

// NewContext returns a copy of ctx with claims attached under the same key
// AuthFunc uses. Exported so interceptor code and tests outside this
// package can construct an authenticated context directly.
func NewContext(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, claims)
}

func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(hashed), err
}

func CheckPassword(hashed, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashed), []byte(password))
}
