package api

import (
	"context"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/user/models"
	"photogallery/internal/user/repository"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	userpb.UnimplementedUserServiceServer
	db        repository.Repository
	jwtSecret string
}

func NewServer(db repository.Repository, jwtSecret string) *Server {
	return &Server{db: db, jwtSecret: jwtSecret}
}

// publicMethods lists the fully-qualified RPC names that do not require a JWT.
// The grpc-gateway auth middleware calls AuthFuncOverride with the full method
// name so we can selectively bypass authentication for Login and Register.
var publicMethods = map[string]bool{
	"/proto.UserService/Login":    true,
	"/proto.UserService/Register": true,
}

// AuthFuncOverride implements grpcauth.ServiceAuthFuncOverride.
// and delegates to the shared AuthFunc for everything else.
func (s *Server) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	if publicMethods[fullMethodName] {
		return ctx, nil // no auth required
	}
	return auth.AuthFunc(s.jwtSecret)(ctx)
}

func (s *Server) Register(ctx context.Context, req *userpb.RegisterRequest) (*userpb.TokenResponse, error) {
	if req.Email == "" || req.Password == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}
	role := userpb.Role_ROLE_USER.String()
	if req.Role == userpb.Role_ROLE_MODERATOR {
		role = userpb.Role_ROLE_MODERATOR.String()
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error for password: %v", err)
	}
	id, err := s.db.CreateUser(ctx, &models.User{Email: req.Email, Password: hash, Role: role})
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "email already registered")
	}
	token, err := auth.SignToken(s.jwtSecret, id, role)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error for token: %v", err)
	}
	return &userpb.TokenResponse{
		Token:     token,
		ExpiresIn: int64(auth.TokenTTL.Seconds()),
	}, nil
}

func (s *Server) Login(ctx context.Context, req *userpb.LoginRequest) (*userpb.TokenResponse, error) {
	if req.Email == "" || req.Password == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}
	user, err := s.db.GetByEmail(ctx, req.Email)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err := auth.CheckPassword(user.Password, req.Password); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	token, err := auth.SignToken(s.jwtSecret, user.ID, user.Role)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error for token: %v", err)
	}
	return &userpb.TokenResponse{
		Token:     token,
		ExpiresIn: int64(auth.TokenTTL.Seconds()),
	}, nil
}

func (s *Server) Info(ctx context.Context, req *userpb.InfoRequest) (*userpb.InfoResponse, error) {
	userID, err := callerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &userpb.InfoResponse{
		UserId: userID.String(),
	}, nil
}

func callerIDFromContext(ctx context.Context) (uuid.UUID, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	return claims.UserID, nil
}
