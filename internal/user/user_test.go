package user

import (
	"context"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/user/api"
	"photogallery/internal/user/mocks"
	"photogallery/internal/user/models"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hashed, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("failed to hash password in test setup: %v", err)
	}
	return hashed
}

func TestRegister_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockRepository(ctrl)

	id := uuid.New()

	//mock
	mockRepo.EXPECT().
		CreateUser(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, u *models.User) (uuid.UUID, error) {
			require.Equal(t, "test@example.com", u.Email)

			// Password should already be hashed
			require.NotEqual(t, "password123", u.Password)
			require.Equal(t, userpb.Role_ROLE_USER.String(), u.Role)

			return id, nil
		})

	s := api.NewServer(mockRepo, "my-secret")

	resp, err := s.Register(context.Background(), &userpb.RegisterRequest{
		Email:    "test@example.com",
		Password: "password123",
		Role:     userpb.Role_ROLE_USER,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.Token)
	require.Positive(t, resp.ExpiresIn)
}

func TestLogin_Success_GoMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockRepository(ctrl)
	defer ctrl.Finish()
	mockRepo.EXPECT().GetByEmail(gomock.Any(), "prova2@mail.com").
		Return(&models.User{Email: "prova2@mail.com", Password: mustHash(t, "mypass"), Role: "ROLE_USER"}, nil)

	s := api.NewServer(mockRepo, "my-secret")
	resp, err := s.Login(context.Background(), &userpb.LoginRequest{
		Email:    "prova2@mail.com",
		Password: "mypass",
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Token == "" {
		t.Error("expected a non-empty token")
	}

	if resp.ExpiresIn <= 0 {
		t.Error("expected a non-zero ExpiresIn")
	}
}

func TestRegister_InvalidRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockRepository(ctrl)

	s := api.NewServer(mockRepo, "my-secret")

	_, err := s.Register(context.Background(), &userpb.RegisterRequest{})

	require.Error(t, err)
}
