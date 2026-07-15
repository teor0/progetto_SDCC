package api

import (
	"context"

	"github.com/google/uuid"

	"photogallery/internal/gallery/models"
)

// CommandRunner is the subset of *command.CommandService that Server calls.
// Extracting it lets grpc.go depend on a small interface instead of a
// concrete type, which is what makes it mockable with gomock.
//
//go:generate mockgen -source=interfaces.go -destination=../mocks/api_mock.go -package=mocks
type CommandRunner interface {
	CreateGallery(ctx context.Context, name, description, moderatorID string) (*models.Gallery, error)
	CloseGallery(ctx context.Context, galleryID uuid.UUID, callerID string) error
	AddMember(ctx context.Context, galleryID uuid.UUID, userID string) error
	RemoveMember(ctx context.Context, galleryID uuid.UUID, targetUserID, callerID string) error
	SendModeratorAlert(ctx context.Context, galleryID uuid.UUID, callerID, message string) error
}

// QueryRunner is the subset of *query.Service that Server calls.
type QueryRunner interface {
	GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error)
	ListGalleries(ctx context.Context, myGalleries bool, callerID string, pageSize int, pageToken string) ([]models.Gallery, string, error)
	ListMembers(ctx context.Context, galleryID uuid.UUID) ([]models.Member, error)
}
