package api

import (
	"context"

	"github.com/google/uuid"

	"photogallery/internal/gallery/models"
)

// CommandRunner is the subset of *command.CommandService that Server calls.
// Extracting it lets grpc.go depend on a small interface instead of a
// concrete type, which is what makes it mockable with gomock.
type CommandRunner interface {
	CreateGallery(ctx context.Context, name, description string, moderatorID uuid.UUID) (*models.Gallery, error)
	CloseGallery(ctx context.Context, galleryID uuid.UUID, callerID uuid.UUID) error
	AddMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error
	RemoveMember(ctx context.Context, galleryID uuid.UUID, targetUserID, callerID uuid.UUID) error
	SendModeratorAlert(ctx context.Context, galleryID, callerID uuid.UUID, message string) error
}

// QueryRunner is the subset of *query.Service that Server calls.
type QueryRunner interface {
	GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error)
	ListGalleries(ctx context.Context, myGalleries bool, callerID uuid.UUID, pageSize int, pageToken string) ([]models.Gallery, string, error)
	ListMembers(ctx context.Context, galleryID uuid.UUID) ([]models.Member, error)
	ListGalleriesByMember(ctx context.Context, userID uuid.UUID, pageSize int, pageToken string) ([]models.Gallery, string, error)
	IsMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) (bool, models.GalleryStatus, error)
}
