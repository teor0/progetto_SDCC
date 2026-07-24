package command

import (
	"context"
	"photogallery/internal/gallery/models"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventPublisher is optional: implement it if/when you wire up RabbitMQ
// to update a denormalized read model asynchronously. Until then, a
// no-op implementation is enough to keep CQRS's write side decoupled
// from the read side.
type EventPublisher interface {
	Publish(ctx context.Context, eventType string, payload any) error
}

// NoopPublisher satisfies EventPublisher without doing anything.
type NoopPublisher struct{}

func (NoopPublisher) Publish(ctx context.Context, eventType string, payload any) error {
	return nil
}

type CommandService struct {
	repo      CommandRepository
	publisher EventPublisher
}

func NewCommandService(repo CommandRepository, publisher EventPublisher) *CommandService {
	if publisher == nil {
		publisher = NoopPublisher{}
	}
	return &CommandService{repo: repo, publisher: publisher}
}

// CreateGallery creates a new gallery. Caller must be MODERATOR;
// that check happens upstream (gRPC interceptor / gateway JWT validation),
// moderatorID arrives here already trusted.
func (s *CommandService) CreateGallery(ctx context.Context, name, description string, moderatorID uuid.UUID) (*models.Gallery, error) {
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "gallery name is required")
	}
	if moderatorID == uuid.Nil {
		return nil, status.Error(codes.Unauthenticated, "missing moderator identity")
	}

	g := &models.Gallery{
		ID:          uuid.New(),
		Name:        name,
		Description: description,
		Status:      models.GalleryOpen,
		ModeratorID: moderatorID,
	}

	if err := s.repo.CreateGallery(ctx, g); err != nil {
		return nil, status.Errorf(codes.Internal, "create gallery: %v", err)
	}

	_ = s.publisher.Publish(ctx, "GalleryCreated", g) // if doing async read projection with RabbitMQ
	return g, nil
}

// CloseGallery marks a gallery closed. Only its moderator may do this.
func (s *CommandService) CloseGallery(ctx context.Context, galleryID uuid.UUID, callerID uuid.UUID) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if g.ModeratorID != callerID {
		return status.Error(codes.PermissionDenied, "only the moderator can close this gallery")
	}
	if g.Status == models.GalleryClosed {
		return status.Error(codes.FailedPrecondition, "gallery already closed")
	}

	g.Status = models.GalleryClosed
	g.UpdatedAt = time.Now()

	if err := s.repo.UpdateGallery(ctx, g); err != nil {
		return status.Errorf(codes.Internal, "close gallery: %v", err)
	}

	_ = s.publisher.Publish(ctx, "GalleryClosed", g)
	return nil
}

// AddMember lets any authenticated user join an open gallery.
func (s *CommandService) AddMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if g.Status == models.GalleryClosed {
		return status.Error(codes.FailedPrecondition, "cannot join a closed gallery")
	}

	if err := s.repo.AddMember(ctx, galleryID, userID); err != nil {
		return status.Errorf(codes.Internal, "add member: %v", err)
	}

	_ = s.publisher.Publish(ctx, "MemberAdded", map[string]string{
		"gallery_id": galleryID.String(),
		"user_id":    userID.String(),
	})
	return nil
}

// RemoveMember allows the moderator, or the member themselves, to leave/be removed.
func (s *CommandService) RemoveMember(ctx context.Context, galleryID uuid.UUID, targetUserID, callerID uuid.UUID) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if callerID != targetUserID && callerID != g.ModeratorID {
		return status.Error(codes.PermissionDenied, "not authorized to remove this member")
	}

	if err := s.repo.RemoveMember(ctx, galleryID, targetUserID); err != nil {
		return status.Errorf(codes.Internal, "remove member: %v", err)
	}

	_ = s.publisher.Publish(ctx, "MemberRemoved", map[string]string{
		"gallery_id": galleryID.String(),
		"user_id":    targetUserID.String(),
	})
	return nil
}

// SendModeratorAlert publishes a broadcast alert to all gallery members.
// This writes no gallery state itself — it's a command in the CQRS sense
// (it triggers a side effect) but persistence stays with the Notification
// Service; here we just validate authorization and hand off the event.
func (s *CommandService) SendModeratorAlert(ctx context.Context, galleryID, callerID uuid.UUID, message string) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if callerID != g.ModeratorID {
		return status.Error(codes.PermissionDenied, "only the moderator can send alerts")
	}
	if message == "" {
		return status.Error(codes.InvalidArgument, "alert message is required")
	}

	return s.publisher.Publish(ctx, "ModeratorAlert", map[string]string{
		"gallery_id": galleryID.String(),
		"message":    message,
	})
}
