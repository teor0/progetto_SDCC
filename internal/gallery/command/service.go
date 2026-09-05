package command

import (
	"context"
	"log"
	"photogallery/internal/gallery/models"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	// Detached from the request: Publish now retries with confirms and can
	// take several seconds under broker degradation.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := s.publisher.Publish(bgCtx, "GalleryCreated", g); err != nil {
			log.Printf("command: failed to publish GalleryCreated for gallery=%s after retries: %v", g.ID, err)
		}
	}()
	return g, nil
}

// DeleteGallery let a MODERATOR delete a gallery. Only its moderator may do this.
func (s *CommandService) DeleteGallery(ctx context.Context, galleryID uuid.UUID, callerID uuid.UUID) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if g.ModeratorID != callerID {
		return status.Error(codes.PermissionDenied, "only the moderator can delete this gallery")
	}
	if err := s.repo.DeleteGallery(ctx, g.ID); err != nil {
		return status.Errorf(codes.Internal, "delete gallery: %v", err)
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := s.publisher.Publish(bgCtx, "GalleryDeleted", g); err != nil {
			log.Printf("command: failed to publish GalleryDeleted for gallery=%s after retries: %v", g.ID, err)
		}
	}()
	return nil
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

	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		// Published as an explicit snake_case map -- matching how
		// MemberAdded/MemberRemoved/ModeratorAlert are published -- rather
		// than marshaling *models.Gallery directly. models.Gallery has no
		// json tags, so a direct marshal would produce Go-cased keys
		// ("ID", "Name", ...), which Notification Service's consumer
		// (galleryClosedPayload, internal/notification/consumer.go) can't
		// parse.
		if err := s.publisher.Publish(bgCtx, "GalleryClosed", map[string]string{
			"gallery_id":   g.ID.String(),
			"gallery_name": g.Name,
		}); err != nil {
			log.Printf("command: failed to publish GalleryClosed for gallery=%s after retries: %v", g.ID, err)
		}
	}()
	return nil
}

// JoinGallery lets any authenticated user join an open gallery.
func (s *CommandService) JoinGallery(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return status.Error(codes.NotFound, "gallery not found")
	}
	if g.Status == models.GalleryClosed {
		return status.Error(codes.FailedPrecondition, "cannot join a closed gallery")
	}

	if err := s.repo.JoinGallery(ctx, galleryID, userID); err != nil {
		return status.Errorf(codes.Internal, "add member: %v", err)
	}

	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := s.publisher.Publish(bgCtx, "MemberAdded", map[string]string{
			"gallery_id": galleryID.String(),
			"user_id":    userID.String()}); err != nil {
			log.Printf("command: failed to publish MemberAdded for gallery=%s after retries: %v", galleryID, err)
		}
	}()
	return nil
}

// LeaveGallery allows the member themselves, to leave.
func (s *CommandService) LeaveGallery(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error {
	if err := s.repo.LeaveGallery(ctx, galleryID, userID); err != nil {
		return status.Errorf(codes.Internal, "remove member: %v", err)
	}

	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := s.publisher.Publish(bgCtx, "MemberRemoved", map[string]string{
			"gallery_id": galleryID.String(),
			"user_id":    userID.String()}); err != nil {
			log.Printf("command: failed to publish MemberRemoved for gallery=%s after retries: %v", galleryID, err)
		}
	}()
	return nil
}

// SendModeratorAlert publishes a broadcast alert to all gallery members.
// Persistence stays with the Notification Service,
// here we just validate authorization and hand off the event.
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

	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := s.publisher.Publish(bgCtx, "ModeratorAlert", map[string]string{
			"gallery_id":   galleryID.String(),
			"gallery_name": g.Name,
			"message":      message,
		}); err != nil {
			log.Printf("command: failed to publish GalleryCreated for gallery=%s after retries: %v", g.ID, err)
		}
	}()

	return nil
}
