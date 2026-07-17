package query

import (
	"context"
	"photogallery/internal/gallery/models"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	repo QueryRepository
}

func NewQueryService(repo QueryRepository) *Service {
	return &Service{repo: repo}
}

// GetGallery returns a single gallery by ID.
func (s *Service) GetGallery(ctx context.Context, galleryID uuid.UUID) (*models.Gallery, error) {
	g, err := s.repo.GetGallery(ctx, galleryID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "gallery not found")
	}
	return g, nil
}

// ListGalleries returns a page of galleries. If myGalleries is true and a
// callerID is provided, results are filtered to galleries the caller
// belongs to; otherwise all (open) galleries are returned.
func (s *Service) ListGalleries(ctx context.Context, myGalleries bool, callerID uuid.UUID, pageSize int, pageToken string) ([]models.Gallery, string, error) {
	var after uuid.UUID
	if pageToken != "" {
		parsed, err := uuid.Parse(pageToken)
		if err != nil {
			return nil, "", status.Error(codes.InvalidArgument, "invalid page token")
		}
		after = parsed
	}

	galleries, err := s.repo.ListGalleries(ctx, pageSize, after)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "list galleries: %v", err)
	}

	if myGalleries {
		if callerID == uuid.Nil {
			return nil, "", status.Error(codes.Unauthenticated, "missing caller identity")
		}
		filtered := make([]models.Gallery, 0, len(galleries))
		for _, g := range galleries {
			for _, m := range g.Members {
				if m.UserID == callerID {
					filtered = append(filtered, g)
					break
				}
			}
		}
		galleries = filtered
	}

	var nextPageToken string
	if len(galleries) > 0 {
		nextPageToken = galleries[len(galleries)-1].ID.String()
	}

	return galleries, nextPageToken, nil
}

// ListMembers returns all members of a gallery.
func (s *Service) ListMembers(ctx context.Context, galleryID uuid.UUID) ([]models.Member, error) {
	exists, err := s.repo.GalleryExists(ctx, galleryID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check gallery existence: %v", err)
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "gallery not found")
	}

	members, err := s.repo.ListMembers(ctx, galleryID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list members: %v", err)
	}
	return members, nil
}

func (s *Service) IsMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) (bool, models.GalleryStatus, error) {
	exists, err := s.repo.GalleryExists(ctx, galleryID)
	if err != nil {
		return false, models.GalleryClosed, status.Errorf(codes.Internal, "check gallery existence: %v", err)
	}
	if !exists {
		return false, models.GalleryClosed, status.Error(codes.NotFound, "gallery not found")
	}
	membership, galleryStatus, err := s.repo.IsMember(ctx, galleryID, userID)
	if err != nil {
		return false, models.GalleryClosed, status.Errorf(codes.Internal, err.Error())
	}
	return membership, galleryStatus, nil

}
