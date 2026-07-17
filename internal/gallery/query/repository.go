package query

import (
	"context"
	"photogallery/internal/gallery/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

//go:generate mockgen -source=repository.go -destination=../mocks/query_mock.go -package=mocks
type QueryRepository interface {
	GetGallery(context.Context, uuid.UUID) (*models.Gallery, error)
	ListGalleries(context.Context, int, uuid.UUID) ([]models.Gallery, error)
	ListMembers(context.Context, uuid.UUID) ([]models.Member, error)
	GalleryExists(context.Context, uuid.UUID) (bool, error)
	IsMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) (bool, models.GalleryStatus, error)
	Close() error
}

type GormQueryRepository struct{ db *gorm.DB }

func NewQueryRepository(db *gorm.DB) *GormQueryRepository {
	return &GormQueryRepository{db: db}
}

func (r *GormQueryRepository) Close() error {
	s, _ := r.db.DB()
	return s.Close()
}

func (r *GormQueryRepository) GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error) {
	var gallery models.Gallery
	result := r.db.WithContext(ctx).First(&gallery, "id=?", id)
	if result.Error != nil {
		return nil, result.Error
	}
	return &gallery, nil
}

func (r *GormQueryRepository) ListGalleries(ctx context.Context, limit int, after uuid.UUID) ([]models.Gallery, error) {
	var g []models.Gallery
	q := r.db.WithContext(ctx)
	if after != uuid.Nil {
		q = q.Where("id>?", after)
	}
	if limit == 0 {
		limit = 20
	}
	err := q.Order("id").Limit(limit).Find(&g).Error
	return g, err
}

func (r *GormQueryRepository) ListMembers(ctx context.Context, id uuid.UUID) ([]models.Member, error) {
	var members []models.Member
	err := r.db.WithContext(ctx).Where("gallery_id=?", id).Find(&members).Error
	if err != nil {
		return nil, err
	}
	return members, nil
}
func (r *GormQueryRepository) GalleryExists(ctx context.Context, id uuid.UUID) (bool, error) {
	var c int64
	err := r.db.WithContext(ctx).Model(&models.Gallery{}).Where("id=?", id).Count(&c).Error
	return c > 0, err
}

func (r *GormQueryRepository) IsMember(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) (bool, models.GalleryStatus, error) {
	var gallery models.Gallery

	err := r.db.WithContext(ctx).
		Preload("Members", "user_id = ?", userID).
		First(&gallery, "id = ?", galleryID).Error

	// oppure
	//err := r.db.WithContext(ctx).
	//	Model(&models.Member{}).
	//	Where("gallery_id = ? AND user_id = ?", galleryID, userID).
	//	Count(&c).Error

	if err != nil {
		return false, models.GalleryClosed, err
	}

	return len(gallery.Members) > 0, gallery.Status, nil
}
