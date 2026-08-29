package command

import (
	"context"
	"photogallery/internal/gallery/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CommandRepository keeps all the functions that the GalleryService must provide
type CommandRepository interface {
	CreateGallery(ctx context.Context, g *models.Gallery) error
	GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error)
	UpdateGallery(ctx context.Context, g *models.Gallery) error
	DeleteGallery(ctx context.Context, id uuid.UUID) error
	JoinGallery(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error
	LeaveGallery(ctx context.Context, galleryID uuid.UUID, userID uuid.UUID) error
	Close() error
}

type GormCommandRepository struct{ db *gorm.DB }

func NewCommandRepository(db *gorm.DB) *GormCommandRepository {
	return &GormCommandRepository{db: db}
}

func (r *GormCommandRepository) Close() error {
	s, _ := r.db.DB()
	return s.Close()
}

func (r *GormCommandRepository) CreateGallery(ctx context.Context, gallery *models.Gallery) error {
	return r.db.WithContext(ctx).Create(gallery).Error
}

func (r *GormCommandRepository) GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error) {
	var g models.Gallery
	result := r.db.WithContext(ctx).First(&g, "id=?", id)
	if result.Error != nil {
		return nil, result.Error
	}
	return &g, nil
}

func (r *GormCommandRepository) UpdateGallery(ctx context.Context, g *models.Gallery) error {
	return r.db.WithContext(ctx).Save(g).Error
}
func (r *GormCommandRepository) DeleteGallery(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Unscoped().Delete(&models.Gallery{}, "id=?", id).Error //without unscoped the gallery is recoverable
}
func (r *GormCommandRepository) JoinGallery(ctx context.Context, id uuid.UUID, userID uuid.UUID) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&models.Member{GalleryID: id, UserID: userID}).Error
}
func (r *GormCommandRepository) LeaveGallery(ctx context.Context, id uuid.UUID, userID uuid.UUID) error {
	return r.db.WithContext(ctx).Delete(&models.Member{}, "gallery_id=? AND user_id=?", id, userID).Error
}
