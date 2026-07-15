package command

import (
	"context"
	"photogallery/internal/gallery/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

//go:generate mockgen -source=repository.go -destination=../mocks/command_mock.go -package=mocks
type CommandRepository interface {
	CreateGallery(ctx context.Context, g *models.Gallery) error
	GetGallery(ctx context.Context, id uuid.UUID) (*models.Gallery, error)
	UpdateGallery(ctx context.Context, g *models.Gallery) error
	DeleteGallery(ctx context.Context, id uuid.UUID) error
	AddMember(ctx context.Context, galleryID uuid.UUID, userID string) error
	RemoveMember(ctx context.Context, galleryID uuid.UUID, userID string) error
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
	return r.db.WithContext(ctx).Delete(&models.Gallery{}, "id=?", id).Error
}
func (r *GormCommandRepository) AddMember(ctx context.Context, id uuid.UUID, u string) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&models.Member{GalleryID: id, UserID: u}).Error
}
func (r *GormCommandRepository) RemoveMember(ctx context.Context, id uuid.UUID, u string) error {
	return r.db.WithContext(ctx).Delete(&models.Member{}, "gallery_id=? AND user_id=?", id, u).Error
}
