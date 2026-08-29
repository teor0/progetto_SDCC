package upload

import (
	"context"
	"errors"
	"photogallery/internal/upload/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PostgresRepository is the Repository implementation to use once you're
// running more than one Upload Service replica, or you care about upload
// history surviving a redeployment.
type PostgresRepository struct {
	db *gorm.DB
}

func NewPostgresRepository(db *gorm.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) Save(ctx context.Context, rec *model.Record) error {
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "photo_id"}},
			UpdateAll: true,
		}).
		Create(rec).Error
}

func (r *PostgresRepository) Get(ctx context.Context, photoID uuid.UUID) (*model.Record, bool, error) {
	var rec model.Record
	err := r.db.WithContext(ctx).First(&rec, "photo_id = ?", photoID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &rec, true, nil
}

func (r *PostgresRepository) ListByGallery(ctx context.Context, galleryID uuid.UUID, offset, limit int) ([]*model.Record, int, error) {
	var records []*model.Record
	var total int64

	if err := r.db.WithContext(ctx).Model(&model.Record{}).
		Where("gallery_id = ?", galleryID).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}

	q := r.db.WithContext(ctx).Where("gallery_id = ?", galleryID).Order("uploaded_at DESC")
	if offset > 0 {
		q = q.Offset(offset)
	}
	if limit > 0 {
		q = q.Limit(limit)
	}

	if err := q.Find(&records).Error; err != nil {
		return nil, 0, err
	}
	return records, int(total), nil
}
