package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type GalleryStatus string

const (
	GalleryOpen   GalleryStatus = "GALLERY_STATUS_OPEN"
	GalleryClosed GalleryStatus = "GALLERY_STATUS_CLOSED"
)

type Gallery struct {
	ID          uuid.UUID     `gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	Name        string        `gorm:"size:255;not null"`
	Description string        `gorm:"type:text"`
	Status      GalleryStatus `gorm:"type:text;default:GALLERY_STATUS_OPEN;index"`
	ModeratorID string        `gorm:"not null;index"`
	Members     []Member      `gorm:"constraint:OnDelete:CASCADE;foreignKey:GalleryID"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}
