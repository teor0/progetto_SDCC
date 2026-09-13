package model

import (
	"time"

	uploadpb "photogallery/gen/upload"

	"github.com/google/uuid"
)

// Record is upload metadata, persisted so GetUploadStatus/ListUploads
type Record struct {
	PhotoID      uuid.UUID `gorm:"type:uuid;primaryKey"`
	GalleryID    uuid.UUID `gorm:"type:uuid;index;not null"`
	UploaderID   uuid.UUID `gorm:"type:uuid;index;not null"`
	Filename     string
	ContentType  string
	StorageKey   string
	SizeBytes    int64
	Status       uploadpb.UploadStatus `gorm:"type:smallint"`
	ErrorMessage string
	UploadedAt   time.Time `gorm:"index"`
	UpdatedAt    time.Time
}
