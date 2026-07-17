package models

import (
	"time"

	"github.com/google/uuid"
)

type Member struct {
	GalleryID uuid.UUID `gorm:"type:uuid;primaryKey;index"`
	UserID    uuid.UUID `gorm:"type:uuid;primaryKey;index"`
	JoinedAt  time.Time
}
