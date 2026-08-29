// Package model holds the internal representation of an upload, decoupled
// from any single wire format (gRPC request/response shapes change more
// often than the underlying record does).
package model

import (
	"time"

	uploadpb "photogallery/gen/upload"

	"github.com/google/uuid"
)

// Record is upload metadata, persisted so GetUploadStatus/ListUploads
// return correct results regardless of which Upload Service replica an
// earlier UploadPhoto -- or a later query -- happens to land on. The photo
// bytes live in MinIO and the "an upload happened" fact is durably
// recorded via RabbitMQ; this table exists purely for query convenience,
// which is exactly why an in-memory map was safe for one replica and
// exactly why it stopped being safe the moment more than one could be
// serving requests.
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
