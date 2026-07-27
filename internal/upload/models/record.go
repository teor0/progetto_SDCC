// Package model holds the internal representation of an upload, decoupled
// from any single wire format (gRPC request/response shapes change more
// often than the underlying record does).
package model

import (
	"time"

	uploadpb "photogallery/gen/upload"

	"github.com/google/uuid"
)

type Record struct {
	PhotoID      uuid.UUID
	GalleryID    uuid.UUID
	UploaderID   uuid.UUID
	Filename     string
	ContentType  string
	StorageKey   string
	SizeBytes    int64
	Status       uploadpb.UploadStatus
	ErrorMessage string
	UploadedAt   time.Time
	UpdatedAt    time.Time
}
