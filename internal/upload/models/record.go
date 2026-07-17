// Package model holds the internal representation of an upload, decoupled
// from any single wire format (gRPC request/response shapes change more
// often than the underlying record does).
package model

import (
	"time"

	uploadpb "photogallery/gen/upload"
)

type Record struct {
	PhotoID      string
	GalleryID    string
	UploaderID   string
	Filename     string
	ContentType  string
	StorageKey   string
	SizeBytes    int64
	Status       uploadpb.UploadStatus
	ErrorMessage string
	UploadedAt   time.Time
	UpdatedAt    time.Time
}
