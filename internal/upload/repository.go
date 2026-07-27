// Package repository stores upload metadata queried back out via
// GetUploadStatus and ListUploads.
// The photo bytes themselves live durably in MinIO, and the "an upload
// happened" fact is durably recorded as a RabbitMQ event -- so the
// in-memory implementation below only affects query convenience, not
// correctness of the write path. That said, it will NOT survive a
// restart and will NOT be shared across multiple Upload Service
// replicas. Swap InMemoryRepository for a Postgres-backed implementation
// (same Repository interface) before running more than one replica or
// caring about history surviving a redeploy.
package upload

import (
	"context"
	"photogallery/internal/upload/models"
	"sync"

	"github.com/google/uuid"
)

type Repository interface {
	Save(ctx context.Context, rec *model.Record) error
	Get(ctx context.Context, photoID uuid.UUID) (*model.Record, bool, error)
	// ListByGallery returns up to `limit` records for a gallery, most
	// recent first, starting after `offset` records, plus the total
	// count of records for that gallery (for pagination).
	ListByGallery(ctx context.Context, galleryID uuid.UUID, offset, limit int) ([]*model.Record, int, error)
}

type InMemoryRepository struct {
	mu           sync.RWMutex
	records      map[uuid.UUID]*model.Record
	galleryIndex map[uuid.UUID][]uuid.UUID // gallery_id -> photo_ids, insertion order
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		records:      make(map[uuid.UUID]*model.Record),
		galleryIndex: make(map[uuid.UUID][]uuid.UUID),
	}
}

func (r *InMemoryRepository) Save(_ context.Context, rec *model.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.records[rec.PhotoID]; !exists {
		r.galleryIndex[rec.GalleryID] = append(r.galleryIndex[rec.GalleryID], rec.PhotoID)
	}
	copyRec := *rec
	r.records[rec.PhotoID] = &copyRec
	return nil
}

func (r *InMemoryRepository) Get(_ context.Context, photoID uuid.UUID) (*model.Record, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rec, ok := r.records[photoID]
	if !ok {
		return nil, false, nil
	}
	copyRec := *rec
	return &copyRec, true, nil
}

func (r *InMemoryRepository) ListByGallery(_ context.Context, galleryID uuid.UUID, offset, limit int) ([]*model.Record, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := r.galleryIndex[galleryID]
	total := len(ids)

	// most recent first
	reversed := make([]uuid.UUID, total)
	for i, id := range ids {
		reversed[total-1-i] = id
	}

	if offset > total {
		offset = total
	}
	end := offset + limit
	if limit <= 0 || end > total {
		end = total
	}

	page := reversed[offset:end]
	out := make([]*model.Record, 0, len(page))
	for _, id := range page {
		if rec, ok := r.records[id]; ok {
			copyRec := *rec
			out = append(out, &copyRec)
		}
	}
	return out, total, nil
}
