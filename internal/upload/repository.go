package upload

import (
	"context"
	"photogallery/internal/upload/models"
	"sync"

	"github.com/google/uuid"
)

// SWAP WITH POSTGRES
type Repository interface {
	Save(ctx context.Context, rec *model.Record) error
	Get(ctx context.Context, photoID uuid.UUID) (*model.Record, bool, error)
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
