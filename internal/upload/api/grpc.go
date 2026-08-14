package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	gallerypb "photogallery/gen/gallery"
	uploadpb "photogallery/gen/upload"
	"photogallery/internal/auth"
	"photogallery/internal/upload"
	"photogallery/internal/upload/events"
	model "photogallery/internal/upload/models"
	"photogallery/internal/upload/storage"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	galleryMaxFailures  = 3
	galleryResetTimeout = 10 * time.Second
	galleryCallTimeout  = 2 * time.Second
	maxPhotoBytes       = 25 * 1024 * 1024 // 25 MB

	defaultListPageSize = 20
	maxListPageSize     = 100
)

type Server struct {
	uploadpb.UnimplementedUploadServiceServer
	storage        storage.Uploader
	publisher      events.Notifier
	galleryClient  gallerypb.GalleryServiceClient
	galleryBreaker *upload.CircuitBreaker
	repo           upload.Repository
	jwtSecret      string
	minioPublicURL string
	minioBucket    string
}

// NewServer constructs a Server with all required dependencies. storage,
// publisher, and repo are accepted as interfaces so tests can pass GoMock
// mocks; the real *storage.Storage, *events.Publisher, and
// *upload.InMemoryRepository types satisfy them unchanged. repo may be
// nil, in which case an InMemoryRepository is used -- convenient for tests
// and for the current single-replica deployment; swap it for a
// Postgres-backed implementation before running more than one replica.
func NewServer(storage storage.Uploader, publisher events.Notifier, galleryClient gallerypb.GalleryServiceClient, repo upload.Repository) *Server {
	if repo == nil {
		repo = upload.NewInMemoryRepository()
	}
	return &Server{
		storage:        storage,
		publisher:      publisher,
		galleryClient:  galleryClient,
		galleryBreaker: upload.NewCircuitBreaker(galleryMaxFailures, galleryResetTimeout),
		repo:           repo,
		jwtSecret:      os.Getenv("JWT_SECRET"),
		minioPublicURL: os.Getenv("MINIO_PUBLIC_URL"),
		minioBucket:    os.Getenv("MINIO_BUCKET"),
	}
}

var publicMethods = map[string]bool{
	"/proto.UploadService/HealthCheck": true,
}

func (s *Server) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	if publicMethods[fullMethodName] {
		return ctx, nil
	}
	return auth.AuthFunc(s.jwtSecret)(ctx)
}

// uploadStream is the subset of uploadpb.UploadService_UploadPhotoServer
// that UploadPhoto actually uses. Depending on your grpc-go /
// protoc-gen-go-grpc version, the generated stream interface may be a type
// alias to a generic grpc.ClientStreamingServer[Req, Res] rather than a
// plain named interface -- GoMock can struggle to mock those directly,
// producing a mock type with unfilled type parameters. Defining our own
// minimal, non-generic interface sidesteps that: the real stream value
// satisfies it automatically (Go interfaces are structural), and tests can
// mock this instead without caring what grpc-go's internals look like.
type uploadStream interface {
	Context() context.Context
	Recv() (*uploadpb.UploadPhotoRequest, error)
	SendAndClose(*uploadpb.UploadPhotoResponse) error
}

// UploadPhoto receives a client-streaming sequence of chunks:
//  1. First chunk must contain UploadMetadata.
//  2. All subsequent chunks carry raw binary data.
//
// On completion, it:
//   - stores the assembled image in MinIO
//   - resolves gallery members via GalleryService.ListMembers
//   - publishes a UploadedEvent to RabbitMQ (circuit-breaker protected)
//   - returns an UploadResponse to the caller
func (s *Server) UploadPhoto(stream uploadpb.UploadService_UploadPhotoServer) error {
	return s.uploadPhoto(stream)
}

func (s *Server) uploadPhoto(stream uploadStream) error {
	// Extract caller identity forwarded by the gateway.
	uploaderID, err := uploaderFromMeta(stream.Context())
	if err != nil {
		return err
	}
	// ── 1. Receive all chunks ────────────────────────────────────────────────
	var (
		meta *uploadpb.UploadMetadata
		buf  bytes.Buffer
		//hasher = sha256.New()
	)

	for {
		chunk, err := stream.Recv()
		if err != nil {
			// io.EOF signals the client has finished sending.
			if isEOF(err) {
				break
			}
			return status.Errorf(codes.Internal, "recv chunk: %v", err)
		}

		switch p := chunk.Payload.(type) {
		case *uploadpb.UploadPhotoRequest_Metadata:
			if meta != nil {
				return status.Error(codes.InvalidArgument, "metadata chunk sent more than once")
			}
			meta = p.Metadata
			if meta.TotalSizeBytes > maxPhotoBytes {
				return status.Errorf(codes.InvalidArgument,
					"declared size %d exceeds max of %d bytes", meta.TotalSizeBytes, maxPhotoBytes)
			}
		case *uploadpb.UploadPhotoRequest_ChunkData:
			if meta == nil {
				return status.Error(codes.InvalidArgument, "data chunk received before metadata")
			}
			// Check BEFORE writing, so a malicious/broken client can't force
			// us to hold more than the cap in memory even momentarily.
			if buf.Len()+len(p.ChunkData) > maxPhotoBytes {
				return status.Errorf(codes.InvalidArgument,
					"upload exceeds max size of %d bytes", maxPhotoBytes)
			}
			buf.Write(p.ChunkData)
			//hasher.Write(p.ChunkData)
		default:
			return status.Error(codes.InvalidArgument, "unknown chunk payload type")
		}
	}

	if meta == nil {
		return status.Error(codes.InvalidArgument, "no metadata received")
	}
	if meta.GalleryId == "" {
		return status.Error(codes.InvalidArgument, "gallery_id is required")
	}
	if buf.Len() == 0 {
		return status.Error(codes.InvalidArgument, "no image data received")
	}

	// Declared vs. actual size: catch truncated/partial streams and clients
	// lying about total_size_bytes.
	if meta.TotalSizeBytes > 0 && int64(buf.Len()) != meta.TotalSizeBytes {
		return status.Errorf(codes.InvalidArgument,
			"declared size %d does not match received size %d", meta.TotalSizeBytes, buf.Len())
	}

	// Checksum is optional — only verify if the client actually sent one.
	/*if meta.ChecksumSha256 != "" {
		sum := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(sum, meta.ChecksumSha256) {
			return status.Errorf(codes.DataLoss,
				"checksum mismatch: expected %s, got %s", meta.ChecksumSha256, sum)
		}
	}*/

	galleryID, _ := uuid.Parse(meta.GalleryId)
	if err != nil {
		return status.Error(codes.InvalidArgument, "gallery_id is invalid")
	}

	membership, galleryStatus, err := s.isMember(stream.Context(), galleryID, uploaderID)
	if err != nil {
		return err
	}
	if !membership {
		return status.Error(codes.InvalidArgument, "user is not a member of the gallery")
	}
	if galleryStatus == gallerypb.GalleryStatus_GALLERY_STATUS_CLOSED {
		return status.Error(codes.InvalidArgument, "gallery is closed")
	}

	log.Printf("UploadPhoto: gallery=%s uploader=%s size=%d bytes filename=%s",
		galleryID, uploaderID, buf.Len(), meta.Filename)

	// ── 2. Store in MinIO ────────────────────────────────────────────────────
	photoID := uuid.New()
	objectKey := fmt.Sprintf("galleries/%s/%s", galleryID.String(), photoID.String())
	if meta.Filename != "" {
		objectKey = fmt.Sprintf("galleries/%s/%s_%s", galleryID, photoID, meta.Filename)
	}

	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	photoURL, err := s.storage.Upload(stream.Context(), objectKey, contentType, buf.Bytes())
	if err != nil {
		log.Printf("UploadPhoto: storage error: %v", err)
		return status.Errorf(codes.Internal, "store photo: %v", err)
	}

	if s.minioPublicURL != "" {
		photoURL = fmt.Sprintf("%s/%s/%s", s.minioPublicURL, s.minioBucket, objectKey)
	}

	log.Printf("UploadPhoto: stored photo_id=%s url=%s", photoID, photoURL)

	// Persist a queryable record for GetUploadStatus/ListUploads. This is
	// best-effort: the RabbitMQ event below is the durable "an upload
	// happened" fact, so a save failure here shouldn't fail the request --
	// it just means this upload won't show up in status/list queries.
	now := time.Now().UTC()
	rec := &model.Record{
		PhotoID:     photoID,
		GalleryID:   galleryID,
		UploaderID:  uploaderID,
		Filename:    meta.Filename,
		ContentType: contentType,
		StorageKey:  objectKey,
		SizeBytes:   int64(buf.Len()),
		Status:      uploadpb.UploadStatus_UPLOAD_STATUS_COMPLETED,
		UploadedAt:  now,
		UpdatedAt:   now,
	}
	if err := s.repo.Save(stream.Context(), rec); err != nil {
		log.Printf("UploadPhoto: failed to save upload record photo_id=%s: %v", photoID, err)
	}

	memberIDs := s.resolveMembers(galleryID)

	// ── 4. Publish event (circuit-breaker protected, fire-and-forget) ────────
	s.publisher.PublishPhoto(stream.Context(), &events.UploadEvent{
		PhotoID:     photoID,
		GalleryID:   galleryID,
		UploaderID:  uploaderID,
		StorageKey:  objectKey,
		PhotoURL:    photoURL,
		ContentType: contentType,
		SizeBytes:   int64(buf.Len()),
		MemberIDs:   memberIDs,
		UploadedAt:  time.Now().UTC(),
	})

	return stream.SendAndClose(&uploadpb.UploadPhotoResponse{
		PhotoId:    photoID.String(),
		GalleryId:  galleryID.String(),
		StorageKey: objectKey,
		SizeBytes:  int64(buf.Len()),
		Url:        photoURL,
		Status:     uploadpb.UploadStatus_UPLOAD_STATUS_COMPLETED,
		UploadedAt: timestamppb.Now(),
	})
}

// resolveMembers calls GalleryService.ListMembers and returns the member IDs.
// On any error it logs and returns an empty slice — the upload still succeeds,
// but no notifications will be delivered for this event.
func (s *Server) resolveMembers(galleryID uuid.UUID) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := s.galleryClient.ListMembers(ctx, &gallerypb.ListMembersRequest{
		GalleryId: galleryID.String(),
	})
	if err != nil {
		log.Printf("resolveMembers: ListMembers failed for gallery=%s: %v", galleryID, err)
		return nil
	}

	ids := make([]string, 0, len(resp.Members))
	for _, m := range resp.Members {
		ids = append(ids, m.UserId)
	}
	return ids
}

// isMember calls GalleryService.IsMember through the gallery circuit
// breaker. ctx should be the caller's context (e.g. stream.Context()) so
// that if the client disconnects or the parent request is cancelled, that
// cancellation actually propagates into the outbound call instead of it
// running to its own independent timeout regardless.
//
// If the breaker is open (or trips as a result of this call), it returns
// a gRPC Unavailable error — the caller treats that the same as any other
// rejection, it just doesn't retry synchronously against a dependency
// that's already known to be unhealthy.
func (s *Server) isMember(ctx context.Context, galleryID, userID uuid.UUID) (bool, gallerypb.GalleryStatus, error) {
	var resp *gallerypb.IsMemberResponse

	err := s.galleryBreaker.Call(func() error {
		callCtx, cancel := context.WithTimeout(ctx, galleryCallTimeout)
		defer cancel()

		r, err := s.galleryClient.IsMember(callCtx, &gallerypb.IsMemberRequest{
			GalleryId: galleryID.String(),
			UserId:    userID.String(),
		})
		if err != nil {
			return err
		}
		resp = r
		return nil
	})

	if err != nil {
		if errors.Is(err, upload.ErrCircuitOpen) {
			return false, gallerypb.GalleryStatus_GALLERY_STATUS_UNSPECIFIED,
				status.Error(codes.Unavailable, "gallery service unavailable (circuit breaker open)")
		}
		return false, gallerypb.GalleryStatus_GALLERY_STATUS_UNSPECIFIED,
			status.Errorf(codes.Internal, "checking membership: %v", err)
	}

	return resp.GetIsMember(), resp.GetGalleryStatus(), nil
}

// GetUploadStatus returns the persisted status of a single upload.
func (s *Server) GetUploadStatus(ctx context.Context, req *uploadpb.GetUploadStatusRequest) (*uploadpb.GetUploadStatusResponse, error) {
	if req.GetPhotoId() == "" {
		return nil, status.Error(codes.InvalidArgument, "photo_id is required")
	}

	photoID, _ := uuid.Parse(req.GetPhotoId())

	rec, found, err := s.repo.Get(ctx, photoID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get upload status: %v", err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "upload not found")
	}

	return &uploadpb.GetUploadStatusResponse{
		PhotoId:      rec.PhotoID.String(),
		GalleryId:    rec.GalleryID.String(),
		Status:       rec.Status,
		ErrorMessage: rec.ErrorMessage,
		UpdatedAt:    timestamppb.New(rec.UpdatedAt),
	}, nil
}

// ListUploads returns a page of uploads for a gallery, most recent first.
// Pagination is offset-based: page_token is the decimal offset to resume
// from, and next_page_token comes back empty once the last page has been
// returned.
func (s *Server) ListUploads(ctx context.Context, req *uploadpb.ListUploadsRequest) (*uploadpb.ListUploadsResponse, error) {
	if req.GetGalleryId() == "" {
		return nil, status.Error(codes.InvalidArgument, "gallery_id is required")
	}

	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}

	limit := int(req.GetPageSize())
	if limit <= 0 {
		limit = defaultListPageSize
	}
	if limit > maxListPageSize {
		limit = maxListPageSize
	}

	galleryID, _ := uuid.Parse(req.GetGalleryId())

	records, total, err := s.repo.ListByGallery(ctx, galleryID, offset, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list uploads: %v", err)
	}

	resp := &uploadpb.ListUploadsResponse{
		Uploads: make([]*uploadpb.UploadSummary, 0, len(records)),
	}
	for _, rec := range records {
		resp.Uploads = append(resp.Uploads, &uploadpb.UploadSummary{
			PhotoId:        rec.PhotoID.String(),
			GalleryId:      rec.GalleryID.String(),
			UploaderUserId: rec.UploaderID.String(),
			StorageKey:     rec.StorageKey,
			SizeBytes:      rec.SizeBytes,
			Status:         rec.Status,
			UploadedAt:     timestamppb.New(rec.UploadedAt),
		})
	}

	if next := offset + len(records); next < total {
		resp.NextPageToken = strconv.Itoa(next)
	}

	return resp, nil
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid page token %q", token)
	}
	return offset, nil
}

// HealthCheck reports SERVING as long as the process is up and able to
// answer gRPC calls. It deliberately does not probe MinIO, RabbitMQ, or
// Gallery Service -- those already have their own failure handling (the
// gallery circuit breaker, and the publisher's own breaker + reconnect
// logic), and a health check that itself blocks on a flaky dependency
// defeats the point of a fast liveness probe.
func (s *Server) HealthCheck(_ context.Context, _ *uploadpb.HealthCheckRequest) (*uploadpb.HealthCheckResponse, error) {
	return &uploadpb.HealthCheckResponse{
		Status: uploadpb.HealthCheckResponse_SERVING,
	}, nil
}

// uploaderFromMeta extracts x-user-id from the incoming gRPC metadata set by
// the API Gateway after JWT validation.
func uploaderFromMeta(ctx context.Context) (uuid.UUID, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	return claims.UserID, nil
}

// isEOF checks whether err signals the end of a client stream.
func isEOF(err error) bool {
	return err != nil && errors.Is(err, io.EOF)
}
