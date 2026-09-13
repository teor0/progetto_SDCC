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
// mocks or a plain in-memory implementation; the real *storage.Storage,
// *events.Publisher, and *upload.PostgresRepository types satisfy them
// unchanged. repo must be provided explicitly -- callers that don't need
// persistence (most unit tests) should pass upload.NewInMemoryRepository()
// directly. There used to be an implicit "if repo == nil, use in-memory"
// fallback here; removed because it was production-constructor logic that
// existed purely to serve test convenience, and silently masked the same
// class of bug the Postgres migration was meant to close: a real code
// path that forgot to wire up real persistence would fail loudly now
// (nil pointer on first Save/Get/ListByGallery call) instead of quietly
// losing data on every restart.
func NewServer(storage storage.Uploader, publisher events.Notifier, galleryClient gallerypb.GalleryServiceClient, repo upload.Repository) *Server {
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
	uploaderID, err := uploaderFromMeta(stream.Context())
	if err != nil {
		return err
	}
	var (
		meta *uploadpb.UploadMetadata
		buf  bytes.Buffer
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

	if meta.TotalSizeBytes > 0 && int64(buf.Len()) != meta.TotalSizeBytes {
		return status.Errorf(codes.InvalidArgument,
			"declared size %d does not match received size %d", meta.TotalSizeBytes, buf.Len())
	}

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

	// Persist a queryable record for GetUploadStatus/ListUploads.
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
// On any error it logs and returns an empty slice the upload still succeeds,
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
// If the breaker is open it returns a gRPC Unavailable error
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

// ListUploads returns a page of uploads for a gallery, most recent first.
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

// HealthCheck reports SERVING as long as the process is up and able to answer gRPC calls.
func (s *Server) HealthCheck(_ context.Context, _ *uploadpb.HealthCheckRequest) (*uploadpb.HealthCheckResponse, error) {
	return &uploadpb.HealthCheckResponse{
		Status: uploadpb.HealthCheckResponse_SERVING,
	}, nil
}

func uploaderFromMeta(ctx context.Context) (uuid.UUID, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	return claims.UserID, nil
}

func isEOF(err error) bool {
	return err != nil && errors.Is(err, io.EOF)
}
