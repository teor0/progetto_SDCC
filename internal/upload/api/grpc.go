package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	gallerypb "photogallery/gen/gallery"
	uploadpb "photogallery/gen/upload"
	"photogallery/internal/auth"
	"photogallery/internal/upload"
	"photogallery/internal/upload/events"
	"photogallery/internal/upload/storage"
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
)

type Server struct {
	uploadpb.UnimplementedUploadServiceServer
	storage        storage.Uploader
	publisher      events.Notifier
	galleryClient  gallerypb.GalleryServiceClient
	galleryBreaker *upload.CircuitBreaker
}

// NewServer constructs a Server with all required dependencies. storage and
// publisher are accepted as interfaces so tests can pass GoMock mocks; the
// real *storage.Storage and *events.Publisher types satisfy them unchanged.
func NewServer(storage storage.Uploader, publisher events.Notifier, galleryClient gallerypb.GalleryServiceClient) *Server {
	return &Server{
		storage:        storage,
		publisher:      publisher,
		galleryClient:  galleryClient,
		galleryBreaker: upload.NewCircuitBreaker(galleryMaxFailures, galleryResetTimeout),
	}
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

	galleryID, err := parseUUID(meta.GalleryId)
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
		meta.GalleryId, uploaderID, buf.Len(), meta.Filename)

	// ── 2. Store in MinIO ────────────────────────────────────────────────────
	photoID := uuid.NewString()
	objectKey := fmt.Sprintf("galleries/%s/%s", galleryID.String(), photoID)
	if meta.Filename != "" {
		objectKey = fmt.Sprintf("galleries/%s/%s_%s", meta.GalleryId, photoID, meta.Filename)
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

	log.Printf("UploadPhoto: stored photo_id=%s url=%s", photoID, photoURL)

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
		PhotoId:    photoID,
		GalleryId:  meta.GalleryId,
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
		if err == upload.ErrCircuitOpen {
			return false, gallerypb.GalleryStatus_GALLERY_STATUS_UNSPECIFIED,
				status.Error(codes.Unavailable, "gallery service unavailable (circuit breaker open)")
		}
		return false, gallerypb.GalleryStatus_GALLERY_STATUS_UNSPECIFIED,
			status.Errorf(codes.Internal, "checking membership: %v", err)
	}

	return resp.GetIsMember(), resp.GetGalleryStatus(), nil
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

func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
