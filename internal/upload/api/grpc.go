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
	"photogallery/internal/upload/events"
	"photogallery/internal/upload/storage"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxPhotoBytes = 25 * 1024 * 1024 // 25 MB

type Server struct {
	uploadpb.UnimplementedUploadServiceServer
	storage       *storage.Storage
	publisher     *events.Publisher
	galleryClient gallerypb.GalleryServiceClient
}

// NewServer constructs a Server with all required dependencies.
func NewServer(storage *storage.Storage, publisher *events.Publisher, galleryClient gallerypb.GalleryServiceClient) *Server {
	return &Server{
		storage:       storage,
		publisher:     publisher,
		galleryClient: galleryClient,
	}
}

// UploadPhoto receives a client-streaming sequence of chunks:
//  1. First chunk must contain UploadMetadata.
//  2. All subsequent chunks carry raw binary data.
//
// On completion it:
//   - stores the assembled image in MinIO
//   - resolves gallery members via GalleryService.ListMembers
//   - publishes a UploadedEvent to RabbitMQ (circuit-breaker protected)
//   - returns an UploadResponse to the caller
func (s *Server) UploadPhoto(stream uploadpb.UploadService_UploadPhotoServer) error {
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

	membership, galleryStatus, err := s.isMember(meta.GalleryId, uploaderID)
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
	objectKey := fmt.Sprintf("galleries/%s/%s", meta.GalleryId, photoID)
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

	memberIDs := s.resolveMembers(meta.GalleryId)

	// ── 4. Publish event (circuit-breaker protected, fire-and-forget) ────────
	s.publisher.PublishPhoto(stream.Context(), &events.UploadEvent{
		PhotoID:     photoID,
		GalleryID:   meta.GalleryId,
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
func (s *Server) resolveMembers(galleryID string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := s.galleryClient.ListMembers(ctx, &gallerypb.ListMembersRequest{
		GalleryId: galleryID,
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

func (s *Server) isMember(galleryID string, userID string) (bool, gallerypb.GalleryStatus, error) {
	resp, err := s.galleryClient.IsMember(context.Background(), &gallerypb.IsMemberRequest{
		GalleryId: galleryID,
		UserId:    userID,
	})
	if err != nil {
		return resp.GetIsMember(), resp.GetGalleryStatus(), err
	}
	return resp.IsMember, resp.GalleryStatus, err
}

// uploaderFromMeta extracts x-user-id from the incoming gRPC metadata set by
// the API Gateway after JWT validation.
func uploaderFromMeta(ctx context.Context) (string, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return "", err
	}
	return claims.UserID, nil
}

// isEOF checks whether err signals the end of a client stream.
func isEOF(err error) bool {
	return err != nil && errors.Is(err, io.EOF)
}
