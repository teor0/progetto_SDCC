package handlers

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	uploadpb "photogallery/gen/upload"
	"photogallery/internal/clients"

	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const chunkSize = 64 * 1024 // 64KB

type UploadHandler struct {
	client         *clients.UploadClient
	minioPublicURL string
	minioBucket    string
}

type uploadSummaryDTO struct {
	PhotoID    string `json:"photoId"`
	GalleryID  string `json:"galleryId"`
	UploaderID string `json:"uploaderUserId"`
	SizeBytes  int64  `json:"sizeBytes"`
	Status     string `json:"status"`
	UploadedAt string `json:"uploadedAt"`
	URL        string `json:"url"`
}

func NewUploadHandler(client *clients.UploadClient, minioPublicURL, minioBucket string) *UploadHandler {
	return &UploadHandler{
		client:         client,
		minioPublicURL: minioPublicURL,
		minioBucket:    minioBucket,
	}
}

// respondWithGRPCError maps a gRPC status error to the same HTTP status
// grpc-gateway itself uses for every other proxied endpoint in this
// project (User Service, Gallery Service), via runtime.HTTPStatusFromCode
// -- so "not a member" (InvalidArgument), a tripped circuit breaker
// (Unavailable), a missing gallery (NotFound), etc. all now produce the
// SAME HTTP status here as they would through grpc-gateway directly,
// instead of this handler flattening every failure to a bare 500
// regardless of what actually went wrong.
func respondWithGRPCError(c *gin.Context, err error) {
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status error at all (e.g. a raw network/connection
		// error) -- codes.Unknown maps to 500, the correct fallback
		// outside the gRPC error model.
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(runtime.HTTPStatusFromCode(st.Code()), gin.H{"error": st.Message()})
}

// respondFromStreamSendError handles a failed stream.Send call during an
// upload. gRPC-Go's client-streaming semantics mean that if the SERVER
// already returned an error and closed the stream, the client's next
// Send() call surfaces as plain io.EOF -- NOT the server's actual status;
// the real error is only obtainable via CloseAndRecv(). Reporting io.EOF
// directly would turn a legitimate rejection (not a member, gallery
// closed, etc.) into a useless bare 500 "EOF".
//
// In this handler's current protocol (metadata message, then chunks, with
// the server's business-logic checks only running after ALL chunks have
// been received via CloseSend -- see internal/upload/api/grpc.go), the
// server in practice never returns before the client finishes sending, so
// this io.EOF path isn't exercised by anything the server checks today.
// It's handled correctly anyway, since this is standard documented
// gRPC-Go behavior and leaving it unhandled would be a landmine for the
// day an early validation check gets added server-side.
func respondFromStreamSendError(c *gin.Context, stream uploadpb.UploadService_UploadPhotoClient, sendErr error) {
	if errors.Is(sendErr, io.EOF) {
		if _, err := stream.CloseAndRecv(); err != nil {
			respondWithGRPCError(c, err)
			return
		}
		// Send returned EOF but CloseAndRecv reported success -- extremely
		// unlikely, but don't pretend the upload succeeded from a code
		// path that bailed out early.
		c.JSON(http.StatusInternalServerError, gin.H{"error": "upload stream closed unexpectedly"})
		return
	}
	respondWithGRPCError(c, sendErr)
}

func (h *UploadHandler) UploadPhoto(c *gin.Context) {
	file, header, err := c.Request.FormFile("photo")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "photo is required"})
		return
	}
	defer file.Close()

	galleryID := c.PostForm("galleryId")
	if galleryID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "galleryId is required"})
		return
	}

	// Forward browser JWT to UploadService.
	authHeader := c.GetHeader("Authorization")
	ctx := metadata.NewOutgoingContext(
		c.Request.Context(),
		metadata.Pairs("authorization", authHeader),
	)

	stream, err := h.client.Upload.UploadPhoto(ctx)
	if err != nil {
		respondWithGRPCError(c, err)
		return
	}

	// First message: metadata
	if err := stream.Send(&uploadpb.UploadPhotoRequest{
		Payload: &uploadpb.UploadPhotoRequest_Metadata{
			Metadata: &uploadpb.UploadMetadata{
				GalleryId:      galleryID,
				Filename:       header.Filename,
				ContentType:    header.Header.Get("Content-Type"),
				TotalSizeBytes: header.Size,
			},
		},
	}); err != nil {
		respondFromStreamSendError(c, stream, err)
		return
	}

	// Photo chunks
	buffer := make([]byte, chunkSize)
	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			if err := stream.Send(&uploadpb.UploadPhotoRequest{
				Payload: &uploadpb.UploadPhotoRequest_ChunkData{ChunkData: buffer[:n]},
			}); err != nil {
				respondFromStreamSendError(c, stream, err)
				return
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			// A local read failure (e.g. the client disconnected
			// mid-upload) -- not a gRPC status from the server, so
			// there's nothing to map; a generic 500 is the right
			// fallback here.
			c.JSON(http.StatusInternalServerError, gin.H{"error": readErr.Error()})
			return
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		respondWithGRPCError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"photoId": response.PhotoId,
	})
}

// ListUploads returns a gallery's photos with browser-usable URLs,
// reconstructed from each object's storage_key rather than trusting any
// stored URL -- none is persisted (model.Record has no URL field), and
// UploadPhotoResponse's own url is built from MinIO's internal Docker
// hostname, which a browser can never resolve.
func (h *UploadHandler) ListUploads(c *gin.Context) {
	galleryID := c.Param("galleryId")
	if galleryID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "galleryId is required"})
		return
	}

	ctx := metadata.NewOutgoingContext(
		c.Request.Context(),
		metadata.Pairs("authorization", c.GetHeader("Authorization")),
	)

	resp, err := h.client.Upload.ListUploads(ctx, &uploadpb.ListUploadsRequest{
		GalleryId: galleryID,
		PageSize:  100,
	})
	if err != nil {
		// Same fix applied here as UploadPhoto: this used to be a flat
		// http.StatusBadGateway for every failure -- an unmapped gallery
		// vs. an internal error looked identical to the caller.
		respondWithGRPCError(c, err)
		return
	}

	uploads := make([]uploadSummaryDTO, 0, len(resp.Uploads))
	for _, u := range resp.Uploads {
		uploads = append(uploads, uploadSummaryDTO{
			PhotoID:    u.GetPhotoId(),
			GalleryID:  u.GetGalleryId(),
			UploaderID: u.GetUploaderUserId(),
			SizeBytes:  u.GetSizeBytes(),
			Status:     u.GetStatus().String(),
			UploadedAt: u.GetUploadedAt().AsTime().Format(time.RFC3339),
			URL:        fmt.Sprintf("%s/%s/%s", h.minioPublicURL, h.minioBucket, u.GetStorageKey()),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"uploads":       uploads,
		"nextPageToken": resp.NextPageToken,
	})
}
