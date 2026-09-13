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

// uploadSummaryDTO is the JSON shape for browser usage.
type uploadSummaryDTO struct {
	PhotoID    string `json:"photoId"`
	GalleryID  string `json:"galleryId"`
	UploaderID string `json:"uploaderUserId"`
	SizeBytes  int64  `json:"sizeBytes"`
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
func respondWithGRPCError(c *gin.Context, err error) {
	st, ok := status.FromError(err)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(runtime.HTTPStatusFromCode(st.Code()), gin.H{"error": st.Message()})
}

func respondFromStreamSendError(c *gin.Context, stream uploadpb.UploadService_UploadPhotoClient, sendErr error) {
	if errors.Is(sendErr, io.EOF) {
		if _, err := stream.CloseAndRecv(); err != nil {
			respondWithGRPCError(c, err)
			return
		}
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
			// client disconnected mid-upload
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

// ListUploads returns a gallery's photos with browser-usable URLs
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
			UploadedAt: u.GetUploadedAt().AsTime().Format(time.RFC3339),
			URL:        fmt.Sprintf("%s/%s/%s", h.minioPublicURL, h.minioBucket, u.GetStorageKey()),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"uploads":       uploads,
		"nextPageToken": resp.NextPageToken,
	})
}
