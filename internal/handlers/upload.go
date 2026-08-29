package handlers

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	uploadpb "photogallery/gen/upload"
	"photogallery/internal/clients"

	"google.golang.org/grpc/metadata"
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

func (h *UploadHandler) UploadPhoto(c *gin.Context) {

	file, header, err := c.Request.FormFile("photo")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "photo is required",
		})
		return
	}
	defer file.Close()
	galleryID := c.PostForm("galleryId")
	if galleryID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "galleryId is required",
		})
		return
	}
	fmt.Println(c.GetHeader("Authorization"))

	// Forward browser JWT to UploadService.
	authHeader := c.GetHeader("Authorization")
	//token := strings.TrimPrefix(authHeader, "Bearer ")
	//token = strings.TrimSpace(token)
	//fmt.Println(token)
	ctx := c.Request.Context()
	ctx = metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs(
			"authorization",
			authHeader,
		),
	)
	stream, err := h.client.Upload.UploadPhoto(ctx)
	if err != nil {
		c.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}
	// First message: metadata
	err = stream.Send(
		&uploadpb.UploadPhotoRequest{
			Payload: &uploadpb.UploadPhotoRequest_Metadata{
				Metadata: &uploadpb.UploadMetadata{
					GalleryId: galleryID,
					Filename:  header.Filename,
					ContentType: header.Header.Get(
						"Content-Type",
					),
					TotalSizeBytes: header.Size,
				},
			},
		},
	)
	if err != nil {
		c.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}
	// Photo chunks
	buffer := make([]byte, chunkSize)
	for {
		n, err := file.Read(buffer)
		if n > 0 {

			err = stream.Send(
				&uploadpb.UploadPhotoRequest{
					Payload: &uploadpb.UploadPhotoRequest_ChunkData{
						ChunkData: buffer[:n],
					},
				},
			)
			if err != nil {
				c.JSON(500, gin.H{
					"error": err.Error(),
				})
				return
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			c.JSON(500, gin.H{
				"error": err.Error(),
			})
			return
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		c.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"photoId": response.PhotoId,
	})
}

// ListUploads returns a gallery's photos
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
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
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
