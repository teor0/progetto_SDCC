package handlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	uploadpb "photogallery/gen/upload"
	"photogallery/internal/clients"

	"google.golang.org/grpc/metadata"
)

const chunkSize = 64 * 1024 // 64KB

type UploadHandler struct {
	client *clients.UploadClient
}

func NewUploadHandler(client *clients.UploadClient) *UploadHandler {
	return &UploadHandler{
		client: client,
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
