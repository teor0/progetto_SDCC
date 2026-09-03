package api

import (
	"context"
	"errors"
	"io"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/upload"
	"testing"

	gallerypb "photogallery/gen/gallery"
	uploadpb "photogallery/gen/upload"
	"photogallery/internal/upload/events"
	"photogallery/internal/upload/mocks"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newAuthedContext builds a context carrying the incoming gRPC metadata
// that internal/auth.FromContext reads to resolve the caller's identity.
// Adjust this helper if your actual auth implementation resolves claims
// a different way (e.g. a context value set by an interceptor).
func newAuthedContext(userID uuid.UUID) context.Context {
	return auth.NewContext(context.Background(), &auth.Claims{
		UserID: userID,
		Role:   userpb.Role_ROLE_USER.String(),
	})
}

// expectRecvSequence queues up the standard "metadata, then one chunk,
// then EOF" sequence a well-behaved client sends.
func expectRecvSequence(stream *mocks.MockuploadStream, meta *uploadpb.UploadMetadata, chunk []byte) {
	gomock.InOrder(
		stream.EXPECT().Recv().Return(&uploadpb.UploadPhotoRequest{
			Payload: &uploadpb.UploadPhotoRequest_Metadata{Metadata: meta},
		}, nil),
		stream.EXPECT().Recv().Return(&uploadpb.UploadPhotoRequest{
			Payload: &uploadpb.UploadPhotoRequest_ChunkData{ChunkData: chunk},
		}, nil),
		stream.EXPECT().Recv().Return(nil, io.EOF),
	)
}

func TestUploadPhoto_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	stream := mocks.NewMockuploadStream(ctrl)
	galleryClient := mocks.NewMockGalleryServiceClient(ctrl)
	uploader := mocks.NewMockUploader(ctrl)
	notifier := mocks.NewMockNotifier(ctrl)

	galleryID := uuid.New()
	userID := uuid.New()

	ctx := newAuthedContext(userID)
	stream.EXPECT().Context().Return(ctx).AnyTimes()

	meta := &uploadpb.UploadMetadata{
		GalleryId:   galleryID.String(),
		Filename:    "photo.jpg",
		ContentType: "image/jpeg",
	}
	expectRecvSequence(stream, meta, []byte("fake-image-bytes"))

	galleryClient.EXPECT().
		IsMember(gomock.Any(), &gallerypb.IsMemberRequest{GalleryId: galleryID.String(), UserId: userID.String()}).
		Return(&gallerypb.IsMemberResponse{
			IsMember:      true,
			GalleryStatus: gallerypb.GalleryStatus_GALLERY_STATUS_OPEN,
		}, nil)

	// UploadPhoto also calls resolveMembers (-> ListMembers) to build the
	// notification fan-out list before publishing.
	galleryClient.EXPECT().
		ListMembers(gomock.Any(), &gallerypb.ListMembersRequest{GalleryId: galleryID.String()}).
		Return(&gallerypb.ListMembersResponse{
			Members: []*gallerypb.Member{{UserId: userID.String(), GalleryId: galleryID.String()}},
		}, nil)

	uploader.EXPECT().
		Upload(gomock.Any(), gomock.Any(), "image/jpeg", []byte("fake-image-bytes")).
		Return("https://minio.local/gallery-1/photo.jpg", nil)

	// PublishPhoto is fire-and-forget (no error return), so assert on the
	// event contents from inside Do rather than matching it up front.
	notifier.EXPECT().
		PublishPhoto(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, e *events.UploadEvent) {
			if e.GalleryID != galleryID || e.UploaderID != userID {
				t.Errorf("unexpected event: gallery=%s uploader=%s", e.GalleryID, e.UploaderID)
			}
		})

	stream.EXPECT().
		SendAndClose(gomock.Any()).
		DoAndReturn(func(resp *uploadpb.UploadPhotoResponse) error {
			if resp.Status != uploadpb.UploadStatus_UPLOAD_STATUS_COMPLETED {
				t.Errorf("expected status COMPLETED, got %v", resp.Status)
			}
			if resp.GalleryId != galleryID.String() {
				t.Errorf("expected gallery_id gallery-1, got %q", resp.GalleryId)
			}
			return nil
		})

	srv := NewServer(uploader, notifier, galleryClient, upload.NewInMemoryRepository())

	if err := srv.uploadPhoto(stream); err != nil {
		t.Fatalf("UploadPhoto returned unexpected error: %v", err)
	}
}

func TestUploadPhoto_RejectsNonMember(t *testing.T) {
	ctrl := gomock.NewController(t)

	stream := mocks.NewMockuploadStream(ctrl)
	galleryClient := mocks.NewMockGalleryServiceClient(ctrl)
	uploader := mocks.NewMockUploader(ctrl) // no calls expected
	notifier := mocks.NewMockNotifier(ctrl) // no calls expected

	galleryID := uuid.New()
	userID := uuid.New()
	ctx := newAuthedContext(userID)
	stream.EXPECT().Context().Return(ctx).AnyTimes()

	meta := &uploadpb.UploadMetadata{GalleryId: galleryID.String(), Filename: "x.jpg"}
	expectRecvSequence(stream, meta, []byte("bytes"))

	galleryClient.EXPECT().
		IsMember(gomock.Any(), gomock.Any()).
		Return(&gallerypb.IsMemberResponse{
			IsMember:      false,
			GalleryStatus: gallerypb.GalleryStatus_GALLERY_STATUS_OPEN,
		}, nil)

	// Deliberately no EXPECT() on uploader/notifier/stream.SendAndClose --
	// if UploadPhoto calls any of them, the test fails on an unexpected call.

	srv := NewServer(uploader, notifier, galleryClient, upload.NewInMemoryRepository())

	err := srv.uploadPhoto(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}
}

func TestUploadPhoto_RejectsClosedGallery(t *testing.T) {
	ctrl := gomock.NewController(t)

	stream := mocks.NewMockuploadStream(ctrl)
	galleryClient := mocks.NewMockGalleryServiceClient(ctrl)
	uploader := mocks.NewMockUploader(ctrl)
	notifier := mocks.NewMockNotifier(ctrl)

	galleryID := uuid.New()
	userID := uuid.New()
	ctx := newAuthedContext(userID)
	stream.EXPECT().Context().Return(ctx).AnyTimes()

	meta := &uploadpb.UploadMetadata{GalleryId: galleryID.String(), Filename: "x.jpg"}
	expectRecvSequence(stream, meta, []byte("bytes"))

	galleryClient.EXPECT().
		IsMember(gomock.Any(), gomock.Any()).
		Return(&gallerypb.IsMemberResponse{
			IsMember:      true,
			GalleryStatus: gallerypb.GalleryStatus_GALLERY_STATUS_CLOSED,
		}, nil)

	srv := NewServer(uploader, notifier, galleryClient, upload.NewInMemoryRepository())

	err := srv.uploadPhoto(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}
}

func TestUploadPhoto_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	stream := mocks.NewMockuploadStream(ctrl)
	galleryClient := mocks.NewMockGalleryServiceClient(ctrl)
	uploader := mocks.NewMockUploader(ctrl)
	notifier := mocks.NewMockNotifier(ctrl) // no calls expected: we never get to publish

	galleryID := uuid.New()
	userID := uuid.New()
	ctx := newAuthedContext(userID)
	stream.EXPECT().Context().Return(ctx).AnyTimes()

	meta := &uploadpb.UploadMetadata{GalleryId: galleryID.String(), Filename: "x.jpg"}
	expectRecvSequence(stream, meta, []byte("bytes"))

	galleryClient.EXPECT().
		IsMember(gomock.Any(), gomock.Any()).
		Return(&gallerypb.IsMemberResponse{
			IsMember:      true,
			GalleryStatus: gallerypb.GalleryStatus_GALLERY_STATUS_OPEN,
		}, nil)

	uploader.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return("", errors.New("minio: connection refused"))

	srv := NewServer(uploader, notifier, galleryClient, upload.NewInMemoryRepository())

	err := srv.uploadPhoto(stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got: %v", err)
	}
}

// TestIsMember_CircuitBreakerOpensAfterConsecutiveFailures verifies that
// once Gallery Service fails enough times in a row, isMember stops calling
// it at all and fails fast instead -- the actual behavior the breaker
// exists to provide.
func TestIsMember_CircuitBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	galleryClient := mocks.NewMockGalleryServiceClient(ctrl)

	// galleryMaxFailures is defined in grpc.go; referencing it directly
	// (same package) instead of a hardcoded number means this test can't
	// silently drift out of sync if that constant changes.
	galleryClient.EXPECT().
		IsMember(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("gallery service unreachable")).
		Times(galleryMaxFailures)

	srv := NewServer(nil, nil, galleryClient, nil)
	ctx := context.Background()

	galleryID := uuid.New()
	userID := uuid.New()

	var lastErr error
	for range galleryMaxFailures + 1 {
		_, _, lastErr = srv.isMember(ctx, galleryID, userID)
	}

	if status.Code(lastErr) != codes.Unavailable {
		t.Fatalf("expected Unavailable once breaker is open, got: %v", lastErr)
	}
}
