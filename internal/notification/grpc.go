package notification

import (
	"context"
	"errors"
	gallerypb "photogallery/gen/gallery"
	notificationpb "photogallery/gen/notification"
	"photogallery/internal/auth"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// membershipPageSize batches ListGalleriesByMember calls when resolving a
// subscriber's memberships.
const membershipPageSize = 100

type Server struct {
	notificationpb.UnimplementedNotificationServiceServer
	registry      *Registry
	galleryClient gallerypb.GalleryServiceClient
}

func NewServer(registry *Registry, galleryClient gallerypb.GalleryServiceClient) *Server {
	return &Server{
		registry:      registry,
		galleryClient: galleryClient,
	}
}

// Subscribe opens a persistent server-streaming connection: it resolves
// every gallery the caller belongs to, registers the stream against each
// one so Consumer.Consume can fan events out to it, then blocks until the
// client disconnects. Notification delivery itself happens out-of-band --
// this method's only job is registration and cleanup, not sending.
func (s *Server) Subscribe(
	_ *notificationpb.SubscribeRequest,
	stream notificationpb.NotificationService_SubscribeServer,
) error {
	ctx := stream.Context()

	claims, err := auth.FromContext(ctx)
	if err != nil {
		return err
	}

	userID := claims.UserID

	if userID == uuid.Nil {
		return status.Error(
			codes.Unauthenticated,
			"missing caller identity",
		)
	}

	galleryIDs, err := s.resolveMemberships(ctx, userID)
	if err != nil {
		return err
	}

	// One connection ID for the entire streaming RPC.
	connectionID := s.registry.CreateClient(userID, stream)

	// Register this connection for every gallery
	// the user currently belongs to.
	for _, galleryID := range galleryIDs {
		s.registry.Subscribe(
			connectionID,
			galleryID,
		)
	}

	// When THIS browser stream disconnects, remove
	// only THIS connection.
	defer s.registry.RemoveClient(connectionID)

	<-ctx.Done()

	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}

	return ctx.Err()
}

// resolveMemberships asks Gallery Service which galleries userID belongs
// to via the dedicated internal query, rather than paginating over every
// gallery and filtering client-side. Unlike a plain ListGalleries call,
// this doesn't need any identity forwarding: userID is passed explicitly,
// the same trust model as IsMember.
func (s *Server) resolveMemberships(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	var galleryIDs []uuid.UUID

	pageToken := ""

	for {
		resp, err := s.galleryClient.ListGalleriesByMember(ctx, &gallerypb.ListGalleriesByMemberRequest{
			UserId:    userID.String(),
			PageSize:  membershipPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolving gallery memberships: %v", err)
		}

		for _, g := range resp.Galleries {
			id, err := uuid.Parse(g.Id)
			if err != nil {
				return nil, status.Errorf(
					codes.Internal,
					"gallery service returned invalid gallery id: %v",
					err,
				)
			}

			galleryIDs = append(galleryIDs, id)
		}

		if resp.NextPageToken == "" || resp.NextPageToken == pageToken {
			break
		}

		pageToken = resp.NextPageToken
	}

	return galleryIDs, nil
}
