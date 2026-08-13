package api

import (
	"context"
	"log"
	userpb "photogallery/gen/user"
	"photogallery/internal/auth"
	"photogallery/internal/gallery/models"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	gallerypb "photogallery/gen/gallery"
)

// Server implements proto.GalleryServiceServer, delegating writes
// to CommandService and reads to QueryService. It contains no business
// logic itself — only request/response translation and auth-context extraction.
type Server struct {
	gallerypb.UnimplementedGalleryServiceServer
	cmd       CommandRunner
	qry       QueryRunner
	jwtSecret string
}

func NewServer(cmd CommandRunner, qry QueryRunner, jwtSecret string) *Server {
	return &Server{cmd: cmd, qry: qry, jwtSecret: jwtSecret}
}

// publicMethods lists the fully-qualified RPC names that do not require a JWT.
// The grpc-gateway auth middleware calls AuthFuncOverride with the full method
// name so we can selectively bypass authentication for Login and Register.
var publicMethods = map[string]bool{
	"/proto.GalleryService/GetGallery":    true,
	"/proto.GalleryService/ListGalleries": true,
}

// AuthFuncOverride implements grpcauth.ServiceAuthFuncOverride.
// and delegates to the shared AuthFunc for everything else.
func (s *Server) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	if publicMethods[fullMethodName] {
		// Best-effort: attach claims if a valid token was sent, but don't
		// require one. GetGallery never needs claims; ListGalleries only
		// needs them when the caller passes my_galleries=true, which is
		// enforced in the handler itself, not here.
		if authedCtx, err := auth.AuthFunc(s.jwtSecret)(ctx); err == nil {
			return authedCtx, nil
		}
		return ctx, nil
	}
	return auth.AuthFunc(s.jwtSecret)(ctx)
}

// --- Commands ---

func (s *Server) CreateGallery(ctx context.Context, req *gallerypb.CreateGalleryRequest) (*gallerypb.Gallery, error) {
	moderatorID, err := moderatorIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	g, err := s.cmd.CreateGallery(ctx, req.GetName(), req.GetDescription(), moderatorID)
	if err != nil {
		return nil, err
	}
	return toProtoGallery(g), nil
}

func (s *Server) CloseGallery(ctx context.Context, req *gallerypb.CloseGalleryRequest) (*gallerypb.CloseGalleryResponse, error) {
	callerID, err := callerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}

	if err := s.cmd.CloseGallery(ctx, id, callerID); err != nil {
		return nil, err
	}
	return &gallerypb.CloseGalleryResponse{}, nil
}

func (s *Server) JoinGallery(ctx context.Context, req *gallerypb.JoinGalleryRequest) (*gallerypb.JoinGalleryResponse, error) {
	callerID, err := callerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	galleryID, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}
	if err := s.cmd.JoinGallery(ctx, galleryID, callerID); err != nil {
		return nil, err
	}
	return &gallerypb.JoinGalleryResponse{}, nil
}

func (s *Server) LeaveGallery(ctx context.Context, req *gallerypb.LeaveGalleryRequest) (*gallerypb.LeaveGalleryResponse, error) {
	callerID, err := callerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	galleryID, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}
	if err := s.cmd.LeaveGallery(ctx, galleryID, callerID); err != nil {
		return nil, err
	}
	return &gallerypb.LeaveGalleryResponse{}, nil
}

func (s *Server) SendModeratorAlert(ctx context.Context, req *gallerypb.SendModeratorAlertRequest) (*gallerypb.SendModeratorAlertResponse, error) {
	callerID, err := callerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}

	if err := s.cmd.SendModeratorAlert(ctx, id, callerID, req.GetBody()); err != nil {
		return nil, err
	}
	return &gallerypb.SendModeratorAlertResponse{}, nil
}

// --- Queries ---

func (s *Server) GetGallery(ctx context.Context, req *gallerypb.GetGalleryRequest) (*gallerypb.Gallery, error) {
	id, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}
	log.Printf("GetGallery: %+v", id)
	g, err := s.qry.GetGallery(ctx, id)
	if err != nil {
		return nil, err
	}
	return toProtoGallery(g), nil
}

func (s *Server) ListGalleries(ctx context.Context, req *gallerypb.ListGalleriesRequest) (*gallerypb.ListGalleriesResponse, error) {
	var callerID uuid.UUID
	if req.GetMyGalleries() {
		id, err := callerIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		callerID = id
	}

	galleries, nextToken, err := s.qry.ListGalleries(ctx, req.GetMyGalleries(), callerID, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, err
	}

	resp := &gallerypb.ListGalleriesResponse{
		Galleries:     make([]*gallerypb.Gallery, 0, len(galleries)),
		NextPageToken: nextToken,
	}
	for i := range galleries {
		resp.Galleries = append(resp.Galleries, toProtoGallery(&galleries[i]))
	}
	return resp, nil
}

func (s *Server) ListGalleriesByMember(ctx context.Context, req *gallerypb.ListGalleriesByMemberRequest) (*gallerypb.ListGalleriesResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}

	galleries, nextPageToken, err := s.qry.ListGalleriesByMember(ctx, userID, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, err
	}

	pbGalleries := make([]*gallerypb.Gallery, 0, len(galleries))
	for i := range galleries {
		pbGalleries = append(pbGalleries, toProtoGallery(&galleries[i]))
	}

	return &gallerypb.ListGalleriesResponse{
		Galleries:     pbGalleries,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *Server) ListMembers(ctx context.Context, req *gallerypb.ListMembersRequest) (*gallerypb.ListMembersResponse, error) {
	id, err := parseUUID(req.GetGalleryId())
	if err != nil {
		return nil, err
	}

	members, err := s.qry.ListMembers(ctx, id)
	if err != nil {
		return nil, err
	}

	resp := &gallerypb.ListMembersResponse{Members: make([]*gallerypb.Member, 0, len(members))}
	for _, m := range members {
		resp.Members = append(resp.Members, toProtoMember(&m))
	}
	return resp, nil
}

func (s *Server) IsMember(ctx context.Context, req *gallerypb.IsMemberRequest) (*gallerypb.IsMemberResponse, error) {

	galleryID, err := uuid.Parse(req.GalleryId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid gallery id")
	}
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}

	isMember, galleryStatus, err := s.qry.IsMember(ctx, galleryID, userID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &gallerypb.IsMemberResponse{
		IsMember:      isMember,
		GalleryStatus: gallerypb.GalleryStatus(gallerypb.GalleryStatus_value[string(galleryStatus)]),
	}, nil
}

// --- helpers ---

func parseUUID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid gallery_id")
	}
	return id, nil
}

func toProtoGallery(g *models.Gallery) *gallerypb.Gallery {
	pg := &gallerypb.Gallery{
		Id:          g.ID.String(),
		Name:        g.Name,
		Description: g.Description,
		ModeratorId: g.ModeratorID.String(),
		CreatedAt:   timestamppb.New(g.CreatedAt),
		UpdatedAt:   timestamppb.New(g.UpdatedAt),
	}
	if g.Status == models.GalleryClosed {
		pg.Status = gallerypb.GalleryStatus_GALLERY_STATUS_CLOSED
	} else {
		pg.Status = gallerypb.GalleryStatus_GALLERY_STATUS_OPEN
	}
	return pg
}

func toProtoMember(m *models.Member) *gallerypb.Member {
	return &gallerypb.Member{
		UserId:    m.UserID.String(),
		GalleryId: m.GalleryID.String(),
		JoinedAt:  timestamppb.New(m.JoinedAt),
	}
}

// callerIDFromContext returns the authenticated caller's user ID.
// The interceptor guarantees claims are present; FromContext already
// returns Unauthenticated if somehow missing.
func callerIDFromContext(ctx context.Context) (uuid.UUID, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	return claims.UserID, nil
}

// moderatorIDFromContext returns the caller's user ID, but only if their
// role is MODERATOR. Used for endpoints restricted to moderators
// (CreateGallery, CloseGallery, SendModeratorAlert).
func moderatorIDFromContext(ctx context.Context) (uuid.UUID, error) {
	claims, err := auth.FromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	if claims.Role != userpb.Role_ROLE_MODERATOR.String() {
		return uuid.Nil, status.Error(codes.PermissionDenied, "moderator role required")
	}
	return claims.UserID, nil
}
