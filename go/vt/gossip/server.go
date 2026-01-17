package gossip

import (
	"context"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

type Service struct {
	gossippb.UnimplementedGossipServer
	Agent *Gossip
}

func (s *Service) Join(ctx context.Context, req *gossippb.GossipJoinRequest) (*gossippb.GossipJoinResponse, error) {
	if s.Agent == nil {
		return &gossippb.GossipJoinResponse{}, nil
	}
	resp := s.Agent.HandleJoin(fromProtoJoinRequest(req))
	return toProtoJoinResponse(resp), nil
}

func (s *Service) PushPull(ctx context.Context, msg *gossippb.GossipMessage) (*gossippb.GossipMessage, error) {
	if s.Agent == nil {
		return &gossippb.GossipMessage{}, nil
	}
	resp := s.Agent.HandlePushPull(fromProtoMessage(msg))
	return toProtoMessage(resp), nil
}
