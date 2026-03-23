/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gossip

import (
	"context"
	"errors"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// Service implements the gossip gRPC server by delegating to a Gossip agent.
type Service struct {
	gossippb.UnimplementedGossipServer
	Agent *Gossip
}

// Join handles an incoming gossip join RPC.
func (s *Service) Join(ctx context.Context, req *gossippb.GossipJoinRequest) (*gossippb.GossipJoinResponse, error) {
	if s.Agent == nil {
		return &gossippb.GossipJoinResponse{}, nil
	}
	resp := s.Agent.HandleJoin(fromProtoJoinRequest(req))
	if resp == nil {
		return nil, errors.New("invalid join request")
	}
	return toProtoJoinResponse(resp), nil
}

// PushPull handles an incoming gossip push-pull RPC.
func (s *Service) PushPull(ctx context.Context, msg *gossippb.GossipMessage) (*gossippb.GossipMessage, error) {
	if s.Agent == nil {
		return &gossippb.GossipMessage{}, nil
	}
	decoded, err := fromProtoMessage(msg)
	if err != nil {
		return nil, err
	}
	resp := s.Agent.HandlePushPull(decoded)
	return toProtoMessage(resp), nil
}
