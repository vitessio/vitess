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
	"time"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

type grpcTransport struct {
	dialer Dialer
}

// Dialer provides a gossip client for a target address.
type Dialer interface {
	Dial(ctx context.Context, target string) (gossippb.GossipClient, error)
}

func NewGRPCTransport(dialer Dialer) Transport {
	return &grpcTransport{dialer: dialer}
}

func (t *grpcTransport) PushPull(ctx context.Context, addr string, msg *Message) (*Message, error) {
	client, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	response, err := client.PushPull(ctx, toProtoMessage(msg))
	if err != nil {
		return nil, err
	}
	return fromProtoMessage(response), nil
}

func (t *grpcTransport) Join(ctx context.Context, addr string, req *JoinRequest) (*JoinResponse, error) {
	client, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	response, err := client.Join(ctx, toProtoJoinRequest(req))
	if err != nil {
		return nil, err
	}

	return fromProtoJoinResponse(response), nil
}

func (t *grpcTransport) dial(ctx context.Context, addr string) (gossippb.GossipClient, error) {
	if t.dialer == nil {
		return nil, nil
	}
	return t.dialer.Dial(ctx, addr)
}

func toProtoMessage(msg *Message) *gossippb.GossipMessage {
	if msg == nil {
		return &gossippb.GossipMessage{}
	}

	members := make([]*gossippb.Member, 0, len(msg.Members))
	for _, member := range msg.Members {
		members = append(members, toProtoMember(member))
	}

	states := make([]*gossippb.GossipState, 0, len(msg.States))
	for _, state := range msg.States {
		states = append(states, toProtoState(state))
	}

	return &gossippb.GossipMessage{
		Members: members,
		States:  states,
		Epoch:   msg.Epoch,
	}
}

func toProtoMessageValue(msg Message) *gossippb.GossipMessage {
	return toProtoMessage(&msg)
}

func fromProtoMessage(msg *gossippb.GossipMessage) *Message {
	if msg == nil {
		return &Message{}
	}

	members := make([]Member, 0, len(msg.Members))
	for _, member := range msg.Members {
		members = append(members, fromProtoMember(member))
	}

	states := make([]StateDigest, 0, len(msg.States))
	for _, state := range msg.States {
		states = append(states, fromProtoState(state))
	}

	return &Message{
		Members: members,
		States:  states,
		Epoch:   msg.Epoch,
	}
}

func toProtoJoinRequest(req *JoinRequest) *gossippb.GossipJoinRequest {
	if req == nil {
		return &gossippb.GossipJoinRequest{}
	}

	members := make([]*gossippb.Member, 0, len(req.Seeds))
	for _, member := range req.Seeds {
		members = append(members, toProtoMember(member))
	}

	return &gossippb.GossipJoinRequest{
		Member: toProtoMember(req.Member),
		Seeds:  members,
	}
}

func fromProtoJoinRequest(req *gossippb.GossipJoinRequest) *JoinRequest {
	if req == nil {
		return &JoinRequest{}
	}

	seeds := make([]Member, 0, len(req.Seeds))
	for _, member := range req.Seeds {
		seeds = append(seeds, fromProtoMember(member))
	}

	return &JoinRequest{
		Member: fromProtoMember(req.Member),
		Seeds:  seeds,
	}
}

func toProtoJoinResponse(resp *JoinResponse) *gossippb.GossipJoinResponse {
	if resp == nil {
		return &gossippb.GossipJoinResponse{}
	}

	members := make([]*gossippb.Member, 0, len(resp.Members))
	for _, member := range resp.Members {
		members = append(members, toProtoMember(member))
	}

	return &gossippb.GossipJoinResponse{
		Members: members,
		Initial: toProtoMessageValue(resp.Initial),
	}
}

func fromProtoJoinResponse(resp *gossippb.GossipJoinResponse) *JoinResponse {
	if resp == nil {
		return &JoinResponse{}
	}

	members := make([]Member, 0, len(resp.Members))
	for _, member := range resp.Members {
		members = append(members, fromProtoMember(member))
	}

	return &JoinResponse{
		Members: members,
		Initial: *fromProtoMessage(resp.Initial),
	}
}

func toProtoMember(member Member) *gossippb.Member {
	if member.Meta == nil {
		member.Meta = map[string]string{}
	}
	return &gossippb.Member{
		Id:   string(member.ID),
		Addr: member.Addr,
		Meta: member.Meta,
	}
}

func fromProtoMember(member *gossippb.Member) Member {
	if member == nil {
		return Member{}
	}
	return Member{
		ID:   NodeID(member.Id),
		Addr: member.Addr,
		Meta: member.Meta,
	}
}

func toProtoState(state StateDigest) *gossippb.GossipState {
	return &gossippb.GossipState{
		NodeId:         string(state.NodeID),
		Status:         toProtoStatus(state.Status),
		Phi:            state.Phi,
		LastUpdateUnix: state.LastUpdate.UnixNano(),
	}
}

func fromProtoState(state *gossippb.GossipState) StateDigest {
	if state == nil {
		return StateDigest{}
	}
	return StateDigest{
		NodeID:     NodeID(state.NodeId),
		Status:     fromProtoStatus(state.Status),
		Phi:        state.Phi,
		LastUpdate: time.Unix(0, state.LastUpdateUnix),
	}
}

func toProtoStatus(status Status) gossippb.Status {
	switch status {
	case StatusAlive:
		return gossippb.Status_STATUS_ALIVE
	case StatusSuspect:
		return gossippb.Status_STATUS_SUSPECT
	case StatusDown:
		return gossippb.Status_STATUS_DOWN
	default:
		return gossippb.Status_STATUS_UNKNOWN
	}
}

func fromProtoStatus(status gossippb.Status) Status {
	switch status {
	case gossippb.Status_STATUS_ALIVE:
		return StatusAlive
	case gossippb.Status_STATUS_SUSPECT:
		return StatusSuspect
	case gossippb.Status_STATUS_DOWN:
		return StatusDown
	default:
		return StatusUnknown
	}
}
