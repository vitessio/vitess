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
	"fmt"
	"time"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

type grpcTransport struct {
	dialer Dialer
}

// Dialer provides a gossip client for a target address. Implementations
// SHOULD cache per-target connections so the gossip hot path does not
// pay a full TCP/TLS handshake on every RPC; Close releases them.
type Dialer interface {
	Dial(ctx context.Context, target string) (gossippb.GossipClient, error)
	Close()
}

// NewGRPCTransport creates a gossip transport backed by gRPC using the given dialer.
func NewGRPCTransport(dialer Dialer) Transport {
	return &grpcTransport{dialer: dialer}
}

// Close releases any per-target connections held by the transport.
func (t *grpcTransport) Close() {
	if t.dialer != nil {
		t.dialer.Close()
	}
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
	return fromProtoMessage(response)
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

	return fromProtoJoinResponse(response)
}

func (t *grpcTransport) dial(ctx context.Context, addr string) (gossippb.GossipClient, error) {
	if t.dialer == nil {
		return nil, errors.New("gossip transport dialer is nil")
	}
	client, err := t.dialer.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("gossip transport dialer returned nil client for %q", addr)
	}
	return client, nil
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

func fromProtoMessage(msg *gossippb.GossipMessage) (*Message, error) {
	if msg == nil {
		return &Message{}, nil
	}

	members := make([]Member, 0, len(msg.Members))
	for _, member := range msg.Members {
		members = append(members, fromProtoMember(member))
	}

	states := make([]StateDigest, 0, len(msg.States))
	for _, state := range msg.States {
		digest, err := fromProtoState(state)
		if err != nil {
			return nil, err
		}
		states = append(states, digest)
	}

	return &Message{
		Members: members,
		States:  states,
		Epoch:   msg.Epoch,
	}, nil
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

func fromProtoJoinResponse(resp *gossippb.GossipJoinResponse) (*JoinResponse, error) {
	if resp == nil {
		return &JoinResponse{}, nil
	}

	members := make([]Member, 0, len(resp.Members))
	for _, member := range resp.Members {
		members = append(members, fromProtoMember(member))
	}

	initial, err := fromProtoMessage(resp.Initial)
	if err != nil {
		return nil, err
	}

	return &JoinResponse{
		Members: members,
		Initial: *initial,
	}, nil
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
	var lastUpdateUnix int64
	if !state.LastUpdate.IsZero() {
		lastUpdateUnix = state.LastUpdate.UnixNano()
	}
	return &gossippb.GossipState{
		NodeId:         string(state.NodeID),
		Status:         toProtoStatus(state.Status),
		Phi:            state.Phi,
		LastUpdateUnix: lastUpdateUnix,
	}
}

func fromProtoState(state *gossippb.GossipState) (StateDigest, error) {
	if state == nil {
		return StateDigest{}, nil
	}
	status, err := fromProtoStatus(state.Status)
	if err != nil {
		return StateDigest{}, err
	}
	var lastUpdate time.Time
	if state.LastUpdateUnix != 0 {
		lastUpdate = time.Unix(0, state.LastUpdateUnix)
	}
	return StateDigest{
		NodeID:     NodeID(state.NodeId),
		Status:     status,
		Phi:        state.Phi,
		LastUpdate: lastUpdate,
	}, nil
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

func fromProtoStatus(status gossippb.Status) (Status, error) {
	switch status {
	case gossippb.Status_STATUS_ALIVE:
		return StatusAlive, nil
	case gossippb.Status_STATUS_SUSPECT:
		return StatusSuspect, nil
	case gossippb.Status_STATUS_DOWN:
		return StatusDown, nil
	case gossippb.Status_STATUS_UNKNOWN:
		return StatusUnknown, nil
	default:
		return StatusUnknown, fmt.Errorf("unknown gossip status: %d", status)
	}
}
