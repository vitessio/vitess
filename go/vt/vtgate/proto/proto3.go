// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// SessionToProto transforms a Session into proto3
func SessionToProto(s *Session) *pb.Session {
	if s == nil {
		return nil
	}
	result := &pb.Session{
		InTransaction: s.InTransaction,
	}
	result.ShardSessions = make([]*pb.Session_ShardSession, len(s.ShardSessions))
	for i, ss := range s.ShardSessions {
		result.ShardSessions[i] = &pb.Session_ShardSession{
			Target: &pbq.Target{
				Keyspace:   ss.Keyspace,
				Shard:      ss.Shard,
				TabletType: topo.TabletTypeToProto(ss.TabletType),
			},
			TransactionId: ss.TransactionId,
		}
	}
	return result
}

// ProtoToSession transforms a proto3 Session into native types
func ProtoToSession(s *pb.Session) *Session {
	if s == nil {
		return nil
	}
	result := &Session{
		InTransaction: s.InTransaction,
	}
	result.ShardSessions = make([]*ShardSession, len(s.ShardSessions))
	for i, ss := range s.ShardSessions {
		result.ShardSessions[i] = &ShardSession{
			Keyspace:      ss.Target.Keyspace,
			Shard:         ss.Target.Shard,
			TabletType:    topo.ProtoToTabletType(ss.Target.TabletType),
			TransactionId: ss.TransactionId,
		}
	}
	return result
}

// EntityIdsToProto converts an array of EntityId to proto3
func EntityIdsToProto(l []EntityId) []*pb.ExecuteEntityIdsRequest_EntityId {
	if len(l) == 0 {
		return nil
	}
	result := make([]*pb.ExecuteEntityIdsRequest_EntityId, len(l))
	for i, e := range l {
		result[i] = &pb.ExecuteEntityIdsRequest_EntityId{
			KeyspaceId: []byte(e.KeyspaceID),
		}
		v, err := tproto.BindVariableToValue(e.ExternalID)
		if err != nil {
			panic(err)
		}
		result[i].XidType = v.Type
		result[i].XidValue = v.Value
	}
	return result
}

// ProtoToEntityIds converts an array of EntityId from proto3
func ProtoToEntityIds(l []*pb.ExecuteEntityIdsRequest_EntityId) []EntityId {
	if len(l) == 0 {
		return nil
	}
	result := make([]EntityId, len(l))
	for i, e := range l {
		result[i].KeyspaceID = key.KeyspaceId(e.KeyspaceId)
		v, err := tproto.SQLToNative(e.XidType, e.XidValue)
		if err != nil {
			panic(err)
		}
		result[i].ExternalID = v
	}
	return result
}

// BoundShardQueriesToProto transforms a list of BoundShardQuery to proto3
func BoundShardQueriesToProto(bsq []BoundShardQuery) ([]*pb.BoundShardQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]*pb.BoundShardQuery, len(bsq))
	for i, q := range bsq {
		qq, err := tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &pb.BoundShardQuery{
			Query:    qq,
			Keyspace: q.Keyspace,
			Shards:   q.Shards,
		}
	}
	return result, nil
}

// ProtoToBoundShardQueries transforms a list of BoundShardQuery from proto3
func ProtoToBoundShardQueries(bsq []*pb.BoundShardQuery) ([]BoundShardQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]BoundShardQuery, len(bsq))
	for i, q := range bsq {
		result[i].Sql = string(q.Query.Sql)
		bv, err := tproto.Proto3ToBindVariables(q.Query.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i].BindVariables = bv
		result[i].Keyspace = q.Keyspace
		result[i].Shards = q.Shards
	}
	return result, nil
}

// BoundKeyspaceIdQueriesToProto transforms a list of BoundKeyspaceIdQuery to proto3
func BoundKeyspaceIdQueriesToProto(bsq []BoundKeyspaceIdQuery) ([]*pb.BoundKeyspaceIdQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]*pb.BoundKeyspaceIdQuery, len(bsq))
	for i, q := range bsq {
		qq, err := tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &pb.BoundKeyspaceIdQuery{
			Query:       qq,
			Keyspace:    q.Keyspace,
			KeyspaceIds: key.KeyspaceIdsToProto(q.KeyspaceIds),
		}
	}
	return result, nil
}

// ProtoToBoundKeyspaceIdQueries transforms a list of BoundKeyspaceIdQuery from proto3
func ProtoToBoundKeyspaceIdQueries(bsq []*pb.BoundKeyspaceIdQuery) ([]BoundKeyspaceIdQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]BoundKeyspaceIdQuery, len(bsq))
	for i, q := range bsq {
		bv, err := tproto.Proto3ToBindVariables(q.Query.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i].Sql = string(q.Query.Sql)
		result[i].BindVariables = bv
		result[i].Keyspace = q.Keyspace
		result[i].KeyspaceIds = key.ProtoToKeyspaceIds(q.KeyspaceIds)
	}
	return result, nil
}
