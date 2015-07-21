// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

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
		switch v := e.ExternalID.(type) {
		case string:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES
			result[i].XidBytes = []byte(v)
		case []byte:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES
			result[i].XidBytes = v
		case int:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_INT
			result[i].XidInt = int64(v)
		case int16:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_INT
			result[i].XidInt = int64(v)
		case int32:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_INT
			result[i].XidInt = int64(v)
		case int64:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_INT
			result[i].XidInt = v
		case uint:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_UINT
			result[i].XidUint = uint64(v)
		case uint16:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_UINT
			result[i].XidUint = uint64(v)
		case uint32:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_UINT
			result[i].XidUint = uint64(v)
		case uint64:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_UINT
			result[i].XidUint = v
		case float32:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_FLOAT
			result[i].XidFloat = float64(v)
		case float64:
			result[i].XidType = pb.ExecuteEntityIdsRequest_EntityId_TYPE_FLOAT
			result[i].XidFloat = v
		default:
			panic(fmt.Errorf("Unsupported value %v", v))
		}
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
		switch e.XidType {
		case pb.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES:
			result[i].ExternalID = e.XidBytes
		case pb.ExecuteEntityIdsRequest_EntityId_TYPE_INT:
			result[i].ExternalID = e.XidInt
		case pb.ExecuteEntityIdsRequest_EntityId_TYPE_UINT:
			result[i].ExternalID = e.XidUint
		case pb.ExecuteEntityIdsRequest_EntityId_TYPE_FLOAT:
			result[i].ExternalID = e.XidFloat
		default:
			panic(fmt.Errorf("Unsupported XidType %v", e.XidType))
		}
	}
	return result
}

// BoundShardQueriesToProto transforms a list of BoundShardQuery to proto3
func BoundShardQueriesToProto(bsq []BoundShardQuery) []*pb.BoundShardQuery {
	if len(bsq) == 0 {
		return nil
	}
	result := make([]*pb.BoundShardQuery, len(bsq))
	for i, q := range bsq {
		result[i] = &pb.BoundShardQuery{
			Query:    tproto.BoundQueryToProto3(q.Sql, q.BindVariables),
			Keyspace: q.Keyspace,
			Shards:   q.Shards,
		}
	}
	return result
}

// ProtoToBoundShardQueries transforms a list of BoundShardQuery from proto3
func ProtoToBoundShardQueries(bsq []*pb.BoundShardQuery) []BoundShardQuery {
	if len(bsq) == 0 {
		return nil
	}
	result := make([]BoundShardQuery, len(bsq))
	for i, q := range bsq {
		result[i].Sql = string(q.Query.Sql)
		result[i].BindVariables = tproto.Proto3ToBindVariables(q.Query.BindVariables)
		result[i].Keyspace = q.Keyspace
		result[i].Shards = q.Shards
	}
	return result
}

// BoundKeyspaceIdQueriesToProto transforms a list of BoundKeyspaceIdQuery to proto3
func BoundKeyspaceIdQueriesToProto(bsq []BoundKeyspaceIdQuery) []*pb.BoundKeyspaceIdQuery {
	if len(bsq) == 0 {
		return nil
	}
	result := make([]*pb.BoundKeyspaceIdQuery, len(bsq))
	for i, q := range bsq {
		result[i] = &pb.BoundKeyspaceIdQuery{
			Query:       tproto.BoundQueryToProto3(q.Sql, q.BindVariables),
			Keyspace:    q.Keyspace,
			KeyspaceIds: key.KeyspaceIdsToProto(q.KeyspaceIds),
		}
	}
	return result
}

// ProtoToBoundKeyspaceIdQueries transforms a list of BoundKeyspaceIdQuery from proto3
func ProtoToBoundKeyspaceIdQueries(bsq []*pb.BoundKeyspaceIdQuery) []BoundKeyspaceIdQuery {
	if len(bsq) == 0 {
		return nil
	}
	result := make([]BoundKeyspaceIdQuery, len(bsq))
	for i, q := range bsq {
		result[i].Sql = string(q.Query.Sql)
		result[i].BindVariables = tproto.Proto3ToBindVariables(q.Query.BindVariables)
		result[i].Keyspace = q.Keyspace
		result[i].KeyspaceIds = key.ProtoToKeyspaceIds(q.KeyspaceIds)
	}
	return result
}

// SplitQueryPartsToproto transforms a SplitQueryResponse into proto
func SplitQueryPartsToProto(sqp []SplitQueryPart) *pb.SplitQueryResponse {
	result := &pb.SplitQueryResponse{}
	if len(sqp) == 0 {
		return result
	}
	result.Splits = make([]*pb.SplitQueryResponse_Part, len(sqp))
	for i, split := range sqp {
		result.Splits[i] = &pb.SplitQueryResponse_Part{
			Size: split.Size,
		}
		if split.Query != nil {
			result.Splits[i].Query = tproto.BoundQueryToProto3(split.Query.Sql, split.Query.BindVariables)
			result.Splits[i].KeyRangePart = &pb.SplitQueryResponse_KeyRangePart{
				Keyspace:  split.Query.Keyspace,
				KeyRanges: key.KeyRangesToProto(split.Query.KeyRanges),
			}
		}
		if split.QueryShard != nil {
			result.Splits[i].Query = tproto.BoundQueryToProto3(split.QueryShard.Sql, split.QueryShard.BindVariables)
			result.Splits[i].ShardPart = &pb.SplitQueryResponse_ShardPart{
				Keyspace: split.QueryShard.Keyspace,
				Shards:   split.QueryShard.Shards,
			}
		}
	}
	return result
}

// ProtoToSplitQueryParts transforms a proto3 SplitQueryResponse into
// native types
func ProtoToSplitQueryParts(sqr *pb.SplitQueryResponse) []SplitQueryPart {
	if len(sqr.Splits) == 0 {
		return nil
	}
	result := make([]SplitQueryPart, len(sqr.Splits))
	for i, split := range sqr.Splits {
		if split.KeyRangePart != nil {
			result[i].Query = &KeyRangeQuery{
				Sql:           string(split.Query.Sql),
				BindVariables: tproto.Proto3ToBindVariables(split.Query.BindVariables),
				Keyspace:      split.KeyRangePart.Keyspace,
				KeyRanges:     key.ProtoToKeyRanges(split.KeyRangePart.KeyRanges),
			}
		}
		if split.ShardPart != nil {
			result[i].QueryShard = &QueryShard{
				Sql:           string(split.Query.Sql),
				BindVariables: tproto.Proto3ToBindVariables(split.Query.BindVariables),
				Keyspace:      split.ShardPart.Keyspace,
				Shards:        split.ShardPart.Shards,
			}
		}
		result[i].Size = split.Size
	}
	return result
}
