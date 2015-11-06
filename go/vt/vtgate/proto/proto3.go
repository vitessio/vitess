// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

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
		result[i].KeyspaceID = e.KeyspaceId
		bv := &pbq.BindVariable{
			Type:  e.XidType,
			Value: e.XidValue,
		}
		v, err := tproto.BindVariableToNative(bv)
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
			KeyspaceIds: q.KeyspaceIds,
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
		result[i].KeyspaceIds = q.KeyspaceIds
	}
	return result, nil
}
