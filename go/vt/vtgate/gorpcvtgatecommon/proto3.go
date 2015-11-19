// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcvtgatecommon

import (
	"github.com/youtube/vitess/go/sqltypes"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"

	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// This file contains methods to convert the bson rpc structures to and from
// proto3.

// EntityIdsToProto converts an array of EntityId to proto3
func EntityIdsToProto(l []EntityId) []*vtgatepb.ExecuteEntityIdsRequest_EntityId {
	if len(l) == 0 {
		return nil
	}
	result := make([]*vtgatepb.ExecuteEntityIdsRequest_EntityId, len(l))
	for i, e := range l {
		result[i] = &vtgatepb.ExecuteEntityIdsRequest_EntityId{
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
func ProtoToEntityIds(l []*vtgatepb.ExecuteEntityIdsRequest_EntityId) []EntityId {
	if len(l) == 0 {
		return nil
	}
	result := make([]EntityId, len(l))
	for i, e := range l {
		v, err := sqltypes.ValueFromBytes(e.XidType, e.XidValue)
		if err != nil {
			panic(err)
		}
		result[i].KeyspaceID = e.KeyspaceId
		result[i].ExternalID = v.ToNative()
	}
	return result
}

// BoundShardQueriesToProto transforms a list of BoundShardQuery to proto3
func BoundShardQueriesToProto(bsq []BoundShardQuery) ([]*vtgatepb.BoundShardQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]*vtgatepb.BoundShardQuery, len(bsq))
	for i, q := range bsq {
		qq, err := tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &vtgatepb.BoundShardQuery{
			Query:    qq,
			Keyspace: q.Keyspace,
			Shards:   q.Shards,
		}
	}
	return result, nil
}

// ProtoToBoundShardQueries transforms a list of BoundShardQuery from proto3
func ProtoToBoundShardQueries(bsq []*vtgatepb.BoundShardQuery) ([]BoundShardQuery, error) {
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
func BoundKeyspaceIdQueriesToProto(bsq []BoundKeyspaceIdQuery) ([]*vtgatepb.BoundKeyspaceIdQuery, error) {
	if len(bsq) == 0 {
		return nil, nil
	}
	result := make([]*vtgatepb.BoundKeyspaceIdQuery, len(bsq))
	for i, q := range bsq {
		qq, err := tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &vtgatepb.BoundKeyspaceIdQuery{
			Query:       qq,
			Keyspace:    q.Keyspace,
			KeyspaceIds: q.KeyspaceIds,
		}
	}
	return result, nil
}

// ProtoToBoundKeyspaceIdQueries transforms a list of BoundKeyspaceIdQuery from proto3
func ProtoToBoundKeyspaceIdQueries(bsq []*vtgatepb.BoundKeyspaceIdQuery) ([]BoundKeyspaceIdQuery, error) {
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
