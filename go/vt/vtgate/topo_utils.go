// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func mapKeyspaceIdsToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType pb.TabletType, keyspaceIds [][]byte) (string, []string, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	var shards = make(map[string]bool)
	for _, ksID := range keyspaceIds {
		shard, err := getShardForKeyspaceID(allShards, ksID)
		if err != nil {
			return "", nil, err
		}
		shards[shard] = true
	}
	var res = make([]string, 0, len(shards))
	for s := range shards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

func getKeyspaceShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType pb.TabletType) (string, *pb.SrvKeyspace, []*pb.ShardReference, error) {
	srvKeyspace, err := topoServ.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		return "", nil, nil, vterrors.NewVitessError(
			vtrpc.ErrorCode_INTERNAL_ERROR, err,
			"keyspace %v fetch error: %v", keyspace, err,
		)
	}

	// check if the keyspace has been redirected for this tabletType.
	for _, sf := range srvKeyspace.ServedFrom {
		if sf.TabletType == tabletType {
			keyspace = sf.Keyspace
			srvKeyspace, err = topoServ.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				return "", nil, nil, vterrors.NewVitessError(
					vtrpc.ErrorCode_INTERNAL_ERROR, err,
					"keyspace %v fetch error: %v", keyspace, err,
				)
			}
		}
	}

	partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
	if partition == nil {
		return "", nil, nil, vterrors.NewVitessError(
			vtrpc.ErrorCode_INTERNAL_ERROR, err,
			"No partition found for tabletType %v in keyspace %v", strings.ToLower(tabletType.String()), keyspace,
		)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}

func getShardForKeyspaceID(allShards []*pb.ShardReference, keyspaceID []byte) (string, error) {
	if len(allShards) == 0 {
		return "", vterrors.FromError(vtrpc.ErrorCode_BAD_INPUT,
			fmt.Errorf("No shards found for this tabletType"),
		)
	}

	for _, shardReference := range allShards {
		if key.KeyRangeContains(shardReference.KeyRange, keyspaceID) {
			return shardReference.Name, nil
		}
	}
	return "", vterrors.FromError(vtrpc.ErrorCode_BAD_INPUT,
		fmt.Errorf("KeyspaceId %v didn't match any shards %+v", hex.EncodeToString(keyspaceID), allShards),
	)
}

func mapEntityIdsToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, entityIds []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType) (string, map[string][]interface{}, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	var shards = make(map[string][]interface{})
	for _, eid := range entityIds {
		shard, err := getShardForKeyspaceID(allShards, eid.KeyspaceId)
		if err != nil {
			return "", nil, err
		}
		bv := &query.BindVariable{
			Type:  eid.XidType,
			Value: eid.XidValue,
		}
		v, _ := tproto.BindVariableToNative(bv)
		shards[shard] = append(shards[shard], v)
	}
	return keyspace, shards, nil
}

// Given a collection of key-ranges, returns the set of shards that "intersect"
// them; that is, a shard is included if and only if its corresponding key-space ids
// are in one of the key-ranges.
func mapKeyRangesToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType pb.TabletType, krs []*pb.KeyRange) (string, []string, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	uniqueShards := make(map[string]bool)
	for _, kr := range krs {
		shards, err := resolveKeyRangeToShards(allShards, kr)
		if err != nil {
			return "", nil, err
		}
		for _, shard := range shards {
			uniqueShards[shard] = true
		}
	}
	var res = make([]string, 0, len(uniqueShards))
	for s := range uniqueShards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

// This maps a list of keyranges to shard names.
func resolveKeyRangeToShards(allShards []*pb.ShardReference, kr *pb.KeyRange) ([]string, error) {
	shards := make([]string, 0, 1)

	if !key.KeyRangeIsPartial(kr) {
		for j := 0; j < len(allShards); j++ {
			shards = append(shards, allShards[j].Name)
		}
		return shards, nil
	}
	for j := 0; j < len(allShards); j++ {
		shard := allShards[j]
		if key.KeyRangesIntersect(kr, shard.KeyRange) {
			shards = append(shards, shard.Name)
		}
	}
	return shards, nil
}

// mapExactShards maps a keyrange to shards only if there's a complete
// match. If there's any partial match the function returns no match.
func mapExactShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType pb.TabletType, kr *pb.KeyRange) (newkeyspace string, shards []string, err error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	shardnum := 0
	for shardnum < len(allShards) {
		if bytes.Compare(kr.Start, []byte(allShards[shardnum].KeyRange.Start)) == 0 {
			break
		}
		shardnum++
	}
	for shardnum < len(allShards) {
		shards = append(shards, allShards[shardnum].Name)
		if bytes.Compare(kr.End, []byte(allShards[shardnum].KeyRange.End)) == 0 {
			return keyspace, shards, nil
		}
		shardnum++
	}
	return keyspace, nil, vterrors.FromError(vtrpc.ErrorCode_BAD_INPUT,
		fmt.Errorf("keyrange %v does not exactly match shards", key.KeyRangeString(kr)),
	)
}

func boundShardQueriesToScatterBatchRequest(boundQueries []proto.BoundShardQuery) *scatterBatchRequest {
	requests := &scatterBatchRequest{
		Length:   len(boundQueries),
		Requests: make(map[string]*shardBatchRequest),
	}
	for i, boundQuery := range boundQueries {
		for shard := range unique(boundQuery.Shards) {
			key := boundQuery.Keyspace + ":" + shard
			request := requests.Requests[key]
			if request == nil {
				request = &shardBatchRequest{
					Keyspace: boundQuery.Keyspace,
					Shard:    shard,
				}
				requests.Requests[key] = request
			}
			request.Queries = append(request.Queries, tproto.BoundQuery{
				Sql:           boundQuery.Sql,
				BindVariables: boundQuery.BindVariables,
			})
			request.ResultIndexes = append(request.ResultIndexes, i)
		}
	}
	return requests
}

func boundKeyspaceIDQueriesToBoundShardQueries(ctx context.Context, topoServ SrvTopoServer, cell string, tabletType pb.TabletType, idQueries []proto.BoundKeyspaceIdQuery) ([]proto.BoundShardQuery, error) {
	shardQueries := make([]proto.BoundShardQuery, len(idQueries))
	for i, idQuery := range idQueries {
		keyspace, shards, err := mapKeyspaceIdsToShards(ctx, topoServ, cell, idQuery.Keyspace, tabletType, key.KeyspaceIdsToProto(idQuery.KeyspaceIds))
		if err != nil {
			return nil, err
		}
		shardQueries[i] = proto.BoundShardQuery{
			Sql:           idQuery.Sql,
			BindVariables: idQuery.BindVariables,
			Keyspace:      keyspace,
			Shards:        shards,
		}
	}
	return shardQueries, nil
}
