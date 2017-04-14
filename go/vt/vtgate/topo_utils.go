// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/hex"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func mapKeyspaceIdsToShards(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, tabletType topodatapb.TabletType, keyspaceIds [][]byte) (string, []string, error) {
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

func getAnyShard(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, tabletType topodatapb.TabletType) (ks, shard string, err error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", "", err
	}
	if len(allShards) == 0 {
		return "", "", vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "No shards found for this tabletType")
	}
	return keyspace, allShards[0].Name, nil
}

func getAllKeyspaces(ctx context.Context, topoServ topo.SrvTopoServer, cell string) ([]string, error) {
	keyspaces, err := topoServ.GetSrvKeyspaceNames(ctx, cell)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace names fetch error: %v", err)
	}

	return keyspaces, nil
}

func getKeyspaceShards(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, tabletType topodatapb.TabletType) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
	srvKeyspace, err := topoServ.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
	}

	// check if the keyspace has been redirected for this tabletType.
	for _, sf := range srvKeyspace.ServedFrom {
		if sf.TabletType == tabletType {
			keyspace = sf.Keyspace
			srvKeyspace, err = topoServ.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
			}
		}
	}

	partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
	if partition == nil {
		return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No partition found for tabletType %v in keyspace %v", topoproto.TabletTypeLString(tabletType), keyspace)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}

func getShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	if len(allShards) == 0 {
		return "", vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "No shards found for this tabletType")
	}

	for _, shardReference := range allShards {
		if key.KeyRangeContains(shardReference.KeyRange, keyspaceID) {
			return shardReference.Name, nil
		}
	}
	return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "KeyspaceId %v didn't match any shards %+v", hex.EncodeToString(keyspaceID), allShards)
}

func mapEntityIdsToShards(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, entityIds []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType) (string, map[string][]interface{}, error) {
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
		v, err := sqltypes.ValueFromBytes(eid.Type, eid.Value)
		if err != nil {
			return "", nil, err
		}
		shards[shard] = append(shards[shard], v.ToNative())
	}
	return keyspace, shards, nil
}

// Given a collection of key-ranges, returns the set of shards that "intersect"
// them; that is, a shard is included if and only if its corresponding key-space ids
// are in one of the key-ranges.
func mapKeyRangesToShards(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, tabletType topodatapb.TabletType, krs []*topodatapb.KeyRange) (string, []string, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	uniqueShards := make(map[string]bool)
	for _, kr := range krs {
		resolveKeyRangeToShards(allShards, uniqueShards, kr)
	}
	var res = make([]string, 0, len(uniqueShards))
	for s := range uniqueShards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

// This maps a list of keyranges to shard names.
func resolveKeyRangeToShards(allShards []*topodatapb.ShardReference, matches map[string]bool, kr *topodatapb.KeyRange) {
	if !key.KeyRangeIsPartial(kr) {
		for _, shard := range allShards {
			matches[shard.Name] = true
		}
		return
	}
	for _, shard := range allShards {
		if key.KeyRangesIntersect(kr, shard.KeyRange) {
			matches[shard.Name] = true
		}
	}
}

// mapExactShards maps a keyrange to shards only if there's a complete
// match. If there's any partial match the function returns no match.
func mapExactShards(ctx context.Context, topoServ topo.SrvTopoServer, cell, keyspace string, tabletType topodatapb.TabletType, kr *topodatapb.KeyRange) (newkeyspace string, shards []string, err error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	shardnum := 0
	for shardnum < len(allShards) {
		if key.KeyRangeStartEqual(kr, allShards[shardnum].KeyRange) {
			break
		}
		shardnum++
	}
	for shardnum < len(allShards) {
		shards = append(shards, allShards[shardnum].Name)
		if key.KeyRangeEndEqual(kr, allShards[shardnum].KeyRange) {
			return keyspace, shards, nil
		}
		shardnum++
	}
	return keyspace, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyrange %v does not exactly match shards", key.KeyRangeString(kr))
}

func boundShardQueriesToScatterBatchRequest(boundQueries []*vtgatepb.BoundShardQuery) (*scatterBatchRequest, error) {
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
			bq, err := querytypes.Proto3ToBoundQuery(boundQuery.Query)
			if err != nil {
				return nil, err
			}
			request.Queries = append(request.Queries, *bq)
			request.ResultIndexes = append(request.ResultIndexes, i)
		}
	}
	return requests, nil
}

func boundKeyspaceIDQueriesToBoundShardQueries(ctx context.Context, topoServ topo.SrvTopoServer, cell string, tabletType topodatapb.TabletType, idQueries []*vtgatepb.BoundKeyspaceIdQuery) ([]*vtgatepb.BoundShardQuery, error) {
	shardQueries := make([]*vtgatepb.BoundShardQuery, len(idQueries))
	for i, idQuery := range idQueries {
		keyspace, shards, err := mapKeyspaceIdsToShards(ctx, topoServ, cell, idQuery.Keyspace, tabletType, idQuery.KeyspaceIds)
		if err != nil {
			return nil, err
		}
		shardQueries[i] = &vtgatepb.BoundShardQuery{
			Query:    idQuery.Query,
			Keyspace: keyspace,
			Shards:   shards,
		}
	}
	return shardQueries, nil
}
