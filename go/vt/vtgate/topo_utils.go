// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

func mapKeyspaceIdsToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, keyspaceIds []key.KeyspaceId) (string, []string, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	var shards = make(map[string]bool)
	for _, ksId := range keyspaceIds {
		shard, err := getShardForKeyspaceId(allShards, ksId)
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

func getKeyspaceShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType) (string, *topo.SrvKeyspace, []topo.ShardReference, error) {
	srvKeyspace, err := topoServ.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		return "", nil, nil, fmt.Errorf("keyspace %v fetch error: %v", keyspace, err)
	}

	// check if the keyspace has been redirected for this tabletType.
	if servedFrom, ok := srvKeyspace.ServedFrom[tabletType]; ok {
		keyspace = servedFrom
		srvKeyspace, err = topoServ.GetSrvKeyspace(ctx, cell, keyspace)
		if err != nil {
			return "", nil, nil, fmt.Errorf("keyspace %v fetch error: %v", keyspace, err)
		}
	}

	partition, ok := srvKeyspace.Partitions[tabletType]
	if !ok {
		return "", nil, nil, fmt.Errorf("No partition found for tabletType %v in keyspace %v", tabletType, keyspace)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}

func getShardForKeyspaceId(allShards []topo.ShardReference, keyspaceId key.KeyspaceId) (string, error) {
	if len(allShards) == 0 {
		return "", fmt.Errorf("No shards found for this tabletType")
	}

	for _, shardReference := range allShards {
		if shardReference.KeyRange.Contains(keyspaceId) {
			return shardReference.Name, nil
		}
	}
	return "", fmt.Errorf("KeyspaceId %v didn't match any shards %+v", keyspaceId, allShards)
}

func mapEntityIdsToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, entityIds []proto.EntityId, tabletType topo.TabletType) (string, map[string][]interface{}, error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	var shards = make(map[string][]interface{})
	for _, eid := range entityIds {
		shard, err := getShardForKeyspaceId(allShards, eid.KeyspaceID)
		if err != nil {
			return "", nil, err
		}
		shards[shard] = append(shards[shard], eid.ExternalID)
	}
	return keyspace, shards, nil
}

// This function implements the restriction of handling one keyrange
// and one shard since streaming doesn't support merge sorting the results.
// The input/output api is generic though.
func mapKeyRangesToShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, krs []key.KeyRange) (string, []string, error) {
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
func resolveKeyRangeToShards(allShards []topo.ShardReference, kr key.KeyRange) ([]string, error) {
	shards := make([]string, 0, 1)

	if !kr.IsPartial() {
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
func mapExactShards(ctx context.Context, topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, kr key.KeyRange) (newkeyspace string, shards []string, err error) {
	keyspace, _, allShards, err := getKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	shardnum := 0
	for shardnum < len(allShards) {
		if kr.Start == allShards[shardnum].KeyRange.Start {
			break
		}
		shardnum++
	}
	for shardnum < len(allShards) {
		shards = append(shards, allShards[shardnum].Name)
		if kr.End == allShards[shardnum].KeyRange.End {
			return keyspace, shards, nil
		}
		shardnum++
	}
	return keyspace, nil, fmt.Errorf("keyrange %v does not exactly match shards", kr)
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

func boundKeyspaceIdQueriesToBoundShardQueries(ctx context.Context, topoServ SrvTopoServer, cell string, tabletType topo.TabletType, idQueries []proto.BoundKeyspaceIdQuery) ([]proto.BoundShardQuery, error) {
	shardQueries := make([]proto.BoundShardQuery, len(idQueries))
	for i, idQuery := range idQueries {
		keyspace, shards, err := mapKeyspaceIdsToShards(ctx, topoServ, cell, idQuery.Keyspace, tabletType, idQuery.KeyspaceIds)
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
