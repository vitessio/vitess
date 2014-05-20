// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

func mapKeyspaceIdsToShards(topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, keyspaceIds []key.KeyspaceId) (string, []string, error) {
	keyspace, allShards, err := getKeyspaceShards(topoServ, cell, keyspace, tabletType)
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
	for s, _ := range shards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

func getKeyspaceShards(topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType) (string, []topo.SrvShard, error) {
	// check keyspace redirection
	keyspace, err := getKeyspaceAlias(topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	srvKeyspace, err := topoServ.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		return "", nil, fmt.Errorf("keyspace %v fetch error: %v", keyspace, err)
	}
	partition, ok := srvKeyspace.Partitions[tabletType]
	if !ok {
		return "", nil, fmt.Errorf("No partition found for tabletType %v in keyspace %v", tabletType, keyspace)
	}
	return keyspace, partition.Shards, nil
}

func getShardForKeyspaceId(allShards []topo.SrvShard, keyspaceId key.KeyspaceId) (string, error) {
	if len(allShards) == 0 {
		return "", fmt.Errorf("No shards found for this tabletType")
	}

	for _, srvShard := range allShards {
		if srvShard.KeyRange.Contains(keyspaceId) {
			return srvShard.ShardName(), nil
		}
	}
	return "", fmt.Errorf("KeyspaceId didn't match any shards")
}

func mapEntityIdsToShards(topoServ SrvTopoServer, cell, keyspace string, entityIds []proto.EntityId, tabletType topo.TabletType) (string, map[string][]interface{}, error) {
	keyspace, allShards, err := getKeyspaceShards(topoServ, cell, keyspace, tabletType)
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

func getKeyspaceAlias(topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType) (string, error) {
	srvKeyspace, err := topoServ.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		return "", fmt.Errorf("keyspace fetch error: %v", err)
	}

	// check if the keyspace has been redirected for this tabletType.
	if servedFrom, ok := srvKeyspace.ServedFrom[tabletType]; ok {
		return servedFrom, nil
	}

	return keyspace, nil
}

// This function implements the restriction of handling one keyrange
// and one shard since streaming doesn't support merge sorting the results.
// The input/output api is generic though.
func mapKeyRangesToShards(topoServ SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, krs []key.KeyRange) (string, []string, error) {
	keyspace, allShards, err := getKeyspaceShards(topoServ, cell, keyspace, tabletType)
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
	for s, _ := range uniqueShards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

// This maps a list of keyranges to shard names.
func resolveKeyRangeToShards(allShards []topo.SrvShard, kr key.KeyRange) ([]string, error) {
	shards := make([]string, 0, 1)
	topo.SrvShardArray(allShards).Sort()

	if !kr.IsPartial() {
		for j := 0; j < len(allShards); j++ {
			shards = append(shards, allShards[j].ShardName())
		}
		return shards, nil
	}
	for j := 0; j < len(allShards); j++ {
		shard := allShards[j]
		if key.KeyRangesIntersect(kr, shard.KeyRange) {
			shards = append(shards, shard.ShardName())
		}
		if kr.End != key.MaxKey && kr.End < shard.KeyRange.Start {
			break
		}
	}
	return shards, nil
}
