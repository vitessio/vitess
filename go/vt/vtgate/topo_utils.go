// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func getShardForKeyspaceId(topoServ SrvTopoServer, cell, keyspace string, keyspaceId key.KeyspaceId, tabletType topo.TabletType) (string, error) {
	srvKeyspace, err := topoServ.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		return "", fmt.Errorf("keyspace fetch error: %v", err)
	}

	partition, ok := srvKeyspace.Partitions[tabletType]
	if !ok {
		return "", fmt.Errorf("No partition found for this tabletType")
	}

	allShards := partition.Shards
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

// This maps a list of keyranges to shard names.
func resolveKeyRangeToShards(topoServer SrvTopoServer, cell, keyspace string, tabletType topo.TabletType, kr key.KeyRange) ([]string, error) {
	srvKeyspace, err := topoServer.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		return nil, fmt.Errorf("Error in reading the keyspace %v", err)
	}

	tabletTypePartition, ok := srvKeyspace.Partitions[tabletType]
	if !ok {
		return nil, fmt.Errorf("No shards available for tablet type '%v' in keyspace '%v'", tabletType, keyspace)
	}

	topo.SrvShardArray(tabletTypePartition.Shards).Sort()

	shards := make([]string, 0, 1)
	if !kr.IsPartial() {
		for j := 0; j < len(tabletTypePartition.Shards); j++ {
			shards = append(shards, tabletTypePartition.Shards[j].ShardName())
		}
		return shards, nil
	}
	for j := 0; j < len(tabletTypePartition.Shards); j++ {
		shard := tabletTypePartition.Shards[j]
		if key.KeyRangesIntersect(kr, shard.KeyRange) {
			shards = append(shards, shard.ShardName())
		}
		if kr.End != key.MaxKey && kr.End < shard.KeyRange.Start {
			break
		}
	}
	return shards, nil
}
