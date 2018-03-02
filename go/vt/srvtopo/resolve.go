/*
Copyright 2017 Google Inc.

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

package srvtopo

import (
	"encoding/hex"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// MapKeyspaceIdsToShards maps the given keyspaceIds to a concrete set
// of shards.
func MapKeyspaceIdsToShards(ctx context.Context, topoServ Server, cell, keyspace string, tabletType topodatapb.TabletType, keyspaceIds [][]byte) (string, []string, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	var shards = make(map[string]bool)
	for _, ksID := range keyspaceIds {
		shard, err := GetShardForKeyspaceID(allShards, ksID)
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

// GetAnyShard returns a shard from the keyspace. In practice, the
// implementation now returns the first one.
func GetAnyShard(ctx context.Context, topoServ Server, cell, keyspace string, tabletType topodatapb.TabletType) (ks, shard string, err error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", "", err
	}
	if len(allShards) == 0 {
		return "", "", vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "No shards found for this tabletType")
	}
	return keyspace, allShards[0].Name, nil
}

// GetKeyspaceShards return all the shards in a keyspace. It follows
// redirection if ServedFrom is set.
func GetKeyspaceShards(ctx context.Context, topoServ Server, cell, keyspace string, tabletType topodatapb.TabletType) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
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

// GetShardForKeyspaceID finds the right shard for a keyspace id.
func GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
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

// MapKeyRangesToShards returns the set of shards that "intersect"
// with a collection of key-ranges; that is, a shard is included if
// and only if its corresponding key-space ids are in one of the key-ranges.
func MapKeyRangesToShards(ctx context.Context, topoServ Server, cell, keyspace string, tabletType topodatapb.TabletType, krs []*topodatapb.KeyRange) (string, []string, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	uniqueShards := make(map[string]bool)
	for _, kr := range krs {
		keyRangeToShardMap(allShards, uniqueShards, kr)
	}
	var res = make([]string, 0, len(uniqueShards))
	for s := range uniqueShards {
		res = append(res, s)
	}
	return keyspace, res, nil
}

// keyRangeToShardMap adds shards to a map based on the input KeyRange.
func keyRangeToShardMap(allShards []*topodatapb.ShardReference, matches map[string]bool, kr *topodatapb.KeyRange) {
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

// GetShardsForKeyRange maps keyranges to shards.
func GetShardsForKeyRange(allShards []*topodatapb.ShardReference, kr *topodatapb.KeyRange) []string {
	isPartial := key.KeyRangeIsPartial(kr)
	var shards []string
	for _, shard := range allShards {
		if !isPartial || key.KeyRangesIntersect(kr, shard.KeyRange) {
			shards = append(shards, shard.Name)
		}
	}
	return shards
}

// MapExactShards maps a keyrange to shards only if there's a complete
// match. If there's any partial match the function returns no match.
func MapExactShards(ctx context.Context, topoServ Server, cell, keyspace string, tabletType topodatapb.TabletType, kr *topodatapb.KeyRange) (newkeyspace string, shards []string, err error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, topoServ, cell, keyspace, tabletType)
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
