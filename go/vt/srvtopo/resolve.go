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
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

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
