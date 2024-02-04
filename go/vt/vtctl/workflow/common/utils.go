/*
Copyright 2024 The Vitess Authors.

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

package common

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// GetShards returns a subset of shards in a keyspace. If no subset is provided, all shards are returned.
func GetShards(ctx context.Context, ts *topo.Server, keyspace string, shardSubset []string) ([]string, error) {
	var err error
	allShards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if len(allShards) == 0 {
		return nil, fmt.Errorf("no shards found in keyspace %s", keyspace)
	}

	if len(shardSubset) == 0 {
		return allShards, nil
	}
	existingShards := make(map[string]bool, len(allShards))
	for _, shard := range allShards {
		existingShards[shard] = true
	}
	// Validate that the provided shards are part of the keyspace.
	for _, shard := range shardSubset {
		_, found := existingShards[shard]
		if !found {
			return nil, fmt.Errorf("shard %s not found in keyspace %s", shard, keyspace)
		}
	}
	log.Infof("Selecting subset of shards in keyspace %s: %d from %d :: %+v",
		keyspace, len(shardSubset), len(allShards), shardSubset)
	return shardSubset, nil
}
