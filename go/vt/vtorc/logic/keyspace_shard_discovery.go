/*
Copyright 2022 The Vitess Authors.

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

package logic

import (
	"context"
	"sync"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

var statsShardsWatched = stats.NewGaugesFuncWithMultiLabels("ShardsWatched",
	"Keyspace/shards currently watched",
	[]string{"Keyspace", "Shard"},
	getShardsWatchedStats)

// getShardsWatchedStats returns the keyspace/shards watched in a format for stats.
func getShardsWatchedStats() map[string]int64 {
	shardsWatched := make(map[string]int64)
	allShardNames, err := inst.ReadAllShardNames()
	if err != nil {
		log.Errorf("Failed to read all shard names: %+v", err)
		return shardsWatched
	}
	for ks, shards := range allShardNames {
		for _, shard := range shards {
			shardsWatched[ks+"."+shard] = 1
		}
	}
	return shardsWatched
}

// refreshAllKeyspacesAndShardsMu ensures RefreshAllKeyspacesAndShards
// is not executed concurrently.
var refreshAllKeyspacesAndShardsMu sync.Mutex

// RefreshAllKeyspacesAndShards reloads the keyspace and shard information for the keyspaces that vtorc is concerned with.
func RefreshAllKeyspacesAndShards(ctx context.Context) error {
	refreshAllKeyspacesAndShardsMu.Lock()
	defer refreshAllKeyspacesAndShardsMu.Unlock()

	var keyspaces []string
	if len(shardsToWatch) == 0 { // all known keyspaces
		ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()
		var err error
		// Get all the keyspaces
		keyspaces, err = ts.GetKeyspaces(ctx)
		if err != nil {
			return err
		}
	} else {
		// Get keyspaces to watch from the list of known keyspaces.
		keyspaces = maps.Keys(shardsToWatch)
	}

	refreshCtx, refreshCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer refreshCancel()
	var wg sync.WaitGroup
	for idx, keyspace := range keyspaces {
		// Check if the current keyspace name is the same as the last one.
		// If it is, then we know we have already refreshed its information.
		// We do not need to do it again.
		if idx != 0 && keyspace == keyspaces[idx-1] {
			continue
		}
		wg.Add(2)
		go func(keyspace string) {
			defer wg.Done()
			_ = refreshKeyspaceHelper(refreshCtx, keyspace)
		}(keyspace)
		go func(keyspace string) {
			defer wg.Done()
			_ = refreshAllShards(refreshCtx, keyspace)
		}(keyspace)
	}
	wg.Wait()

	return nil
}

// RefreshKeyspaceAndShard refreshes the keyspace record and shard record for the given keyspace and shard.
func RefreshKeyspaceAndShard(keyspaceName string, shardName string) error {
	err := refreshKeyspace(keyspaceName)
	if err != nil {
		return err
	}
	return refreshShard(keyspaceName, shardName)
}

// shouldWatchShard returns true if a shard is within the shardsToWatch
// ranges for it's keyspace.
func shouldWatchShard(shard *topo.ShardInfo) bool {
	if len(shardsToWatch) == 0 {
		return true
	}

	watchRanges, found := shardsToWatch[shard.Keyspace()]
	if !found {
		return false
	}

	for _, keyRange := range watchRanges {
		if key.KeyRangeContainsKeyRange(keyRange, shard.GetKeyRange()) {
			return true
		}
	}
	return false
}

// refreshKeyspace refreshes the keyspace's information for the given keyspace from the topo
func refreshKeyspace(keyspaceName string) error {
	refreshCtx, refreshCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer refreshCancel()
	return refreshKeyspaceHelper(refreshCtx, keyspaceName)
}

// refreshShard refreshes the shard's information for the given keyspace/shard from the topo
func refreshShard(keyspaceName, shardName string) error {
	refreshCtx, refreshCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer refreshCancel()
	return refreshSingleShardHelper(refreshCtx, keyspaceName, shardName)
}

// refreshKeyspaceHelper is a helper function which reloads the given keyspace's information
func refreshKeyspaceHelper(ctx context.Context, keyspaceName string) error {
	keyspaceInfo, err := ts.GetKeyspace(ctx, keyspaceName)
	if err != nil {
		log.Error(err)
		return err
	}
	err = inst.SaveKeyspace(keyspaceInfo)
	if err != nil {
		log.Error(err)
	}
	return err
}

// refreshAllShards refreshes all the shard records in the given keyspace.
func refreshAllShards(ctx context.Context, keyspaceName string) error {
	// get all shards for keyspace name.
	shardInfos, err := ts.FindAllShardsInKeyspace(ctx, keyspaceName, &topo.FindAllShardsInKeyspaceOptions{
		// Fetch shard records concurrently to speed up discovery. A typical
		// Vitess cluster will have 1-3 vtorc instances deployed, so there is
		// little risk of a thundering herd.
		Concurrency: 8,
	})
	if err != nil {
		log.Error(err)
		return err
	}

	// save shards that should be watched.
	savedShards := make(map[string]bool, len(shardInfos))
	for _, shardInfo := range shardInfos {
		if !shouldWatchShard(shardInfo) {
			continue
		}
		if err = inst.SaveShard(shardInfo); err != nil {
			log.Error(err)
			return err
		}
		savedShards[shardInfo.ShardName()] = true
	}

	// delete shards that were not saved, indicating they are stale.
	shards, err := inst.ReadShardNames(keyspaceName)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, shard := range shards {
		if savedShards[shard] {
			continue
		}
		shardName := topoproto.KeyspaceShardString(keyspaceName, shard)
		log.Infof("Forgetting shard: %s", shardName)
		if err = inst.DeleteShard(keyspaceName, shard); err != nil {
			log.Errorf("Failed to delete shard %s: %+v", shardName, err)
			return err
		}
	}

	return nil
}

// refreshSingleShardHelper is a helper function that refreshes the shard record of the given keyspace/shard.
func refreshSingleShardHelper(ctx context.Context, keyspaceName string, shardName string) error {
	shardInfo, err := ts.GetShard(ctx, keyspaceName, shardName)
	if err != nil {
		log.Error(err)
		return err
	}
	err = inst.SaveShard(shardInfo)
	if err != nil {
		log.Error(err)
	}
	return err
}
