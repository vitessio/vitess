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
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// setupKeyspaceAndShardRecordsWatch sets up a watch on all keyspace and shard records.
func setupKeyspaceAndShardRecordsWatch(ctx context.Context, ts *topo.Server) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			initialRecs, updateChan, err := ts.WatchAllKeyspaceAndShardRecords(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Back for a while and then try setting up the watch again.
				time.Sleep(10 * time.Second)
				continue
			}
			for _, rec := range initialRecs {
				err = processKeyspacePrefixWatchUpdate(rec)
				if err != nil {
					log.Errorf("failed to process initial keyspace/shard record: %+v", err)
					break
				}
			}
			if err != nil {
				continue
			}

			for data := range updateChan {
				err = processKeyspacePrefixWatchUpdate(data)
				if err != nil {
					log.Errorf("failed to process keyspace/shard record update: %+v", err)
					break
				}
			}
		}
	}()
}

// processKeyspacePrefixWatchUpdate processes a keyspace prefix watch update.
func processKeyspacePrefixWatchUpdate(wd *topo.WatchKeyspacePrefixData) error {
	// We ignore the error in the watch data.
	// If there is an error that closes the watch, then
	// we will open it again.
	if wd.Err != nil {
		return nil
	}
	if wd.KeyspaceInfo != nil {
		return processKeyspaceUpdate(wd)
	} else if wd.ShardInfo != nil {
		return processShardUpdate(wd)
	}
	return wd.Err
}

// processShardUpdate processes a shard update.
func processShardUpdate(wd *topo.WatchKeyspacePrefixData) error {
	if !shardPartOfWatch(wd.ShardInfo.Keyspace(), wd.ShardInfo.GetKeyRange()) {
		return nil
	}
	return inst.SaveShard(wd.ShardInfo)
}

// processKeyspaceUpdate processes a keyspace update.
func processKeyspaceUpdate(wd *topo.WatchKeyspacePrefixData) error {
	if !keyspacePartOfWatch(wd.KeyspaceInfo.KeyspaceName()) {
		return nil
	}
	return inst.SaveKeyspace(wd.KeyspaceInfo)
}

// RefreshKeyspaceAndShard refreshes the keyspace record and shard record for the given keyspace and shard.
func RefreshKeyspaceAndShard(keyspaceName string, shardName string) error {
	err := refreshKeyspace(keyspaceName)
	if err != nil {
		return err
	}
	return refreshShard(keyspaceName, shardName)
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
