// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RebuildKeyspace rebuilds the serving graph data while locking out other changes.
func RebuildKeyspace(ctx context.Context, log logutil.Logger, ts topo.Server, keyspace string, cells []string, rebuildSrvShards bool) error {
	node := actionnode.RebuildKeyspace()
	lockPath, err := node.LockKeyspace(ctx, ts, keyspace)
	if err != nil {
		return err
	}

	err = rebuildKeyspace(ctx, log, ts, keyspace, cells, rebuildSrvShards)
	return node.UnlockKeyspace(ctx, ts, keyspace, lockPath, err)
}

// findCellsForRebuild will find all the cells in the given keyspace
// and create an entry if the map for them
func findCellsForRebuild(ki *topo.KeyspaceInfo, shardMap map[string]*topo.ShardInfo, cells []string, srvKeyspaceMap map[string]*topodatapb.SrvKeyspace) {
	for _, si := range shardMap {
		for _, cell := range si.Cells {
			if !topo.InCellList(cell, cells) {
				continue
			}
			if _, ok := srvKeyspaceMap[cell]; !ok {
				srvKeyspaceMap[cell] = &topodatapb.SrvKeyspace{
					ShardingColumnName: ki.ShardingColumnName,
					ShardingColumnType: ki.ShardingColumnType,
					ServedFrom:         ki.ComputeCellServedFrom(cell),
					SplitShardCount:    ki.SplitShardCount,
				}
			}
		}
	}
}

// rebuildKeyspace should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func rebuildKeyspace(ctx context.Context, log logutil.Logger, ts topo.Server, keyspace string, cells []string, rebuildSrvShards bool) error {
	log.Infof("rebuildKeyspace %v", keyspace)

	ki, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	var shardCache map[string]*topo.ShardInfo
	if rebuildSrvShards {
		shards, err := ts.GetShardNames(ctx, keyspace)
		if err != nil {
			return nil
		}

		// Rebuild all shards in parallel, save the shards
		shardCache = make(map[string]*topo.ShardInfo)
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		rec := concurrency.FirstErrorRecorder{}
		for _, shard := range shards {
			wg.Add(1)
			go func(shard string) {
				if shardInfo, err := RebuildShard(ctx, log, ts, keyspace, shard, cells); err != nil {
					rec.RecordError(fmt.Errorf("RebuildShard failed: %v/%v %v", keyspace, shard, err))
				} else {
					mu.Lock()
					shardCache[shard] = shardInfo
					mu.Unlock()
				}
				wg.Done()
			}(shard)
		}
		wg.Wait()
		if rec.HasErrors() {
			return rec.Error()
		}

	} else {
		shardCache, err = ts.FindAllShardsInKeyspace(ctx, keyspace)
		if err != nil {
			return err
		}
	}

	// Build the list of cells to work on: we get the union
	// of all the Cells of all the Shards, limited to the provided cells.
	//
	// srvKeyspaceMap is a map:
	//   key: cell
	//   value: topo.SrvKeyspace object being built
	srvKeyspaceMap := make(map[string]*topodatapb.SrvKeyspace)
	findCellsForRebuild(ki, shardCache, cells, srvKeyspaceMap)

	// Then we add the cells from the keyspaces we might be 'ServedFrom'.
	for _, ksf := range ki.ServedFroms {
		servedFromShards, err := ts.FindAllShardsInKeyspace(ctx, ksf.Keyspace)
		if err != nil {
			return err
		}
		findCellsForRebuild(ki, servedFromShards, cells, srvKeyspaceMap)
	}

	// for each entry in the srvKeyspaceMap map, we do the following:
	// - read the SrvShard structures for each shard / cell
	// - if not present, build an empty one from global Shard
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for cell, srvKeyspace := range srvKeyspaceMap {
		for _, si := range shardCache {
			servedTypes := si.GetServedTypesPerCell(cell)

			// for each type this shard is supposed to serve,
			// add it to srvKeyspace.Partitions
			for _, tabletType := range servedTypes {
				partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
				if partition == nil {
					partition = &topodatapb.SrvKeyspace_KeyspacePartition{
						ServedType: tabletType,
					}
					srvKeyspace.Partitions = append(srvKeyspace.Partitions, partition)
				}
				partition.ShardReferences = append(partition.ShardReferences, &topodatapb.ShardReference{
					Name:     si.ShardName(),
					KeyRange: si.KeyRange,
				})
			}
		}

		if err := orderAndCheckPartitions(cell, srvKeyspace); err != nil {
			return err
		}
	}

	// and then finally save the keyspace objects
	for cell, srvKeyspace := range srvKeyspaceMap {
		log.Infof("updating keyspace serving graph in cell %v for %v", cell, keyspace)
		if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// orderAndCheckPartitions will re-order the partition list, and check
// it's correct.
func orderAndCheckPartitions(cell string, srvKeyspace *topodatapb.SrvKeyspace) error {
	// now check them all
	for _, partition := range srvKeyspace.Partitions {
		tabletType := partition.ServedType
		topoproto.ShardReferenceArray(partition.ShardReferences).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		first := partition.ShardReferences[0]
		if first.KeyRange != nil && len(first.KeyRange.Start) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not start with min key", tabletType, cell)
		}
		last := partition.ShardReferences[len(partition.ShardReferences)-1]
		if last.KeyRange != nil && len(last.KeyRange.End) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not end with max key", tabletType, cell)
		}
		for i := range partition.ShardReferences[0 : len(partition.ShardReferences)-1] {
			currShard := partition.ShardReferences[i]
			nextShard := partition.ShardReferences[i+1]
			currHasKeyRange := currShard.KeyRange != nil
			nextHasKeyRange := nextShard.KeyRange != nil
			if currHasKeyRange != nextHasKeyRange {
				return fmt.Errorf("shards with inconsistent KeyRanges for %v in cell %v. shards: %v, %v", tabletType, cell, currShard, nextShard)
			}
			if !currHasKeyRange {
				// this is the custom sharding case, all KeyRanges must be nil
				continue
			}
			if bytes.Compare(currShard.KeyRange.End, nextShard.KeyRange.Start) != 0 {
				return fmt.Errorf("non-contiguous KeyRange values for %v in cell %v at shard %v to %v: %v != %v", tabletType, cell, i, i+1, hex.EncodeToString(currShard.KeyRange.End), hex.EncodeToString(nextShard.KeyRange.Start))
			}
		}
	}

	return nil
}
