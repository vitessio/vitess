// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

// RebuildShardGraph rebuilds the serving and replication rollup data data while locking
// out other changes.
func (wr *Wrangler) RebuildShardGraph(ctx context.Context, keyspace, shard string, cells []string) (*topo.ShardInfo, error) {
	return topotools.RebuildShard(ctx, wr.logger, wr.ts, keyspace, shard, cells, wr.lockTimeout)
}

// RebuildKeyspaceGraph rebuilds the serving graph data while locking out other changes.
// If some shards were recently read / updated, pass them in the cache so
// we don't read them again (and possible get stale replicated data)
func (wr *Wrangler) RebuildKeyspaceGraph(ctx context.Context, keyspace string, cells []string, rebuildSrvShards bool) error {
	actionNode := actionnode.RebuildKeyspace()
	lockPath, err := wr.lockKeyspace(ctx, keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildKeyspace(ctx, keyspace, cells, rebuildSrvShards)
	return wr.unlockKeyspace(ctx, keyspace, actionNode, lockPath, err)
}

// findCellsForRebuild will find all the cells in the given keyspace
// and create an entry if the map for them
func (wr *Wrangler) findCellsForRebuild(ki *topo.KeyspaceInfo, shardMap map[string]*topo.ShardInfo, cells []string, srvKeyspaceMap map[string]*topo.SrvKeyspace) {
	for _, si := range shardMap {
		for _, cell := range si.Cells {
			if !topo.InCellList(cell, cells) {
				continue
			}
			if _, ok := srvKeyspaceMap[cell]; !ok {
				srvKeyspaceMap[cell] = &topo.SrvKeyspace{
					ShardingColumnName: ki.ShardingColumnName,
					ShardingColumnType: key.ProtoToKeyspaceIdType(ki.ShardingColumnType),
					ServedFrom:         ki.ComputeCellServedFrom(cell),
					SplitShardCount:    ki.SplitShardCount,
				}
			}
		}
	}
}

// This function should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func (wr *Wrangler) rebuildKeyspace(ctx context.Context, keyspace string, cells []string, rebuildSrvShards bool) error {
	wr.logger.Infof("rebuildKeyspace %v", keyspace)

	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	var shardCache map[string]*topo.ShardInfo
	if rebuildSrvShards {
		shards, err := wr.ts.GetShardNames(ctx, keyspace)
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
				if shardInfo, err := wr.RebuildShardGraph(ctx, keyspace, shard, cells); err != nil {
					rec.RecordError(fmt.Errorf("RebuildShardGraph failed: %v/%v %v", keyspace, shard, err))
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
		shardCache, err = wr.ts.FindAllShardsInKeyspace(ctx, keyspace)
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
	srvKeyspaceMap := make(map[string]*topo.SrvKeyspace)
	wr.findCellsForRebuild(ki, shardCache, cells, srvKeyspaceMap)

	// Then we add the cells from the keyspaces we might be 'ServedFrom'.
	for _, ksf := range ki.ServedFroms {
		servedFromShards, err := wr.ts.FindAllShardsInKeyspace(ctx, ksf.Keyspace)
		if err != nil {
			return err
		}
		wr.findCellsForRebuild(ki, servedFromShards, cells, srvKeyspaceMap)
	}

	// for each entry in the srvKeyspaceMap map, we do the following:
	// - read the SrvShard structures for each shard / cell
	// - if not present, build an empty one from global Shard
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for cell, srvKeyspace := range srvKeyspaceMap {
		srvKeyspace.Partitions = make(map[topo.TabletType]*topo.KeyspacePartition)
		for _, si := range shardCache {
			servedTypes := si.GetServedTypesPerCell(cell)

			// for each type this shard is supposed to serve,
			// add it to srvKeyspace.Partitions
			for _, tabletType := range servedTypes {
				if _, ok := srvKeyspace.Partitions[tabletType]; !ok {
					srvKeyspace.Partitions[tabletType] = &topo.KeyspacePartition{
						ShardReferences: make([]topo.ShardReference, 0),
					}
				}
				srvKeyspace.Partitions[tabletType].ShardReferences = append(srvKeyspace.Partitions[tabletType].ShardReferences, topo.ShardReference{
					Name:     si.ShardName(),
					KeyRange: key.ProtoToKeyRange(si.KeyRange),
				})
			}
		}

		if err := wr.orderAndCheckPartitions(cell, srvKeyspace); err != nil {
			return err
		}
	}

	// and then finally save the keyspace objects
	for cell, srvKeyspace := range srvKeyspaceMap {
		wr.logger.Infof("updating keyspace serving graph in cell %v for %v", cell, keyspace)
		if err := wr.ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// orderAndCheckPartitions will re-order the partition list, and check
// it's correct.
func (wr *Wrangler) orderAndCheckPartitions(cell string, srvKeyspace *topo.SrvKeyspace) error {

	// now check them all
	for tabletType, partition := range srvKeyspace.Partitions {
		topo.ShardReferenceArray(partition.ShardReferences).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		if partition.ShardReferences[0].KeyRange.Start != key.MinKey {
			return fmt.Errorf("keyspace partition for %v in cell %v does not start with %v", tabletType, cell, key.MinKey)
		}
		if partition.ShardReferences[len(partition.ShardReferences)-1].KeyRange.End != key.MaxKey {
			return fmt.Errorf("keyspace partition for %v in cell %v does not end with %v", tabletType, cell, key.MaxKey)
		}
		for i := range partition.ShardReferences[0 : len(partition.ShardReferences)-1] {
			if partition.ShardReferences[i].KeyRange.End != partition.ShardReferences[i+1].KeyRange.Start {
				return fmt.Errorf("non-contiguous KeyRange values for %v in cell %v at shard %v to %v: %v != %v", tabletType, cell, i, i+1, partition.ShardReferences[i].KeyRange.End.Hex(), partition.ShardReferences[i+1].KeyRange.Start.Hex())
			}
		}
	}

	return nil
}

func strInList(sl []string, s string) bool {
	for _, x := range sl {
		if x == s {
			return true
		}
	}
	return false
}

// RebuildReplicationGraph is a quick and dirty tool to resurrect the TopologyServer data from the
// canonical data stored in the tablet nodes.
//
// cells: local vt cells to scan for all tablets
// keyspaces: list of keyspaces to rebuild
func (wr *Wrangler) RebuildReplicationGraph(ctx context.Context, cells []string, keyspaces []string) error {
	if cells == nil || len(cells) == 0 {
		return fmt.Errorf("must specify cells to rebuild replication graph")
	}
	if keyspaces == nil || len(keyspaces) == 0 {
		return fmt.Errorf("must specify keyspaces to rebuild replication graph")
	}

	allTablets := make([]*topo.TabletInfo, 0, 1024)
	for _, cell := range cells {
		tablets, err := topotools.GetAllTablets(ctx, wr.ts, cell)
		if err != nil {
			return err
		}
		allTablets = append(allTablets, tablets...)
	}

	for _, keyspace := range keyspaces {
		wr.logger.Infof("delete keyspace shards: %v", keyspace)
		if err := wr.ts.DeleteKeyspaceShards(ctx, keyspace); err != nil {
			return err
		}
	}

	keyspacesToRebuild := make(map[string]bool)
	shardsCreated := make(map[string]bool)
	hasErr := false
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, ti := range allTablets {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			defer wg.Done()
			if !ti.IsInReplicationGraph() {
				return
			}
			if !strInList(keyspaces, ti.Keyspace) {
				return
			}
			mu.Lock()
			keyspacesToRebuild[ti.Keyspace] = true
			shardPath := ti.Keyspace + "/" + ti.Shard
			if !shardsCreated[shardPath] {
				if err := wr.ts.CreateShard(ctx, ti.Keyspace, ti.Shard); err != nil && err != topo.ErrNodeExists {
					wr.logger.Warningf("failed re-creating shard %v: %v", shardPath, err)
					hasErr = true
				} else {
					shardsCreated[shardPath] = true
				}
			}
			mu.Unlock()
			err := topo.UpdateTabletReplicationData(ctx, wr.ts, ti.Tablet)
			if err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				wr.logger.Warningf("failed updating replication data: %v", err)
			}
		}(ti)
	}
	wg.Wait()

	for keyspace := range keyspacesToRebuild {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil, true); err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				wr.logger.Warningf("RebuildKeyspaceGraph(%v) failed: %v", keyspace, err)
				return
			}
		}(keyspace)
	}
	wg.Wait()

	if hasErr {
		return fmt.Errorf("some errors occurred rebuilding replication graph, consult log")
	}
	return nil
}
