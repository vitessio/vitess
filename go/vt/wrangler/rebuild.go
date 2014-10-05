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
)

// Rebuild the serving and replication rollup data data while locking
// out other changes.
func (wr *Wrangler) RebuildShardGraph(keyspace, shard string, cells []string) error {
	return topotools.RebuildShard(wr.logger, wr.ts, keyspace, shard, cells, wr.lockTimeout, interrupted)
}

// Rebuild the serving graph data while locking out other changes.
// If some shards were recently read / updated, pass them in the cache so
// we don't read them again (and possible get stale replicated data)
func (wr *Wrangler) RebuildKeyspaceGraph(keyspace string, cells []string, shardCache map[string]*topo.ShardInfo) error {
	actionNode := actionnode.RebuildKeyspace()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildKeyspace(keyspace, cells, shardCache)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

// findCellsForRebuild will find all the cells in the given keyspace
// and create an entry if the map for them
func (wr *Wrangler) findCellsForRebuild(ki *topo.KeyspaceInfo, keyspace string, shards []string, cells []string, srvKeyspaceMap map[string]*topo.SrvKeyspace) error {
	er := concurrency.FirstErrorRecorder{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			if si, err := wr.ts.GetShard(keyspace, shard); err != nil {
				er.RecordError(fmt.Errorf("GetShard(%v,%v) failed: %v", keyspace, shard, err))
			} else {
				mu.Lock()
				for _, cell := range si.Cells {
					if !topo.InCellList(cell, cells) {
						continue
					}
					if _, ok := srvKeyspaceMap[cell]; !ok {
						srvKeyspaceMap[cell] = &topo.SrvKeyspace{
							Shards:             make([]topo.SrvShard, 0, 16),
							ShardingColumnName: ki.ShardingColumnName,
							ShardingColumnType: ki.ShardingColumnType,
							ServedFrom:         ki.ServedFrom,
						}
					}
				}
				mu.Unlock()
			}
			wg.Done()
		}(shard)
	}
	wg.Wait()
	return er.Error()
}

// This function should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func (wr *Wrangler) rebuildKeyspace(keyspace string, cells []string, shardCache map[string]*topo.ShardInfo) error {
	wr.logger.Infof("rebuildKeyspace %v", keyspace)

	ki, err := wr.ts.GetKeyspace(keyspace)
	if err != nil {
		return err
	}

	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return err
	}

	// Rebuild all shards in parallel.
	wg := sync.WaitGroup{}
	er := concurrency.FirstErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			if err := wr.RebuildShardGraph(keyspace, shard, cells); err != nil {
				er.RecordError(fmt.Errorf("RebuildShardGraph failed: %v/%v %v", keyspace, shard, err))
			}
			wg.Done()
		}(shard)
	}
	wg.Wait()
	if er.HasErrors() {
		return er.Error()
	}

	// Build the list of cells to work on: we get the union
	// of all the Cells of all the Shards, limited to the provided cells.
	//
	// srvKeyspaceMap is a map:
	//   key: cell
	//   value: topo.SrvKeyspace object being built
	srvKeyspaceMap := make(map[string]*topo.SrvKeyspace)
	if err := wr.findCellsForRebuild(ki, keyspace, shards, cells, srvKeyspaceMap); err != nil {
		return err
	}

	// Then we add the cells from the keyspaces we might be 'ServedFrom'.
	for _, servedFrom := range ki.ServedFrom {
		servedFromShards, err := wr.ts.GetShardNames(servedFrom)
		if err != nil {
			return err
		}
		if err := wr.findCellsForRebuild(ki, servedFrom, servedFromShards, cells, srvKeyspaceMap); err != nil {
			return err
		}
	}

	// for each entry in the srvKeyspaceMap map, we do the following:
	// - read the SrvShard structures for each shard / cell
	// - if not present, build an empty one from global Shard
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	if shardCache == nil {
		shardCache = make(map[string]*topo.ShardInfo)
	}
	for cell, srvKeyspace := range srvKeyspaceMap {
		keyspaceDbTypes := make(map[topo.TabletType]bool)
		srvKeyspace.Partitions = make(map[topo.TabletType]*topo.KeyspacePartition)
		for _, shard := range shards {
			srvShard, err := wr.ts.GetSrvShard(cell, keyspace, shard)
			switch err {
			case nil:
				// we keep going
			case topo.ErrNoNode:
				wr.logger.Infof("Cell %v for %v/%v has no SvrShard, using Shard data with no TabletTypes instead", cell, keyspace, shard)
				si, ok := shardCache[shard]
				if !ok {
					si, err = wr.ts.GetShard(keyspace, shard)
					if err != nil {
						return fmt.Errorf("GetShard(%v, %v) (backup for GetSrvShard in cell %v) failed: %v", keyspace, shard, cell, err)
					}
					shardCache[shard] = si
				}
				srvShard = &topo.SrvShard{
					Name:        si.ShardName(),
					KeyRange:    si.KeyRange,
					ServedTypes: si.ServedTypes,
					MasterCell:  si.MasterAlias.Cell,
				}
			default:
				return err
			}
			for _, tabletType := range srvShard.TabletTypes {
				keyspaceDbTypes[tabletType] = true
			}

			// for each type this shard is supposed to serve,
			// add it to srvKeyspace.Partitions
			for _, tabletType := range srvShard.ServedTypes {
				if _, ok := srvKeyspace.Partitions[tabletType]; !ok {
					srvKeyspace.Partitions[tabletType] = &topo.KeyspacePartition{
						Shards: make([]topo.SrvShard, 0)}
				}
				srvKeyspace.Partitions[tabletType].Shards = append(srvKeyspace.Partitions[tabletType].Shards, *srvShard)
			}
		}

		srvKeyspace.TabletTypes = make([]topo.TabletType, 0, len(keyspaceDbTypes))
		for dbType := range keyspaceDbTypes {
			srvKeyspace.TabletTypes = append(srvKeyspace.TabletTypes, dbType)
		}

		first := true
		for tabletType, partition := range srvKeyspace.Partitions {
			topo.SrvShardArray(partition.Shards).Sort()

			// check the first Start is MinKey, the last End is MaxKey,
			// and the values in between match: End[i] == Start[i+1]
			if partition.Shards[0].KeyRange.Start != key.MinKey {
				return fmt.Errorf("keyspace partition for %v in cell %v does not start with %v", tabletType, cell, key.MinKey)
			}
			if partition.Shards[len(partition.Shards)-1].KeyRange.End != key.MaxKey {
				return fmt.Errorf("keyspace partition for %v in cell %v does not end with %v", tabletType, cell, key.MaxKey)
			}
			for i := range partition.Shards[0 : len(partition.Shards)-1] {
				if partition.Shards[i].KeyRange.End != partition.Shards[i+1].KeyRange.Start {
					return fmt.Errorf("non-contiguous KeyRange values for %v in cell %v at shard %v to %v: %v != %v", tabletType, cell, i, i+1, partition.Shards[i].KeyRange.End.Hex(), partition.Shards[i+1].KeyRange.Start.Hex())
				}
			}

			// backfill Shards
			if first {
				first = false
				srvKeyspace.Shards = partition.Shards
			}
		}
	}

	// and then finally save the keyspace objects
	for cell, srvKeyspace := range srvKeyspaceMap {
		wr.logger.Infof("updating keyspace serving graph in cell %v for %v", cell, keyspace)
		if err := wr.ts.UpdateSrvKeyspace(cell, keyspace, srvKeyspace); err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// This is a quick and dirty tool to resurrect the TopologyServer data from the
// canonical data stored in the tablet nodes.
//
// cells: local vt cells to scan for all tablets
// keyspaces: list of keyspaces to rebuild
func (wr *Wrangler) RebuildReplicationGraph(cells []string, keyspaces []string) error {
	if cells == nil || len(cells) == 0 {
		return fmt.Errorf("must specify cells to rebuild replication graph")
	}
	if keyspaces == nil || len(keyspaces) == 0 {
		return fmt.Errorf("must specify keyspaces to rebuild replication graph")
	}

	allTablets := make([]*topo.TabletInfo, 0, 1024)
	for _, cell := range cells {
		tablets, err := topotools.GetAllTablets(wr.ts, cell)
		if err != nil {
			return err
		}
		allTablets = append(allTablets, tablets...)
	}

	for _, keyspace := range keyspaces {
		wr.logger.Infof("delete keyspace shards: %v", keyspace)
		if err := wr.ts.DeleteKeyspaceShards(keyspace); err != nil {
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
				if err := topo.CreateShard(wr.ts, ti.Keyspace, ti.Shard); err != nil && err != topo.ErrNodeExists {
					wr.logger.Warningf("failed re-creating shard %v: %v", shardPath, err)
					hasErr = true
				} else {
					shardsCreated[shardPath] = true
				}
			}
			mu.Unlock()
			err := topo.UpdateTabletReplicationData(wr.ts, ti.Tablet)
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
			if err := wr.RebuildKeyspaceGraph(keyspace, nil, nil); err != nil {
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
