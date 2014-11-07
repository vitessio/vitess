// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// UseSrvShardLocks is a deprecated flag. We leave it here until it's removed
// from all invocations of the tools.
var UseSrvShardLocks = flag.Bool("use_srv_shard_locks", true, "DEPRECATED: If true, takes the SrvShard lock for each shard being rebuilt")

// Update shard file with new master, replicas, etc.
//
// Re-read from TopologyServer to make sure we are using the side
// effects of all actions.
//
// This function locks individual SvrShard paths, so it doesn't need a lock
// on the shard.
func RebuildShard(log logutil.Logger, ts topo.Server, keyspace, shard string, cells []string, timeout time.Duration, interrupted chan struct{}) (*topo.ShardInfo, error) {
	log.Infof("RebuildShard %v/%v", keyspace, shard)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(keyspace, shard)
	if err != nil {
		return nil, err
	}

	// rebuild all cells in parallel
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range shardInfo.Cells {
		// skip this cell if we shouldn't rebuild it
		if !topo.InCellList(cell, cells) {
			continue
		}

		// start with the master if it's in the current cell
		tabletsAsMap := make(map[topo.TabletAlias]bool)
		if shardInfo.MasterAlias.Cell == cell {
			tabletsAsMap[shardInfo.MasterAlias] = true
		}

		wg.Add(1)
		go func(cell string) {
			defer wg.Done()

			// Lock the SrvShard so we don't race with other rebuilds of the same
			// shard in the same cell (e.g. from our peer tablets).
			actionNode := actionnode.RebuildSrvShard()
			lockPath, err := actionNode.LockSrvShard(ts, cell, keyspace, shard, timeout, interrupted)
			if err != nil {
				rec.RecordError(err)
				return
			}

			// read the ShardReplication object to find tablets
			sri, err := ts.GetShardReplication(cell, keyspace, shard)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err))
				return
			}

			// add all relevant tablets to the map
			for _, rl := range sri.ReplicationLinks {
				tabletsAsMap[rl.TabletAlias] = true
				if rl.Parent.Cell == cell {
					tabletsAsMap[rl.Parent] = true
				}
			}

			// convert the map to a list
			aliases := make([]topo.TabletAlias, 0, len(tabletsAsMap))
			for a := range tabletsAsMap {
				aliases = append(aliases, a)
			}

			// read all the Tablet records
			tablets, err := topo.GetTabletMap(ts, aliases)
			switch err {
			case nil:
				// keep going, we're good
			case topo.ErrPartialResult:
				log.Warningf("Got ErrPartialResult from topo.GetTabletMap in cell %v, some tablets may not be added properly to serving graph", cell)
			default:
				rec.RecordError(fmt.Errorf("GetTabletMap in cell %v failed: %v", cell, err))
				return
			}

			// write the data we need to
			rebuildErr := rebuildCellSrvShard(log, ts, shardInfo, cell, tablets)

			// and unlock
			if err := actionNode.UnlockSrvShard(ts, cell, keyspace, shard, lockPath, rebuildErr); err != nil {
				rec.RecordError(err)
			}
		}(cell)
	}
	wg.Wait()

	return shardInfo, rec.Error()
}

// rebuildCellSrvShard computes and writes the serving graph data to a
// single cell
func rebuildCellSrvShard(log logutil.Logger, ts topo.Server, shardInfo *topo.ShardInfo, cell string, tablets map[topo.TabletAlias]*topo.TabletInfo) error {
	log.Infof("rebuildCellSrvShard %v/%v in cell %v", shardInfo.Keyspace(), shardInfo.ShardName(), cell)

	// Get all existing db types so they can be removed if nothing
	// had been edited.
	existingTabletTypes, err := ts.GetSrvTabletTypesPerShard(cell, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		if err != topo.ErrNoNode {
			return err
		}
	}

	// Update db type addresses in the serving graph
	//
	// locationAddrsMap is a map:
	//   key: tabletType
	//   value: EndPoints (list of server records)
	locationAddrsMap := make(map[topo.TabletType]*topo.EndPoints)
	for _, tablet := range tablets {
		if !tablet.IsInReplicationGraph() {
			// only valid case is a scrapped master in the
			// catastrophic reparent case
			if tablet.Parent.Uid != topo.NO_TABLET {
				log.Warningf("Tablet %v should not be in the replication graph, please investigate (it is being ignored in the rebuild)", tablet.Alias)
			}
			continue
		}

		// Check IsInServingGraph, we don't want to add tablets that
		// are not serving
		if !tablet.IsInServingGraph() {
			continue
		}

		// Check the Keyspace and Shard for the tablet are right
		if tablet.Keyspace != shardInfo.Keyspace() || tablet.Shard != shardInfo.ShardName() {
			return fmt.Errorf("CRITICAL: tablet %v is in replication graph for shard %v/%v but belongs to shard %v:%v", tablet.Alias, shardInfo.Keyspace(), shardInfo.ShardName(), tablet.Keyspace, tablet.Shard)
		}

		// Add the tablet to the list
		addrs, ok := locationAddrsMap[tablet.Type]
		if !ok {
			addrs = topo.NewEndPoints()
			locationAddrsMap[tablet.Type] = addrs
		}
		entry, err := tablet.Tablet.EndPoint()
		if err != nil {
			log.Warningf("EndPointForTablet failed for tablet %v: %v", tablet.Alias, err)
			continue
		}
		addrs.Entries = append(addrs.Entries, *entry)
	}

	// we're gonna parallelize a lot here:
	// - writing all the tabletTypes records
	// - removing the unused records
	// - writing SrvShard
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// write all the EndPoints nodes everywhere we want them
	for tabletType, addrs := range locationAddrsMap {
		wg.Add(1)
		go func(tabletType topo.TabletType, addrs *topo.EndPoints) {
			log.Infof("saving serving graph for cell %v shard %v/%v tabletType %v", cell, shardInfo.Keyspace(), shardInfo.ShardName(), tabletType)
			if err := ts.UpdateEndPoints(cell, shardInfo.Keyspace(), shardInfo.ShardName(), tabletType, addrs); err != nil {
				rec.RecordError(fmt.Errorf("writing endpoints for cell %v shard %v/%v tabletType %v failed: %v", cell, shardInfo.Keyspace(), shardInfo.ShardName(), tabletType, err))
			}
			wg.Done()
		}(tabletType, addrs)
	}

	// Delete any pre-existing paths that were not updated by this process.
	// That's the existingTabletTypes - locationAddrsMap
	for _, tabletType := range existingTabletTypes {
		if _, ok := locationAddrsMap[tabletType]; !ok {
			wg.Add(1)
			go func(tabletType topo.TabletType) {
				log.Infof("removing stale db type from serving graph: %v", tabletType)
				if err := ts.DeleteEndPoints(cell, shardInfo.Keyspace(), shardInfo.ShardName(), tabletType); err != nil {
					log.Warningf("unable to remove stale db type %v from serving graph: %v", tabletType, err)
				}
				wg.Done()
			}(tabletType)
		}
	}

	// Update srvShard object
	wg.Add(1)
	go func() {
		log.Infof("updating shard serving graph in cell %v for %v/%v", cell, shardInfo.Keyspace(), shardInfo.ShardName())
		srvShard := &topo.SrvShard{
			Name:        shardInfo.ShardName(),
			KeyRange:    shardInfo.KeyRange,
			ServedTypes: shardInfo.GetServedTypesPerCell(cell),
			MasterCell:  shardInfo.MasterAlias.Cell,
			TabletTypes: make([]topo.TabletType, 0, len(locationAddrsMap)),
		}
		for tabletType := range locationAddrsMap {
			srvShard.TabletTypes = append(srvShard.TabletTypes, tabletType)
		}

		if err := ts.UpdateSrvShard(cell, shardInfo.Keyspace(), shardInfo.ShardName(), srvShard); err != nil {
			rec.RecordError(fmt.Errorf("writing serving data in cell %v for %v/%v failed: %v", cell, shardInfo.Keyspace(), shardInfo.ShardName(), err))
		}
		wg.Done()
	}()

	wg.Wait()
	return rec.Error()
}
