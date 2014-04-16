// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"flag"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// UseSrvShardLocks is a temporary flag. Once tested and rolled out,
// it will be defaulted to true, and removed.
var UseSrvShardLocks = flag.Bool("use_srv_shard_locks", false, "If true, takes the SrvShard lock for each shard being rebuilt")

// RebuildShardOptions are options for rebuildShard
type RebuildShardOptions struct {
	// Cells that should be rebuilt. If nil, rebuild in all cells.
	Cells []string
	// It is OK to ignore ErrPartialResult (which may mean
	// that some cell is unavailable).
	IgnorePartialResult bool
}

// Update shard file with new master, replicas, etc.
//
// Re-read from TopologyServer to make sure we are using the side
// effects of all actions.
//
// This function should only be used with an action lock on the shard
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
func RebuildShard(ts topo.Server, keyspace, shard string, options RebuildShardOptions, timeout time.Duration, interrupted chan struct{}) error {
	if *UseSrvShardLocks {
		return rebuildShardSrvShardLocks(ts, keyspace, shard, options, timeout, interrupted)
	}

	log.Infof("RebuildShard %v/%v", keyspace, shard)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShardByCell(ts, keyspace, shard, options.Cells)
	if err != nil {
		if options.IgnorePartialResult && err == topo.ErrPartialResult {
			log.Warningf("rebuildShard: got ErrPartialResult from GetTabletMapForShard, but skipping error as it was expected")
		} else {
			return err
		}
	}

	tablets := make([]*topo.TabletInfo, 0, len(tabletMap))
	for _, ti := range tabletMap {
		if ti.Keyspace != shardInfo.Keyspace() || ti.Shard != shardInfo.ShardName() {
			return fmt.Errorf("CRITICAL: tablet %v is in replication graph for shard %v/%v but belongs to shard %v:%v (maybe remove its replication path in shard %v/%v)", ti.Alias, keyspace, shard, ti.Keyspace, ti.Shard, keyspace, shard)
		}
		if !ti.IsInReplicationGraph() {
			// only valid case is a scrapped master in the
			// catastrophic reparent case
			if ti.Parent.Uid != topo.NO_TABLET {
				log.Warningf("Tablet %v should not be in the replication graph, please investigate (it will be ignored in the rebuild)", ti.Alias)
			}
		}
		tablets = append(tablets, ti)
	}

	return rebuildShardSrvGraph(ts, shardInfo, tablets, options.Cells)
}

// the following types are used as locations in the serving graph

type cellKeyspaceShard struct {
	cell     string
	keyspace string
	shard    string
}

type cellKeyspaceShardType struct {
	cell       string
	keyspace   string
	shard      string
	tabletType topo.TabletType
}

// Write serving graph data to the cells
func rebuildShardSrvGraph(ts topo.Server, shardInfo *topo.ShardInfo, tablets []*topo.TabletInfo, cells []string) error {
	log.Infof("rebuildShardSrvGraph %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())

	// Get all existing db types so they can be removed if nothing
	// had been editted.  This applies to all cells, which can't
	// be determined until you walk through all the tablets.
	//
	// existingDbTypeLocations is a map:
	//   key: {cell,keyspace,shard,tabletType}
	//   value: true
	existingDbTypeLocations := make(map[cellKeyspaceShardType]bool)

	// Update db type addresses in the serving graph
	//
	// locationAddrsMap is a map:
	//   key: {cell,keyspace,shard,tabletType}
	//   value: EndPoints (list of server records)
	locationAddrsMap := make(map[cellKeyspaceShardType]*topo.EndPoints)

	// we keep track of the existingDbTypeLocations we've already looked at
	knownShardLocations := make(map[cellKeyspaceShard]bool)

	for _, tablet := range tablets {
		// only look at tablets in the cells we want to rebuild
		if !topo.InCellList(tablet.Tablet.Alias.Cell, cells) {
			continue
		}

		// this is {cell,keyspace,shard}
		// we'll get the children to find the existing types
		shardLocation := cellKeyspaceShard{tablet.Tablet.Alias.Cell, tablet.Tablet.Keyspace, tablet.Shard}
		// only need to do this once per cell
		if !knownShardLocations[shardLocation] {
			log.Infof("Getting tablet types on cell %v for %v/%v", tablet.Tablet.Alias.Cell, tablet.Tablet.Keyspace, tablet.Shard)
			tabletTypes, err := ts.GetSrvTabletTypesPerShard(tablet.Tablet.Alias.Cell, tablet.Tablet.Keyspace, tablet.Shard)
			if err != nil {
				if err != topo.ErrNoNode {
					return err
				}
			} else {
				for _, tabletType := range tabletTypes {
					existingDbTypeLocations[cellKeyspaceShardType{tablet.Tablet.Alias.Cell, tablet.Tablet.Keyspace, tablet.Shard, tabletType}] = true
				}
			}
			knownShardLocations[shardLocation] = true
		}

		// Check IsInServingGraph after we have populated
		// existingDbTypeLocations so we properly prune data
		// if the definition of serving type changes.
		if !tablet.IsInServingGraph() {
			continue
		}

		location := cellKeyspaceShardType{tablet.Tablet.Alias.Cell, tablet.Keyspace, tablet.Shard, tablet.Type}
		addrs, ok := locationAddrsMap[location]
		if !ok {
			addrs = topo.NewEndPoints()
			locationAddrsMap[location] = addrs
		}

		entry, err := tablet.Tablet.EndPoint()
		if err != nil {
			log.Warningf("EndPointForTablet failed for tablet %v: %v", tablet.Alias, err)
			continue
		}
		addrs.Entries = append(addrs.Entries, *entry)
	}

	// we're gonna parallelize a lot here
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// write all the {cell,keyspace,shard,type}
	// nodes everywhere we want them
	for location, addrs := range locationAddrsMap {
		wg.Add(1)
		go func(location cellKeyspaceShardType, addrs *topo.EndPoints) {
			log.Infof("saving serving graph for cell %v shard %v/%v tabletType %v", location.cell, location.keyspace, location.shard, location.tabletType)
			if err := ts.UpdateEndPoints(location.cell, location.keyspace, location.shard, location.tabletType, addrs); err != nil {
				rec.RecordError(fmt.Errorf("writing endpoints for cell %v shard %v/%v tabletType %v failed: %v", location.cell, location.keyspace, location.shard, location.tabletType, err))
			}
			wg.Done()
		}(location, addrs)
	}

	// Delete any pre-existing paths that were not updated by this process.
	// That's the existingDbTypeLocations - locationAddrsMap
	for dbTypeLocation := range existingDbTypeLocations {
		if _, ok := locationAddrsMap[dbTypeLocation]; !ok {
			cell := dbTypeLocation.cell
			if !topo.InCellList(cell, cells) {
				continue
			}

			wg.Add(1)
			go func(dbTypeLocation cellKeyspaceShardType) {
				log.Infof("removing stale db type from serving graph: %v", dbTypeLocation)
				if err := ts.DeleteSrvTabletType(dbTypeLocation.cell, dbTypeLocation.keyspace, dbTypeLocation.shard, dbTypeLocation.tabletType); err != nil {
					log.Warningf("unable to remove stale db type %v from serving graph: %v", dbTypeLocation, err)
				}
				wg.Done()
			}(dbTypeLocation)
		}
	}

	// wait until we're done with the background stuff to do the rest
	// FIXME(alainjobart) this wouldn't be necessary if UpdateSrvShard
	// below was creating the zookeeper nodes recursively.
	wg.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Update per-shard information per cell-specific serving path.
	//
	// srvShardByPath is a map:
	//   key: {cell,keyspace,shard}
	//   value: SrvShard
	// this will create all the SrvShard objects
	srvShardByPath := make(map[cellKeyspaceShard]*topo.SrvShard)
	for location := range locationAddrsMap {
		// location will be {cell,keyspace,shard,type}
		srvShardPath := cellKeyspaceShard{location.cell, location.keyspace, location.shard}
		srvShard, ok := srvShardByPath[srvShardPath]
		if !ok {
			srvShard = &topo.SrvShard{
				KeyRange:    shardInfo.KeyRange,
				ServedTypes: shardInfo.ServedTypes,
				TabletTypes: make([]topo.TabletType, 0, 2),
			}
			srvShardByPath[srvShardPath] = srvShard
		}
		foundType := false
		for _, t := range srvShard.TabletTypes {
			if t == location.tabletType {
				foundType = true
			}
		}
		if !foundType {
			srvShard.TabletTypes = append(srvShard.TabletTypes, location.tabletType)
		}
	}

	// Save the shard entries
	for cks, srvShard := range srvShardByPath {
		wg.Add(1)
		go func(cks cellKeyspaceShard, srvShard *topo.SrvShard) {
			log.Infof("updating shard serving graph in cell %v for %v/%v", cks.cell, cks.keyspace, cks.shard)
			if err := ts.UpdateSrvShard(cks.cell, cks.keyspace, cks.shard, srvShard); err != nil {
				rec.RecordError(fmt.Errorf("writing serving data in cell %v for %v/%v failed: %v", cks.cell, cks.keyspace, cks.shard, err))
			}
			wg.Done()
		}(cks, srvShard)
	}
	wg.Wait()
	return rec.Error()
}

// rebuildShardSrvShardLocks rebuilds the shard while taking the SrvShardLock
func rebuildShardSrvShardLocks(ts topo.Server, keyspace, shard string, options RebuildShardOptions, timeout time.Duration, interrupted chan struct{}) error {
	log.Infof("rebuildShardSrvShardLocks %v/%v", keyspace, shard)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// rebuild all cells in parallel
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range shardInfo.Cells {
		// skip this cell if we shouldn't rebuild it
		if !topo.InCellList(cell, options.Cells) {
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

			// Lock the SrvShard so we write a consistent data set.
			actionNode := actionnode.RebuildSrvShard()
			lockPath, err := actionNode.LockSrvShard(ts, cell, keyspace, shard, timeout, interrupted)
			if err != nil {
				rec.RecordError(err)
				return
			}

			// write the data we need to
			rebuildErr := rebuildCellSrvShard(ts, shardInfo, cell, tablets)

			// and unlock
			if err := actionNode.UnlockSrvShard(ts, cell, keyspace, shard, lockPath, rebuildErr); err != nil {
				rec.RecordError(err)
			}
		}(cell)
	}
	wg.Wait()

	return rec.Error()
}

// rebuildCellSrvShard computes and writes the serving graph data to a
// single cell
func rebuildCellSrvShard(ts topo.Server, shardInfo *topo.ShardInfo, cell string, tablets map[topo.TabletAlias]*topo.TabletInfo) error {
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
				if err := ts.DeleteSrvTabletType(cell, shardInfo.Keyspace(), shardInfo.ShardName(), tabletType); err != nil {
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
			KeyRange:    shardInfo.KeyRange,
			ServedTypes: shardInfo.ServedTypes,
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
