// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
)

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
func RebuildShard(ts Server, keyspace, shard string, options RebuildShardOptions) error {
	log.Infof("RebuildShard %v/%v", keyspace, shard)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShardByCell(ts, keyspace, shard, options.Cells)
	if err != nil {
		if options.IgnorePartialResult && err == ErrPartialResult {
			log.Warningf("rebuildShard: got ErrPartialResult from GetTabletMapForShard, but skipping error as it was expected")
		} else {
			return err
		}
	}

	tablets := make([]*TabletInfo, 0, len(tabletMap))
	for _, ti := range tabletMap {
		if ti.Keyspace != shardInfo.Keyspace() || ti.Shard != shardInfo.ShardName() {
			return fmt.Errorf("CRITICAL: tablet %v is in replication graph for shard %v/%v but belongs to shard %v:%v (maybe remove its replication path in shard %v/%v)", ti.Alias, keyspace, shard, ti.Keyspace, ti.Shard, keyspace, shard)
		}
		if !ti.IsInReplicationGraph() {
			// only valid case is a scrapped master in the
			// catastrophic reparent case
			if ti.Parent.Uid != NO_TABLET {
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
	tabletType TabletType
}

// Write serving graph data to the cells
func rebuildShardSrvGraph(ts Server, shardInfo *ShardInfo, tablets []*TabletInfo, cells []string) error {
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
	locationAddrsMap := make(map[cellKeyspaceShardType]*EndPoints)

	// we keep track of the existingDbTypeLocations we've already looked at
	knownShardLocations := make(map[cellKeyspaceShard]bool)

	for _, tablet := range tablets {
		// only look at tablets in the cells we want to rebuild
		if !InCellList(tablet.Tablet.Alias.Cell, cells) {
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
				if err != ErrNoNode {
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
			addrs = NewEndPoints()
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
		go func(location cellKeyspaceShardType, addrs *EndPoints) {
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
			if !InCellList(cell, cells) {
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
	srvShardByPath := make(map[cellKeyspaceShard]*SrvShard)
	for location := range locationAddrsMap {
		// location will be {cell,keyspace,shard,type}
		srvShardPath := cellKeyspaceShard{location.cell, location.keyspace, location.shard}
		srvShard, ok := srvShardByPath[srvShardPath]
		if !ok {
			srvShard = &SrvShard{
				KeyRange:    shardInfo.KeyRange,
				ServedTypes: shardInfo.ServedTypes,
				TabletTypes: make([]TabletType, 0, 2),
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
		go func(cks cellKeyspaceShard, srvShard *SrvShard) {
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
