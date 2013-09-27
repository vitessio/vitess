// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
)

func inCellList(cell string, cells []string) bool {
	if len(cells) == 0 {
		return true
	}
	for _, c := range cells {
		if c == cell {
			return true
		}
	}
	return false
}

// Rebuild the serving and replication rollup data data while locking
// out other changes.
//
// NOTE(alainjobart): The updateMaster boolean is going to be removed
// eventually. We don't want to update the master during rebuild,
// just during reparent. When all code is converted to updateMaster=false,
// we're remove the flag.
func (wr *Wrangler) RebuildShardGraph(keyspace, shard string, cells []string, updateMaster bool) error {
	actionNode := wr.ai.RebuildShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildShard(keyspace, shard, cells, updateMaster)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

// Update shard file with new master, replicas, etc.
//
// Re-read from TopologyServer to make sure we are using the side
// effects of all actions.
//
// This function should only be used with an action lock on the shard
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// NOTE(alainjobart): The updateMaster boolean is going to be removed
// eventually. We don't want to update the master during rebuild,
// just during reparent. When all code is converted to updateMaster=false,
// we're remove the flag.
func (wr *Wrangler) rebuildShard(keyspace, shard string, cells []string, updateMaster bool) error {
	log.Infof("rebuildShard %v/%v", keyspace, shard)

	// read the existing shard info. It has to exist.
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	tablets := make([]*topo.TabletInfo, 0, len(tabletMap))
	for _, ti := range tabletMap {
		if ti.Keyspace != shardInfo.Keyspace() || ti.Shard != shardInfo.ShardName() {
			return fmt.Errorf("CRITICAL: tablet %v is in replication graph for shard %v/%v but belongs to shard %v:%v (maybe remove its replication path in shard %v/%v)", ti.Alias(), keyspace, shard, ti.Keyspace, ti.Shard, keyspace, shard)
		}
		if !ti.IsInReplicationGraph() {
			// only valid case is a scrapped master in the
			// catastrophic reparent case
			if ti.Parent.Uid != topo.NO_TABLET {
				log.Warningf("Tablet %v should not be in the replication graph, please investigate (it will be ignored in the rebuild)", ti.Alias())
			}
		}
		tablets = append(tablets, ti)
	}

	// Rebuild the rollup data in the replication graph.
	// There is only MasterAlias left now.
	if updateMaster {
		if err = shardInfo.Rebuild(tablets); err != nil {
			return err
		}
		if err = wr.ts.UpdateShard(shardInfo); err != nil {
			return err
		}
	}
	return wr.rebuildShardSrvGraph(shardInfo, tablets, cells)
}

// the following types are used as locations in the serving graph

type cellKeyspace struct {
	cell     string
	keyspace string
}

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
func (wr *Wrangler) rebuildShardSrvGraph(shardInfo *topo.ShardInfo, tablets []*topo.TabletInfo, cells []string) error {
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
	//   value: topo.EndPoints (list of server records)
	locationAddrsMap := make(map[cellKeyspaceShardType]*topo.EndPoints)

	// we keep track of the existingDbTypeLocations we've already looked at
	knownShardLocations := make(map[cellKeyspaceShard]bool)

	for _, tablet := range tablets {
		// only look at tablets in the cells we want to rebuild
		// we also include masters from everywhere, so we can
		// write the right aliases
		if !inCellList(tablet.Tablet.Cell, cells) {
			continue
		}

		// this is {cell,keyspace,shard}
		// we'll get the children to find the existing types
		shardLocation := cellKeyspaceShard{tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard}
		// only need to do this once per cell
		if !knownShardLocations[shardLocation] {
			log.Infof("Getting tablet types on cell %v for %v/%v", tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard)
			tabletTypes, err := wr.ts.GetSrvTabletTypesPerShard(tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard)
			if err != nil {
				if err != topo.ErrNoNode {
					return err
				}
			} else {
				for _, tabletType := range tabletTypes {
					existingDbTypeLocations[cellKeyspaceShardType{tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard, tabletType}] = true
				}
			}
			knownShardLocations[shardLocation] = true
		}

		// Check IsServingType after we have populated existingDbTypeLocations
		// so we properly prune data if the definition of serving type
		// changes.
		if !tablet.IsServingType() {
			continue
		}

		location := cellKeyspaceShardType{tablet.Tablet.Cell, tablet.Keyspace, tablet.Shard, tablet.Type}
		addrs, ok := locationAddrsMap[location]
		if !ok {
			addrs = topo.NewEndPoints()
			locationAddrsMap[location] = addrs
		}

		entry, err := tm.EndPointForTablet(tablet.Tablet)
		if err != nil {
			log.Warningf("EndPointForTablet failed for tablet %v: %v", tablet.Alias(), err)
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
			if err := wr.ts.UpdateEndPoints(location.cell, location.keyspace, location.shard, location.tabletType, addrs); err != nil {
				rec.RecordError(fmt.Errorf("writing endpoints for cell %v shard %v/%v tabletType %v failed: %v", location.cell, location.keyspace, location.shard, location.tabletType, err))
			}
			wg.Done()
		}(location, addrs)
	}

	// Delete any pre-existing paths that were not updated by this process.
	// That's the existingDbTypeLocations - locationAddrsMap
	for dbTypeLocation, _ := range existingDbTypeLocations {
		if _, ok := locationAddrsMap[dbTypeLocation]; !ok {
			cell := dbTypeLocation.cell
			if !inCellList(cell, cells) {
				continue
			}

			wg.Add(1)
			go func(dbTypeLocation cellKeyspaceShardType) {
				log.Infof("removing stale db type from serving graph: %v", dbTypeLocation)
				if err := wr.ts.DeleteSrvTabletType(dbTypeLocation.cell, dbTypeLocation.keyspace, dbTypeLocation.shard, dbTypeLocation.tabletType); err != nil {
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
	//   value: topo.SrvShard
	// this will create all the SrvShard objects
	srvShardByPath := make(map[cellKeyspaceShard]*topo.SrvShard)
	for location, _ := range locationAddrsMap {
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
			if err := wr.ts.UpdateSrvShard(cks.cell, cks.keyspace, cks.shard, srvShard); err != nil {
				rec.RecordError(fmt.Errorf("writing serving data in cell %v for %v/%v failed: %v", cks.cell, cks.keyspace, cks.shard, err))
			}
			wg.Done()
		}(cks, srvShard)
	}
	wg.Wait()
	return rec.Error()
}

// Rebuild the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(keyspace string, cells []string, useServedTypes bool) error {
	actionNode := wr.ai.RebuildKeyspace()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildKeyspace(keyspace, cells, useServedTypes)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

// This function should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func (wr *Wrangler) rebuildKeyspace(keyspace string, cells []string, useServedTypes bool) error {
	log.Infof("rebuildKeyspace %v", keyspace)
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
			if err := wr.RebuildShardGraph(keyspace, shard, cells, false); err != nil {
				er.RecordError(fmt.Errorf("RebuildShardGraph failed: %v/%v %v", keyspace, shard, err))
			}
			wg.Done()
		}(shard)
	}
	wg.Wait()
	if er.HasErrors() {
		return er.Error()
	}

	// Scan the first shard to discover which cells need local serving data.
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shards[0])
	if err != nil {
		return err
	}

	// srvKeyspaceMap is a map:
	//   key: local keyspace {cell,keyspace}
	//   value: topo.SrvKeyspace object being built
	srvKeyspaceMap := make(map[cellKeyspace]*topo.SrvKeyspace)
	for _, alias := range aliases {
		keyspaceLocation := cellKeyspace{alias.Cell, keyspace}
		if _, ok := srvKeyspaceMap[keyspaceLocation]; !ok {
			// before adding keyspaceLocation to the map of
			// of KeyspaceByPath, we check this is a
			// serving tablet. No serving tablet in shard
			// 0 means we're not rebuilding the serving
			// graph in that cell.  This is somewhat
			// expensive, but we only do it on all the
			// non-serving tablets in a shard before we
			// find a serving tablet.
			ti, err := wr.ts.GetTablet(alias)
			if err != nil {
				return err
			}
			if !ti.IsServingType() {
				continue
			}

			srvKeyspaceMap[keyspaceLocation] = &topo.SrvKeyspace{
				Shards: make([]topo.SrvShard, 0, 16),
			}
		}
	}

	if useServedTypes {
		// Use the new code. Only works in ServeTypes in
		// Shard objects are populated and correct.
		return wr.rebuildKeyspaceWithServedTypes(shards, srvKeyspaceMap)
	}

	// for each entry in the srvKeyspaceMap map, we do the following:
	// - read the ShardInfo structures for each shard
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for ck, srvKeyspace := range srvKeyspaceMap {
		keyspaceDbTypes := make(map[topo.TabletType]bool)
		for _, shard := range shards {
			srvShard, err := wr.ts.GetSrvShard(ck.cell, ck.keyspace, shard)
			if err != nil {
				return err
			}
			for _, tabletType := range srvShard.TabletTypes {
				keyspaceDbTypes[tabletType] = true
			}
			srvKeyspace.Shards = append(srvKeyspace.Shards, *srvShard)
		}
		tabletTypes := make([]topo.TabletType, 0, len(keyspaceDbTypes))
		for dbType, _ := range keyspaceDbTypes {
			tabletTypes = append(tabletTypes, dbType)
		}
		srvKeyspace.TabletTypes = tabletTypes
		// FIXME(msolomon) currently this only works when the shards are range-based
		topo.SrvShardArray(srvKeyspace.Shards).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		if srvKeyspace.Shards[0].KeyRange.Start != key.MinKey {
			return fmt.Errorf("Keyspace does not start with %v", key.MinKey)
		}
		if srvKeyspace.Shards[len(srvKeyspace.Shards)-1].KeyRange.End != key.MaxKey {
			return fmt.Errorf("Keyspace does not end with %v", key.MaxKey)
		}
		for i, _ := range srvKeyspace.Shards[0 : len(srvKeyspace.Shards)-1] {
			if srvKeyspace.Shards[i].KeyRange.End != srvKeyspace.Shards[i+1].KeyRange.Start {
				return fmt.Errorf("Non-contiguous KeyRange values at shard %v to %v: %v != %v", i, i+1, srvKeyspace.Shards[i].KeyRange.End.Hex(), srvKeyspace.Shards[i+1].KeyRange.Start.Hex())
			}
		}
	}

	// and then finally save the keyspace objects
	for ck, srvKeyspace := range srvKeyspaceMap {
		if err := wr.ts.UpdateSrvKeyspace(ck.cell, ck.keyspace, srvKeyspace); err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

func (wr *Wrangler) rebuildKeyspaceWithServedTypes(shards []string, srvKeyspaceMap map[cellKeyspace]*topo.SrvKeyspace) error {
	// for each entry in the srvKeyspaceMap map, we do the following:
	// - read the ShardInfo structures for each shard
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for ck, srvKeyspace := range srvKeyspaceMap {
		keyspaceDbTypes := make(map[topo.TabletType]bool)
		srvKeyspace.Partitions = make(map[topo.TabletType]*topo.KeyspacePartition)
		for _, shard := range shards {
			srvShard, err := wr.ts.GetSrvShard(ck.cell, ck.keyspace, shard)
			if err != nil {
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
		for dbType, _ := range keyspaceDbTypes {
			srvKeyspace.TabletTypes = append(srvKeyspace.TabletTypes, dbType)
		}

		first := true
		for tabletType, partition := range srvKeyspace.Partitions {
			topo.SrvShardArray(partition.Shards).Sort()

			// check the first Start is MinKey, the last End is MaxKey,
			// and the values in between match: End[i] == Start[i+1]
			if partition.Shards[0].KeyRange.Start != key.MinKey {
				return fmt.Errorf("Keyspace partition for %v does not start with %v", tabletType, key.MinKey)
			}
			if partition.Shards[len(partition.Shards)-1].KeyRange.End != key.MaxKey {
				return fmt.Errorf("Keyspace partition for %v does not end with %v", tabletType, key.MaxKey)
			}
			for i, _ := range partition.Shards[0 : len(partition.Shards)-1] {
				if partition.Shards[i].KeyRange.End != partition.Shards[i+1].KeyRange.Start {
					return fmt.Errorf("Non-contiguous KeyRange values for %v at shard %v to %v: %v != %v", tabletType, i, i+1, partition.Shards[i].KeyRange.End.Hex(), partition.Shards[i+1].KeyRange.Start.Hex())
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
	for ck, srvKeyspace := range srvKeyspaceMap {
		if err := wr.ts.UpdateSrvKeyspace(ck.cell, ck.keyspace, srvKeyspace); err != nil {
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
		tablets, err := GetAllTablets(wr.ts, cell)
		if err != nil {
			return err
		}
		allTablets = append(allTablets, tablets...)
	}

	for _, keyspace := range keyspaces {
		log.V(6).Infof("delete keyspace shards: %v", keyspace)
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
					log.Warningf("failed re-creating shard %v: %v", shardPath, err)
					hasErr = true
				} else {
					shardsCreated[shardPath] = true
				}
			}
			mu.Unlock()
			err := topo.CreateTabletReplicationData(wr.ts, ti.Tablet)
			if err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				log.Warningf("failed creating replication path: %v", err)
			}
		}(ti)
	}
	wg.Wait()

	for keyspace, _ := range keyspacesToRebuild {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := wr.RebuildKeyspaceGraph(keyspace, nil, false); err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				log.Warningf("RebuildKeyspaceGraph(%v) failed: %v", keyspace, err)
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
