// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/key"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
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
func (wr *Wrangler) RebuildShardGraph(keyspace, shard string, cells []string) error {
	actionNode := wr.ai.RebuildShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildShard(keyspace, shard, cells)
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
func (wr *Wrangler) rebuildShard(keyspace, shard string, cells []string) error {
	relog.Info("rebuildShard %v/%v", keyspace, shard)
	// NOTE(msolomon) nasty hack - pass non-empty string to bypass data check
	shardInfo, err := topo.NewShardInfo(keyspace, shard, "{}")
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
				relog.Warning("Tablet %v should not be in the replication graph, please investigate (it will be ignored in the rebuild)", ti.Alias())
			}
		}
		tablets = append(tablets, ti)
	}

	// Rebuild the rollup data in the replication graph.
	if err = shardInfo.Rebuild(tablets); err != nil {
		return err
	}
	if err = wr.ts.UpdateShard(shardInfo); err != nil {
		return err
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
	relog.Info("rebuildShardSrvGraph %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())

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
	//   value: topo.VtnsAddrs (list of server records)
	locationAddrsMap := make(map[cellKeyspaceShardType]*topo.VtnsAddrs)

	// we keep track of the existingDbTypeLocations we've already looked at
	knownShardLocations := make(map[cellKeyspaceShard]bool)

	for _, tablet := range tablets {
		// only look at tablets in the cells we want to rebuild
		// we also include masters from everywhere, so we can
		// write the right aliases
		if !inCellList(tablet.Tablet.Cell, cells) && tablet.Type != topo.TYPE_MASTER {
			continue
		}

		// this is {cell,keyspace,shard}
		// we'll get the children to find the existing types
		shardLocation := cellKeyspaceShard{tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard}
		// only need to do this once per cell
		if !knownShardLocations[shardLocation] {
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
			addrs = topo.NewAddrs()
			locationAddrsMap[location] = addrs
		}

		entry, err := tm.VtnsAddrForTablet(tablet.Tablet)
		if err != nil {
			relog.Warning("VtnsAddrForTablet failed for tablet %v: %v", tablet.Alias(), err)
			continue
		}
		addrs.Entries = append(addrs.Entries, *entry)
	}

	// if there is a master in one cell, put it in all of them
	var masterRecord *topo.VtnsAddrs
	for shardLocation, _ := range knownShardLocations {
		loc := cellKeyspaceShardType{shardLocation.cell, shardLocation.keyspace, shardLocation.shard, topo.TYPE_MASTER}
		if addrs, ok := locationAddrsMap[loc]; ok {
			if masterRecord != nil {
				relog.Warning("Multiple master records in %v", shardLocation)
			} else {
				relog.Info("Found master record in %v", shardLocation)
				masterRecord = addrs
			}
		}
	}
	if masterRecord != nil {
		for shardLocation, _ := range knownShardLocations {
			location := cellKeyspaceShardType{shardLocation.cell, shardLocation.keyspace, shardLocation.shard, topo.TYPE_MASTER}
			if _, ok := locationAddrsMap[location]; !ok {
				relog.Info("Adding remote master record in %v", location)
				locationAddrsMap[location] = masterRecord
			}
		}
	}

	// write all the {cell,keyspace,shard,type}
	// nodes everywhere we want them
	for location, addrs := range locationAddrsMap {
		cell := location.cell
		if !inCellList(cell, cells) {
			continue
		}

		if err := wr.ts.UpdateSrvTabletType(location.cell, location.keyspace, location.shard, location.tabletType, addrs); err != nil {
			return fmt.Errorf("writing endpoints failed: %v", err)
		}
	}

	// Delete any pre-existing paths that were not updated by this process.
	// That's the existingDbTypeLocations - locationAddrsMap
	for dbTypeLocation, _ := range existingDbTypeLocations {
		if _, ok := locationAddrsMap[dbTypeLocation]; !ok {
			cell := dbTypeLocation.cell
			if !inCellList(cell, cells) {
				continue
			}

			relog.Info("removing stale db type from serving graph: %v", dbTypeLocation)
			if err := wr.ts.DeleteSrvTabletType(dbTypeLocation.cell, dbTypeLocation.keyspace, dbTypeLocation.shard, dbTypeLocation.tabletType); err != nil {
				relog.Warning("unable to remove stale db type from serving graph: %v", err)
			}
		}
	}

	// Update per-shard information per cell-specific serving path.
	//
	// srvShardByPath is a map:
	//   key: {cell,keyspace,shard}
	//   value: topo.SrvShard
	// this will fill in the AddrsByType part for each shard
	srvShardByPath := make(map[cellKeyspaceShard]*topo.SrvShard)
	for location, addrs := range locationAddrsMap {
		// location will be {cell,keyspace,shard,type}
		srvShardPath := cellKeyspaceShard{location.cell, location.keyspace, location.shard}
		tabletType := location.tabletType

		srvShard, ok := srvShardByPath[srvShardPath]
		if !ok {
			srvShard = &topo.SrvShard{KeyRange: shardInfo.KeyRange, AddrsByType: make(map[string]topo.VtnsAddrs)}
			srvShardByPath[srvShardPath] = srvShard
		}
		srvShard.AddrsByType[string(tabletType)] = *addrs
	}

	// Save the shard entries
	for srvPath, srvShard := range srvShardByPath {
		if err := wr.ts.UpdateSrvShard(srvPath.cell, srvPath.keyspace, srvPath.shard, srvShard); err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// Rebuild the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(keyspace string, cells []string) error {
	actionNode := wr.ai.RebuildKeyspace()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.rebuildKeyspace(keyspace, cells)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

// This function should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func (wr *Wrangler) rebuildKeyspace(keyspace string, cells []string) error {
	relog.Info("rebuildKeyspace %v", keyspace)
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

	// Scan the first shard to discover which cells need local serving data.
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shards[0])
	if err != nil {
		return err
	}

	// srvKeyspaceByPath is a map:
	//   key: local keyspace {cell,keyspace}
	//   value: topo.SrvKeyspace object being built
	srvKeyspaceByPath := make(map[cellKeyspace]*topo.SrvKeyspace)
	for _, alias := range aliases {
		keyspaceLocation := cellKeyspace{alias.Cell, keyspace}
		if _, ok := srvKeyspaceByPath[keyspaceLocation]; !ok {
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

			srvKeyspaceByPath[keyspaceLocation] = &topo.SrvKeyspace{Shards: make([]topo.SrvShard, 0, 16)}
		}
	}

	// for each entry in the srvKeyspaceByPath map, we do the following:
	// - read the ShardInfo structures for each shard
	//    - prune the AddrsByType field, result would be too big
	// - compute the union of the db types (replica, master, ...)
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for srvPath, srvKeyspace := range srvKeyspaceByPath {
		keyspaceDbTypes := make(map[topo.TabletType]bool)
		for _, shard := range shards {
			srvShard, err := wr.ts.GetSrvShard(srvPath.cell, srvPath.keyspace, shard)
			if err != nil {
				return err
			}
			for dbType, _ := range srvShard.AddrsByType {
				keyspaceDbTypes[topo.TabletType(dbType)] = true
			}
			// Prune addrs, this is unnecessarily expensive right now. It is easier to
			// load on-demand since we have to do that anyway on a reconnect.
			srvShard.AddrsByType = nil
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
	for srvPath, srvKeyspace := range srvKeyspaceByPath {
		if err := wr.ts.UpdateSrvKeyspace(srvPath.cell, srvPath.keyspace, srvKeyspace); err != nil {
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
		relog.Debug("delete keyspace shards: %v", keyspace)
		if err := wr.ts.DeleteKeyspaceShards(keyspace); err != nil {
			return err
		}
	}

	keyspacesToRebuild := make(map[string]bool)
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
			mu.Unlock()
			err := topo.CreateTabletReplicationPaths(wr.ts, ti.Tablet)
			if err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				relog.Warning("failed creating replication path: %v", err)
			}
		}(ti)
	}
	wg.Wait()

	for keyspace, _ := range keyspacesToRebuild {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := wr.RebuildKeyspaceGraph(keyspace, nil); err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				relog.Warning("RebuildKeyspaceGraph(%v) failed: %v", keyspace, err)
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
