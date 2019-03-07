/*
Copyright 2017 Google Inc.

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

package wrangler

import (
	"fmt"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// shard related methods for Wrangler

// updateShardCellsAndMaster will update the 'Cells' and possibly
// MasterAlias records for the shard, if needed.
func (wr *Wrangler) updateShardMaster(ctx context.Context, si *topo.ShardInfo, tabletAlias *topodatapb.TabletAlias, tabletType topodatapb.TabletType, allowMasterOverride bool) error {
	// See if we need to update the Shard:
	// - add the tablet's cell to the shard's Cells if needed
	// - change the master if needed
	shardUpdateRequired := false
	if tabletType == topodatapb.TabletType_MASTER && !topoproto.TabletAliasEqual(si.MasterAlias, tabletAlias) {
		shardUpdateRequired = true
	}
	if !shardUpdateRequired {
		return nil
	}

	// run the update
	_, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(s *topo.ShardInfo) error {
		wasUpdated := false

		if tabletType == topodatapb.TabletType_MASTER && !topoproto.TabletAliasEqual(s.MasterAlias, tabletAlias) {
			if !topoproto.TabletAliasIsZero(s.MasterAlias) && !allowMasterOverride {
				return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", topoproto.TabletAliasString(s.MasterAlias), si.Keyspace(), si.ShardName())
			}
			s.MasterAlias = tabletAlias
			wasUpdated = true
		}

		if !wasUpdated {
			return topo.NewError(topo.NoUpdateNeeded, si.Keyspace()+"/"+si.ShardName())
		}
		return nil
	})
	return err
}

// SetShardServedTypes changes the ServedTypes parameter of a shard.
// It does not rebuild any serving graph or do any consistency check.
// This is an emergency manual operation.
func (wr *Wrangler) SetShardServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, remove bool) (err error) {
	// lock the keyspace to not conflict with resharding operations
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, fmt.Sprintf("SetShardServedTypes(%v,%v,%v)", cells, servedType, remove))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// and update the shard
	// TODO: What should we do with this method?
	//_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
	//return si.UpdateServedTypesMap(servedType, cells, remove)
	//})
	return err
}

// SetShardTabletControl changes the TabletControl records
// for a shard.  It does not rebuild any serving graph or do
// cross-shard consistency check.
// - sets black listed tables in tablet control record
//
// This takes the keyspace lock as to not interfere with resharding operations.
func (wr *Wrangler) SetShardTabletControl(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, cells []string, remove bool, blacklistedTables []string) (err error) {
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "SetShardTabletControl")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// update the shard
	_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		// we are setting / removing the blacklisted tables only
		return si.UpdateSourceBlacklistedTables(ctx, tabletType, cells, remove, blacklistedTables)
	})
	return err
}

// UpdateDisableQueryService changes the TabletControl records
// for a shard.  It updates serving graph
//
// This takes the keyspace lock as to not interfere with resharding operations.
func (wr *Wrangler) UpdateDisableQueryService(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, cells []string, disableQueryService bool) (err error) {
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "SetShardTabletControl")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	// disable query service for shard
	return wr.ts.UpdateDisableQueryService(ctx, keyspace, []*topo.ShardInfo{si}, tabletType, cells, disableQueryService)
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard.
func (wr *Wrangler) DeleteShard(ctx context.Context, keyspace, shard string, recursive, evenIfServing bool) error {
	// Read the Shard object. If it's not there, try to clean up
	// the topology anyway.
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Infof("Shard %v/%v doesn't seem to exist, cleaning up any potential leftover", keyspace, shard)
			return wr.ts.DeleteShard(ctx, keyspace, shard)
		}
		return err
	}

	servingCells, err := wr.ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		return err
	}
	// Check the Serving map for the shard, we don't want to
	// remove a serving shard if not absolutely sure.
	if !evenIfServing && len(servingCells) > 0 {
		return fmt.Errorf("shard %v/%v is still serving, cannot delete it, use even_if_serving flag if needed", keyspace, shard)
	}

	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		return err
	}

	// Go through all the cells.
	for _, cell := range cells {
		var aliases []*topodatapb.TabletAlias

		// Get the ShardReplication object for that cell. Try
		// to find all tablets that may belong to our shard.
		sri, err := wr.ts.GetShardReplication(ctx, cell, keyspace, shard)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// No ShardReplication object. It means the
			// topo is inconsistent. Let's read all the
			// tablets for that cell, and if we find any
			// in our keyspace / shard, either abort or
			// try to delete them.
			aliases, err = wr.ts.GetTabletsByCell(ctx, cell)
			if err != nil {
				return fmt.Errorf("GetTabletsByCell(%v) failed: %v", cell, err)
			}
		case err == nil:
			// We found a ShardReplication object. We
			// trust it to have all tablet records.
			aliases = make([]*topodatapb.TabletAlias, len(sri.Nodes))
			for i, n := range sri.Nodes {
				aliases[i] = n.TabletAlias
			}
		default:
			return fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
		}

		// Get the corresponding Tablet records. Note
		// GetTabletMap ignores ErrNoNode, and it's good for
		// our purpose, it means a tablet was deleted but is
		// still referenced.
		tabletMap, err := wr.ts.GetTabletMap(ctx, aliases)
		if err != nil {
			return fmt.Errorf("GetTabletMap() failed: %v", err)
		}

		// Remove the tablets that don't belong to our
		// keyspace/shard from the map.
		for a, ti := range tabletMap {
			if ti.Keyspace != keyspace || ti.Shard != shard {
				delete(tabletMap, a)
			}
		}

		// Now see if we need to DeleteTablet, and if we can, do it.
		if len(tabletMap) > 0 {
			if !recursive {
				return fmt.Errorf("shard %v/%v still has %v tablets in cell %v; use -recursive or remove them manually", keyspace, shard, len(tabletMap), cell)
			}

			wr.Logger().Infof("Deleting all tablets in shard %v/%v cell %v", keyspace, shard, cell)
			for tabletAlias, tabletInfo := range tabletMap {
				// We don't care about scrapping or updating the replication graph,
				// because we're about to delete the entire replication graph.
				wr.Logger().Infof("Deleting tablet %v", tabletAlias)
				if err := wr.TopoServer().DeleteTablet(ctx, tabletInfo.Alias); err != nil && !topo.IsErrType(err, topo.NoNode) {
					// We don't want to continue if a DeleteTablet fails for
					// any good reason (other than missing tablet, in which
					// case it's just a topology server inconsistency we can
					// ignore). If we continue and delete the replication
					// graph, the tablet record will be orphaned, since
					// we'll no longer know it belongs to this shard.
					//
					// If the problem is temporary, or resolved externally, re-running
					// DeleteShard will skip over tablets that were already deleted.
					return fmt.Errorf("can't delete tablet %v: %v", tabletAlias, err)
				}
			}
		}
	}

	// Try to remove the replication graph and serving graph in each cell,
	// regardless of its existence.
	for _, cell := range cells {
		if err := wr.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && !topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}
	}

	return wr.ts.DeleteShard(ctx, keyspace, shard)
}

// RemoveShardCell will remove a cell from the Cells list in a shard.
//
// It will first check the shard has no tablets there. If 'force' is
// specified, it will remove the cell even when the tablet map cannot
// be retrieved. This is intended to be used when a cell is completely
// down and its topology server cannot even be reached.
//
// If 'recursive' is specified, it will delete any tablets in the cell/shard,
// with the assumption that the tablet processes have already been terminated.
func (wr *Wrangler) RemoveShardCell(ctx context.Context, keyspace, shard, cell string, force, recursive bool) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	shardServingCells, err := wr.ts.GetShardServingCells(ctx, shardInfo)

	// check the cell is in the list already
	if !topo.InCellList(cell, shardServingCells) {
		return fmt.Errorf("cell %v in not in shard info", cell)
	}

	// check the master alias is not in the cell
	if shardInfo.MasterAlias != nil && shardInfo.MasterAlias.Cell == cell {
		return fmt.Errorf("master %v is in the cell '%v' we want to remove", topoproto.TabletAliasString(shardInfo.MasterAlias), cell)
	}

	// get the ShardReplication object in the cell
	sri, err := wr.ts.GetShardReplication(ctx, cell, keyspace, shard)
	switch {
	case err == nil:
		if recursive {
			wr.Logger().Infof("Deleting all tablets in shard %v/%v", keyspace, shard)
			for _, node := range sri.Nodes {
				// We don't care about scrapping or updating the replication graph,
				// because we're about to delete the entire replication graph.
				wr.Logger().Infof("Deleting tablet %v", topoproto.TabletAliasString(node.TabletAlias))
				if err := wr.TopoServer().DeleteTablet(ctx, node.TabletAlias); err != nil && !topo.IsErrType(err, topo.NoNode) {
					return fmt.Errorf("can't delete tablet %v: %v", topoproto.TabletAliasString(node.TabletAlias), err)
				}
			}
		} else if len(sri.Nodes) > 0 {
			return fmt.Errorf("cell %v has %v possible tablets in replication graph", cell, len(sri.Nodes))
		}

		// ShardReplication object is now useless, remove it
		if err := wr.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && !topo.IsErrType(err, topo.NoNode) {
			return fmt.Errorf("error deleting ShardReplication object in cell %v: %v", cell, err)
		}

		// we keep going
	case topo.IsErrType(err, topo.NoNode):
		// no ShardReplication object, we keep going
	default:
		// we can't get the object, assume topo server is down there,
		// so we look at force flag
		if !force {
			return err
		}
		wr.Logger().Warningf("Cannot get ShardReplication from cell %v, assuming cell topo server is down, and forcing the removal", cell)
	}

	// now we can update the shard
	wr.Logger().Infof("Removing cell %v from SrvKeyspace %v/%v", cell, keyspace, shard)
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "Locking keyspace to remove shard from SrvKeyspace")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	if err = wr.ts.UpdateSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shardInfo}, topodatapb.TabletType_RDONLY, shardServingCells, true /* remove */); err != nil {
		return err
	}

	if err = wr.ts.UpdateSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shardInfo}, topodatapb.TabletType_REPLICA, shardServingCells, true /* remove */); err != nil {
		return err
	}
	return wr.ts.UpdateSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shardInfo}, topodatapb.TabletType_MASTER, shardServingCells, true /* remove */)
}

// SourceShardDelete will delete a SourceShard inside a shard, by index.
//
// This takes the keyspace lock as not to interfere with resharding operations.
func (wr *Wrangler) SourceShardDelete(ctx context.Context, keyspace, shard string, uid uint32) (err error) {
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, fmt.Sprintf("SourceShardDelete(%v)", uid))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// remove the source shard
	_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		var newSourceShards []*topodatapb.Shard_SourceShard
		for _, ss := range si.SourceShards {
			if ss.Uid != uid {
				newSourceShards = append(newSourceShards, ss)
			}
		}
		if len(newSourceShards) == len(si.SourceShards) {
			return fmt.Errorf("no SourceShard with uid %v", uid)
		}
		si.SourceShards = newSourceShards
		return nil
	})
	return err
}

// SourceShardAdd will add a new SourceShard inside a shard.
func (wr *Wrangler) SourceShardAdd(ctx context.Context, keyspace, shard string, uid uint32, skeyspace, sshard string, keyRange *topodatapb.KeyRange, tables []string) (err error) {
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, fmt.Sprintf("SourceShardAdd(%v)", uid))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// and update the shard
	_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		// check the uid is not used already
		for _, ss := range si.SourceShards {
			if ss.Uid == uid {
				return fmt.Errorf("uid %v is already in use", uid)
			}
		}

		si.SourceShards = append(si.SourceShards, &topodatapb.Shard_SourceShard{
			Uid:      uid,
			Keyspace: skeyspace,
			Shard:    sshard,
			KeyRange: keyRange,
			Tables:   tables,
		})
		return nil
	})
	return err
}
