// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// shard related methods for Wrangler

func (wr *Wrangler) lockShard(ctx context.Context, keyspace, shard string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	return actionNode.LockShard(ctx, wr.ts, keyspace, shard)
}

func (wr *Wrangler) unlockShard(ctx context.Context, keyspace, shard string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	return actionNode.UnlockShard(ctx, wr.ts, keyspace, shard, lockPath, actionError)
}

// updateShardCellsAndMaster will update the 'Cells' and possibly
// MasterAlias records for the shard, if needed.
func (wr *Wrangler) updateShardCellsAndMaster(ctx context.Context, si *topo.ShardInfo, tabletAlias *topodatapb.TabletAlias, tabletType topodatapb.TabletType, allowMasterOverride bool) error {
	// See if we need to update the Shard:
	// - add the tablet's cell to the shard's Cells if needed
	// - change the master if needed
	shardUpdateRequired := false
	if !si.HasCell(tabletAlias.Cell) {
		shardUpdateRequired = true
	}
	if tabletType == topodatapb.TabletType_MASTER && !topoproto.TabletAliasEqual(si.MasterAlias, tabletAlias) {
		shardUpdateRequired = true
	}
	if !shardUpdateRequired {
		return nil
	}

	// we do need to update the shard, lock it to not interfere with
	// reparenting operations.
	actionNode := actionnode.UpdateShard()
	keyspace := si.Keyspace()
	shard := si.ShardName()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// run the update
	_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(s *topodatapb.Shard) error {
		wasUpdated := false
		if !topoproto.ShardHasCell(s, tabletAlias.Cell) {
			s.Cells = append(s.Cells, tabletAlias.Cell)
			wasUpdated = true
		}

		if tabletType == topodatapb.TabletType_MASTER && !topoproto.TabletAliasEqual(s.MasterAlias, tabletAlias) {
			if !topoproto.TabletAliasIsZero(s.MasterAlias) && !allowMasterOverride {
				return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", topoproto.TabletAliasString(s.MasterAlias), keyspace, shard)
			}
			s.MasterAlias = tabletAlias
			wasUpdated = true
		}

		if !wasUpdated {
			return topo.ErrNoUpdateNeeded
		}
		return nil
	})
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

// SetShardServedTypes changes the ServedTypes parameter of a shard.
// It does not rebuild any serving graph or do any consistency check.
func (wr *Wrangler) SetShardServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, remove bool) error {

	actionNode := actionnode.SetShardServedTypes(cells, servedType)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.setShardServedTypes(ctx, keyspace, shard, cells, servedType, remove)
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) setShardServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, remove bool) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	if err := si.UpdateServedTypesMap(servedType, cells, remove); err != nil {
		return err
	}
	return wr.ts.UpdateShard(ctx, si)
}

// SetShardTabletControl changes the TabletControl records
// for a shard.  It does not rebuild any serving graph or do
// cross-shard consistency check.
// - if disableQueryService is set, tables has to be empty
// - if disableQueryService is not set, and tables is empty, we remove
//   the TabletControl record for the cells
func (wr *Wrangler) SetShardTabletControl(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, cells []string, remove, disableQueryService bool, tables []string) error {

	if disableQueryService && len(tables) > 0 {
		return fmt.Errorf("SetShardTabletControl cannot have both DisableQueryService set and tables set")
	}

	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.setShardTabletControl(ctx, keyspace, shard, tabletType, cells, remove, disableQueryService, tables)
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) setShardTabletControl(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, cells []string, remove, disableQueryService bool, tables []string) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	if len(tables) == 0 && !remove {
		// we are setting the DisableQueryService flag only
		if err := shardInfo.UpdateDisableQueryService(tabletType, cells, disableQueryService); err != nil {
			return fmt.Errorf("UpdateDisableQueryService(%v/%v) failed: %v", shardInfo.Keyspace(), shardInfo.ShardName(), err)
		}
	} else {
		// we are setting / removing the blacklisted tables only
		if err := shardInfo.UpdateSourceBlacklistedTables(tabletType, cells, remove, tables); err != nil {
			return fmt.Errorf("UpdateSourceBlacklistedTables(%v/%v) failed: %v", shardInfo.Keyspace(), shardInfo.ShardName(), err)
		}
	}
	return wr.ts.UpdateShard(ctx, shardInfo)
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard.
func (wr *Wrangler) DeleteShard(ctx context.Context, keyspace, shard string, recursive bool) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if recursive {
		wr.Logger().Infof("Deleting all tablets in shard %v/%v", keyspace, shard)
		for tabletAlias := range tabletMap {
			// We don't care about scrapping or updating the replication graph,
			// because we're about to delete the entire replication graph.
			wr.Logger().Infof("Deleting tablet %v", topoproto.TabletAliasString(&tabletAlias))
			if err := wr.TopoServer().DeleteTablet(ctx, &tabletAlias); err != nil && err != topo.ErrNoNode {
				// Unlike the errors below in non-recursive steps, we don't want to
				// continue if a DeleteTablet fails. If we continue and delete the
				// replication graph, the tablet record will be orphaned, since we'll
				// no longer know it belongs to this shard.
				//
				// If the problem is temporary, or resolved externally, re-running
				// DeleteShard will skip over tablets that were already deleted.
				return fmt.Errorf("can't delete tablet %v: %v", topoproto.TabletAliasString(&tabletAlias), err)
			}
		}
	} else if len(tabletMap) > 0 {
		return fmt.Errorf("shard %v/%v still has %v tablets; use -recursive or remove them manually", keyspace, shard, len(tabletMap))
	}

	// remove the replication graph and serving graph in each cell
	for _, cell := range shardInfo.Cells {
		if err := wr.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && err != topo.ErrNoNode {
			wr.Logger().Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}

		for _, t := range topoproto.AllTabletTypes {
			if !topo.IsInServingGraph(t) {
				continue
			}

			if err := wr.ts.DeleteEndPoints(ctx, cell, keyspace, shard, t, -1); err != nil && err != topo.ErrNoNode {
				wr.Logger().Warningf("Cannot delete EndPoints in cell %v for %v/%v/%v: %v", cell, keyspace, shard, t, err)
			}
		}

		if err := wr.ts.DeleteSrvShard(ctx, cell, keyspace, shard); err != nil && err != topo.ErrNoNode {
			wr.Logger().Warningf("Cannot delete SrvShard in cell %v for %v/%v: %v", cell, keyspace, shard, err)
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
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.removeShardCell(ctx, keyspace, shard, cell, force, recursive)
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) removeShardCell(ctx context.Context, keyspace, shard, cell string, force, recursive bool) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// check the cell is in the list already
	if !topo.InCellList(cell, shardInfo.Cells) {
		return fmt.Errorf("cell %v in not in shard info", cell)
	}

	// check the master alias is not in the cell
	if shardInfo.MasterAlias != nil && shardInfo.MasterAlias.Cell == cell {
		return fmt.Errorf("master %v is in the cell '%v' we want to remove", topoproto.TabletAliasString(shardInfo.MasterAlias), cell)
	}

	// get the ShardReplication object in the cell
	sri, err := wr.ts.GetShardReplication(ctx, cell, keyspace, shard)
	switch err {
	case nil:
		if recursive {
			wr.Logger().Infof("Deleting all tablets in shard %v/%v", keyspace, shard)
			for _, node := range sri.Nodes {
				// We don't care about scrapping or updating the replication graph,
				// because we're about to delete the entire replication graph.
				wr.Logger().Infof("Deleting tablet %v", topoproto.TabletAliasString(node.TabletAlias))
				if err := wr.TopoServer().DeleteTablet(ctx, node.TabletAlias); err != nil && err != topo.ErrNoNode {
					return fmt.Errorf("can't delete tablet %v: %v", topoproto.TabletAliasString(node.TabletAlias), err)
				}
			}
		} else if len(sri.Nodes) > 0 {
			return fmt.Errorf("cell %v has %v possible tablets in replication graph", cell, len(sri.Nodes))
		}

		// ShardReplication object is now useless, remove it
		if err := wr.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && err != topo.ErrNoNode {
			return fmt.Errorf("error deleting ShardReplication object in cell %v: %v", cell, err)
		}

		// Rebuild the shard serving graph to reflect the tablets we deleted.
		// This must be done before removing the cell from the global shard record,
		// since this cell will be skipped by all future rebuilds.
		if _, err := wr.RebuildShardGraph(ctx, keyspace, shard, []string{cell}); err != nil {
			return fmt.Errorf("can't rebuild serving graph for shard %v/%v in cell %v: %v", keyspace, shard, cell, err)
		}

		// we keep going
	case topo.ErrNoNode:
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
	wr.Logger().Infof("Removing cell %v from shard %v/%v", cell, keyspace, shard)
	newCells := make([]string, 0, len(shardInfo.Cells)-1)
	for _, c := range shardInfo.Cells {
		if c != cell {
			newCells = append(newCells, c)
		}
	}
	shardInfo.Cells = newCells

	return wr.ts.UpdateShard(ctx, shardInfo)
}

// SourceShardDelete will delete a SourceShard inside a shard, by index.
func (wr *Wrangler) SourceShardDelete(ctx context.Context, keyspace, shard string, uid uint32) error {
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.sourceShardDelete(ctx, keyspace, shard, uid)
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) sourceShardDelete(ctx context.Context, keyspace, shard string, uid uint32) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	newSourceShards := make([]*topodatapb.Shard_SourceShard, 0, 0)
	for _, ss := range si.SourceShards {
		if ss.Uid != uid {
			newSourceShards = append(newSourceShards, ss)
		}
	}
	if len(newSourceShards) == len(si.SourceShards) {
		return fmt.Errorf("no SourceShard with uid %v", uid)
	}
	if len(newSourceShards) == 0 {
		newSourceShards = nil
	}
	si.SourceShards = newSourceShards
	return wr.ts.UpdateShard(ctx, si)
}

// SourceShardAdd will add a new SourceShard inside a shard
func (wr *Wrangler) SourceShardAdd(ctx context.Context, keyspace, shard string, uid uint32, skeyspace, sshard string, keyRange *topodatapb.KeyRange, tables []string) error {
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.sourceShardAdd(ctx, keyspace, shard, uid, skeyspace, sshard, keyRange, tables)
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) sourceShardAdd(ctx context.Context, keyspace, shard string, uid uint32, skeyspace, sshard string, keyRange *topodatapb.KeyRange, tables []string) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

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
	return wr.ts.UpdateShard(ctx, si)
}
