// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"code.google.com/p/go.net/context"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// shard related methods for Wrangler

func (wr *Wrangler) lockShard(keyspace, shard string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	return actionNode.LockShard(context.TODO(), wr.ts, keyspace, shard, wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockShard(keyspace, shard string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	return actionNode.UnlockShard(wr.ts, keyspace, shard, lockPath, actionError)
}

// updateShardCellsAndMaster will update the 'Cells' and possibly
// MasterAlias records for the shard, if needed.
func (wr *Wrangler) updateShardCellsAndMaster(si *topo.ShardInfo, tabletAlias topo.TabletAlias, tabletType topo.TabletType, force bool) error {
	// See if we need to update the Shard:
	// - add the tablet's cell to the shard's Cells if needed
	// - change the master if needed
	shardUpdateRequired := false
	if !si.HasCell(tabletAlias.Cell) {
		shardUpdateRequired = true
	}
	if tabletType == topo.TYPE_MASTER && si.MasterAlias != tabletAlias {
		shardUpdateRequired = true
	}
	if !shardUpdateRequired {
		return nil
	}

	actionNode := actionnode.UpdateShard()
	keyspace := si.Keyspace()
	shard := si.ShardName()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// re-read the shard with the lock
	si, err = wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
	}

	// update it
	wasUpdated := false
	if !si.HasCell(tabletAlias.Cell) {
		si.Cells = append(si.Cells, tabletAlias.Cell)
		wasUpdated = true
	}
	if tabletType == topo.TYPE_MASTER && si.MasterAlias != tabletAlias {
		if !si.MasterAlias.IsZero() && !force {
			return wr.unlockShard(keyspace, shard, actionNode, lockPath, fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", si.MasterAlias, keyspace, shard))
		}
		si.MasterAlias = tabletAlias
		wasUpdated = true
	}

	if wasUpdated {
		// write it back
		if err := topo.UpdateShard(context.TODO(), wr.ts, si); err != nil {
			return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
		}
	}

	// and unlock
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

// SetShardServedTypes changes the ServedTypes parameter of a shard.
// It does not rebuild any serving graph or do any consistency check (yet).
func (wr *Wrangler) SetShardServedTypes(keyspace, shard string, cells []string, servedType topo.TabletType, remove bool) error {

	actionNode := actionnode.SetShardServedTypes(cells, servedType)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.setShardServedTypes(keyspace, shard, cells, servedType, remove)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) setShardServedTypes(keyspace, shard string, cells []string, servedType topo.TabletType, remove bool) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	if err := si.UpdateServedTypesMap(servedType, cells, remove); err != nil {
		return err
	}
	return topo.UpdateShard(context.TODO(), wr.ts, si)
}

// SetShardTabletControl changes the TabletControl records
// for a shard.  It does not rebuild any serving graph or do
// cross-shard consistency check.
// - if disableQueryService is set, tables has to be empty
// - if disableQueryService is not set, and tables is empty, we remove
//   the TabletControl record for the cells
func (wr *Wrangler) SetShardTabletControl(keyspace, shard string, tabletType topo.TabletType, cells []string, remove, disableQueryService bool, tables []string) error {

	if disableQueryService && len(tables) > 0 {
		return fmt.Errorf("SetShardTabletControl cannot have both DisableQueryService set and tables set")
	}

	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.setShardTabletControl(keyspace, shard, tabletType, cells, remove, disableQueryService, tables)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) setShardTabletControl(keyspace, shard string, tabletType topo.TabletType, cells []string, remove, disableQueryService bool, tables []string) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
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
	return topo.UpdateShard(context.TODO(), wr.ts, shardInfo)
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard. It can only work if there are no tablets
// in that shard.
func (wr *Wrangler) DeleteShard(keyspace, shard string) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShard(context.TODO(), wr.ts, keyspace, shard)
	if err != nil {
		return err
	}
	if len(tabletMap) > 0 {
		return fmt.Errorf("shard %v/%v still has %v tablets", keyspace, shard, len(tabletMap))
	}

	// remove the replication graph and serving graph in each cell
	for _, cell := range shardInfo.Cells {
		if err := wr.ts.DeleteShardReplication(cell, keyspace, shard); err != nil {
			wr.Logger().Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}

		for _, t := range topo.AllTabletTypes {
			if !topo.IsInServingGraph(t) {
				continue
			}

			if err := wr.ts.DeleteEndPoints(cell, keyspace, shard, t); err != nil && err != topo.ErrNoNode {
				wr.Logger().Warningf("Cannot delete EndPoints in cell %v for %v/%v/%v: %v", cell, keyspace, shard, t, err)
			}
		}

		if err := wr.ts.DeleteSrvShard(cell, keyspace, shard); err != nil && err != topo.ErrNoNode {
			wr.Logger().Warningf("Cannot delete SrvShard in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}
	}

	return wr.ts.DeleteShard(keyspace, shard)
}

// RemoveShardCell will remove a cell from the Cells list in a shard.
// It will first check the shard has no tablets there.  if 'force' is
// specified, it will remove the cell even when the tablet map cannot
// be retrieved. This is intended to be used when a cell is completely
// down and its topology server cannot even be reached.
func (wr *Wrangler) RemoveShardCell(keyspace, shard, cell string, force bool) error {
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.removeShardCell(keyspace, shard, cell, force)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) removeShardCell(keyspace, shard, cell string, force bool) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// check the cell is in the list already
	if !topo.InCellList(cell, shardInfo.Cells) {
		return fmt.Errorf("cell %v in not in shard info", cell)
	}

	// check the master alias is not in the cell
	if shardInfo.MasterAlias.Cell == cell {
		return fmt.Errorf("master %v is in the cell '%v' we want to remove", shardInfo.MasterAlias, cell)
	}

	// get the ShardReplication object in the cell
	sri, err := wr.ts.GetShardReplication(cell, keyspace, shard)
	switch err {
	case nil:
		if len(sri.ReplicationLinks) > 0 {
			return fmt.Errorf("cell %v has %v possible tablets in replication graph", cell, len(sri.ReplicationLinks))
		}

		// ShardReplication object is now useless, remove it
		if err := wr.ts.DeleteShardReplication(cell, keyspace, shard); err != nil {
			return fmt.Errorf("error deleting ShardReplication object in cell %v: %v", cell, err)
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

	return topo.UpdateShard(context.TODO(), wr.ts, shardInfo)
}

func (wr *Wrangler) SourceShardDelete(keyspace, shard string, uid uint32) error {
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.sourceShardDelete(keyspace, shard, uid)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) sourceShardDelete(keyspace, shard string, uid uint32) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}
	newSourceShards := make([]topo.SourceShard, 0, 0)
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
	return topo.UpdateShard(context.TODO(), wr.ts, si)
}

func (wr *Wrangler) SourceShardAdd(keyspace, shard string, uid uint32, skeyspace, sshard string, keyRange key.KeyRange, tables []string) error {
	actionNode := actionnode.UpdateShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.sourceShardAdd(keyspace, shard, uid, skeyspace, sshard, keyRange, tables)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) sourceShardAdd(keyspace, shard string, uid uint32, skeyspace, sshard string, keyRange key.KeyRange, tables []string) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// check the uid is not used already
	for _, ss := range si.SourceShards {
		if ss.Uid == uid {
			return fmt.Errorf("uid %v is already in use", uid)
		}
	}

	si.SourceShards = append(si.SourceShards, topo.SourceShard{
		Uid:      uid,
		Keyspace: skeyspace,
		Shard:    sshard,
		KeyRange: keyRange,
		Tables:   tables,
	})
	return topo.UpdateShard(context.TODO(), wr.ts, si)
}
