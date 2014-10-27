// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

// ShardExternallyReparented updates the topology after the master
// tablet in a keyspace/shard has changed. We trust that whoever made
// the change completed the change successfully. We lock the shard
// while doing the update.  We will then rebuild the serving graph in
// the cells that need it (the old master cell and the new master cell)
func (wr *Wrangler) ShardExternallyReparented(keyspace, shard string, masterElectTabletAlias topo.TabletAlias) error {
	// grab the shard lock
	actionNode := actionnode.ShardExternallyReparented(masterElectTabletAlias)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.shardExternallyReparentedLocked(keyspace, shard, masterElectTabletAlias)

	// release the lock in any case
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) shardExternallyReparentedLocked(keyspace, shard string, masterElectTabletAlias topo.TabletAlias) (err error) {
	// read the shard, make sure the master is not already good.
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already master", masterElectTabletAlias)
	}

	// Read the tablets, make sure the master elect is known to us.
	// Note we will keep going with a partial tablet map, which usually
	// happens when a cell is not reachable. After these checks, the
	// guarantees we'll have are:
	// - global cell is reachable (we just locked and read the shard)
	// - the local cell that contains the new master is reachable
	//   (as we're going to check the new master is in the list)
	// That should be enough.
	tabletMap, err := topo.GetTabletMapForShard(wr.ts, keyspace, shard)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		wr.logger.Warningf("Got topo.ErrPartialResult from GetTabletMapForShard, may need to re-init some tablets")
	default:
		return err
	}
	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, topotools.MapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		ShardInfo: *shardInfo,
		NewMaster: *masterElectTablet.Tablet,
	}

	if oldMasterTablet, ok := tabletMap[shardInfo.MasterAlias]; ok {
		ev.OldMaster = *oldMasterTablet.Tablet
	}

	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	// sort the tablets, and handle them
	slaveTabletMap, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	err = wr.reparentShardExternal(ev, slaveTabletMap, masterTabletMap, masterElectTablet)
	if err != nil {
		wr.logger.Infof("Skipping shard rebuild with failed reparent")
		return err
	}

	// Compute the list of Cells we need to rebuild: old master and
	// all other cells if reparenting to another cell.
	cells := []string{shardInfo.MasterAlias.Cell}
	if shardInfo.MasterAlias.Cell != masterElectTabletAlias.Cell {
		cells = nil
	}

	// now update the master record in the shard object
	event.DispatchUpdate(ev, "updating shard record")
	wr.logger.Infof("Updating Shard's MasterAlias record")
	shardInfo.MasterAlias = masterElectTabletAlias
	if err = topo.UpdateShard(wr.ts, shardInfo); err != nil {
		return err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	wr.logger.Infof("Rebuilding shard serving graph data")
	if _, err = topotools.RebuildShard(wr.logger, wr.ts, masterElectTablet.Keyspace, masterElectTablet.Shard, cells, wr.lockTimeout, interrupted); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// reparentShardExternal handles an external reparent.
//
// The ev parameter is an event struct prefilled with information that the
// caller has on hand, which would be expensive for us to re-query.
func (wr *Wrangler) reparentShardExternal(ev *events.Reparent, slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo) error {
	event.DispatchUpdate(ev, "starting external")

	// we fix the new master in the replication graph
	event.DispatchUpdate(ev, "checking if new master was promoted")
	err := wr.slaveWasPromoted(masterElectTablet)
	if err != nil {
		// This suggests that the master-elect is dead. This is bad.
		return fmt.Errorf("slaveWasPromoted(%v) failed: %v", masterElectTablet, err)
	}

	// Once the slave is promoted, remove it from our maps
	delete(slaveTabletMap, masterElectTablet.Alias)
	delete(masterTabletMap, masterElectTablet.Alias)

	// Then fix all the slaves, including the old master.  This
	// last step is very likely to time out for some tablets (one
	// random guy is dead, the old master is dead, ...). We
	// execute them all in parallel until we get to
	// wr.ActionTimeout(). After this, no other action with a
	// timeout is executed, so even if we got to the timeout,
	// we're still good.
	event.DispatchUpdate(ev, "restarting slaves")
	topotools.RestartSlavesExternal(wr.ts, wr.logger, slaveTabletMap, masterTabletMap, masterElectTablet.Alias, wr.slaveWasRestarted)
	return nil
}

func (wr *Wrangler) slaveWasPromoted(ti *topo.TabletInfo) error {
	wr.logger.Infof("slaveWasPromoted(%v)", ti.Alias)
	return wr.tmc.SlaveWasPromoted(ti, wr.ActionTimeout())
}

func (wr *Wrangler) slaveWasRestarted(ti *topo.TabletInfo, swra *actionnode.SlaveWasRestartedArgs) (err error) {
	wr.logger.Infof("slaveWasRestarted(%v)", ti.Alias)
	return wr.tmc.SlaveWasRestarted(ti, swra, wr.ActionTimeout())
}
