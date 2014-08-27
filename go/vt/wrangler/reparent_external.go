// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

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
	// critical read, we want up to date info (and the shard is locked).
	shardInfo, err := wr.ts.GetShardCritical(keyspace, shard)
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
	slaveTabletMap, masterTabletMap := sortedTabletMap(tabletMap)
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
	if err = wr.ts.UpdateShard(shardInfo); err != nil {
		return err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	wr.logger.Infof("Rebuilding shard serving graph data")
	if err = topotools.RebuildShard(wr.logger, wr.ts, masterElectTablet.Keyspace, masterElectTablet.Shard, cells, wr.lockTimeout, interrupted); err != nil {
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

	// then fix all the slaves, including the old master
	event.DispatchUpdate(ev, "restarting slaves")
	swr := wr.slaveWasRestartedActionNode
	if wr.UseRPCs {
		swr = wr.slaveWasRestartedRpc
	}
	wr.restartSlavesExternal(slaveTabletMap, masterTabletMap, masterElectTablet.Alias, swr)
	return nil
}

func (wr *Wrangler) restartSlavesExternal(slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTabletAlias topo.TabletAlias, slaveWasRestarted func(*topo.TabletInfo, *actionnode.SlaveWasRestartedArgs) error) {
	wg := sync.WaitGroup{}

	swrd := actionnode.SlaveWasRestartedArgs{
		Parent: masterElectTabletAlias,
	}

	// The following two blocks of actions are very likely to time
	// out for some tablets (one random guy is dead, the old
	// master is dead, ...). We execute them all in parallel until
	// we get to wr.ActionTimeout(). After this, no other action
	// with a timeout is executed, so even if we got to the
	// timeout, we're still good.
	wr.logger.Infof("Updating individual tablets with the right master...")

	// do all the slaves
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			if err := slaveWasRestarted(ti, &swrd); err != nil {
				wr.logger.Warningf("Slave %v had an error: %v", ti.Alias, err)
			}
			wg.Done()
		}(ti)
	}

	// and do the old master and any straggler, if possible.
	for _, ti := range masterTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			err := slaveWasRestarted(ti, &swrd)
			if err != nil {
				// the old master can be annoying if left
				// around in the replication graph, so if we
				// can't restart it, we just scrap it.
				// We don't rebuild the Shard just yet though.
				wr.logger.Warningf("Old master %v is not restarting, scrapping it: %v", ti.Alias, err)
				if err := topotools.Scrap(wr.ts, ti.Alias, true /*force*/); err != nil {
					wr.logger.Warningf("Failed to scrap old master %v: %v", ti.Alias, err)
				}
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()
}

func (wr *Wrangler) slaveWasPromoted(ti *topo.TabletInfo) error {
	wr.logger.Infof("slaveWasPromoted(%v)", ti.Alias)
	if wr.UseRPCs {
		return wr.ai.RpcSlaveWasPromoted(ti, wr.ActionTimeout())
	} else {
		actionPath, err := wr.ai.SlaveWasPromoted(ti.Alias)
		if err != nil {
			return err
		}
		err = wr.WaitForCompletion(actionPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wr *Wrangler) slaveWasRestartedRpc(ti *topo.TabletInfo, swrd *actionnode.SlaveWasRestartedArgs) (err error) {
	wr.logger.Infof("slaveWasRestartedRpc(%v)", ti.Alias)
	return wr.ai.RpcSlaveWasRestarted(ti, swrd, wr.ActionTimeout())
}

func (wr *Wrangler) slaveWasRestartedActionNode(ti *topo.TabletInfo, swrd *actionnode.SlaveWasRestartedArgs) (err error) {
	wr.logger.Infof("slaveWasRestartedActionNode(%v)", ti.Alias)
	actionPath, err := wr.ai.SlaveWasRestarted(ti.Alias, swrd)
	if err != nil {
		return err
	}
	return wr.WaitForCompletion(actionPath)
}
