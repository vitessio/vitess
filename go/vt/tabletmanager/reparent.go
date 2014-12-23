// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"
)

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	tablet := agent.Tablet()

	// fast quick check on the shard to see if we're not the master already
	shardInfo, err := agent.TopoServer.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: Cannot read the shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}
	if shardInfo.MasterAlias == agent.TabletAlias {
		// we are already the master, nothing more to do.
		return nil
	}

	// grab the shard lock
	actionNode := actionnode.ShardExternallyReparented(agent.TabletAlias)
	lockPath, err := actionNode.LockShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: Cannot lock shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// do the work
	runAfterAction, err := agent.tabletExternallyReparentedLocked(ctx, externalID)
	if err != nil {
		log.Warningf("TabletExternallyReparented: internal error: %v", err)
	}

	// release the lock in any case, and run refreshTablet if necessary
	err = actionNode.UnlockShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard, lockPath, err)
	if runAfterAction {
		if refreshErr := agent.refreshTablet(ctx, "RPC(TabletExternallyReparented)"); refreshErr != nil {
			if err == nil {
				// no error yet, now we have one
				err = refreshErr
			} else {
				//have an error already, keep the original one
				log.Warningf("refreshTablet failed with error: %v", refreshErr)
			}
		}
	}
	return err
}

// tabletExternallyReparentedLocked is called with the shard lock.
// It returns if agent.refreshTablet should be called, and the error.
// Note both are set independently (can have both true and an error).
func (agent *ActionAgent) tabletExternallyReparentedLocked(ctx context.Context, externalID string) (bool, error) {
	// re-read the tablet record to be sure we have the latest version
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return false, err
	}

	// read the shard, make sure again the master is not already good.
	shardInfo, err := agent.TopoServer.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		return false, err
	}
	if shardInfo.MasterAlias == tablet.Alias {
		log.Infof("TabletExternallyReparented: tablet became the master before we get the lock?")
		return false, nil
	}
	log.Infof("TabletExternallyReparented called and we're not the master, doing the work")

	// Read the tablets, make sure the master elect is known to the shard
	// (it's this tablet, so it better be!).
	// Note we will keep going with a partial tablet map, which usually
	// happens when a cell is not reachable. After these checks, the
	// guarantees we'll have are:
	// - global cell is reachable (we just locked and read the shard)
	// - the local cell that contains the new master is reachable
	//   (as we're going to check the new master is in the list)
	// That should be enough.
	tabletMap, err := topo.GetTabletMapForShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		log.Warningf("Got topo.ErrPartialResult from GetTabletMapForShard, may need to re-init some tablets")
	default:
		return false, err
	}
	masterElectTablet, ok := tabletMap[tablet.Alias]
	if !ok {
		return false, fmt.Errorf("this master-elect tablet %v not found in replication graph %v/%v %v", tablet.Alias, tablet.Keyspace, tablet.Shard, topotools.MapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		ShardInfo:  *shardInfo,
		NewMaster:  *tablet.Tablet,
		ExternalID: externalID,
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
	event.DispatchUpdate(ev, "starting external from tablet")

	// We fix the new master in the replication graph.
	// Note after this call, we may have changed the tablet record,
	// so we will always return true, so the tablet record is re-read
	// by the agent.
	event.DispatchUpdate(ev, "mark ourself as new master")
	err = agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
	if err != nil {
		// This suggests we can't talk to topo server. This is bad.
		return true, fmt.Errorf("updateReplicationGraphForPromotedSlave failed: %v", err)
	}

	// Once this tablet is promoted, remove it from our maps
	delete(slaveTabletMap, tablet.Alias)
	delete(masterTabletMap, tablet.Alias)

	// Then fix all the slaves, including the old master.  This
	// last step is very likely to time out for some tablets (one
	// random guy is dead, the old master is dead, ...). We
	// execute them all in parallel until we get to
	// wr.ActionTimeout(). After this, no other action with a
	// timeout is executed, so even if we got to the timeout,
	// we're still good.
	event.DispatchUpdate(ev, "restarting slaves")
	logger := logutil.NewConsoleLogger()
	tmc := tmclient.NewTabletManagerClient()
	topotools.RestartSlavesExternal(agent.TopoServer, logger, slaveTabletMap, masterTabletMap, masterElectTablet.Alias, func(ti *topo.TabletInfo, swrd *actionnode.SlaveWasRestartedArgs) error {
		return tmc.SlaveWasRestarted(ctx, ti, swrd)
	})

	// Compute the list of Cells we need to rebuild: old master and
	// all other cells if reparenting to another cell.
	cells := []string{shardInfo.MasterAlias.Cell}
	if shardInfo.MasterAlias.Cell != tablet.Alias.Cell {
		cells = nil
	}

	// now update the master record in the shard object
	event.DispatchUpdate(ev, "updating shard record")
	log.Infof("Updating Shard's MasterAlias record")
	shardInfo.MasterAlias = tablet.Alias
	if err = topo.UpdateShard(ctx, agent.TopoServer, shardInfo); err != nil {
		return true, err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	log.Infof("Rebuilding shard serving graph data")
	if _, err = topotools.RebuildShard(ctx, logger, agent.TopoServer, tablet.Keyspace, tablet.Shard, cells, agent.LockTimeout); err != nil {
		return true, err
	}

	event.DispatchUpdate(ev, "finished")
	return true, nil
}
