// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

// This file contains the actions that exist as RPC only on the ActionAgent.
// The various rpc server implementations just call these.
// (if an action can be both an ActionNode and a RPC, it's implemented
// in the actor code).

// ExecuteFetch will execute the given query, possibly disabling binlogs.
func (agent *ActionAgent) ExecuteFetch(query string, maxrows int, wantFields, disableBinlogs bool) (*proto.QueryResult, error) {
	// get a connection
	conn, err := agent.Mysqld.GetDbaConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	// disable binlogs if necessary
	if disableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	// run the query
	qr, err := conn.ExecuteFetch(query, maxrows, wantFields)

	// re-enable binlogs if necessary
	if disableBinlogs && !conn.IsClosed() {
		conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	return qr, err
}

// DemoteMaster demotes the current master, and marks it read-only in the topo.
func (agent *ActionAgent) DemoteMaster() error {
	_, err := agent.Mysqld.DemoteMaster()
	if err != nil {
		return err
	}

	// There is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will
	// succeed. It will be less noisy to simply leave the entry
	// until well promote the master.
	return agent.TopoServer.UpdateTabletFields(agent.TabletAlias, func(tablet *topo.Tablet) error {
		tablet.State = topo.STATE_READ_ONLY
		return nil
	})
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
func (agent *ActionAgent) SlaveWasPromoted() error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	return agent.updateReplicationGraphForPromotedSlave(tablet)
}

// SlaveWasRestarted updates the parent record for a tablet.
func (agent *ActionAgent) SlaveWasRestarted(swrd *actionnode.SlaveWasRestartedArgs) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	// Once this action completes, update authoritive tablet node first.
	tablet.Parent = swrd.Parent
	if tablet.Type == topo.TYPE_MASTER {
		tablet.Type = topo.TYPE_SPARE
		tablet.State = topo.STATE_READ_ONLY
	}
	err = topo.UpdateTablet(agent.TopoServer, tablet)
	if err != nil {
		return err
	}

	// Update the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(agent.TopoServer, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
func (agent *ActionAgent) TabletExternallyReparented(actionTimeout time.Duration) error {
	// we're apparently not the master yet, so let's do the work
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	// fast quick check on the shard
	shardInfo, err := agent.TopoServer.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias == agent.TabletAlias {
		return nil
	}

	// grab the shard lock
	actionNode := actionnode.ShardExternallyReparented(agent.TabletAlias)
	interrupted := make(chan struct{})
	lockPath, err := actionNode.LockShard(agent.TopoServer, tablet.Keyspace, tablet.Shard, agent.LockTimeout, interrupted)
	if err != nil {
		return err
	}

	// do the work
	err = agent.tabletExternallyReparentedLocked(tablet, actionTimeout, interrupted)

	// release the lock in any case
	return actionNode.UnlockShard(agent.TopoServer, tablet.Keyspace, tablet.Shard, lockPath, err)
}

func (agent *ActionAgent) tabletExternallyReparentedLocked(tablet *topo.TabletInfo, actionTimeout time.Duration, interrupted chan struct{}) (err error) {
	// read the shard, make sure again the master is not already good.
	shardInfo, err := agent.TopoServer.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias == tablet.Alias {
		return fmt.Errorf("this tablet is already the master")
	}

	// Read the tablets, make sure the master elect is known to the shard
	// (it's this tablet, so it better be!).
	// Note we will keep going with a partial tablet map, which usually
	// happens when a cell is not reachable. After these checks, the
	// guarantees we'll have are:
	// - global cell is reachable (we just locked and read the shard)
	// - the local cell that contains the new master is reachable
	//   (as we're going to check the new master is in the list)
	// That should be enough.
	tabletMap, err := topo.GetTabletMapForShard(agent.TopoServer, tablet.Keyspace, tablet.Shard)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		log.Warningf("Got topo.ErrPartialResult from GetTabletMapForShard, may need to re-init some tablets")
	default:
		return err
	}
	masterElectTablet, ok := tabletMap[tablet.Alias]
	if !ok {
		return fmt.Errorf("this master-elect tablet %v not found in replication graph %v/%v %v", tablet.Alias, tablet.Keyspace, tablet.Shard, topotools.MapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		ShardInfo: *shardInfo,
		NewMaster: *tablet.Tablet,
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

	// we fix the new master in the replication graph
	event.DispatchUpdate(ev, "mark ourself as new master")
	err = agent.updateReplicationGraphForPromotedSlave(tablet)
	if err != nil {
		// This suggests we can't talk to topo server. This is bad.
		return fmt.Errorf("updateReplicationGraphForPromotedSlave failed: %v", err)
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
	ai := initiator.NewActionInitiator(agent.TopoServer)
	topotools.RestartSlavesExternal(agent.TopoServer, logger, slaveTabletMap, masterTabletMap, masterElectTablet.Alias, func(ti *topo.TabletInfo, swrd *actionnode.SlaveWasRestartedArgs) error {
		return ai.SlaveWasRestarted(ti, swrd, actionTimeout)
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
	if err = topo.UpdateShard(agent.TopoServer, shardInfo); err != nil {
		return err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	log.Infof("Rebuilding shard serving graph data")
	if err = topotools.RebuildShard(logger, agent.TopoServer, tablet.Keyspace, tablet.Shard, cells, agent.LockTimeout, interrupted); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// updateReplicationGraphForPromotedSlave amkes sure the newly promoted slave
// is correctly represented in the replication graph
func (agent *ActionAgent) updateReplicationGraphForPromotedSlave(tablet *topo.TabletInfo) error {
	// Remove tablet from the replication graph if this is not
	// already the master.
	if tablet.Parent.Uid != topo.NO_TABLET {
		if err := topo.DeleteTabletReplicationData(agent.TopoServer, tablet.Tablet); err != nil && err != topo.ErrNoNode {
			return err
		}
	}

	// Update tablet regardless - trend towards consistency.
	tablet.State = topo.STATE_READ_WRITE
	tablet.Type = topo.TYPE_MASTER
	tablet.Parent.Cell = ""
	tablet.Parent.Uid = topo.NO_TABLET
	tablet.Health = nil
	err := topo.UpdateTablet(agent.TopoServer, tablet)
	if err != nil {
		return err
	}
	// NOTE(msolomon) A serving graph update is required, but in
	// order for the shard to be consistent the old master must be
	// scrapped first. That is externally coordinated by the
	// wrangler reparent action.

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(agent.TopoServer, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}
