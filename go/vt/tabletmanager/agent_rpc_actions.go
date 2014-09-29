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
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
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

// TODO(alainjobart): all the calls mention something like:
// Should be called under RpcWrap.
// Eventually, when all calls are going through RPCs, we'll refactor
// this so there is only one wrapper, and the extra stuff done by the
// RpcWrapXXX methods will be done internally. Until then, it's safer
// to have the comment.

// SetReadOnly makes the mysql instance read-only or read-write
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SetReadOnly(rdonly bool) error {
	err := agent.Mysqld.SetReadOnly(rdonly)
	if err != nil {
		return err
	}

	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if rdonly {
		tablet.State = topo.STATE_READ_ONLY
	} else {
		tablet.State = topo.STATE_READ_WRITE
	}
	return topo.UpdateTablet(agent.TopoServer, tablet)
}

// Scrap scraps the live running tablet
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Scrap() error {
	return topotools.Scrap(agent.TopoServer, agent.TabletAlias, false)
}

// Sleep sleeps for the duration
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ExecuteHook(hk *hook.Hook) *hook.HookResult {
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	return hk.Execute()
}

// PreflightSchema will try out the schema change
func (agent *ActionAgent) PreflightSchema(change string) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// and preflight the change
	return agent.Mysqld.PreflightSchemaChange(tablet.DbName(), change)
}

// ApplySchema will apply a schema change
func (agent *ActionAgent) ApplySchema(change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// and apply the change
	return agent.Mysqld.ApplySchemaChange(tablet.DbName(), change)
}

// ExecuteFetch will execute the given query, possibly disabling binlogs.
// Should be called under RpcWrap.
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

// ReparentPosition returns the RestartSlaveData for the provided
// ReplicationPosition.
// Should be called under RpcWrap.
func (agent *ActionAgent) ReparentPosition(rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	replicationStatus, waitPosition, timePromoted, err := agent.Mysqld.ReparentPosition(*rp)
	if err != nil {
		return nil, err
	}
	rsd := new(actionnode.RestartSlaveData)
	rsd.ReplicationStatus = replicationStatus
	rsd.TimePromoted = timePromoted
	rsd.WaitPosition = waitPosition
	rsd.Parent = agent.TabletAlias
	return rsd, nil
}

// DemoteMaster demotes the current master, and marks it read-only in the topo.
// Should be called under RpcWrapLockAction.
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

// PromoteSlave transforms the current tablet from a slave to a master.
// It returns the data needed for other tablets to become a slave.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) PromoteSlave() (*actionnode.RestartSlaveData, error) {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return nil, err
	}

	// Perform the action.
	rsd := &actionnode.RestartSlaveData{
		Parent: tablet.Alias,
		Force:  (tablet.Parent.Uid == topo.NO_TABLET),
	}
	rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted, err = agent.Mysqld.PromoteSlave(false, agent.hookExtraEnv())
	if err != nil {
		return nil, err
	}
	log.Infof("PromoteSlave response: %v", *rsd)

	return rsd, agent.updateReplicationGraphForPromotedSlave(tablet)
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SlaveWasPromoted() error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	return agent.updateReplicationGraphForPromotedSlave(tablet)
}

// RestartSlave tells the tablet it has a new master
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) RestartSlave(rsd *actionnode.RestartSlaveData) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	// If this check fails, we seem reparented. The only part that
	// could have failed is the insert in the replication
	// graph. Do NOT try to reparent again. That will either wedge
	// replication or corrupt data.
	if tablet.Parent != rsd.Parent {
		log.V(6).Infof("restart with new parent")
		// Remove tablet from the replication graph.
		if err = topo.DeleteTabletReplicationData(agent.TopoServer, tablet.Tablet); err != nil && err != topo.ErrNoNode {
			return err
		}

		// Move a lag slave into the orphan lag type so we can safely ignore
		// this reparenting until replication catches up.
		if tablet.Type == topo.TYPE_LAG {
			tablet.Type = topo.TYPE_LAG_ORPHAN
		} else {
			err = agent.Mysqld.RestartSlave(rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted)
			if err != nil {
				return err
			}
		}
		// Once this action completes, update authoritive tablet node first.
		tablet.Parent = rsd.Parent
		err = topo.UpdateTablet(agent.TopoServer, tablet)
		if err != nil {
			return err
		}
	} else if rsd.Force {
		err = agent.Mysqld.RestartSlave(rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted)
		if err != nil {
			return err
		}
		// Complete the special orphan accounting.
		if tablet.Type == topo.TYPE_LAG_ORPHAN {
			tablet.Type = topo.TYPE_LAG
			err = topo.UpdateTablet(agent.TopoServer, tablet)
			if err != nil {
				return err
			}
		}
	} else {
		// There is nothing to safely reparent, so check replication. If
		// either replication thread is not running, report an error.
		status, err := agent.Mysqld.SlaveStatus()
		if err != nil {
			return fmt.Errorf("cannot verify replication for slave: %v", err)
		}
		if !status.SlaveRunning() {
			return fmt.Errorf("replication not running for slave")
		}
	}

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(agent.TopoServer, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

// SlaveWasRestarted updates the parent record for a tablet.
// Should be called under RpcWrapLockAction.
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

// BreakSlaves will tinker with the replication stream in a way that
// will stop all the slaves.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) BreakSlaves() error {
	return agent.Mysqld.BreakSlaves()
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
// Should be called under RpcWrapLock.
func (agent *ActionAgent) TabletExternallyReparented(actionTimeout time.Duration) error {
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
	interrupted := make(chan struct{})
	lockPath, err := actionNode.LockShard(agent.TopoServer, tablet.Keyspace, tablet.Shard, agent.LockTimeout, interrupted)
	if err != nil {
		log.Warningf("TabletExternallyReparented: Cannot lock shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// do the work
	runAfterAction, err := agent.tabletExternallyReparentedLocked(actionTimeout, interrupted)
	if err != nil {
		log.Warningf("TabletExternallyReparented: internal error: %v", err)
	}

	// release the lock in any case, and run afterAction if necessary
	err = actionNode.UnlockShard(agent.TopoServer, tablet.Keyspace, tablet.Shard, lockPath, err)
	if runAfterAction {
		agent.afterAction("RPC(TabletExternallyReparented)" /*reloadSchema*/, false)
	}
	return err
}

// tabletExternallyReparentedLocked is called with the shard lock.
// It returns if agent.afterAction should be called, and the error.
// Note both are set independently (can have both true and an error).
func (agent *ActionAgent) tabletExternallyReparentedLocked(actionTimeout time.Duration, interrupted chan struct{}) (bool, error) {
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
	tabletMap, err := topo.GetTabletMapForShard(agent.TopoServer, tablet.Keyspace, tablet.Shard)
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

	// We fix the new master in the replication graph.
	// Note after this call, we may have changed the tablet record,
	// so we will always return true, so the tablet record is re-read
	// by the agent.
	event.DispatchUpdate(ev, "mark ourself as new master")
	err = agent.updateReplicationGraphForPromotedSlave(tablet)
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
		return true, err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	log.Infof("Rebuilding shard serving graph data")
	if err = topotools.RebuildShard(logger, agent.TopoServer, tablet.Keyspace, tablet.Shard, cells, agent.LockTimeout, interrupted); err != nil {
		return true, err
	}

	event.DispatchUpdate(ev, "finished")
	return true, nil
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

//
// Backup related methods
//

// Snapshot takes a db snapshot
func (agent *ActionAgent) Snapshot(args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return nil, err
	}
	if tablet.Type != topo.TYPE_BACKUP {
		return nil, fmt.Errorf("expected backup type, not %v", tablet.Type)
	}

	filename, slaveStartRequired, readOnly, err := agent.Mysqld.CreateSnapshot(tablet.DbName(), tablet.Addr(), false, args.Concurrency, args.ServerMode, agent.hookExtraEnv())
	if err != nil {
		return nil, err
	}

	sr := &actionnode.SnapshotReply{ManifestPath: filename, SlaveStartRequired: slaveStartRequired, ReadOnly: readOnly}
	if tablet.Parent.Uid == topo.NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		sr.ParentAlias = tablet.Alias
	} else {
		sr.ParentAlias = tablet.Parent
	}
	return sr, nil
}

func (agent *ActionAgent) SnapshotSourceEnd(args *actionnode.SnapshotSourceEndArgs) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("expected snapshot_source type, not %v", tablet.Type)
	}

	return agent.Mysqld.SnapshotSourceEnd(args.SlaveStartRequired, args.ReadOnly, true, agent.hookExtraEnv())
}

// change a tablet type to RESTORE and set all the other arguments.
// from now on, we can go to:
// - back to IDLE if we don't use the tablet at all (after for instance
//   a successful ReserveForRestore but a failed Snapshot)
// - to SCRAP if something in the process on the target host fails
// - to SPARE if the clone works
func (agent *ActionAgent) changeTypeToRestore(tablet, sourceTablet *topo.TabletInfo, parentAlias topo.TabletAlias, keyRange key.KeyRange) error {
	// run the optional preflight_assigned hook
	hk := hook.NewSimpleHook("preflight_assigned")
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	if err := hk.ExecuteOptional(); err != nil {
		return err
	}

	// change the type
	tablet.Parent = parentAlias
	tablet.Keyspace = sourceTablet.Keyspace
	tablet.Shard = sourceTablet.Shard
	tablet.Type = topo.TYPE_RESTORE
	tablet.KeyRange = keyRange
	tablet.DbNameOverride = sourceTablet.DbNameOverride
	if err := topo.UpdateTablet(agent.TopoServer, tablet); err != nil {
		return err
	}

	// and create the replication graph items
	return topo.CreateTabletReplicationData(agent.TopoServer, tablet.Tablet)
}

func (agent *ActionAgent) ReserveForRestore(args *actionnode.ReserveForRestoreArgs) error {
	// first check mysql, no need to go further if we can't restore
	if err := agent.Mysqld.ValidateCloneTarget(agent.hookExtraEnv()); err != nil {
		return err
	}

	// read our current tablet, verify its state
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v", tablet.Type)
	}

	// read the source tablet
	sourceTablet, err := agent.TopoServer.GetTablet(args.SrcTabletAlias)
	if err != nil {
		return err
	}

	// find the parent tablet alias we will be using
	var parentAlias topo.TabletAlias
	if sourceTablet.Parent.Uid == topo.NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		parentAlias = sourceTablet.Alias
	} else {
		parentAlias = sourceTablet.Parent
	}

	return agent.changeTypeToRestore(tablet, sourceTablet, parentAlias, sourceTablet.KeyRange)
}
