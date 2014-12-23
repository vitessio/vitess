// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"
)

// This file contains the actions that exist as RPC only on the ActionAgent.
// The various rpc server implementations just call these.

// RpcAgent defines the interface implemented by the Agent for RPCs.
// It is useful for RPC implementations to test their full stack.
type RpcAgent interface {
	// RPC calls

	// Various read-only methods

	Ping(ctx context.Context, args string) string

	GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error)

	GetPermissions(ctx context.Context) (*myproto.Permissions, error)

	// Various read-write methods

	SetReadOnly(ctx context.Context, rdonly bool) error

	ChangeType(ctx context.Context, tabletType topo.TabletType) error

	Scrap(ctx context.Context) error

	Sleep(ctx context.Context, duration time.Duration)

	ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult

	RefreshState(ctx context.Context)

	RunHealthCheck(ctx context.Context, targetTabletType topo.TabletType)

	ReloadSchema(ctx context.Context)

	PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error)

	ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error)

	ExecuteFetch(ctx context.Context, query string, maxrows int, wantFields, disableBinlogs bool) (*proto.QueryResult, error)

	// Replication related methods

	SlaveStatus(ctx context.Context) (*myproto.ReplicationStatus, error)

	WaitSlavePosition(ctx context.Context, position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error)

	MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error)

	ReparentPosition(ctx context.Context, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error)

	StopSlave(ctx context.Context) error

	StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	StartSlave(ctx context.Context) error

	TabletExternallyReparented(ctx context.Context, externalID string) error

	GetSlaves(ctx context.Context) ([]string, error)

	WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error

	StopBlp(ctx context.Context) (*blproto.BlpPositionList, error)

	StartBlp(ctx context.Context) error

	RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// Reparenting related functions

	DemoteMaster(ctx context.Context) error

	PromoteSlave(ctx context.Context) (*actionnode.RestartSlaveData, error)

	SlaveWasPromoted(ctx context.Context) error

	RestartSlave(ctx context.Context, rsd *actionnode.RestartSlaveData) error

	SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error

	BreakSlaves(ctx context.Context) error

	// Backup / restore related methods

	Snapshot(ctx context.Context, args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error)

	SnapshotSourceEnd(ctx context.Context, args *actionnode.SnapshotSourceEndArgs) error

	ReserveForRestore(ctx context.Context, args *actionnode.ReserveForRestoreArgs) error

	Restore(ctx context.Context, args *actionnode.RestoreArgs, logger logutil.Logger) error

	// RPC helpers
	RpcWrap(ctx context.Context, name string, args, reply interface{}, f func() error) error
	RpcWrapLock(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error
	RpcWrapLockAction(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error
}

// TODO(alainjobart): all the calls mention something like:
// Should be called under RpcWrap.
// Eventually, when all calls are going through RPCs, we'll refactor
// this so there is only one wrapper, and the extra stuff done by the
// RpcWrapXXX methods will be done internally. Until then, it's safer
// to have the comment.

// Ping makes sure RPCs work, and refreshes the tablet record.
// Should be called under RpcWrap.
func (agent *ActionAgent) Ping(ctx context.Context, args string) string {
	return args
}

// GetSchema returns the schema.
// Should be called under RpcWrap.
func (agent *ActionAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	return agent.MysqlDaemon.GetSchema(agent.Tablet().DbName(), tables, excludeTables, includeViews)
}

// GetPermissions returns the db permissions.
// Should be called under RpcWrap.
func (agent *ActionAgent) GetPermissions(ctx context.Context) (*myproto.Permissions, error) {
	return agent.Mysqld.GetPermissions()
}

// SetReadOnly makes the mysql instance read-only or read-write
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
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
	return topo.UpdateTablet(ctx, agent.TopoServer, tablet)
}

// ChangeType changes the tablet type
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ChangeType(ctx context.Context, tabletType topo.TabletType) error {
	return topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, tabletType, nil, true /*runHooks*/)
}

// Scrap scraps the live running tablet
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Scrap(ctx context.Context) error {
	return topotools.Scrap(ctx, agent.TopoServer, agent.TabletAlias, false)
}

// Sleep sleeps for the duration
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Sleep(ctx context.Context, duration time.Duration) {
	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	return hk.Execute()
}

// RefreshState reload the tablet record from the topo server.
// Should be called under RpcWrapLockAction, so it actually works.
func (agent *ActionAgent) RefreshState(ctx context.Context) {
}

// RunHealthCheck will manually run the health check on the tablet
// Should be called under RpcWrap.
func (agent *ActionAgent) RunHealthCheck(ctx context.Context, targetTabletType topo.TabletType) {
	agent.runHealthCheck(targetTabletType)
}

// ReloadSchema will reload the schema
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ReloadSchema(ctx context.Context) {
	if agent.DBConfigs == nil {
		// we skip this for test instances that can't connect to the DB anyway
		return
	}

	// This adds a dependency between tabletmanager and tabletserver,
	// so it's not ideal. But I (alainjobart) think it's better
	// to have up to date schema in vttablet.
	tabletserver.ReloadSchema()
}

// PreflightSchema will try out the schema change
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// and preflight the change
	return agent.Mysqld.PreflightSchemaChange(tablet.DbName(), change)
}

// ApplySchema will apply a schema change
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// apply the change
	scr, err := agent.Mysqld.ApplySchemaChange(tablet.DbName(), change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	agent.ReloadSchema(ctx)
	return scr, nil
}

// ExecuteFetch will execute the given query, possibly disabling binlogs.
// Should be called under RpcWrap.
func (agent *ActionAgent) ExecuteFetch(ctx context.Context, query string, maxrows int, wantFields, disableBinlogs bool) (*proto.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetDbaConnection()
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

// SlaveStatus returns the replication status
// Should be called under RpcWrap.
func (agent *ActionAgent) SlaveStatus(ctx context.Context) (*myproto.ReplicationStatus, error) {
	return agent.MysqlDaemon.SlaveStatus()
}

// WaitSlavePosition waits until we reach the provided position,
// and returns the current position
// Should be called under RpcWrapLock.
func (agent *ActionAgent) WaitSlavePosition(ctx context.Context, position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error) {
	if err := agent.Mysqld.WaitMasterPos(position, waitTimeout); err != nil {
		return nil, err
	}

	return agent.Mysqld.SlaveStatus()
}

// MasterPosition returns the master position
// Should be called under RpcWrap.
func (agent *ActionAgent) MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error) {
	return agent.Mysqld.MasterPosition()
}

// ReparentPosition returns the RestartSlaveData for the provided
// ReplicationPosition.
// Should be called under RpcWrap.
func (agent *ActionAgent) ReparentPosition(ctx context.Context, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
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

// StopSlave will stop the replication
// Should be called under RpcWrapLock.
func (agent *ActionAgent) StopSlave(ctx context.Context) error {
	return agent.MysqlDaemon.StopSlave(agent.hookExtraEnv())
}

// StopSlaveMinimum will stop the slave after it reaches at least the
// provided position.
func (agent *ActionAgent) StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	if err := agent.Mysqld.WaitMasterPos(position, waitTime); err != nil {
		return nil, err
	}
	if err := agent.Mysqld.StopSlave(agent.hookExtraEnv()); err != nil {
		return nil, err
	}
	return agent.Mysqld.SlaveStatus()
}

// StartSlave will start the replication
// Should be called under RpcWrapLock.
func (agent *ActionAgent) StartSlave(ctx context.Context) error {
	return agent.MysqlDaemon.StartSlave(agent.hookExtraEnv())
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
// Should be called under RpcWrapLock.
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

// GetSlaves returns the address of all the slaves
// Should be called under RpcWrap.
func (agent *ActionAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return agent.Mysqld.FindSlaves()
}

// WaitBlpPosition waits until a specific filtered replication position is
// reached.
// Should be called under RpcWrapLock.
func (agent *ActionAgent) WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	return agent.Mysqld.WaitBlpPosition(blpPosition, waitTime)
}

// StopBlp stops the binlog players, and return their positions.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) StopBlp(ctx context.Context) (*blproto.BlpPositionList, error) {
	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Stop()
	return agent.BinlogPlayerMap.BlpPositionList()
}

// StartBlp starts the binlog players
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) StartBlp(ctx context.Context) error {
	if agent.BinlogPlayerMap == nil {
		return fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Start()
	return nil
}

// RunBlpUntil runs the binlog player server until the position is reached,
// and returns the current mysql master replication position.
func (agent *ActionAgent) RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	if err := agent.BinlogPlayerMap.RunUntil(bpl, waitTime); err != nil {
		return nil, err
	}
	rp, err := agent.Mysqld.MasterPosition()
	return &rp, err
}

//
// Reparenting related functions
//

// DemoteMaster demotes the current master, and marks it read-only in the topo.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) DemoteMaster(ctx context.Context) error {
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
func (agent *ActionAgent) PromoteSlave(ctx context.Context) (*actionnode.RestartSlaveData, error) {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return nil, err
	}

	// Perform the action.
	rsd := &actionnode.RestartSlaveData{
		Parent: tablet.Alias,
		Force:  (tablet.Type == topo.TYPE_MASTER),
	}
	rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted, err = agent.Mysqld.PromoteSlave(false, agent.hookExtraEnv())
	if err != nil {
		return nil, err
	}
	log.Infof("PromoteSlave response: %v", *rsd)

	return rsd, agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SlaveWasPromoted(ctx context.Context) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	return agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
}

// RestartSlave tells the tablet it has a new master
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) RestartSlave(ctx context.Context, rsd *actionnode.RestartSlaveData) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topo.TYPE_LAG && !rsd.Force {
		// if tablet is behind on replication, keep it lagged, but orphan it
		tablet.Type = topo.TYPE_LAG_ORPHAN
		return topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	}
	if err = agent.Mysqld.RestartSlave(rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted); err != nil {
		return err
	}
	// Complete the special orphan accounting.
	if tablet.Type == topo.TYPE_LAG_ORPHAN {
		tablet.Type = topo.TYPE_LAG
		return topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	}
	return nil
}

// SlaveWasRestarted updates the parent record for a tablet.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}

	// Once this action completes, update authoritative tablet node first.
	if tablet.Type == topo.TYPE_MASTER {
		tablet.Type = topo.TYPE_SPARE
		tablet.State = topo.STATE_READ_ONLY
	}
	err = topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	if err != nil {
		return err
	}

	// Update the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.UpdateTabletReplicationData(ctx, agent.TopoServer, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

// BreakSlaves will tinker with the replication stream in a way that
// will stop all the slaves.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) BreakSlaves(ctx context.Context) error {
	return agent.Mysqld.BreakSlaves()
}

// updateReplicationGraphForPromotedSlave makes sure the newly promoted slave
// is correctly represented in the replication graph
func (agent *ActionAgent) updateReplicationGraphForPromotedSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	// Update tablet regardless - trend towards consistency.
	tablet.State = topo.STATE_READ_WRITE
	tablet.Type = topo.TYPE_MASTER
	tablet.Health = nil
	err := topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	if err != nil {
		return err
	}
	// NOTE(msolomon) A serving graph update is required, but in
	// order for the shard to be consistent the old master must be
	// scrapped first. That is externally coordinated by the
	// wrangler reparent action.

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.UpdateTabletReplicationData(ctx, agent.TopoServer, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

//
// Backup / restore related methods
//

// Snapshot takes a db snapshot
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Snapshot(ctx context.Context, args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	// update our type to TYPE_BACKUP
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return nil, err
	}
	originalType := tablet.Type

	// ForceMasterSnapshot: Normally a master is not a viable tablet
	// to snapshot.  However, there are degenerate cases where you need
	// to override this, for instance the initial clone of a new master.
	if tablet.Type == topo.TYPE_MASTER && args.ForceMasterSnapshot {
		// In this case, we don't bother recomputing the serving graph.
		// All queries will have to fail anyway.
		log.Infof("force change type master -> backup")
		// There is a legitimate reason to force in the case of a single
		// master.
		tablet.Tablet.Type = topo.TYPE_BACKUP
		err = topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	} else {
		err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, topo.TYPE_BACKUP, make(map[string]string), true /*runHooks*/)
	}
	if err != nil {
		return nil, err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "snapshotStart"); err != nil {
		return nil, fmt.Errorf("failed to update state before snaphost: %v", err)
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	filename, slaveStartRequired, readOnly, returnErr := agent.Mysqld.CreateSnapshot(l, tablet.DbName(), tablet.Addr(), false, args.Concurrency, args.ServerMode, agent.hookExtraEnv())

	// and change our type to the appropriate value
	newType := originalType
	if returnErr != nil {
		log.Errorf("snapshot failed, restoring tablet type back to %v: %v", newType, returnErr)
	} else {
		if args.ServerMode {
			log.Infof("server mode specified, switching tablet to snapshot_source mode")
			newType = topo.TYPE_SNAPSHOT_SOURCE
		} else {
			log.Infof("change type back after snapshot: %v", newType)
		}
	}
	if originalType == topo.TYPE_MASTER && args.ForceMasterSnapshot && newType != topo.TYPE_SNAPSHOT_SOURCE {
		log.Infof("force change type backup -> master: %v", tablet.Alias)
		tablet.Tablet.Type = topo.TYPE_MASTER
		err = topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	} else {
		err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, newType, nil, true /*runHooks*/)
	}
	if err != nil {
		// failure in changing the topology type is probably worse,
		// so returning that (we logged the snapshot error anyway)
		returnErr = err
	}

	// if anything failed, don't return anything
	if returnErr != nil {
		return nil, returnErr
	}

	// it all worked, return the required information
	sr := &actionnode.SnapshotReply{
		ManifestPath:       filename,
		SlaveStartRequired: slaveStartRequired,
		ReadOnly:           readOnly,
	}
	if tablet.Type == topo.TYPE_MASTER {
		// If this is a master, this will be the new parent.
		sr.ParentAlias = tablet.Alias
	} else {
		// Otherwise get the master from the shard record
		si, err := agent.TopoServer.GetShard(tablet.Keyspace, tablet.Shard)
		if err != nil {
			return nil, err
		}
		sr.ParentAlias = si.MasterAlias
	}
	return sr, nil
}

// SnapshotSourceEnd restores the state of the server after a
// Snapshot(server_mode =true)
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) SnapshotSourceEnd(ctx context.Context, args *actionnode.SnapshotSourceEndArgs) error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("expected snapshot_source type, not %v", tablet.Type)
	}

	if err := agent.Mysqld.SnapshotSourceEnd(args.SlaveStartRequired, args.ReadOnly, true, agent.hookExtraEnv()); err != nil {
		log.Errorf("SnapshotSourceEnd failed, leaving tablet type alone: %v", err)
		return err
	}

	// change the type back
	if args.OriginalType == topo.TYPE_MASTER {
		// force the master update
		tablet.Tablet.Type = topo.TYPE_MASTER
		err = topo.UpdateTablet(ctx, agent.TopoServer, tablet)
	} else {
		err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, args.OriginalType, make(map[string]string), true /*runHooks*/)
	}

	return err
}

// change a tablet type to RESTORE and set all the other arguments.
// from now on, we can go to:
// - back to IDLE if we don't use the tablet at all (after for instance
//   a successful ReserveForRestore but a failed Snapshot)
// - to SCRAP if something in the process on the target host fails
// - to SPARE if the clone works
func (agent *ActionAgent) changeTypeToRestore(ctx context.Context, tablet, sourceTablet *topo.TabletInfo, keyRange key.KeyRange) error {
	// run the optional preflight_assigned hook
	hk := hook.NewSimpleHook("preflight_assigned")
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	if err := hk.ExecuteOptional(); err != nil {
		return err
	}

	// change the type
	tablet.Keyspace = sourceTablet.Keyspace
	tablet.Shard = sourceTablet.Shard
	tablet.Type = topo.TYPE_RESTORE
	tablet.KeyRange = keyRange
	tablet.DbNameOverride = sourceTablet.DbNameOverride
	if err := topo.UpdateTablet(ctx, agent.TopoServer, tablet); err != nil {
		return err
	}

	// and create the replication graph items
	return topo.UpdateTabletReplicationData(ctx, agent.TopoServer, tablet.Tablet)
}

// ReserveForRestore reserves the current tablet for an upcoming
// restore operation.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) ReserveForRestore(ctx context.Context, args *actionnode.ReserveForRestoreArgs) error {
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

	return agent.changeTypeToRestore(ctx, tablet, sourceTablet, sourceTablet.KeyRange)
}

func fetchAndParseJsonFile(addr, filename string, result interface{}) error {
	// read the manifest
	murl := "http://" + addr + filename
	resp, err := http.Get(murl)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Error fetching url %v: %v", murl, resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	// unpack it
	return json.Unmarshal(data, result)
}

// Operate on restore tablet.
// Check that the SnapshotManifest is valid and the master has not changed.
// Shutdown mysqld.
// Load the snapshot from source tablet.
// Restart mysqld and replication.
// Put tablet into the replication graph as a spare.
// Should be called under RpcWrapLockAction.
func (agent *ActionAgent) Restore(ctx context.Context, args *actionnode.RestoreArgs, logger logutil.Logger) error {
	// read our current tablet, verify its state
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	if args.WasReserved {
		if tablet.Type != topo.TYPE_RESTORE {
			return fmt.Errorf("expected restore type, not %v", tablet.Type)
		}
	} else {
		if tablet.Type != topo.TYPE_IDLE {
			return fmt.Errorf("expected idle type, not %v", tablet.Type)
		}
	}
	// read the source tablet, compute args.SrcFilePath if default
	sourceTablet, err := agent.TopoServer.GetTablet(args.SrcTabletAlias)
	if err != nil {
		return err
	}
	if strings.ToLower(args.SrcFilePath) == "default" {
		args.SrcFilePath = path.Join(mysqlctl.SnapshotURLPath, mysqlctl.SnapshotManifestFile)
	}

	// read the parent tablet, verify its state
	parentTablet, err := agent.TopoServer.GetTablet(args.ParentAlias)
	if err != nil {
		return err
	}
	if parentTablet.Type != topo.TYPE_MASTER && parentTablet.Type != topo.TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("restore expected master or snapshot_source parent: %v %v", parentTablet.Type, args.ParentAlias)
	}

	// read & unpack the manifest
	sm := new(mysqlctl.SnapshotManifest)
	if err := fetchAndParseJsonFile(sourceTablet.Addr(), args.SrcFilePath, sm); err != nil {
		return err
	}

	if !args.WasReserved {
		if err := agent.changeTypeToRestore(ctx, tablet, sourceTablet, sourceTablet.KeyRange); err != nil {
			return err
		}
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// do the work
	if err := agent.Mysqld.RestoreFromSnapshot(l, sm, args.FetchConcurrency, args.FetchRetryCount, args.DontWaitForSlaveStart, agent.hookExtraEnv()); err != nil {
		log.Errorf("RestoreFromSnapshot failed (%v), scrapping", err)
		if err := topotools.Scrap(ctx, agent.TopoServer, agent.TabletAlias, false); err != nil {
			log.Errorf("Failed to Scrap after failed RestoreFromSnapshot: %v", err)
		}

		return err
	}

	// reload the schema
	agent.ReloadSchema(ctx)

	// change to TYPE_SPARE, we're done!
	return topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topo.TYPE_SPARE, nil, true)
}
