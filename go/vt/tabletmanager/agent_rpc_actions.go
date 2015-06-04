// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

// This file contains the actions that exist as RPC only on the ActionAgent.
// The various rpc server implementations just call these.

// RPCAgent defines the interface implemented by the Agent for RPCs.
// It is useful for RPC implementations to test their full stack.
type RPCAgent interface {
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

	RegisterHealthStream(chan<- *actionnode.HealthStreamReply) (int, error)
	UnregisterHealthStream(int) error

	ReloadSchema(ctx context.Context)

	PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error)

	ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error)

	ExecuteFetchAsDba(ctx context.Context, query string, dbName string, maxrows int, wantFields, disableBinlogs bool, reloadSchema bool) (*proto.QueryResult, error)

	ExecuteFetchAsApp(ctx context.Context, query string, maxrows int, wantFields bool) (*proto.QueryResult, error)

	// Replication related methods

	SlaveStatus(ctx context.Context) (myproto.ReplicationStatus, error)

	MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error)

	StopSlave(ctx context.Context) error

	StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (myproto.ReplicationPosition, error)

	StartSlave(ctx context.Context) error

	TabletExternallyReparented(ctx context.Context, externalID string) error

	GetSlaves(ctx context.Context) ([]string, error)

	WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error

	StopBlp(ctx context.Context) (*blproto.BlpPositionList, error)

	StartBlp(ctx context.Context) error

	RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// Reparenting related functions

	ResetReplication(ctx context.Context) error

	InitMaster(ctx context.Context) (myproto.ReplicationPosition, error)

	PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias topo.TabletAlias, pos myproto.ReplicationPosition) error

	InitSlave(ctx context.Context, parent topo.TabletAlias, replicationPosition myproto.ReplicationPosition, timeCreatedNS int64) error

	DemoteMaster(ctx context.Context) (myproto.ReplicationPosition, error)

	PromoteSlaveWhenCaughtUp(ctx context.Context, replicationPosition myproto.ReplicationPosition) (myproto.ReplicationPosition, error)

	SlaveWasPromoted(ctx context.Context) error

	SetMaster(ctx context.Context, parent topo.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error

	SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error

	StopReplicationAndGetStatus(ctx context.Context) (myproto.ReplicationStatus, error)

	PromoteSlave(ctx context.Context) (myproto.ReplicationPosition, error)

	// Backup / restore related methods

	Backup(ctx context.Context, concurrency int, logger logutil.Logger) error

	// RPC helpers
	RPCWrap(ctx context.Context, name string, args, reply interface{}, f func() error) error
	RPCWrapLock(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error
	RPCWrapLockAction(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error
}

// TODO(alainjobart): all the calls mention something like:
// Should be called under RPCWrap.
// Eventually, when all calls are going through RPCs, we'll refactor
// this so there is only one wrapper, and the extra stuff done by the
// RPCWrapXXX methods will be done internally. Until then, it's safer
// to have the comment.

// Ping makes sure RPCs work, and refreshes the tablet record.
// Should be called under RPCWrap.
func (agent *ActionAgent) Ping(ctx context.Context, args string) string {
	return args
}

// GetSchema returns the schema.
// Should be called under RPCWrap.
func (agent *ActionAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	return agent.MysqlDaemon.GetSchema(agent.Tablet().DbName(), tables, excludeTables, includeViews)
}

// GetPermissions returns the db permissions.
// Should be called under RPCWrap.
func (agent *ActionAgent) GetPermissions(ctx context.Context) (*myproto.Permissions, error) {
	return mysqlctl.GetPermissions(agent.MysqlDaemon)
}

// SetReadOnly makes the mysql instance read-only or read-write
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
	return agent.MysqlDaemon.SetReadOnly(rdonly)
}

// ChangeType changes the tablet type
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ChangeType(ctx context.Context, tabletType topo.TabletType) error {
	return topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, tabletType, nil)
}

// Scrap scraps the live running tablet
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) Scrap(ctx context.Context) error {
	return topotools.Scrap(ctx, agent.TopoServer, agent.TabletAlias, false)
}

// Sleep sleeps for the duration
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) Sleep(ctx context.Context, duration time.Duration) {
	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	return hk.Execute()
}

// RefreshState reload the tablet record from the topo server.
// Should be called under RPCWrapLockAction, so it actually works.
func (agent *ActionAgent) RefreshState(ctx context.Context) {
}

// RunHealthCheck will manually run the health check on the tablet
// Should be called under RPCWrap.
func (agent *ActionAgent) RunHealthCheck(ctx context.Context, targetTabletType topo.TabletType) {
	agent.runHealthCheck(targetTabletType)
}

// RegisterHealthStream adds a health stream channel to our list
func (agent *ActionAgent) RegisterHealthStream(c chan<- *actionnode.HealthStreamReply) (int, error) {
	agent.healthStreamMutex.Lock()
	defer agent.healthStreamMutex.Unlock()

	id := agent.healthStreamIndex
	agent.healthStreamIndex++
	agent.healthStreamMap[id] = c
	return id, nil
}

// UnregisterHealthStream removes a health stream channel from our list
func (agent *ActionAgent) UnregisterHealthStream(id int) error {
	agent.healthStreamMutex.Lock()
	defer agent.healthStreamMutex.Unlock()

	delete(agent.healthStreamMap, id)
	return nil
}

// ReloadSchema will reload the schema
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ReloadSchema(ctx context.Context) {
	if agent.DBConfigs == nil {
		// we skip this for test instances that can't connect to the DB anyway
		return
	}

	// This adds a dependency between tabletmanager and tabletserver,
	// so it's not ideal. But I (alainjobart) think it's better
	// to have up to date schema in vttablet.
	agent.QueryServiceControl.ReloadSchema()
}

// PreflightSchema will try out the schema change
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// and preflight the change
	return agent.MysqlDaemon.PreflightSchemaChange(tablet.DbName(), change)
}

// ApplySchema will apply a schema change
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	// get the db name from the tablet
	tablet := agent.Tablet()

	// apply the change
	scr, err := agent.MysqlDaemon.ApplySchemaChange(tablet.DbName(), change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	agent.ReloadSchema(ctx)
	return scr, nil
}

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
// Should be called under RPCWrap.
func (agent *ActionAgent) ExecuteFetchAsDba(ctx context.Context, query string, dbName string, maxrows int, wantFields bool, disableBinlogs bool, reloadSchema bool) (*proto.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetDbaConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// disable binlogs if necessary
	if disableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if dbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		conn.ExecuteFetch("USE "+dbName, 1, false)
	}

	// run the query
	qr, err := conn.ExecuteFetch(query, maxrows, wantFields)

	// re-enable binlogs if necessary
	if disableBinlogs && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	if err == nil && reloadSchema {
		agent.QueryServiceControl.ReloadSchema()
	}
	return qr, err
}

// ExecuteFetchAsApp will execute the given query, possibly disabling binlogs.
// Should be called under RPCWrap.
func (agent *ActionAgent) ExecuteFetchAsApp(ctx context.Context, query string, maxrows int, wantFields bool) (*proto.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetAppConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.ExecuteFetch(query, maxrows, wantFields)
}

// SlaveStatus returns the replication status
// Should be called under RPCWrap.
func (agent *ActionAgent) SlaveStatus(ctx context.Context) (myproto.ReplicationStatus, error) {
	return agent.MysqlDaemon.SlaveStatus()
}

// MasterPosition returns the master position
// Should be called under RPCWrap.
func (agent *ActionAgent) MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error) {
	return agent.MysqlDaemon.MasterPosition()
}

// StopSlave will stop the replication. Works both when Vitess manages
// replication or not (using hook if not).
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StopSlave(ctx context.Context) error {
	return mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv())
}

// StopSlaveMinimum will stop the slave after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	if err := agent.MysqlDaemon.WaitMasterPos(position, waitTime); err != nil {
		return myproto.ReplicationPosition{}, err
	}
	if err := mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv()); err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return agent.MysqlDaemon.MasterPosition()
}

// StartSlave will start the replication. Works both when Vitess manages
// replication or not (using hook if not).
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StartSlave(ctx context.Context) error {
	return mysqlctl.StartSlave(agent.MysqlDaemon, agent.hookExtraEnv())
}

// GetSlaves returns the address of all the slaves
// Should be called under RPCWrap.
func (agent *ActionAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return mysqlctl.FindSlaves(agent.MysqlDaemon)
}

// WaitBlpPosition waits until a specific filtered replication position is
// reached.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	return mysqlctl.WaitBlpPosition(agent.MysqlDaemon, blpPosition, waitTime)
}

// StopBlp stops the binlog players, and return their positions.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) StopBlp(ctx context.Context) (*blproto.BlpPositionList, error) {
	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Stop()
	return agent.BinlogPlayerMap.BlpPositionList()
}

// StartBlp starts the binlog players
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) StartBlp(ctx context.Context) error {
	if agent.BinlogPlayerMap == nil {
		return fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Start(agent.batchCtx)
	return nil
}

// RunBlpUntil runs the binlog player server until the position is reached,
// and returns the current mysql master replication position.
func (agent *ActionAgent) RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	if err := agent.BinlogPlayerMap.RunUntil(ctx, bpl, waitTime); err != nil {
		return nil, err
	}
	rp, err := agent.MysqlDaemon.MasterPosition()
	return &rp, err
}

//
// Reparenting related functions
//

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (agent *ActionAgent) ResetReplication(ctx context.Context) error {
	cmds, err := agent.MysqlDaemon.ResetReplicationCommands()
	if err != nil {
		return err
	}

	return agent.MysqlDaemon.ExecuteSuperQueryList(cmds)
}

// InitMaster breaks slaves replication, get the current MySQL replication
// position, insert a row in the reparent_journal table, and returns
// the replication position
func (agent *ActionAgent) InitMaster(ctx context.Context) (myproto.ReplicationPosition, error) {
	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// get the current replication position
	rp, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some slaves to be able to commit
	// transactions.
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// Change our type to master if not already
	if err := topo.UpdateTabletFields(ctx, agent.TopoServer, agent.TabletAlias, func(tablet *topo.Tablet) error {
		tablet.Type = topo.TYPE_MASTER
		tablet.Health = nil
		return nil
	}); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	return rp, nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (agent *ActionAgent) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias topo.TabletAlias, pos myproto.ReplicationPosition) error {
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, masterAlias.String(), pos))

	return agent.MysqlDaemon.ExecuteSuperQueryList(cmds)
}

// InitSlave sets replication master and position, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) InitSlave(ctx context.Context, parent topo.TabletAlias, replicationPosition myproto.ReplicationPosition, timeCreatedNS int64) error {
	ti, err := agent.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	status := &myproto.ReplicationStatus{
		Position:   replicationPosition,
		MasterHost: ti.Hostname,
		MasterPort: ti.Portmap["mysql"],
	}
	cmds, err := agent.MysqlDaemon.StartReplicationCommands(status)
	if err != nil {
		return err
	}

	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return err
	}

	// wait until we get the replicated row, or our context times out
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemoteMaster marks the server read-only, wait until it is done with
// its current transactions, and returns its master position.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) DemoteMaster(ctx context.Context) (myproto.ReplicationPosition, error) {
	// Set the server read-only. Note all active connections are not
	// affected.
	if err := agent.MysqlDaemon.SetReadOnly(true); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// Now stop the query service, to make sure nobody is writing to the
	// database. This will in effect close the connection pools to the
	// database.
	agent.disallowQueries()

	return agent.MysqlDaemon.DemoteMaster()
	// There is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will
	// succeed. It will be less noisy to simply leave the entry
	// until well promote the master.
}

// PromoteSlaveWhenCaughtUp waits for this slave to be caught up on
// replication up to the provided point, and then makes the slave the
// shard master.
func (agent *ActionAgent) PromoteSlaveWhenCaughtUp(ctx context.Context, pos myproto.ReplicationPosition) (myproto.ReplicationPosition, error) {
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// TODO(alainjobart) change the flavor API to take the context directly
	// For now, extract the timeout from the context, or wait forever
	var waitTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		waitTimeout = deadline.Sub(time.Now())
		if waitTimeout <= 0 {
			waitTimeout = time.Millisecond
		}
	}
	if err := agent.MysqlDaemon.WaitMasterPos(pos, waitTimeout); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	rp, err := agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}

	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	return rp, agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
}

// SlaveWasPromoted promotes a slave to master, no questions asked.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) SlaveWasPromoted(ctx context.Context) error {
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}

	return agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
}

// SetMaster sets replication master, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) SetMaster(ctx context.Context, parent topo.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	ti, err := agent.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	// See if we were replicating at all, and should be replicating
	wasReplicating := false
	shouldbeReplicating := false
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err == nil && (rs.SlaveIORunning || rs.SlaveSQLRunning) {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartSlave {
		shouldbeReplicating = true
	}

	// Create the list of commands to set the master
	cmds := []string{}
	if wasReplicating {
		cmds = append(cmds, mysqlctl.SqlStopSlave)
	}
	smc, err := agent.MysqlDaemon.SetMasterCommands(ti.Hostname, ti.Portmap["mysql"])
	if err != nil {
		return err
	}
	cmds = append(cmds, smc...)
	if shouldbeReplicating {
		cmds = append(cmds, mysqlctl.SqlStartSlave)
	}
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(cmds); err != nil {
		return err
	}

	// change our type to spare if we used to be the master
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topo.TYPE_MASTER {
		tablet.Type = topo.TYPE_SPARE
		tablet.Health = nil
		if err := topo.UpdateTablet(ctx, agent.TopoServer, tablet); err != nil {
			return err
		}
	}

	// if needed, wait until we get the replicated row, or our
	// context times out
	if !shouldbeReplicating || timeCreatedNS == 0 {
		return nil
	}
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// SlaveWasRestarted updates the parent record for a tablet.
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}

	// Once this action completes, update authoritative tablet node first.
	if tablet.Type == topo.TYPE_MASTER {
		tablet.Type = topo.TYPE_SPARE
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

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status
func (agent *ActionAgent) StopReplicationAndGetStatus(ctx context.Context) (myproto.ReplicationStatus, error) {
	// get the status before we stop replication
	rs, err := agent.MysqlDaemon.SlaveStatus()
	if err != nil {
		return myproto.ReplicationStatus{}, fmt.Errorf("before status failed: %v", err)
	}
	if !rs.SlaveIORunning && !rs.SlaveSQLRunning {
		// no replication is running, just return what we got
		return rs, nil
	}
	if err := mysqlctl.StopSlave(agent.MysqlDaemon, agent.hookExtraEnv()); err != nil {
		return myproto.ReplicationStatus{}, fmt.Errorf("stop slave failed: %v", err)
	}
	// now patch in the current position
	rs.Position, err = agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return myproto.ReplicationStatus{}, fmt.Errorf("after position failed: %v", err)
	}
	return rs, nil
}

// PromoteSlave makes the current tablet the master
func (agent *ActionAgent) PromoteSlave(ctx context.Context) (myproto.ReplicationPosition, error) {
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}

	rp, err := agent.MysqlDaemon.PromoteSlave(agent.hookExtraEnv())
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}

	// Set the server read-write
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return myproto.ReplicationPosition{}, err
	}

	return rp, agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
}

// updateReplicationGraphForPromotedSlave makes sure the newly promoted slave
// is correctly represented in the replication graph
func (agent *ActionAgent) updateReplicationGraphForPromotedSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	// Update tablet regardless - trend towards consistency.
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

// Backup takes a db backup and sends it to the BackupStorage
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger) error {
	// update our type to TYPE_BACKUP
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topo.TYPE_MASTER {
		return fmt.Errorf("type MASTER cannot take backup, if you really need to do this, restart vttablet in replica mode")
	}
	originalType := tablet.Type
	if err := topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, topo.TYPE_BACKUP, make(map[string]string)); err != nil {
		return err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "backup"); err != nil {
		return fmt.Errorf("failed to update state before backup: %v", err)
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	bucket := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	name := fmt.Sprintf("%v.%v", tablet.Alias, time.Now().UTC().Format("2006-01-02.150405"))
	returnErr := mysqlctl.Backup(ctx, agent.MysqlDaemon, l, bucket, name, concurrency, agent.hookExtraEnv())

	// and change our type back to the appropriate value:
	// - if healthcheck is enabled, go to spare
	// - if not, go back to original type
	if agent.IsRunningHealthCheck() {
		originalType = topo.TYPE_SPARE
	}
	err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, originalType, nil)
	if err != nil {
		// failure in changing the topology type is probably worse,
		// so returning that (we logged the snapshot error anyway)
		if returnErr != nil {
			l.Errorf("mysql backup command returned error: %v", returnErr)
		}
		returnErr = err
	}

	return returnErr
}
