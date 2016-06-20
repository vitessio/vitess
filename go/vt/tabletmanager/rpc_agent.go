// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"time"

	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TabletAction is the name of an action. It is a string (and not an
// enum) so we can print it easily.
type TabletAction string

const (
	// TabletActionPing checks a tablet is alive
	TabletActionPing TabletAction = "Ping"

	// TabletActionSleep will sleep for a duration (used for tests)
	TabletActionSleep TabletAction = "Sleep"

	// TabletActionExecuteHook will execute the provided hook remotely
	TabletActionExecuteHook TabletAction = "ExecuteHook"

	// TabletActionSetReadOnly makes the mysql instance read-only
	TabletActionSetReadOnly TabletAction = "SetReadOnly"

	// TabletActionSetReadWrite makes the mysql instance read-write
	TabletActionSetReadWrite TabletAction = "SetReadWrite"

	// TabletActionChangeType changes the type of the tablet
	TabletActionChangeType TabletAction = "ChangeType"

	// TabletActionResetReplication tells the tablet it should
	// reset its replication state
	TabletActionResetReplication TabletAction = "ResetReplication"

	// TabletActionInitMaster tells the tablet it should make itself the new
	// master for the shard it's currently in.
	TabletActionInitMaster TabletAction = "InitMaster"

	// TabletActionPopulateReparentJournal inserts an entry in the
	// _vt.reparent_journal table
	TabletActionPopulateReparentJournal TabletAction = "PopulateReparentJournal"

	// TabletActionInitSlave tells the tablet it should make
	// itself a slave to the provided master at the given position.
	TabletActionInitSlave TabletAction = "InitSlave"

	// TabletActionDemoteMaster tells the current master it's
	// about to not be a master any more, and should go read-only.
	TabletActionDemoteMaster TabletAction = "DemoteMaster"

	// TabletActionPromoteSlaveWhenCaughtUp tells the tablet to wait
	// for a given replication point, and when it reaches it
	// switch to be a master.
	TabletActionPromoteSlaveWhenCaughtUp TabletAction = "PromoteSlaveWhenCaughtUp"

	// TabletActionSlaveWasPromoted tells a tablet this previously slave
	// tablet is now the master. The tablet will update its
	// own topology record.
	TabletActionSlaveWasPromoted TabletAction = "SlaveWasPromoted"

	// TabletActionSetMaster tells a tablet it has a new master.
	// The tablet will reparent to the new master, and wait for
	// the reparent_journal entry.
	TabletActionSetMaster TabletAction = "SetMaster"

	// TabletActionSlaveWasRestarted tells a tablet the mysql
	// master was changed.  The tablet will check it is indeed the
	// case, and update its own topology record.
	TabletActionSlaveWasRestarted TabletAction = "SlaveWasRestarted"

	// TabletActionStopReplicationAndGetStatus will stop replication,
	// and return the current replication status.
	TabletActionStopReplicationAndGetStatus TabletAction = "StopReplicationAndGetStatus"

	// TabletActionPromoteSlave will make this tablet the master
	TabletActionPromoteSlave TabletAction = "PromoteSlave"

	// TabletActionStopSlave will stop MySQL replication.
	TabletActionStopSlave TabletAction = "StopSlave"

	// TabletActionStopSlaveMinimum will stop MySQL replication
	// after it reaches a minimum point.
	TabletActionStopSlaveMinimum TabletAction = "StopSlaveMinimum"

	// TabletActionStartSlave will start MySQL replication.
	TabletActionStartSlave TabletAction = "StartSlave"

	// TabletActionExternallyReparented is sent directly to the new master
	// tablet when it becomes the master. It is functionnaly equivalent
	// to calling "ShardExternallyReparented" on the topology.
	TabletActionExternallyReparented TabletAction = "TabletExternallyReparented"

	// TabletActionMasterPosition returns the current master position
	TabletActionMasterPosition TabletAction = "MasterPosition"

	// TabletActionSlaveStatus returns the current slave status
	TabletActionSlaveStatus TabletAction = "SlaveStatus"

	// TabletActionWaitBLPPosition waits until the slave reaches a
	// replication position in filtered replication
	TabletActionWaitBLPPosition TabletAction = "WaitBlpPosition"

	// TabletActionStopBLP stops filtered replication
	TabletActionStopBLP TabletAction = "StopBlp"

	// TabletActionStartBLP starts filtered replication
	TabletActionStartBLP TabletAction = "StartBlp"

	// TabletActionRunBLPUntil will run filtered replication until
	// it reaches the provided stop position.
	TabletActionRunBLPUntil TabletAction = "RunBlpUntil"

	// TabletActionGetSchema returns the tablet current schema.
	TabletActionGetSchema TabletAction = "GetSchema"

	// TabletActionRefreshState tells the tablet to refresh its
	// tablet record from the topo server.
	TabletActionRefreshState TabletAction = "RefreshState"

	// TabletActionRunHealthCheck tells the tablet to run a health check.
	TabletActionRunHealthCheck TabletAction = "RunHealthCheck"

	// TabletActionIgnoreHealthError sets the regexp for health errors to ignore.
	TabletActionIgnoreHealthError TabletAction = "IgnoreHealthError"

	// TabletActionReloadSchema tells the tablet to reload its schema.
	TabletActionReloadSchema TabletAction = "ReloadSchema"

	// TabletActionPreflightSchema will check a schema change works
	TabletActionPreflightSchema TabletAction = "PreflightSchema"

	// TabletActionApplySchema will actually apply the schema change
	TabletActionApplySchema TabletAction = "ApplySchema"

	// TabletActionExecuteFetchAsDba uses the DBA connection to run queries.
	TabletActionExecuteFetchAsDba TabletAction = "ExecuteFetchAsDba"

	// TabletActionExecuteFetchAsApp uses the App connection to run queries.
	TabletActionExecuteFetchAsApp TabletAction = "ExecuteFetchAsApp"

	// TabletActionGetPermissions returns the mysql permissions set
	TabletActionGetPermissions TabletAction = "GetPermissions"

	// TabletActionGetSlaves returns the current set of mysql
	// replication slaves.
	TabletActionGetSlaves TabletAction = "GetSlaves"

	// TabletActionBackup takes a db backup and stores it into BackupStorage
	TabletActionBackup TabletAction = "Backup"
)

// RPCAgent defines the interface implemented by the Agent for RPCs.
// It is useful for RPC implementations to test their full stack.
type RPCAgent interface {
	// RPC calls

	// Various read-only methods

	Ping(ctx context.Context, args string) string

	GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error)

	GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error)

	// Various read-write methods

	SetReadOnly(ctx context.Context, rdonly bool) error

	ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error

	Sleep(ctx context.Context, duration time.Duration)

	ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult

	RefreshState(ctx context.Context)

	RunHealthCheck(ctx context.Context)

	IgnoreHealthError(ctx context.Context, pattern string) error

	ReloadSchema(ctx context.Context, waitPosition string) error

	PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)

	ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	ExecuteFetchAsDba(ctx context.Context, query string, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error)

	ExecuteFetchAsApp(ctx context.Context, query string, maxrows int) (*querypb.QueryResult, error)

	// Replication related methods

	SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error)

	MasterPosition(ctx context.Context) (string, error)

	StopSlave(ctx context.Context) error

	StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error)

	StartSlave(ctx context.Context) error

	TabletExternallyReparented(ctx context.Context, externalID string) error

	GetSlaves(ctx context.Context) ([]string, error)

	WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error

	StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error)

	StartBlp(ctx context.Context) error

	RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error)

	// Reparenting related functions

	ResetReplication(ctx context.Context) error

	InitMaster(ctx context.Context) (string, error)

	PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error

	InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error

	DemoteMaster(ctx context.Context) (string, error)

	PromoteSlaveWhenCaughtUp(ctx context.Context, replicationPosition string) (string, error)

	SlaveWasPromoted(ctx context.Context) error

	SetMaster(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error

	SlaveWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error

	StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error)

	PromoteSlave(ctx context.Context) (string, error)

	// Backup / restore related methods

	Backup(ctx context.Context, concurrency int, logger logutil.Logger) error

	// RPC helpers
	RPCWrap(ctx context.Context, name TabletAction, args, reply interface{}, f func() error) error
	RPCWrapLock(ctx context.Context, name TabletAction, args, reply interface{}, verbose bool, f func() error) error
	RPCWrapLockAction(ctx context.Context, name TabletAction, args, reply interface{}, verbose bool, f func() error) error
}
