// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Actions modify the state of a tablet, shard or keyspace.
//
// They are currently managed through a series of queues stored in
// topology server, or RPCs. Switching to RPCs only now.

package actionnode

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"time"
)

const (
	// FIXME(msolomon) why is ActionState a type, but Action is not?

	//
	// Tablet actions. This first list is RPC only. In the process
	// of converting them all to RPCs.
	//

	// TabletActionPing checks a tablet is alive
	TabletActionPing = "Ping"

	// TabletActionSleep will sleep for a duration (used for tests)
	TabletActionSleep = "Sleep"

	// TabletActionExecuteHook will execute the provided hook remotely
	TabletActionExecuteHook = "ExecuteHook"

	// TabletActionSetReadOnly makes the mysql instance read-only
	TabletActionSetReadOnly = "SetReadOnly"

	// TabletActionSetReadWrite makes the mysql instance read-write
	TabletActionSetReadWrite = "SetReadWrite"

	// TabletActionChangeType changes the type of the tablet
	TabletActionChangeType = "ChangeType"

	// TabletActionResetReplication tells the tablet it should
	// reset its replication state
	TabletActionResetReplication = "ResetReplication"

	// TabletActionInitMaster tells the tablet it should make itself the new
	// master for the shard it's currently in.
	TabletActionInitMaster = "InitMaster"

	// TabletActionPopulateReparentJournal inserts an entry in the
	// _vt.reparent_journal table
	TabletActionPopulateReparentJournal = "PopulateReparentJournal"

	// TabletActionInitSlave tells the tablet it should make
	// itself a slave to the provided master at the given position.
	TabletActionInitSlave = "InitSlave"

	// TabletActionDemoteMaster tells the current master it's
	// about to not be a master any more, and should go read-only.
	TabletActionDemoteMaster = "DemoteMaster"

	// TabletActionPromoteSlaveWhenCaughtUp tells the tablet to wait
	// for a given replication point, and when it reaches it
	// switch to be a master.
	TabletActionPromoteSlaveWhenCaughtUp = "PromoteSlaveWhenCaughtUp"

	// TabletActionSlaveWasPromoted tells a tablet this previously slave
	// tablet is now the master. The tablet will update its
	// own topology record.
	TabletActionSlaveWasPromoted = "SlaveWasPromoted"

	// TabletActionSetMaster tells a tablet it has a new master.
	// The tablet will reparent to the new master, and wait for
	// the reparent_journal entry.
	TabletActionSetMaster = "SetMaster"

	// TabletActionSlaveWasRestarted tells a tablet the mysql
	// master was changed.  The tablet will check it is indeed the
	// case, and update its own topology record.
	TabletActionSlaveWasRestarted = "SlaveWasRestarted"

	// TabletActionStopReplicationAndGetStatus will stop replication,
	// and return the current replication status.
	TabletActionStopReplicationAndGetStatus = "StopReplicationAndGetStatus"

	// TabletActionPromoteSlave will make this tablet the master
	TabletActionPromoteSlave = "PromoteSlave"

	// TabletActionStopSlave will stop MySQL replication.
	TabletActionStopSlave = "StopSlave"

	// TabletActionStopSlaveMinimum will stop MySQL replication
	// after it reaches a minimum point.
	TabletActionStopSlaveMinimum = "StopSlaveMinimum"

	// TabletActionStartSlave will start MySQL replication.
	TabletActionStartSlave = "StartSlave"

	// TabletActionExternallyReparented is sent directly to the new master
	// tablet when it becomes the master. It is functionnaly equivalent
	// to calling "ShardExternallyReparented" on the topology.
	TabletActionExternallyReparented = "TabletExternallyReparented"

	// TabletActionMasterPosition returns the current master position
	TabletActionMasterPosition = "MasterPosition"

	// TabletActionSlaveStatus returns the current slave status
	TabletActionSlaveStatus = "SlaveStatus"

	// TabletActionWaitBLPPosition waits until the slave reaches a
	// replication position in filtered replication
	TabletActionWaitBLPPosition = "WaitBlpPosition"

	// TabletActionStopBLP stops filtered replication
	TabletActionStopBLP = "StopBlp"

	// TabletActionStartBLP starts filtered replication
	TabletActionStartBLP = "StartBlp"

	// TabletActionRunBLPUntil will run filtered replication until
	// it reaches the provided stop position.
	TabletActionRunBLPUntil = "RunBlpUntil"

	// TabletActionGetSchema returns the tablet current schema.
	TabletActionGetSchema = "GetSchema"

	// TabletActionRefreshState tells the tablet to refresh its
	// tablet record from the topo server.
	TabletActionRefreshState = "RefreshState"

	// TabletActionRunHealthCheck tells the tablet to run a health check.
	TabletActionRunHealthCheck = "RunHealthCheck"

	// TabletActionIgnoreHealthError sets the regexp for health errors to ignore.
	TabletActionIgnoreHealthError = "IgnoreHealthError"

	// TabletActionReloadSchema tells the tablet to reload its schema.
	TabletActionReloadSchema = "ReloadSchema"

	// TabletActionPreflightSchema will check a schema change works
	TabletActionPreflightSchema = "PreflightSchema"

	// TabletActionApplySchema will actually apply the schema change
	TabletActionApplySchema = "ApplySchema"

	// TabletActionExecuteFetchAsDba uses the DBA connection to run queries.
	TabletActionExecuteFetchAsDba = "ExecuteFetchAsDba"

	// TabletActionExecuteFetchAsApp uses the App connection to run queries.
	TabletActionExecuteFetchAsApp = "ExecuteFetchAsApp"

	// TabletActionGetPermissions returns the mysql permissions set
	TabletActionGetPermissions = "GetPermissions"

	// TabletActionGetSlaves returns the current set of mysql
	// replication slaves.
	TabletActionGetSlaves = "GetSlaves"

	// TabletActionBackup takes a db backup and stores it into BackupStorage
	TabletActionBackup = "Backup"

	//
	// Shard actions - involve all tablets in a shard.
	// These are just descriptive and used for locking / logging.
	//

	// ShardActionReparent handles reparenting of the shard
	ShardActionReparent = "ReparentShard"

	// ShardActionExternallyReparented locks the shard when it's
	// been reparented
	ShardActionExternallyReparented = "ShardExternallyReparented"

	// ShardActionRebuild recomputes derived shard-wise data
	ShardActionRebuild = "RebuildShard"

	// ShardActionCheck takes a generic read lock for inexpensive
	// shard-wide actions.
	ShardActionCheck = "CheckShard"

	// ShardActionSetServedTypes changes the ServedTypes inside a shard
	ShardActionSetServedTypes = "SetShardServedTypes"

	// ShardActionMigrateServedTypes migratew served types from
	// one shard to another
	ShardActionMigrateServedTypes = "MigrateServedTypes"

	// ShardActionUpdateShard updates the Shard object (Cells, ...)
	ShardActionUpdateShard = "UpdateShard"

	//
	// Keyspace actions - require very high level locking for consistency.
	// These are just descriptive and used for locking / logging.
	//

	// KeyspaceActionRebuild rebuilds the keyspace serving graph
	KeyspaceActionRebuild = "RebuildKeyspace"

	// KeyspaceActionApplySchema applies a schema change on the keyspace
	KeyspaceActionApplySchema = "ApplySchemaKeyspace"

	// KeyspaceActionSetShardingInfo updates the sharding info
	KeyspaceActionSetShardingInfo = "SetKeyspaceShardingInfo"

	// KeyspaceActionMigrateServedFrom migrates ServedFrom to
	// another keyspace
	KeyspaceActionMigrateServedFrom = "MigrateServedFrom"

	// KeyspaceActionSetServedFrom updates ServedFrom
	KeyspaceActionSetServedFrom = "SetKeyspaceServedFrom"

	// KeyspaceActionCreateShard protects shard creation within the keyspace
	KeyspaceActionCreateShard = "KeyspaceCreateShard"

	//
	// SrvShard actions - very local locking, for consistency.
	// These are just descriptive and used for locking / logging.
	//

	// SrvShardActionRebuild locks the SvrShard for rebuild
	SrvShardActionRebuild = "RebuildSrvShard"

	// all the valid states for an action

	// ActionStateQueued is for an action that is going to be executed
	ActionStateQueued = ActionState("") // All actions are queued initially

	// ActionStateRunning is for an action that is running
	ActionStateRunning = ActionState("Running") // Running inside vtaction process

	// ActionStateFailed is for an action that has failed
	ActionStateFailed = ActionState("Failed") // Ended with a failure

	// ActionStateDone is for an action that is done and successful
	ActionStateDone = ActionState("Done") // Ended with no failure
)

// ActionState is the state an ActionNode
type ActionState string

// ActionNode describes a long-running action on a tablet, or an action
// on a shard or keyspace that locks it.
type ActionNode struct {
	Action     string
	ActionGuid string
	Error      string
	State      ActionState
	Pid        int // only != 0 if State == ActionStateRunning

	// do not serialize the next fields
	// path in topology server representing this action
	Path  string      `json:"-"`
	Args  interface{} `json:"-"`
	Reply interface{} `json:"-"`
}

// ToJSON returns a JSON representation of the object.
func (n *ActionNode) ToJSON() (string, error) {
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return "", fmt.Errorf("cannot JSON-marshal node: %v", err)
	}
	result := string(data) + "\n"
	if n.Args == nil {
		result += "{}\n"
	} else {
		data, err := json.MarshalIndent(n.Args, "", "  ")
		if err != nil {
			return "", fmt.Errorf("cannot JSON-marshal node args: %v", err)
		}
		result += string(data) + "\n"
	}
	if n.Reply == nil {
		result += "{}\n"
	} else {
		data, err := json.MarshalIndent(n.Reply, "", "  ")
		if err != nil {
			return "", fmt.Errorf("cannot JSON-marshal node reply: %v", err)
		}
		result += string(data) + "\n"
	}
	return result, nil
}

// SetGuid will set the ActionGuid field for the action node
// and return the action node.
func (n *ActionNode) SetGuid() *ActionNode {
	now := time.Now().Format(time.RFC3339)
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	hostname := "unknown"
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	n.ActionGuid = fmt.Sprintf("%v-%v-%v", now, username, hostname)
	return n
}
