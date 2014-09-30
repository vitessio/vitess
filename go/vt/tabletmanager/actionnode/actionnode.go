// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Actions modify the state of a tablet, shard or keyspace.
//
// They are currenty managed through a series of queues stored in
// topology server, or RPCs. Switching to RPCs only now.

package actionnode

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/youtube/vitess/go/jscfg"
)

const (
	// FIXME(msolomon) why is ActionState a type, but Action is not?

	//
	// Tablet actions. This first list is RPC only. In the process
	// of converting them all to RPCs.
	//

	// Ping checks a tablet is alive
	TABLET_ACTION_PING = "Ping"

	// Sleep will sleep for a duration (used for tests)
	TABLET_ACTION_SLEEP = "Sleep"

	// ExecuteHook will execute the provided hook remotely
	TABLET_ACTION_EXECUTE_HOOK = "ExecuteHook"

	// SetReadOnly makes the mysql instance read-only
	TABLET_ACTION_SET_RDONLY = "SetReadOnly"

	// SetReadWrite makes the mysql instance read-write
	TABLET_ACTION_SET_RDWR = "SetReadWrite"

	// ChangeType changes the type of the tablet
	TABLET_ACTION_CHANGE_TYPE = "ChangeType"

	// Scrap scraps the live running tablet
	TABLET_ACTION_SCRAP = "Scrap"

	// DemoteMaster tells the current master it's about to not be a master
	// any more, and should go read-only.
	TABLET_ACTION_DEMOTE_MASTER = "DemoteMaster"

	// PromoteSlave tells the tablet it is going to be the master.
	TABLET_ACTION_PROMOTE_SLAVE = "PromoteSlave"

	// SlaveWasPromoted tells a tablet this previously slave
	// tablet is now the master. The tablet will update its
	// own topology record.
	TABLET_ACTION_SLAVE_WAS_PROMOTED = "SlaveWasPromoted"

	// RestartSlave tells the remote tablet it has a new master.
	TABLET_ACTION_RESTART_SLAVE = "RestartSlave"

	// SlaveWasRestarted tells a tablet the mysql master was changed.
	// The tablet will check it is indeed the case, and update its own
	// topology record.
	TABLET_ACTION_SLAVE_WAS_RESTARTED = "SlaveWasRestarted"

	// BreakSlaves will tinker with the replication stream in a way
	// that will stop all the slaves.
	TABLET_ACTION_BREAK_SLAVES = "BreakSlaves"

	// StopSlave will stop MySQL replication.
	TABLET_ACTION_STOP_SLAVE = "StopSlave"

	// StopSlave will stop MySQL replication after it reaches a
	// minimum point.
	TABLET_ACTION_STOP_SLAVE_MINIMUM = "StopSlaveMinimum"

	// StartSlave will start MySQL replication.
	TABLET_ACTION_START_SLAVE = "StartSlave"

	// TabletExternallyReparented is sent directly to the new master
	// tablet when it becomes the master. It is functionnaly equivalent
	// to calling "ShardExternallyReparented" on the topology.
	TABLET_ACTION_EXTERNALLY_REPARENTED = "TabletExternallyReparented"

	// MasterPosition returns the current master position
	TABLET_ACTION_MASTER_POSITION = "MasterPosition"

	// ReparentPosition returns the data for a slave to use to reparent
	// to the target tablet at the given position.
	TABLET_ACTION_REPARENT_POSITION = "ReparentPosition"

	// SlaveStatus returns the current slave status
	TABLET_ACTION_SLAVE_STATUS = "SlaveStatus"

	// WaitSlavePosition waits until the slave reaches a
	// replication position in MySQL replication
	TABLET_ACTION_WAIT_SLAVE_POSITION = "WaitSlavePosition"

	// WaitBlpPosition waits until the slave reaches a
	// replication position in filtered replication
	TABLET_ACTION_WAIT_BLP_POSITION = "WaitBlpPosition"

	// Stop and Start filtered replication
	TABLET_ACTION_STOP_BLP  = "StopBlp"
	TABLET_ACTION_START_BLP = "StartBlp"

	// RunBlpUntil will run filtered replication until it reaches
	// the provided stop position.
	TABLET_ACTION_RUN_BLP_UNTIL = "RunBlpUntil"

	// GetSchema returns the tablet current schema.
	TABLET_ACTION_GET_SCHEMA = "GetSchema"

	// ReloadSchema tells the tablet to reload its schema.
	TABLET_ACTION_RELOAD_SCHEMA = "ReloadSchema"

	// PreflightSchema will check a schema change works
	TABLET_ACTION_PREFLIGHT_SCHEMA = "PreflightSchema"

	// ApplySchema will actually apply the schema change
	TABLET_ACTION_APPLY_SCHEMA = "ApplySchema"

	// ExecuteFetch uses the DBA connection pool to run queries.
	TABLET_ACTION_EXECUTE_FETCH = "ExecuteFetch"

	// GetPermissions returns the mysql permissions set
	TABLET_ACTION_GET_PERMISSIONS = "GetPermissions"

	// GetSlaves returns the current set of mysql replication slaves.
	TABLET_ACTION_GET_SLAVES = "GetSlaves"

	// Snapshot takes a db snapshot
	TABLET_ACTION_SNAPSHOT = "Snapshot"

	// SnapshotSourceEnd restarts the mysql server
	TABLET_ACTION_SNAPSHOT_SOURCE_END = "SnapshotSourceEnd"

	// ReserveForRestore will prepare a server for restore
	TABLET_ACTION_RESERVE_FOR_RESTORE = "ReserveForRestore"

	// Restore will restore a backup
	TABLET_ACTION_RESTORE = "Restore"

	// MultiSnapshot takes a split snapshot
	TABLET_ACTION_MULTI_SNAPSHOT = "MultiSnapshot"

	// MultiRestore restores a split snapshot
	TABLET_ACTION_MULTI_RESTORE = "MultiRestore"

	//
	// Shard actions - involve all tablets in a shard.
	// These are just descriptive and used for locking / logging.
	//

	SHARD_ACTION_REPARENT              = "ReparentShard"
	SHARD_ACTION_EXTERNALLY_REPARENTED = "ShardExternallyReparented"
	// Recompute derived shard-wise data
	SHARD_ACTION_REBUILD = "RebuildShard"
	// Generic read lock for inexpensive shard-wide actions.
	SHARD_ACTION_CHECK = "CheckShard"
	// Apply a schema change on an entire shard
	SHARD_ACTION_APPLY_SCHEMA = "ApplySchemaShard"
	// Changes the ServedTypes inside a shard
	SHARD_ACTION_SET_SERVED_TYPES = "SetShardServedTypes"
	// Multi-restore on all tablets of a shard in parallel
	SHARD_ACTION_MULTI_RESTORE = "ShardMultiRestore"
	// Migrate served types from one shard to another
	SHARD_ACTION_MIGRATE_SERVED_TYPES = "MigrateServedTypes"
	// Update the Shard object (Cells, ...)
	SHARD_ACTION_UPDATE_SHARD = "UpdateShard"

	//
	// Keyspace actions - require very high level locking for consistency.
	// These are just descriptive and used for locking / logging.
	//

	KEYSPACE_ACTION_REBUILD             = "RebuildKeyspace"
	KEYSPACE_ACTION_APPLY_SCHEMA        = "ApplySchemaKeyspace"
	KEYSPACE_ACTION_SET_SHARDING_INFO   = "SetKeyspaceShardingInfo"
	KEYSPACE_ACTION_MIGRATE_SERVED_FROM = "MigrateServedFrom"

	//
	// SrvShard actions - very local locking, for consistency.
	// These are just descriptive and used for locking / logging.
	//

	SRV_SHARD_ACTION_REBUILD = "RebuildSrvShard"

	// all the valid states for an action

	ACTION_STATE_QUEUED  = ActionState("")        // All actions are queued initially
	ACTION_STATE_RUNNING = ActionState("Running") // Running inside vtaction process
	ACTION_STATE_FAILED  = ActionState("Failed")  // Ended with a failure
	ACTION_STATE_DONE    = ActionState("Done")    // Ended with no failure
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
	Pid        int // only != 0 if State == ACTION_STATE_RUNNING

	// do not serialize the next fields
	// path in topology server representing this action
	Path  string      `json:"-"`
	Args  interface{} `json:"-"`
	Reply interface{} `json:"-"`
}

// ToJson returns a JSON representation of the object.
func (n *ActionNode) ToJson() string {
	result := jscfg.ToJson(n) + "\n"
	if n.Args == nil {
		result += "{}\n"
	} else {
		result += jscfg.ToJson(n.Args) + "\n"
	}
	if n.Reply == nil {
		result += "{}\n"
	} else {
		result += jscfg.ToJson(n.Reply) + "\n"
	}
	return result
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
