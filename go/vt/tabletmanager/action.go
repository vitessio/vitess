// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Actions modify the state of a tablet, shard or keyspace.
//
// They are currenty managed through a series of queues stored in zookeeper.

package tabletmanager

import (
	"encoding/json"

	"code.google.com/p/vitess/go/jscfg"
)

const (
	// FIXME(msolomon) why is ActionState a type, but Action is not?
	TABLET_ACTION_PING  = "Ping"
	TABLET_ACTION_SLEEP = "Sleep"

	TABLET_ACTION_SET_RDONLY  = "SetReadOnly"
	TABLET_ACTION_SET_RDWR    = "SetReadWrite"
	TABLET_ACTION_CHANGE_TYPE = "ChangeType"

	TABLET_ACTION_DEMOTE_MASTER       = "DemoteMaster"
	TABLET_ACTION_PROMOTE_SLAVE       = "PromoteSlave"
	TABLET_ACTION_RESTART_SLAVE       = "RestartSlave"
	TABLET_ACTION_STOP_SLAVE          = "StopSlave"
	TABLET_ACTION_BREAK_SLAVES        = "BreakSlaves"
	TABLET_ACTION_MASTER_POSITION     = "MasterPosition"
	TABLET_ACTION_REPARENT_POSITION   = "ReparentPosition"
	TABLET_ACTION_SLAVE_POSITION      = "SlavePosition"
	TABLET_ACTION_WAIT_SLAVE_POSITION = "WaitSlavePosition"
	TABLET_ACTION_SCRAP               = "Scrap"
	TABLET_ACTION_GET_SCHEMA          = "GetSchema"
	TABLET_ACTION_PREFLIGHT_SCHEMA    = "PreflightSchema"
	TABLET_ACTION_APPLY_SCHEMA        = "ApplySchema"
	TABLET_ACTION_EXECUTE_HOOK        = "ExecuteHook"

	TABLET_ACTION_SNAPSHOT         = "Snapshot"
	TABLET_ACTION_RESTORE          = "Restore"
	TABLET_ACTION_PARTIAL_SNAPSHOT = "PartialSnapshot"
	TABLET_ACTION_PARTIAL_RESTORE  = "PartialRestore"

	// Shard actions - involve all tablets in a shard
	SHARD_ACTION_REPARENT = "ReparentShard"
	// Recompute derived shard-wise data
	SHARD_ACTION_REBUILD = "RebuildShard"
	// Generic read lock for inexpensive shard-wide actions.
	SHARD_ACTION_CHECK = "CheckShard"
	// Apply a schema change on an entire shard
	SHARD_ACTION_APPLY_SCHEMA = "ApplySchemaShard"

	// Keyspace actions - require very high level locking for consistency
	KEYSPACE_ACTION_REBUILD = "RebuildKeyspace"

	ACTION_STATE_QUEUED  = ActionState("")        // All actions are queued initially
	ACTION_STATE_RUNNING = ActionState("Running") // Running inside vtaction process
	ACTION_STATE_FAILED  = ActionState("Failed")  // Ended with a failure
	ACTION_STATE_DONE    = ActionState("Done")    // Ended with no failure
)

type ActionState string

type ActionNode struct {
	Action     string
	ActionGuid string
	Error      string
	State      ActionState
	Args       map[string]string
	Results    map[string]string
	path       string // path in zookeeper representing this action, don't serialize
}

func ActionNodeFromJson(data, path string) (*ActionNode, error) {
	node := &ActionNode{Args: make(map[string]string)}
	err := json.Unmarshal([]byte(data), node)
	if err != nil {
		return nil, err
	}
	node.path = path
	return node, nil
}

func (n *ActionNode) Path() string {
	return n.path
}

func ActionNodeToJson(n *ActionNode) string {
	return jscfg.ToJson(n)
}
