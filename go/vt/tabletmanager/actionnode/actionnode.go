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
	//
	// Shard actions - involve all tablets in a shard.
	// These are just descriptive and used for locking / logging.
	//

	// ShardActionReparent handles reparenting of the shard
	ShardActionReparent = "ReparentShard"

	// ShardActionExternallyReparented locks the shard when it's
	// been reparented
	ShardActionExternallyReparented = "ShardExternallyReparented"

	// ShardActionCheck takes a generic read lock for inexpensive
	// shard-wide actions.
	ShardActionCheck = "CheckShard"

	// ShardActionSetServedTypes changes the ServedTypes inside a shard
	ShardActionSetServedTypes = "SetShardServedTypes"

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

	// KeyspaceActionMigrateServedTypes migrates ServedType from
	// one shard to another in a keyspace
	KeyspaceActionMigrateServedTypes = "MigrateServedTypes"

	// KeyspaceActionMigrateServedFrom migrates ServedFrom to
	// another keyspace
	KeyspaceActionMigrateServedFrom = "MigrateServedFrom"

	// KeyspaceActionSetServedFrom updates ServedFrom
	KeyspaceActionSetServedFrom = "SetKeyspaceServedFrom"

	// KeyspaceActionCreateShard protects shard creation within the keyspace
	KeyspaceActionCreateShard = "KeyspaceCreateShard"
)

// ActionNode describes a long-running action on a tablet, or an action
// on a shard or keyspace that locks it.
// Note it cannot be JSON-deserialized, because of the interface{} variable
type ActionNode struct {
	// Action and the following fields are set at construction time
	Action   string
	Args     interface{}
	HostName string
	UserName string
	Time     string

	// Status is the current status of the action.
	Status string
}

// ToJSON returns a JSON representation of the object.
func (n *ActionNode) ToJSON() (string, error) {
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return "", fmt.Errorf("cannot JSON-marshal node: %v", err)
	}
	return string(data), nil
}

// Init will set the action identifier fields for the action node
// and return the action node.
func (n *ActionNode) Init() *ActionNode {
	n.Status = "Running"
	n.Time = time.Now().Format(time.RFC3339)
	n.UserName = "unknown"
	if u, err := user.Current(); err == nil {
		n.UserName = u.Username
	}
	n.HostName = "unknown"
	if h, err := os.Hostname(); err == nil {
		n.HostName = h
	}
	return n
}
