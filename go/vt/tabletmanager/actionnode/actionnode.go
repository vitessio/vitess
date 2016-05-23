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

// Action is the type for all Lock objects
type Action string

const (
	//
	// Shard actions - involve all tablets in a shard.
	// These are just descriptive and used for locking / logging.
	//

	// ShardActionReparent handles reparenting of the shard
	ShardActionReparent = Action("ReparentShard")

	// ShardActionExternallyReparented locks the shard when it's
	// been reparented
	ShardActionExternallyReparented = Action("ShardExternallyReparented")

	// ShardActionCheck takes a generic read lock for inexpensive
	// shard-wide actions.
	ShardActionCheck = Action("CheckShard")

	// ShardActionSetServedTypes changes the ServedTypes inside a shard
	ShardActionSetServedTypes = Action("SetShardServedTypes")

	// ShardActionUpdateShard updates the Shard object (Cells, ...)
	ShardActionUpdateShard = Action("UpdateShard")

	//
	// Keyspace actions - require very high level locking for consistency.
	// These are just descriptive and used for locking / logging.
	//

	// KeyspaceActionRebuild rebuilds the keyspace serving graph
	KeyspaceActionRebuild = Action("RebuildKeyspace")

	// KeyspaceActionApplySchema applies a schema change on the keyspace
	KeyspaceActionApplySchema = Action("ApplySchemaKeyspace")

	// KeyspaceActionSetShardingInfo updates the sharding info
	KeyspaceActionSetShardingInfo = Action("SetKeyspaceShardingInfo")

	// KeyspaceActionMigrateServedTypes migrates ServedType from
	// one shard to another in a keyspace
	KeyspaceActionMigrateServedTypes = Action("MigrateServedTypes")

	// KeyspaceActionMigrateServedFrom migrates ServedFrom to
	// another keyspace
	KeyspaceActionMigrateServedFrom = Action("MigrateServedFrom")

	// KeyspaceActionSetServedFrom updates ServedFrom
	KeyspaceActionSetServedFrom = Action("SetKeyspaceServedFrom")

	// KeyspaceActionCreateShard protects shard creation within the keyspace
	KeyspaceActionCreateShard = Action("KeyspaceCreateShard")
)

// Lock describes a long-running lock on a keyspace or a shard.
// Note it cannot be JSON-deserialized, because of the interface{} variable,
// but we only serialize it for debugging / logging purposes.
type Lock struct {
	// Action and the following fields are set at construction time
	Action   Action
	Args     interface{}
	HostName string
	UserName string
	Time     string

	// Status is the current status of the Lock.
	Status string
}

// NewLock creates a new Lock.
func NewLock(action Action, args interface{}) *Lock {
	l := &Lock{
		Action:   action,
		Args:     args,
		HostName: "unknown",
		UserName: "unknown",
		Time:     time.Now().Format(time.RFC3339),
		Status:   "Running",
	}
	if h, err := os.Hostname(); err == nil {
		l.HostName = h
	}
	if u, err := user.Current(); err == nil {
		l.UserName = u.Username
	}
	return l
}

// ToJSON returns a JSON representation of the object.
func (l *Lock) ToJSON() (string, error) {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return "", fmt.Errorf("cannot JSON-marshal node: %v", err)
	}
	return string(data), nil
}
