// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

import topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

/*
This file defines all the payload structures for the Lock objects.

Note it's OK to rename the structures as the type name is not saved in json.
*/

// shard action node structures

// ReparentShardArgs is the payload for ReparentShard
type ReparentShardArgs struct {
	Operation        string
	MasterElectAlias *topodatapb.TabletAlias
}

// ReparentShard returns a Lock
func ReparentShard(operation string, masterElectAlias *topodatapb.TabletAlias) *Lock {
	return NewLock(ShardActionReparent, &ReparentShardArgs{
		Operation:        operation,
		MasterElectAlias: masterElectAlias,
	})
}

// ShardExternallyReparented returns a Lock
func ShardExternallyReparented(tabletAlias *topodatapb.TabletAlias) *Lock {
	return NewLock(ShardActionExternallyReparented, tabletAlias)
}

// CheckShard returns a Lock
func CheckShard() *Lock {
	return NewLock(ShardActionCheck, nil)
}

// keyspace action node structures

// SetShardServedTypesArgs is the payload for SetShardServedTypes
type SetShardServedTypesArgs struct {
	Cells      []string
	ServedType topodatapb.TabletType
}

// SetShardServedTypes returns a Lock
func SetShardServedTypes(cells []string, servedType topodatapb.TabletType) *Lock {
	return NewLock(ShardActionSetServedTypes, &SetShardServedTypesArgs{
		Cells:      cells,
		ServedType: servedType,
	})
}

// MigrateServedTypesArgs is the payload for MigrateServedTypes
type MigrateServedTypesArgs struct {
	ServedType topodatapb.TabletType
}

// MigrateServedTypes returns a Lock
func MigrateServedTypes(servedType topodatapb.TabletType) *Lock {
	return NewLock(KeyspaceActionMigrateServedTypes, &MigrateServedTypesArgs{
		ServedType: servedType,
	})
}

// UpdateShard returns a Lock
func UpdateShard() *Lock {
	return NewLock(ShardActionUpdateShard, nil)
}

// methods to build the keyspace action nodes

// RebuildKeyspace returns a Lock
func RebuildKeyspace() *Lock {
	return NewLock(KeyspaceActionRebuild, nil)
}

// SetKeyspaceShardingInfo returns a Lock
func SetKeyspaceShardingInfo() *Lock {
	return NewLock(KeyspaceActionSetShardingInfo, nil)
}

// SetKeyspaceServedFrom returns a Lock
func SetKeyspaceServedFrom() *Lock {
	return NewLock(KeyspaceActionSetServedFrom, nil)
}

// ApplySchemaKeyspaceArgs is the payload for ApplySchemaKeyspace
type ApplySchemaKeyspaceArgs struct {
	Change string
}

// ApplySchemaKeyspace returns a Lock
func ApplySchemaKeyspace(change string) *Lock {
	return NewLock(KeyspaceActionApplySchema, &ApplySchemaKeyspaceArgs{
		Change: change,
	})
}

// MigrateServedFromArgs is the payload for MigrateServedFrom
type MigrateServedFromArgs struct {
	ServedType topodatapb.TabletType
}

// MigrateServedFrom returns a Lock
func MigrateServedFrom(servedType topodatapb.TabletType) *Lock {
	return NewLock(KeyspaceActionMigrateServedFrom, &MigrateServedFromArgs{
		ServedType: servedType,
	})
}

// KeyspaceCreateShard returns a Lock to use to lock a keyspace
// for shard creation
func KeyspaceCreateShard() *Lock {
	return NewLock(KeyspaceActionCreateShard, nil)
}
