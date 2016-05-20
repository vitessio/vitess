// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

import topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

/*
This file defines all the payload structures for the ActionNode objects.

The naming conventions are:
- a structure used for Args only is suffixed by 'Args'.
- a structure used for Reply only is suffixed by 'Reply'.
- a structure used for both Args and Reply is suffixed by 'Data'.

Note it's OK to rename the structures as the type name is not saved in json.
*/

// tablet action node structures

// SlaveWasRestartedArgs is the paylod for SlaveWasRestarted
type SlaveWasRestartedArgs struct {
	Parent *topodatapb.TabletAlias
}

// shard action node structures

// SetShardServedTypesArgs is the payload for SetShardServedTypes
type SetShardServedTypesArgs struct {
	Cells      []string
	ServedType topodatapb.TabletType
}

// MigrateServedTypesArgs is the payload for MigrateServedTypes
type MigrateServedTypesArgs struct {
	ServedType topodatapb.TabletType
}

// keyspace action node structures

// ApplySchemaKeyspaceArgs is the payload for ApplySchemaKeyspace
type ApplySchemaKeyspaceArgs struct {
	Change string
}

// MigrateServedFromArgs is the payload for MigrateServedFrom
type MigrateServedFromArgs struct {
	ServedType topodatapb.TabletType
}

// methods to build the shard action nodes

// ReparentShardArgs is the payload for ReparentShard
type ReparentShardArgs struct {
	Operation        string
	MasterElectAlias *topodatapb.TabletAlias
}

// ReparentShard returns an ActionNode
func ReparentShard(operation string, masterElectAlias *topodatapb.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: ShardActionReparent,
		Args: &ReparentShardArgs{
			Operation:        operation,
			MasterElectAlias: masterElectAlias,
		},
	}).SetGuid()
}

// ShardExternallyReparented returns an ActionNode
func ShardExternallyReparented(tabletAlias *topodatapb.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: ShardActionExternallyReparented,
		Args:   &tabletAlias,
	}).SetGuid()
}

// CheckShard returns an ActionNode
func CheckShard() *ActionNode {
	return (&ActionNode{
		Action: ShardActionCheck,
	}).SetGuid()
}

// SetShardServedTypes returns an ActionNode
func SetShardServedTypes(cells []string, servedType topodatapb.TabletType) *ActionNode {
	return (&ActionNode{
		Action: ShardActionSetServedTypes,
		Args: &SetShardServedTypesArgs{
			Cells:      cells,
			ServedType: servedType,
		},
	}).SetGuid()
}

// MigrateServedTypes returns an ActionNode
func MigrateServedTypes(servedType topodatapb.TabletType) *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionMigrateServedTypes,
		Args: &MigrateServedTypesArgs{
			ServedType: servedType,
		},
	}).SetGuid()
}

// UpdateShard returns an ActionNode
func UpdateShard() *ActionNode {
	return (&ActionNode{
		Action: ShardActionUpdateShard,
	}).SetGuid()
}

// methods to build the keyspace action nodes

// RebuildKeyspace returns an ActionNode
func RebuildKeyspace() *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionRebuild,
	}).SetGuid()
}

// SetKeyspaceShardingInfo returns an ActionNode
func SetKeyspaceShardingInfo() *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionSetShardingInfo,
	}).SetGuid()
}

// SetKeyspaceServedFrom returns an ActionNode
func SetKeyspaceServedFrom() *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionSetServedFrom,
	}).SetGuid()
}

// ApplySchemaKeyspace returns an ActionNode
func ApplySchemaKeyspace(change string) *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionApplySchema,
		Args: &ApplySchemaKeyspaceArgs{
			Change: change,
		},
	}).SetGuid()
}

// MigrateServedFrom returns an ActionNode
func MigrateServedFrom(servedType topodatapb.TabletType) *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionMigrateServedFrom,
		Args: &MigrateServedFromArgs{
			ServedType: servedType,
		},
	}).SetGuid()
}

// KeyspaceCreateShard returns an ActionNode to use to lock a keyspace
// for shard creation
func KeyspaceCreateShard() *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionCreateShard,
	}).SetGuid()
}
