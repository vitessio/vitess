// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

import (
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

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
	Parent topo.TabletAlias
}

// shard action node structures

// ApplySchemaShardArgs is the payload for ApplySchemaShard
type ApplySchemaShardArgs struct {
	MasterTabletAlias topo.TabletAlias
	Change            string
	Simple            bool
}

// SetShardServedTypesArgs is the payload for SetShardServedTypes
type SetShardServedTypesArgs struct {
	Cells      []string
	ServedType pb.TabletType
}

// MigrateServedTypesArgs is the payload for MigrateServedTypes
type MigrateServedTypesArgs struct {
	ServedType pb.TabletType
}

// keyspace action node structures

// ApplySchemaKeyspaceArgs is the payload for ApplySchemaKeyspace
type ApplySchemaKeyspaceArgs struct {
	Change string
	Simple bool
}

// MigrateServedFromArgs is the payload for MigrateServedFrom
type MigrateServedFromArgs struct {
	ServedType pb.TabletType
}

// methods to build the shard action nodes

// ReparentShardArgs is the payload for ReparentShard
type ReparentShardArgs struct {
	Operation        string
	MasterElectAlias topo.TabletAlias
}

// ReparentShard returns an ActionNode
func ReparentShard(operation string, masterElectAlias topo.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: ShardActionReparent,
		Args: &ReparentShardArgs{
			Operation:        operation,
			MasterElectAlias: masterElectAlias,
		},
	}).SetGuid()
}

// ShardExternallyReparented returns an ActionNode
func ShardExternallyReparented(tabletAlias topo.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: ShardActionExternallyReparented,
		Args:   &tabletAlias,
	}).SetGuid()
}

// RebuildShard returns an ActionNode
func RebuildShard() *ActionNode {
	return (&ActionNode{
		Action: ShardActionRebuild,
	}).SetGuid()
}

// CheckShard returns an ActionNode
func CheckShard() *ActionNode {
	return (&ActionNode{
		Action: ShardActionCheck,
	}).SetGuid()
}

// ApplySchemaShard returns an ActionNode
func ApplySchemaShard(masterTabletAlias topo.TabletAlias, change string, simple bool) *ActionNode {
	return (&ActionNode{
		Action: ShardActionApplySchema,
		Args: &ApplySchemaShardArgs{
			MasterTabletAlias: masterTabletAlias,
			Change:            change,
			Simple:            simple,
		},
	}).SetGuid()
}

// SetShardServedTypes returns an ActionNode
func SetShardServedTypes(cells []string, servedType pb.TabletType) *ActionNode {
	return (&ActionNode{
		Action: ShardActionSetServedTypes,
		Args: &SetShardServedTypesArgs{
			Cells:      cells,
			ServedType: servedType,
		},
	}).SetGuid()
}

// MigrateServedTypes returns an ActionNode
func MigrateServedTypes(servedType pb.TabletType) *ActionNode {
	return (&ActionNode{
		Action: ShardActionMigrateServedTypes,
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
func ApplySchemaKeyspace(change string, simple bool) *ActionNode {
	return (&ActionNode{
		Action: KeyspaceActionApplySchema,
		Args: &ApplySchemaKeyspaceArgs{
			Change: change,
			Simple: simple,
		},
	}).SetGuid()
}

// MigrateServedFrom returns an ActionNode
func MigrateServedFrom(servedType pb.TabletType) *ActionNode {
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

//methods to build the serving shard action nodes

// RebuildSrvShard returns an ActionNode
func RebuildSrvShard() *ActionNode {
	return (&ActionNode{
		Action: SrvShardActionRebuild,
	}).SetGuid()
}
