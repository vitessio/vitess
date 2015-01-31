// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

import (
	"fmt"
	"time"

	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
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

// HealthStreamReply is the structure we stream from HealthStream
type HealthStreamReply struct {
	// Tablet is the current tablet record, as cached by tabletmanager
	Tablet *topo.Tablet

	// BinlogPlayerMapSize is the size of the binlog player map.
	// If non zero, the ReplicationDelay is the binlog players' maximum
	// replication delay.
	BinlogPlayerMapSize int64

	// HealthError is the last error we got from health check,
	// or empty is the server is healthy.
	HealthError string

	// ReplicationDelay is either from MySQL replication, or from
	// filtered replication
	ReplicationDelay time.Duration

	// TODO(alainjobart) add some QPS reporting data here
}

// RestartSlaveData is returned by the master, and used to promote or
// restart slaves
type RestartSlaveData struct {
	ReplicationStatus *myproto.ReplicationStatus
	WaitPosition      myproto.ReplicationPosition
	TimePromoted      int64 // used to verify replication - a row will be inserted with this timestamp
	Parent            topo.TabletAlias
	Force             bool
}

func (rsd *RestartSlaveData) String() string {
	return fmt.Sprintf("RestartSlaveData{ReplicationStatus:%#v WaitPosition:%#v TimePromoted:%v Parent:%v Force:%v}", rsd.ReplicationStatus, rsd.WaitPosition, rsd.TimePromoted, rsd.Parent, rsd.Force)
}

// SlaveWasRestartedArgs is the paylod for SlaveWasRestarted
type SlaveWasRestartedArgs struct {
	Parent topo.TabletAlias
}

// SnapshotArgs is the paylod for Snapshot
type SnapshotArgs struct {
	Concurrency         int
	ServerMode          bool
	ForceMasterSnapshot bool
}

// SnapshotReply is the response for Snapshot
type SnapshotReply struct {
	ParentAlias  topo.TabletAlias
	ManifestPath string

	// these two are only used for ServerMode=true full snapshot
	SlaveStartRequired bool
	ReadOnly           bool
}

// SnapshotSourceEndArgs is the payload for SnapshotSourceEnd
type SnapshotSourceEndArgs struct {
	SlaveStartRequired bool
	ReadOnly           bool
	OriginalType       topo.TabletType
}

// ReserveForRestoreArgs is the payload for ReserveForRestore
type ReserveForRestoreArgs struct {
	SrcTabletAlias topo.TabletAlias
}

// RestoreArgs is the payload for Restore
type RestoreArgs struct {
	SrcTabletAlias        topo.TabletAlias
	SrcFilePath           string
	ParentAlias           topo.TabletAlias
	FetchConcurrency      int
	FetchRetryCount       int
	WasReserved           bool
	DontWaitForSlaveStart bool
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
	ServedType topo.TabletType
}

// MigrateServedTypesArgs is the payload for MigrateServedTypes
type MigrateServedTypesArgs struct {
	ServedType topo.TabletType
}

// keyspace action node structures

// ApplySchemaKeyspaceArgs is the payload for ApplySchemaKeyspace
type ApplySchemaKeyspaceArgs struct {
	Change string
	Simple bool
}

// MigrateServedFromArgs is the payload for MigrateServedFrom
type MigrateServedFromArgs struct {
	ServedType topo.TabletType
}

// methods to build the shard action nodes

// ReparentShard returns an ActionNode
func ReparentShard(tabletAlias topo.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_REPARENT,
		Args:   &tabletAlias,
	}).SetGuid()
}

// ShardExternallyReparented returns an ActionNode
func ShardExternallyReparented(tabletAlias topo.TabletAlias) *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_EXTERNALLY_REPARENTED,
		Args:   &tabletAlias,
	}).SetGuid()
}

// RebuildShard returns an ActionNode
func RebuildShard() *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_REBUILD,
	}).SetGuid()
}

// CheckShard returns an ActionNode
func CheckShard() *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_CHECK,
	}).SetGuid()
}

// ApplySchemaShard returns an ActionNode
func ApplySchemaShard(masterTabletAlias topo.TabletAlias, change string, simple bool) *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_APPLY_SCHEMA,
		Args: &ApplySchemaShardArgs{
			MasterTabletAlias: masterTabletAlias,
			Change:            change,
			Simple:            simple,
		},
	}).SetGuid()
}

// SetShardServedTypes returns an ActionNode
func SetShardServedTypes(cells []string, servedType topo.TabletType) *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_SET_SERVED_TYPES,
		Args: &SetShardServedTypesArgs{
			Cells:      cells,
			ServedType: servedType,
		},
	}).SetGuid()
}

// MigrateServedTypes returns an ActionNode
func MigrateServedTypes(servedType topo.TabletType) *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_MIGRATE_SERVED_TYPES,
		Args: &MigrateServedTypesArgs{
			ServedType: servedType,
		},
	}).SetGuid()
}

// UpdateShard returns an ActionNode
func UpdateShard() *ActionNode {
	return (&ActionNode{
		Action: SHARD_ACTION_UPDATE_SHARD,
	}).SetGuid()
}

// methods to build the keyspace action nodes

// RebuildKeyspace returns an ActionNode
func RebuildKeyspace() *ActionNode {
	return (&ActionNode{
		Action: KEYSPACE_ACTION_REBUILD,
	}).SetGuid()
}

// SetKeyspaceShardingInfo returns an ActionNode
func SetKeyspaceShardingInfo() *ActionNode {
	return (&ActionNode{
		Action: KEYSPACE_ACTION_SET_SHARDING_INFO,
	}).SetGuid()
}

// SetKeyspaceServedFrom returns an ActionNode
func SetKeyspaceServedFrom() *ActionNode {
	return (&ActionNode{
		Action: KEYSPACE_ACTION_SET_SERVED_FROM,
	}).SetGuid()
}

// ApplySchemaKeyspace returns an ActionNode
func ApplySchemaKeyspace(change string, simple bool) *ActionNode {
	return (&ActionNode{
		Action: KEYSPACE_ACTION_APPLY_SCHEMA,
		Args: &ApplySchemaKeyspaceArgs{
			Change: change,
			Simple: simple,
		},
	}).SetGuid()
}

// MigrateServedFrom returns an ActionNode
func MigrateServedFrom(servedType topo.TabletType) *ActionNode {
	return (&ActionNode{
		Action: KEYSPACE_ACTION_MIGRATE_SERVED_FROM,
		Args: &MigrateServedFromArgs{
			ServedType: servedType,
		},
	}).SetGuid()
}

//methods to build the serving shard action nodes

// RebuildSrvShard returns an ActionNode
func RebuildSrvShard() *ActionNode {
	return (&ActionNode{
		Action: SRV_SHARD_ACTION_REBUILD,
	}).SetGuid()
}
