// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
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

type RestartSlaveData struct {
	ReplicationState *mysqlctl.ReplicationState
	WaitPosition     *mysqlctl.ReplicationPosition
	TimePromoted     int64 // used to verify replication - a row will be inserted with this timestamp
	Parent           topo.TabletAlias
	Force            bool
}

func (rsd *RestartSlaveData) String() string {
	return fmt.Sprintf("RestartSlaveData{ReplicationState:%#v WaitPosition:%#v TimePromoted:%v Parent:%v Force:%v}", rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted, rsd.Parent, rsd.Force)
}

type SlaveWasRestartedArgs struct {
	Parent               topo.TabletAlias
	ExpectedMasterAddr   string
	ExpectedMasterIpAddr string
	ScrapStragglers      bool
}

type SnapshotArgs struct {
	Concurrency int
	ServerMode  bool
}

type SnapshotReply struct {
	ParentAlias  topo.TabletAlias
	ManifestPath string

	// these two are only used for ServerMode=true full snapshot
	SlaveStartRequired bool
	ReadOnly           bool
}

type MultiSnapshotReply struct {
	ParentAlias   topo.TabletAlias
	ManifestPaths []string
}

type SnapshotSourceEndArgs struct {
	SlaveStartRequired bool
	ReadOnly           bool
}

type MultiSnapshotArgs struct {
	KeyRanges        []key.KeyRange
	Tables           []string
	Concurrency      int
	SkipSlaveRestart bool
	MaximumFilesize  uint64
}

type MultiRestoreArgs struct {
	SrcTabletAliases       []topo.TabletAlias
	Concurrency            int
	FetchConcurrency       int
	InsertTableConcurrency int
	FetchRetryCount        int
	Strategy               string
}

type ReserveForRestoreArgs struct {
	SrcTabletAlias topo.TabletAlias
}

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

type ApplySchemaShardArgs struct {
	MasterTabletAlias topo.TabletAlias
	Change            string
	Simple            bool
}

type SetShardServedTypesArgs struct {
	ServedTypes []topo.TabletType
}

type MigrateServedTypesArgs struct {
	ServedType topo.TabletType
}

// keyspace action node structures

type ApplySchemaKeyspaceArgs struct {
	Change string
	Simple bool
}

type MigrateServedFromArgs struct {
	ServedType topo.TabletType
}
