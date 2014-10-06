// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package initiator

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

var tabletManagerProtocol = flag.String("tablet_manager_protocol", "bson", "the protocol to use to talk to vttablet")

// ErrFunc is used by streaming RPCs that don't return a specific result
type ErrFunc func() error

// SnapshotReplyFunc is used by Snapshot to return result and error
type SnapshotReplyFunc func() (*actionnode.SnapshotReply, error)

// MultiSnapshotReplyFunc is used by MultiSnapshot to return result and error
type MultiSnapshotReplyFunc func() (*actionnode.MultiSnapshotReply, error)

// TabletManagerConn defines the interface used to talk to a remote tablet
type TabletManagerConn interface {
	//
	// Various read-only methods
	//

	// Ping will try to ping the remote tablet
	Ping(tablet *topo.TabletInfo, waitTime time.Duration) error

	// GetSchema asks the remote tablet for its database schema
	GetSchema(tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error)

	// GetPermissions asks the remote tablet for its permissions list
	GetPermissions(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.Permissions, error)

	//
	// Various read-write methods
	//

	// SetReadOnly makes the mysql instance read-only
	SetReadOnly(tablet *topo.TabletInfo, waitTime time.Duration) error

	// SetReadWrite makes the mysql instance read-write
	SetReadWrite(tablet *topo.TabletInfo, waitTime time.Duration) error

	// ChangeType asks the remote tablet to change its type
	ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error

	// Scrap scraps the live running tablet
	Scrap(tablet *topo.TabletInfo, waitTime time.Duration) error

	// Sleep will sleep for a duration (used for tests)
	Sleep(tablet *topo.TabletInfo, duration, waitTime time.Duration) error

	// ExecuteHook executes the provided hook remotely
	ExecuteHook(tablet *topo.TabletInfo, hk *hook.Hook, waitTime time.Duration) (*hook.HookResult, error)

	// RefreshState asks the remote tablet to reload its tablet record
	RefreshState(tablet *topo.TabletInfo, waitTime time.Duration) error

	// ReloadSchema asks the remote tablet to reload its schema
	ReloadSchema(tablet *topo.TabletInfo, waitTime time.Duration) error

	// PreflightSchema will test a schema change
	PreflightSchema(tablet *topo.TabletInfo, change string, waitTime time.Duration) (*myproto.SchemaChangeResult, error)

	// ApplySchema will apply a schema change
	ApplySchema(tablet *topo.TabletInfo, change *myproto.SchemaChange, waitTime time.Duration) (*myproto.SchemaChangeResult, error)

	// ExecuteFetch executes a query remotely using the DBA pool
	ExecuteFetch(tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool, waitTime time.Duration) (*mproto.QueryResult, error)

	//
	// Replication related methods
	//

	// SlaveStatus returns the tablet's mysql slave status.
	SlaveStatus(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// WaitSlavePosition asks the tablet to wait until it reaches that
	// position in mysql replication
	WaitSlavePosition(tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// MasterPosition returns the tablet's master position
	MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (myproto.ReplicationPosition, error)

	// ReparentPosition returns the data for a slave to use to reparent
	// to the target tablet at the given position.
	ReparentPosition(tablet *topo.TabletInfo, rp *myproto.ReplicationPosition, waitTime time.Duration) (*actionnode.RestartSlaveData, error)

	// StopSlave stops the mysql replication
	StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error

	// StopSlaveMinimum stops the mysql replication after it reaches
	// the provided minimum point
	StopSlaveMinimum(tablet *topo.TabletInfo, stopPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// StartSlave starts the mysql replication
	StartSlave(tablet *topo.TabletInfo, waitTime time.Duration) error

	// TabletExternallyReparented tells a tablet it is now the master
	TabletExternallyReparented(tablet *topo.TabletInfo, waitTime time.Duration) error

	// GetSlaves returns the addresses of the slaves
	GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) ([]string, error)

	// WaitBlpPosition asks the tablet to wait until it reaches that
	// position in replication
	WaitBlpPosition(tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error

	// StopBlp asks the tablet to stop all its binlog players,
	// and returns the current position for all of them
	StopBlp(tablet *topo.TabletInfo, waitTime time.Duration) (*blproto.BlpPositionList, error)

	// StartBlp asks the tablet to restart its binlog players
	StartBlp(tablet *topo.TabletInfo, waitTime time.Duration) error

	// RunBlpUntil asks the tablet to restart its binlog players until
	// it reaches the given positions, if not there yet.
	RunBlpUntil(tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error)

	//
	// Reparenting related functions
	//

	// DemoteMaster tells the soon-to-be-former master it's gonna change
	DemoteMaster(tablet *topo.TabletInfo, waitTime time.Duration) error

	// PromoteSlave transforms the tablet from a slave to a master.
	PromoteSlave(tablet *topo.TabletInfo, waitTime time.Duration) (*actionnode.RestartSlaveData, error)

	// SlaveWasPromoted tells the remote tablet it is now the master
	SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error

	// RestartSlave tells the remote tablet it has a new master
	RestartSlave(tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData, waitTime time.Duration) error

	// SlaveWasRestarted tells the remote tablet its master has changed
	SlaveWasRestarted(tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error

	// BreakSlaves will tinker with the replication stream in a
	// way that will stop all the slaves.
	BreakSlaves(tablet *topo.TabletInfo, waitTime time.Duration) error

	//
	// Backup / restore related methods
	//

	// Snapshot takes a database snapshot
	Snapshot(tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, SnapshotReplyFunc, error)

	// SnapshotSourceEnd restarts the mysql server
	SnapshotSourceEnd(tablet *topo.TabletInfo, ssea *actionnode.SnapshotSourceEndArgs, waitTime time.Duration) error

	// ReserveForRestore will prepare a server for restore
	ReserveForRestore(tablet *topo.TabletInfo, rfra *actionnode.ReserveForRestoreArgs, waitTime time.Duration) error

	// Restore restores a database snapshot
	Restore(tablet *topo.TabletInfo, sa *actionnode.RestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc, error)

	// MultiSnapshot takes a database snapshot
	MultiSnapshot(tablet *topo.TabletInfo, sa *actionnode.MultiSnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, MultiSnapshotReplyFunc, error)

	// MultiRestore restores a database snapshot
	MultiRestore(tablet *topo.TabletInfo, sa *actionnode.MultiRestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc, error)
}

type TabletManagerConnFactory func() TabletManagerConn

var tabletManagerConnFactories = make(map[string]TabletManagerConnFactory)

func RegisterTabletManagerConnFactory(name string, factory TabletManagerConnFactory) {
	if _, ok := tabletManagerConnFactories[name]; ok {
		log.Fatalf("RegisterTabletManagerConn %s already exists", name)
	}
	tabletManagerConnFactories[name] = factory
}

func NewTabletManagerConn() TabletManagerConn {
	f, ok := tabletManagerConnFactories[*tabletManagerProtocol]
	if !ok {
		log.Fatalf("No TabletManagerProtocol registered with name %s", *tabletManagerProtocol)
	}

	return &ActionInitiator{f()}
}
