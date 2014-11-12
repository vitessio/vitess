// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tmclient

import (
	"flag"
	"time"

	"code.google.com/p/go.net/context"
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

// TabletManagerClient defines the interface used to talk to a remote tablet
type TabletManagerClient interface {
	//
	// Various read-only methods
	//

	// Ping will try to ping the remote tablet
	Ping(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// GetSchema asks the remote tablet for its database schema
	GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error)

	// GetPermissions asks the remote tablet for its permissions list
	GetPermissions(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.Permissions, error)

	//
	// Various read-write methods
	//

	// SetReadOnly makes the mysql instance read-only
	SetReadOnly(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// SetReadWrite makes the mysql instance read-write
	SetReadWrite(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// ChangeType asks the remote tablet to change its type
	ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error

	// Scrap scraps the live running tablet
	Scrap(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// Sleep will sleep for a duration (used for tests)
	Sleep(ctx context.Context, tablet *topo.TabletInfo, duration, waitTime time.Duration) error

	// ExecuteHook executes the provided hook remotely
	ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook, waitTime time.Duration) (*hook.HookResult, error)

	// RefreshState asks the remote tablet to reload its tablet record
	RefreshState(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// RunHealthCheck asks the remote tablet to run a health check cycle
	RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topo.TabletType, waitTime time.Duration) error

	// ReloadSchema asks the remote tablet to reload its schema
	ReloadSchema(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// PreflightSchema will test a schema change
	PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string, waitTime time.Duration) (*myproto.SchemaChangeResult, error)

	// ApplySchema will apply a schema change
	ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *myproto.SchemaChange, waitTime time.Duration) (*myproto.SchemaChangeResult, error)

	// ExecuteFetch executes a query remotely using the DBA pool
	ExecuteFetch(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool, waitTime time.Duration) (*mproto.QueryResult, error)

	//
	// Replication related methods
	//

	// SlaveStatus returns the tablet's mysql slave status.
	SlaveStatus(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// WaitSlavePosition asks the tablet to wait until it reaches that
	// position in mysql replication
	WaitSlavePosition(ctx context.Context, tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// MasterPosition returns the tablet's master position
	MasterPosition(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) (myproto.ReplicationPosition, error)

	// ReparentPosition returns the data for a slave to use to reparent
	// to the target tablet at the given position.
	ReparentPosition(ctx context.Context, tablet *topo.TabletInfo, rp *myproto.ReplicationPosition, waitTime time.Duration) (*actionnode.RestartSlaveData, error)

	// StopSlave stops the mysql replication
	StopSlave(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// StopSlaveMinimum stops the mysql replication after it reaches
	// the provided minimum point
	StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, stopPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error)

	// StartSlave starts the mysql replication
	StartSlave(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// TabletExternallyReparented tells a tablet it is now the master
	TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// GetSlaves returns the addresses of the slaves
	GetSlaves(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) ([]string, error)

	// WaitBlpPosition asks the tablet to wait until it reaches that
	// position in replication
	WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error

	// StopBlp asks the tablet to stop all its binlog players,
	// and returns the current position for all of them
	StopBlp(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) (*blproto.BlpPositionList, error)

	// StartBlp asks the tablet to restart its binlog players
	StartBlp(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// RunBlpUntil asks the tablet to restart its binlog players until
	// it reaches the given positions, if not there yet.
	RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error)

	//
	// Reparenting related functions
	//

	// DemoteMaster tells the soon-to-be-former master it's gonna change
	DemoteMaster(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// PromoteSlave transforms the tablet from a slave to a master.
	PromoteSlave(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) (*actionnode.RestartSlaveData, error)

	// SlaveWasPromoted tells the remote tablet it is now the master
	SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	// RestartSlave tells the remote tablet it has a new master
	RestartSlave(ctx context.Context, tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData, waitTime time.Duration) error

	// SlaveWasRestarted tells the remote tablet its master has changed
	SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error

	// BreakSlaves will tinker with the replication stream in a
	// way that will stop all the slaves.
	BreakSlaves(ctx context.Context, tablet *topo.TabletInfo, waitTime time.Duration) error

	//
	// Backup / restore related methods
	//

	// Snapshot takes a database snapshot
	Snapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, SnapshotReplyFunc, error)

	// SnapshotSourceEnd restarts the mysql server
	SnapshotSourceEnd(ctx context.Context, tablet *topo.TabletInfo, ssea *actionnode.SnapshotSourceEndArgs, waitTime time.Duration) error

	// ReserveForRestore will prepare a server for restore
	ReserveForRestore(ctx context.Context, tablet *topo.TabletInfo, rfra *actionnode.ReserveForRestoreArgs, waitTime time.Duration) error

	// Restore restores a database snapshot
	Restore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.RestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc, error)

	// MultiSnapshot takes a database snapshot
	MultiSnapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.MultiSnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, MultiSnapshotReplyFunc, error)

	// MultiRestore restores a database snapshot
	MultiRestore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.MultiRestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc, error)
}

type TabletManagerClientFactory func() TabletManagerClient

var tabletManagerClientFactories = make(map[string]TabletManagerClientFactory)

func RegisterTabletManagerClientFactory(name string, factory TabletManagerClientFactory) {
	if _, ok := tabletManagerClientFactories[name]; ok {
		log.Fatalf("RegisterTabletManagerClient %s already exists", name)
	}
	tabletManagerClientFactories[name] = factory
}

func NewTabletManagerClient() TabletManagerClient {
	f, ok := tabletManagerClientFactories[*tabletManagerProtocol]
	if !ok {
		log.Fatalf("No TabletManagerProtocol registered with name %s", *tabletManagerProtocol)
	}

	return f()
}
