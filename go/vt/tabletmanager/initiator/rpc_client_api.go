// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package initiator

import (
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

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

	// ChangeType asks the remote tablet to change its type
	ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error

	// ReloadSchema asks the remote tablet to reload its schema
	ReloadSchema(tablet *topo.TabletInfo, waitTime time.Duration) error

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

	// SlaveWasPromoted tells the remote tablet it is now the master
	SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error

	// SlaveWasRestarted tells the remote tablet its master has changed
	SlaveWasRestarted(tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error
}

type TabletManagerConnFactory func(topo.Server) TabletManagerConn

var tabletManagerConnFactories = make(map[string]TabletManagerConnFactory)

func RegisterTabletManagerConnFactory(name string, factory TabletManagerConnFactory) {
	if _, ok := tabletManagerConnFactories[name]; ok {
		log.Fatalf("RegisterTabletManagerConn %s already exists", name)
	}
	tabletManagerConnFactories[name] = factory
}
