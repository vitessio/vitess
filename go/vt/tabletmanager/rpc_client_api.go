// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"time"

	log "github.com/golang/glog"
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
	GetSchema(tablet *topo.TabletInfo, tables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error)

	// GetPermissions asks the remote tablet for its permissions list
	GetPermissions(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.Permissions, error)

	//
	// Various read-write methods
	//

	// ChangeType asks the remote tablet to change its type
	ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error

	// SetBlacklistedTables asks the remote tablet to change its
	// blacklisted tables list
	SetBlacklistedTables(tablet *topo.TabletInfo, tables []string, waitTime time.Duration) error

	//
	// Replication related methods
	//

	// SlavePosition returns the tablet's mysql slave position
	SlavePosition(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// WaitSlavePosition asks the tablet to wait until it reaches that
	// position in mysql replication
	WaitSlavePosition(tablet *topo.TabletInfo, replicationPosition *myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// MasterPosition returns the tablet's master position
	MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// StopSlave stops the mysql replication
	StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error

	// StopSlaveMinimum stops the mysql replication after it reaches
	// the provided minimum point
	StopSlaveMinimum(tablet *topo.TabletInfo, groupId int64, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	// StartSlave starts the mysql replication
	StartSlave(tablet *topo.TabletInfo, waitTime time.Duration) error

	// GetSlaves returns the addresses of the slaves
	GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) (*SlaveList, error)

	// WaitBlpPosition asks the tablet to wait until it reaches that
	// position in replication
	WaitBlpPosition(tablet *topo.TabletInfo, blpPosition myproto.BlpPosition, waitTime time.Duration) error

	// StopBlp asks the tablet to stop all its binlog players,
	// and returns the current position for all of them
	StopBlp(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.BlpPositionList, error)

	// StartBlp asks the tablet to restart its binlog players
	StartBlp(tablet *topo.TabletInfo, waitTime time.Duration) error

	// RunBlpUntil asks the tablet to restart its binlog players until
	// it reaches the given positions, if not there yet.
	RunBlpUntil(tablet *topo.TabletInfo, positions *myproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error)

	//
	// Reparenting related functions
	//

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
