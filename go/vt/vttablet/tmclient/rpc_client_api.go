/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tmclient

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletManagerProtocol is the implementation to use for tablet
// manager protocol. It is exported for tests only.
var TabletManagerProtocol = flag.String("tablet_manager_protocol", "grpc", "the protocol to use to talk to vttablet")

// TabletManagerClient defines the interface used to talk to a remote tablet
type TabletManagerClient interface {
	//
	// Various read-only methods
	//

	// Ping will try to ping the remote tablet
	Ping(ctx context.Context, tablet *topodatapb.Tablet) error

	// GetSchema asks the remote tablet for its database schema
	GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error)

	// GetPermissions asks the remote tablet for its permissions list
	GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error)

	//
	// Various read-write methods
	//

	// SetReadOnly makes the mysql instance read-only
	SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error

	// SetReadWrite makes the mysql instance read-write
	SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error

	// ChangeType asks the remote tablet to change its type
	ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error

	// Sleep will sleep for a duration (used for tests)
	Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error

	// ExecuteHook executes the provided hook remotely
	ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error)

	// RefreshState asks the remote tablet to reload its tablet record
	RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error

	// RunHealthCheck asks the remote tablet to run a health check cycle
	RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error

	// IgnoreHealthError sets the regexp for health errors to ignore.
	IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error

	// ReloadSchema asks the remote tablet to reload its schema
	ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error

	// PreflightSchema will test a list of schema changes.
	PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)

	// ApplySchema will apply a schema change
	ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	LockTables(ctx context.Context, tablet *topodatapb.Tablet) error

	UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error

	// ExecuteFetchAsDba executes a query remotely using the DBA pool.
	// If usePool is set, a connection pool may be used to make the
	// query faster. Close() should close the pool in that case.
	ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error)

	// ExecuteFetchAsAllPrivs executes a query remotely using the allprivs user.
	ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, query []byte, maxRows int, reloadSchema bool) (*querypb.QueryResult, error)

	// ExecuteFetchAsApp executes a query remotely using the App pool
	// If usePool is set, a connection pool may be used to make the
	// query faster. Close() should close the pool in that case.
	ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int) (*querypb.QueryResult, error)

	//
	// Replication related methods
	//

	// SlaveStatus returns the tablet's mysql slave status.
	SlaveStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error)

	// MasterPosition returns the tablet's master position
	MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error)

	// StopSlave stops the mysql replication
	StopSlave(ctx context.Context, tablet *topodatapb.Tablet) error

	// StopSlaveMinimum stops the mysql replication after it reaches
	// the provided minimum point
	StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, stopPos string, waitTime time.Duration) (string, error)

	// StartSlave starts the mysql replication
	StartSlave(ctx context.Context, tablet *topodatapb.Tablet) error

	// StartSlaveUntilAfter starts replication until after the position specified
	StartSlaveUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, duration time.Duration) error

	// TabletExternallyReparented tells a tablet it is now the master, after an
	// external tool has already promoted the underlying mysqld to master and
	// reparented the other mysqld servers to it.
	//
	// externalID is an optional string provided by the external tool that
	// vttablet will emit in logs to facilitate cross-referencing.
	TabletExternallyReparented(ctx context.Context, tablet *topodatapb.Tablet, externalID string) error

	// GetSlaves returns the addresses of the slaves
	GetSlaves(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error)

	// VReplicationExec executes a VReplication command
	VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error)
	VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error

	//
	// Reparenting related functions
	//

	// ResetReplication tells a tablet to completely reset its
	// replication.  All binary and relay logs are flushed. All
	// replication positions are reset.
	ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error

	// InitMaster tells a tablet to make itself the new master,
	// and return the replication position the slaves should use to
	// reparent to it.
	InitMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error)

	// PopulateReparentJournal asks the master to insert a row in
	// its reparent_journal table.
	PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error

	// InitSlave tells a tablet to make itself a slave to the
	// passed in master tablet alias, and wait for the row in the
	// reparent_journal table.
	InitSlave(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error

	// DemoteMaster tells the soon-to-be-former master it's gonna change,
	// and it should go read-only and return its current position.
	DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error)

	// UndoDemoteMaster reverts all changes made by DemoteMaster
	// To be used if we are unable to promote the chosen new master
	UndoDemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) error

	// PromoteSlaveWhenCaughtUp transforms the tablet from a slave to a master.
	PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topodatapb.Tablet, pos string) (string, error)

	// SlaveWasPromoted tells the remote tablet it is now the master
	SlaveWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error

	// SetMaster tells a tablet to make itself a slave to the
	// passed in master tablet alias, and wait for the row in the
	// reparent_journal table (if timeCreatedNS is non-zero).
	SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error

	// SlaveWasRestarted tells the remote tablet its master has changed
	SlaveWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error

	// StopReplicationAndGetStatus stops replication and returns the
	// current position.
	StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error)

	// PromoteSlave makes the tablet the new master
	PromoteSlave(ctx context.Context, tablet *topodatapb.Tablet) (string, error)

	//
	// Backup / restore related methods
	//

	// Backup creates a database backup
	Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int) (logutil.EventStream, error)

	// RestoreFromBackup deletes local data and restores database from backup
	RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet) (logutil.EventStream, error)

	//
	// Management methods
	//

	// Close will be called when this TabletManagerClient won't be
	// used any more. It can be used to free any resource.
	Close()
}

// TabletManagerClientFactory is the factory method to create
// TabletManagerClient objects.
type TabletManagerClientFactory func() TabletManagerClient

var tabletManagerClientFactories = make(map[string]TabletManagerClientFactory)

// RegisterTabletManagerClientFactory allows modules to register
// TabletManagerClient implementations. Should be called on init().
func RegisterTabletManagerClientFactory(name string, factory TabletManagerClientFactory) {
	if _, ok := tabletManagerClientFactories[name]; ok {
		log.Fatalf("RegisterTabletManagerClient %s already exists", name)
	}
	tabletManagerClientFactories[name] = factory
}

// NewTabletManagerClient creates a new TabletManagerClient. Should be
// called after flags are parsed.
func NewTabletManagerClient() TabletManagerClient {
	f, ok := tabletManagerClientFactories[*TabletManagerProtocol]
	if !ok {
		log.Exitf("No TabletManagerProtocol registered with name %s", *TabletManagerProtocol)
	}

	return f()
}
