/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"time"

	"context"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// RPCTM defines the interface implemented by the TM for RPCs.
// It is useful for RPC implementations to test their full stack.
type RPCTM interface {
	// RPC calls

	// Various read-only methods

	Ping(ctx context.Context, args string) string

	GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error)

	GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error)

	// Various read-write methods

	SetReadOnly(ctx context.Context, rdonly bool) error

	ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error

	Sleep(ctx context.Context, duration time.Duration)

	ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult

	RefreshState(ctx context.Context) error

	RunHealthCheck(ctx context.Context)

	IgnoreHealthError(ctx context.Context, pattern string) error

	ReloadSchema(ctx context.Context, waitPosition string) error

	PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)

	ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	LockTables(ctx context.Context) error

	UnlockTables(ctx context.Context) error

	ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error)

	ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error)

	ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error)

	// Replication related methods
	MasterStatus(ctx context.Context) (*replicationdatapb.MasterStatus, error)

	ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error)

	StopReplication(ctx context.Context) error

	StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error)

	StartReplication(ctx context.Context) error

	StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error

	GetReplicas(ctx context.Context) ([]string, error)

	MasterPosition(ctx context.Context) (string, error)

	WaitForPosition(ctx context.Context, pos string) error

	// VExec generic API
	VExec(ctx context.Context, query, workflow, keyspace string) (*querypb.QueryResult, error)

	// VReplication API
	VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error)
	VReplicationWaitForPos(ctx context.Context, id int, pos string) error

	// Reparenting related functions

	ResetReplication(ctx context.Context) error

	InitMaster(ctx context.Context) (string, error)

	PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error

	InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error

	DemoteMaster(ctx context.Context) (*replicationdatapb.MasterStatus, error)

	UndoDemoteMaster(ctx context.Context) error

	ReplicaWasPromoted(ctx context.Context) error

	SetMaster(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error

	StopReplicationAndGetStatus(ctx context.Context, stopReplicationMode replicationdatapb.StopReplicationMode) (StopReplicationAndGetStatusResponse, error)

	ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error

	PromoteReplica(ctx context.Context) (string, error)

	// Backup / restore related methods

	Backup(ctx context.Context, concurrency int, logger logutil.Logger, allowMaster bool) error

	RestoreFromBackup(ctx context.Context, logger logutil.Logger) error

	// HandleRPCPanic is to be called in a defer statement in each
	// RPC input point.
	HandleRPCPanic(ctx context.Context, name string, args, reply interface{}, verbose bool, err *error)
}
