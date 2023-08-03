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
	"context"
	"time"

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

	GetSchema(ctx context.Context, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error)

	GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error)

	// Various read-write methods

	SetReadOnly(ctx context.Context, rdonly bool) error

	ChangeType(ctx context.Context, tabletType topodatapb.TabletType, semiSync bool) error

	Sleep(ctx context.Context, duration time.Duration)

	ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult

	RefreshState(ctx context.Context) error

	RunHealthCheck(ctx context.Context)

	ReloadSchema(ctx context.Context, waitPosition string) error

	PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)

	ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	ResetSequences(ctx context.Context, tables []string) error

	LockTables(ctx context.Context) error

	UnlockTables(ctx context.Context) error

	ExecuteQuery(ctx context.Context, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error)

	ExecuteFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error)

	ExecuteFetchAsAllPrivs(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error)

	ExecuteFetchAsApp(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error)

	// Replication related methods
	PrimaryStatus(ctx context.Context) (*replicationdatapb.PrimaryStatus, error)

	ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error)

	FullStatus(ctx context.Context) (*replicationdatapb.FullStatus, error)

	StopReplication(ctx context.Context) error

	StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error)

	StartReplication(ctx context.Context, semiSync bool) error

	StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error

	GetReplicas(ctx context.Context) ([]string, error)

	PrimaryPosition(ctx context.Context) (string, error)

	WaitForPosition(ctx context.Context, pos string) error

	// VReplication API
	CreateVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error)
	DeleteVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (*tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, error)
	ReadVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error)
	VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error)
	VReplicationWaitForPos(ctx context.Context, id int32, pos string) error
	UpdateVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error)

	// VDiff API
	VDiff(ctx context.Context, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error)

	// Reparenting related functions

	ResetReplication(ctx context.Context) error

	InitPrimary(ctx context.Context, semiSync bool) (string, error)

	PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, tabletAlias *topodatapb.TabletAlias, pos string) error

	InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64, semiSync bool) error

	DemotePrimary(ctx context.Context) (*replicationdatapb.PrimaryStatus, error)

	UndoDemotePrimary(ctx context.Context, semiSync bool) error

	ReplicaWasPromoted(ctx context.Context) error

	ResetReplicationParameters(ctx context.Context) error

	SetReplicationSource(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool) error

	StopReplicationAndGetStatus(ctx context.Context, stopReplicationMode replicationdatapb.StopReplicationMode) (StopReplicationAndGetStatusResponse, error)

	ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error

	PromoteReplica(ctx context.Context, semiSync bool) (string, error)

	// Backup / restore related methods

	Backup(ctx context.Context, logger logutil.Logger, request *tabletmanagerdatapb.BackupRequest) error

	RestoreFromBackup(ctx context.Context, logger logutil.Logger, request *tabletmanagerdatapb.RestoreFromBackupRequest) error

	// HandleRPCPanic is to be called in a defer statement in each
	// RPC input point.
	HandleRPCPanic(ctx context.Context, name string, args, reply any, verbose bool, err *error)

	// Throttler
	CheckThrottler(ctx context.Context, request *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error)
}
