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

package tabletmanager

import (
	"time"

	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RPCAgent defines the interface implemented by the Agent for RPCs.
// It is useful for RPC implementations to test their full stack.
type RPCAgent interface {
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

	ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error)

	ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error)

	ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error)

	// Replication related methods

	SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error)

	MasterPosition(ctx context.Context) (string, error)

	StopSlave(ctx context.Context) error

	StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error)

	StartSlave(ctx context.Context) error

	TabletExternallyReparented(ctx context.Context, externalID string) error

	GetSlaves(ctx context.Context) ([]string, error)

	WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error

	StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error)

	StartBlp(ctx context.Context) error

	RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error)

	// Reparenting related functions

	ResetReplication(ctx context.Context) error

	InitMaster(ctx context.Context) (string, error)

	PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error

	InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error

	DemoteMaster(ctx context.Context) (string, error)

	PromoteSlaveWhenCaughtUp(ctx context.Context, replicationPosition string) (string, error)

	SlaveWasPromoted(ctx context.Context) error

	SetMaster(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error

	SlaveWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error

	StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error)

	PromoteSlave(ctx context.Context) (string, error)

	// Backup / restore related methods

	Backup(ctx context.Context, concurrency int, logger logutil.Logger) error

	RestoreFromBackup(ctx context.Context, logger logutil.Logger) error

	// HandleRPCPanic is to be called in a defer statement in each
	// RPC input point.
	HandleRPCPanic(ctx context.Context, name string, args, reply interface{}, verbose bool, err *error)
}
