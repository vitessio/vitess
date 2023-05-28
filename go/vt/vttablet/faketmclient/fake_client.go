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

package faketmclient

// This file contains a "fake" implementation of the TabletManagerClient interface, which
// may be useful for running tests without having to bring up a cluster. The implementation
// is very minimal, and only works for specific use-cases. If you find that it doesn't work
// for yours, feel free to extend this implementation.

import (
	"context"
	"io"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// NewFakeTabletManagerClient should be used to create a new FakeTabletManagerClient.
// There is intentionally no init in this file with a call to RegisterTabletManagerClientFactory.
// There shouldn't be any legitimate use-case where we would want to start a vitess cluster
// with a FakeTMC, and we don't want to do it by accident.
func NewFakeTabletManagerClient() tmclient.TabletManagerClient {
	return &FakeTabletManagerClient{
		tmc: tmclient.NewTabletManagerClient(),
	}
}

// FakeTabletManagerClient implements tmclient.TabletManagerClient
// TODO(aaijazi): this is a pretty complicated and inconsistent implementation. It can't
// make up its mind on whether it wants to be a fake, a mock, or act like the real thing.
// We probably want to move it more consistently towards being a mock, once we standardize
// how we want to do mocks in vitess. We don't currently have a good way to configure
// specific return values.
type FakeTabletManagerClient struct {
	// Keep a real TMC, so we can pass through certain calls.
	// This is to let us essentially fake out only part of the interface, while deferring
	// to the real implementation for things that we don't need to fake out yet.
	tmc tmclient.TabletManagerClient
}

func (client *FakeTabletManagerClient) UpdateVRWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVRWorkflowRequest) (*tabletmanagerdatapb.UpdateVRWorkflowResponse, error) {
	return nil, nil
}

func (client *FakeTabletManagerClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	return nil, nil
}

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// Sleep is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error {
	return nil
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error) {
	var hr hook.HookResult
	return &hr, nil
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return client.tmc.GetSchema(ctx, tablet, request)
}

// GetPermissions is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	return &tabletmanagerdatapb.Permissions{}, nil
}

// LockTables is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) LockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// UnlockTables is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType, semiSync bool) error {
	return nil
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	return nil
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	return make([]*tabletmanagerdatapb.SchemaChangeResult, len(changes)), nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	return &tabletmanagerdatapb.SchemaChangeResult{}, nil
}

// ExecuteQuery is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteQuery(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsAllPrivs is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

//
// Replication related methods
//

// ReplicationStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return &replicationdatapb.Status{}, nil
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	return &replicationdatapb.FullStatus{}, nil
}

// StopReplication is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// StopReplicationMinimum is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopReplicationMinimum(ctx context.Context, tablet *topodatapb.Tablet, stopPos string, waitTime time.Duration) (string, error) {
	return "", nil
}

// PrimaryStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return &replicationdatapb.PrimaryStatus{}, nil
}

// StartReplication is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartReplication(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	return nil
}

// StartReplicationUntilAfter is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartReplicationUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, duration time.Duration) error {
	return nil
}

// GetReplicas is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetReplicas(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	return nil, nil
}

// InitReplica is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitReplica(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64, semiSync bool) error {
	return nil
}

// ReplicaWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ReplicaWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// ResetReplicationParameters is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ResetReplicationParameters(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// ReplicaWasRestarted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ReplicaWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	return nil
}

// PrimaryPosition is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", nil
}

// WaitForPosition is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	return nil
}

// VReplicationExec is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	// This result satisfies 'select pos from _vt.vreplication...' called from split clone unit tests in go/vt/worker.
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("pos", "varchar"),
		"MariaDB/1-1-1",
	)
	return sqltypes.ResultToProto3(result), nil
}

// VReplicationWaitForPos is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	return nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// InitPrimary is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitPrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	return "", nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	return nil
}

// DemotePrimary is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return nil, nil
}

// UndoDemotePrimary is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) UndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	return nil
}

// SetReplicationSource is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetReplicationSource(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool) error {
	return nil
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, stopReplicationMode replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	return &replicationdatapb.StopReplicationStatus{}, nil
}

// PromoteReplica is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	return "", nil
}

//
// Backup related methods
//

type eofEventStream struct{}

func (e *eofEventStream) Recv() (*logutilpb.Event, error) {
	return nil, io.EOF
}

// Backup is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Backup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.BackupRequest) (logutil.EventStream, error) {
	return &eofEventStream{}, nil
}

// RestoreFromBackup is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.RestoreFromBackupRequest) (logutil.EventStream, error) {
	return &eofEventStream{}, nil
}

//
// Management related methods
//

// Close is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Close() {
}
