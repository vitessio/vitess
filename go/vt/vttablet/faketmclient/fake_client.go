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

package faketmclient

// This file contains a "fake" implementation of the TabletManagerClient interface, which
// may be useful for running tests without having to bring up a cluster. The implementation
// is very minimal, and only works for specific use-cases. If you find that it doesn't work
// for yours, feel free to extend this implementation.

import (
	"io"
	"time"

	"golang.org/x/net/context"

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
func (client *FakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return client.tmc.GetSchema(ctx, tablet, tables, excludeTables, includeViews)
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
func (client *FakeTabletManagerClient) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
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

// IgnoreHealthError is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error {
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

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsAllPrivs is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, query []byte, maxRows int, reloadSchema bool) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return &replicationdatapb.Status{}, nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", nil
}

// StopSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, minPos string, waitTime time.Duration) (string, error) {
	return "", nil
}

// StartSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// StartSlaveUntilAfter is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartSlaveUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, duration time.Duration) error {
	return nil
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topodatapb.Tablet, externalID string) error {
	return nil
}

// GetSlaves is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetSlaves(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	return nil, nil
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
func (client *FakeTabletManagerClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error {
	return nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// InitMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	return nil
}

// InitSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitSlave(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	return nil
}

// DemoteMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return "", nil
}

// PromoteSlaveWhenCaughtUp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topodatapb.Tablet, position string) (string, error) {
	return "", nil
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	return nil
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	return nil
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	return &replicationdatapb.Status{}, nil
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
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
func (client *FakeTabletManagerClient) Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int) (logutil.EventStream, error) {
	return &eofEventStream{}, nil
}

// RestoreFromBackup is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet) (logutil.EventStream, error) {
	return &eofEventStream{}, nil
}

//
// Management related methods
//

// Close is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Close() {
}
