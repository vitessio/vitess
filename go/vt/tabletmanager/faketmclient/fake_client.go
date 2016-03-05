// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package faketmclient

// This file contains a "fake" implementation of the TabletManagerClient interface, which
// may be useful for running tests without having to bring up a cluster. The implementation
// is very minimal, and only works for specific use-cases. If you find that it doesn't work
// for yours, feel free to extend this implementation.

import (
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type timeoutError error

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
func (client *FakeTabletManagerClient) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// Sleep is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	return nil
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	var hr hook.HookResult
	return &hr, nil
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return client.tmc.GetSchema(ctx, tablet, tables, excludeTables, includeViews)
}

// GetPermissions is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*tabletmanagerdatapb.Permissions, error) {
	return &tabletmanagerdatapb.Permissions{}, nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topodatapb.TabletType) error {
	return nil
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topodatapb.TabletType) error {
	return nil
}

// IgnoreHealthError is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) IgnoreHealthError(ctx context.Context, tablet *topo.TabletInfo, pattern string) error {
	return nil
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*tmutils.SchemaChangeResult, error) {
	return &tmutils.SchemaChangeResult{}, nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	return &tmutils.SchemaChangeResult{}, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (*replicationdatapb.Status, error) {
	return &replicationdatapb.Status{}, nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", nil
}

// StopSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, minPos string, waitTime time.Duration) (string, error) {
	return "", nil
}

// StartSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	return nil
}

// GetSlaves is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	return nil, nil
}

// WaitBlpPosition is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	return nil
}

// StopBlp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopBlp(ctx context.Context, tablet *topo.TabletInfo) ([]*tabletmanagerdatapb.BlpPosition, error) {
	// TODO(aaijazi): this works because all tests so far only need to rely on Uid 0.
	// Ideally, this should turn into a full mock, where the caller can configure the exact
	// return value.
	bpl := []*tabletmanagerdatapb.BlpPosition{
		{
			Uid: uint32(0),
		},
	}
	return bpl, nil
}

// StartBlp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RunBlpUntil is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	return "", nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) ResetReplication(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// InitMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitMaster(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PopulateReparentJournal(ctx context.Context, tablet *topo.TabletInfo, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	return nil
}

// InitSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) InitSlave(ctx context.Context, tablet *topo.TabletInfo, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	return nil
}

// DemoteMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", nil
}

// PromoteSlaveWhenCaughtUp is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topo.TabletInfo, position string) (string, error) {
	return "", nil
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SetMaster(ctx context.Context, tablet *topo.TabletInfo, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	return nil
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	return nil
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topo.TabletInfo) (*replicationdatapb.Status, error) {
	return &replicationdatapb.Status{}, nil
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (string, error) {
	return "", nil
}

//
// Backup related methods
//

// Backup is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) Backup(ctx context.Context, tablet *topo.TabletInfo, concurrency int) (<-chan *logutilpb.Event, tmclient.ErrFunc, error) {
	logstream := make(chan *logutilpb.Event, 10)
	return logstream, func() error {
		return nil
	}, nil
}

//
// RPC related methods
//

// IsTimeoutError is part of the tmclient.TabletManagerClient interface.
func (client *FakeTabletManagerClient) IsTimeoutError(err error) bool {
	switch err.(type) {
	case timeoutError:
		return true
	default:
		return false
	}
}
