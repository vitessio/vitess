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

	mproto "github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpcproto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
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

// Ping is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// Sleep is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	return nil
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	var hr hook.HookResult
	return &hr, nil
}

// GetSchema is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	return client.tmc.GetSchema(ctx, tablet, tables, excludeTables, includeViews)
	// var sd myproto.SchemaDefinition
	// return &sd, nil
}

// GetPermissions is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*myproto.Permissions, error) {
	var p myproto.Permissions
	return &p, nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// ChangeType is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topo.TabletType) error {
	return nil
}

// Scrap is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) Scrap(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RefreshState is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topo.TabletType) error {
	return nil
}

// HealthStream is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) HealthStream(ctx context.Context, tablet *topo.TabletInfo) (<-chan *actionnode.HealthStreamReply, tmclient.ErrFunc, error) {
	logstream := make(chan *actionnode.HealthStreamReply, 10)
	return logstream, func() error {
		return nil
	}, nil
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	return &scr, nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	return &scr, nil
}

// ExecuteFetch is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ExecuteFetch(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	return &qr, nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	return &status, nil
}

// WaitSlavePosition is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) WaitSlavePosition(ctx context.Context, tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	return &status, nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	var rp myproto.ReplicationPosition
	return rp, nil
}

// ReparentPosition is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ReparentPosition(ctx context.Context, tablet *topo.TabletInfo, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	return &rsd, nil
}

// StopSlave is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	return &status, nil
}

// StartSlave is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	return nil
}

// GetSlaves is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	var sl gorpcproto.GetSlavesReply
	return sl.Addrs, nil
}

// WaitBlpPosition is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	return nil
}

// StopBlp is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) StopBlp(ctx context.Context, tablet *topo.TabletInfo) (*blproto.BlpPositionList, error) {
	// TODO(aaijazi): this works because all tests so far only need to rely on Uid 0.
	// Ideally, this should turn into a full mock, where the caller can configure the exact
	// return value.
	bpl := blproto.BlpPositionList{
		Entries: []blproto.BlpPosition{
			blproto.BlpPosition{
				Uid: uint32(0),
			},
		},
	}
	return &bpl, nil
}

// StartBlp is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RunBlpUntil is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	var pos myproto.ReplicationPosition
	return pos, nil
}

//
// Reparenting related functions
//

// DemoteMaster is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	return &rsd, nil
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

// RestartSlave is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) RestartSlave(ctx context.Context, tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData) error {
	return nil
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	return nil
}

// BreakSlaves is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) BreakSlaves(ctx context.Context, tablet *topo.TabletInfo) error {
	return nil
}

//
// Backup related methods
//

// Snapshot is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) Snapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs) (<-chan *logutil.LoggerEvent, tmclient.SnapshotReplyFunc, error) {
	logstream := make(chan *logutil.LoggerEvent, 10)
	return logstream, func() (*actionnode.SnapshotReply, error) {
		return &actionnode.SnapshotReply{}, nil
	}, nil
}

// SnapshotSourceEnd is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) SnapshotSourceEnd(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SnapshotSourceEndArgs) error {
	return nil
}

// ReserveForRestore is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) ReserveForRestore(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.ReserveForRestoreArgs) error {
	return nil
}

// Restore is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) Restore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.RestoreArgs) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
	logstream := make(chan *logutil.LoggerEvent, 10)
	return logstream, func() error {
		return nil
	}, nil
}

//
// RPC related methods
//

// IsTimeoutError is part of the tmclient.TabletManagerClient interface
func (client *FakeTabletManagerClient) IsTimeoutError(err error) bool {
	switch err.(type) {
	case timeoutError:
		return true
	default:
		return false
	}
}
