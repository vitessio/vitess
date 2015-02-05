// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmclient

import (
	"fmt"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpcproto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

type timeoutError error

func init() {
	tmclient.RegisterTabletManagerClientFactory("bson", func() tmclient.TabletManagerClient {
		return &GoRPCTabletManagerClient{}
	})
}

// GoRPCTabletManagerClient implements tmclient.TabletManagerClient
type GoRPCTabletManagerClient struct{}

// rpcCallTablet wil execute the RPC on the remote server.
func (client *GoRPCTabletManagerClient) rpcCallTablet(ctx context.Context, tablet *topo.TabletInfo, name string, args, reply interface{}) error {
	// create the RPC client, using ctx.Deadline if set, or no timeout.
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return timeoutError(fmt.Errorf("timeout connecting to TabletManager.%v on %v", name, tablet.Alias))
		}
	}
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), connectTimeout, nil)
	if err != nil {
		return fmt.Errorf("RPC error for %v: %v", tablet.Alias, err.Error())
	}
	defer rpcClient.Close()

	// use the context Done() channel. Will handle context timeout.
	call := rpcClient.Go(ctx, "TabletManager."+name, args, reply, nil)
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return timeoutError(fmt.Errorf("timeout waiting for TabletManager.%v to %v", name, tablet.Alias))
		}
		return fmt.Errorf("interrupted waiting for TabletManager.%v to %v", name, tablet.Alias)
	case <-call.Done:
		if call.Error != nil {
			return fmt.Errorf("remote error for %v: %v", tablet.Alias, call.Error.Error())
		}
		return nil
	}
}

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	var result string
	err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PING, "payload", &result)
	if err != nil {
		return err
	}
	if result != "payload" {
		return fmt.Errorf("bad ping result: %v", result)
	}
	return nil
}

// Sleep is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLEEP, &duration, &rpc.Unused{})
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	var hr hook.HookResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXECUTE_HOOK, hk, &hr); err != nil {
		return nil, err
	}
	return &hr, nil
}

// GetSchema is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	var sd myproto.SchemaDefinition
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_SCHEMA, &gorpcproto.GetSchemaArgs{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}, &sd); err != nil {
		return nil, err
	}
	return &sd, nil
}

// GetPermissions is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*myproto.Permissions, error) {
	var p myproto.Permissions
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_PERMISSIONS, &rpc.Unused{}, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SET_RDONLY, &rpc.Unused{}, &rpc.Unused{})
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SET_RDWR, &rpc.Unused{}, &rpc.Unused{})
}

// ChangeType is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topo.TabletType) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_CHANGE_TYPE, &dbType, &rpc.Unused{})
}

// Scrap is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) Scrap(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SCRAP, &rpc.Unused{}, &rpc.Unused{})
}

// RefreshState is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_REFRESH_STATE, &rpc.Unused{}, &rpc.Unused{})
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topo.TabletType) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RUN_HEALTH_CHECK, &targetTabletType, &rpc.Unused{})
}

// HealthStream is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) HealthStream(ctx context.Context, tablet *topo.TabletInfo) (<-chan *actionnode.HealthStreamReply, tmclient.ErrFunc, error) {
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return nil, nil, timeoutError(fmt.Errorf("timeout connecting to TabletManager.HealthStream on %v", tablet.Alias))
		}
	}
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), connectTimeout, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *actionnode.HealthStreamReply, 10)
	rpcstream := make(chan *actionnode.HealthStreamReply, 10)
	c := rpcClient.StreamGo("TabletManager.HealthStream", "", rpcstream)
	interrupted := false
	go func() {
		for {
			select {
			case <-ctx.Done():
				// context is done
				interrupted = true
				close(logstream)
				rpcClient.Close()
				return
			case hsr, ok := <-rpcstream:
				if !ok {
					close(logstream)
					rpcClient.Close()
					return
				}
				logstream <- hsr
			}
		}
	}()
	return logstream, func() error {
		// this is only called after streaming is done
		if interrupted {
			return fmt.Errorf("TabletManager.HealthStreamReply interrupted by context")
		}
		return c.Error
	}, nil
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RELOAD_SCHEMA, &rpc.Unused{}, &rpc.Unused{})
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, change, &scr); err != nil {
		return nil, err
	}
	return &scr, nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_APPLY_SCHEMA, change, &scr); err != nil {
		return nil, err
	}
	return &scr, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ExecuteFetchAsDba(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXECUTE_FETCH, &gorpcproto.ExecuteFetchArgs{
		Query:          query,
		MaxRows:        maxRows,
		WantFields:     wantFields,
		DisableBinlogs: disableBinlogs,
		DBConfigName:   "dba",
	}, &qr); err != nil {
		return nil, err
	}
	return &qr, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ExecuteFetchAsApp(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXECUTE_FETCH, &gorpcproto.ExecuteFetchArgs{
		Query:          query,
		MaxRows:        maxRows,
		WantFields:     wantFields,
		DisableBinlogs: disableBinlogs,
		DBConfigName:   "app",
	}, &qr); err != nil {
		return nil, err
	}
	return &qr, nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_STATUS, &rpc.Unused{}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// WaitSlavePosition is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) WaitSlavePosition(ctx context.Context, tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, &gorpcproto.WaitSlavePositionArgs{
		Position:    waitPos,
		WaitTimeout: waitTime,
	}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	var rp myproto.ReplicationPosition
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_MASTER_POSITION, &rpc.Unused{}, &rp); err != nil {
		return rp, err
	}
	return rp, nil
}

// ReparentPosition is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ReparentPosition(ctx context.Context, tablet *topo.TabletInfo, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_REPARENT_POSITION, rp, &rsd); err != nil {
		return nil, err
	}
	return &rsd, nil
}

// StopSlave is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_SLAVE, &rpc.Unused{}, &rpc.Unused{})
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, &gorpcproto.StopSlaveMinimumArgs{
		Position: minPos,
		WaitTime: waitTime,
	}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// StartSlave is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_START_SLAVE, &rpc.Unused{}, &rpc.Unused{})
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, &gorpcproto.TabletExternallyReparentedArgs{ExternalID: externalID}, &rpc.Unused{})
}

// GetSlaves is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	var sl gorpcproto.GetSlavesReply
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_SLAVES, &rpc.Unused{}, &sl); err != nil {
		return nil, err
	}
	return sl.Addrs, nil
}

// WaitBlpPosition is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, &gorpcproto.WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: waitTime,
	}, &rpc.Unused{})
}

// StopBlp is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) StopBlp(ctx context.Context, tablet *topo.TabletInfo) (*blproto.BlpPositionList, error) {
	var bpl blproto.BlpPositionList
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_BLP, &rpc.Unused{}, &bpl); err != nil {
		return nil, err
	}
	return &bpl, nil
}

// StartBlp is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_START_BLP, &rpc.Unused{}, &rpc.Unused{})
}

// RunBlpUntil is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	var pos myproto.ReplicationPosition
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RUN_BLP_UNTIL, &gorpcproto.RunBlpUntilArgs{
		BlpPositionList: positions,
		WaitTimeout:     waitTime,
	}, &pos); err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return pos, nil
}

//
// Reparenting related functions
//

// DemoteMaster is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_DEMOTE_MASTER, &rpc.Unused{}, &rpc.Unused{})
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PROMOTE_SLAVE, &rpc.Unused{}, &rsd); err != nil {
		return nil, err
	}
	return &rsd, nil
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, &rpc.Unused{}, &rpc.Unused{})
}

// RestartSlave is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) RestartSlave(ctx context.Context, tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RESTART_SLAVE, rsd, &rpc.Unused{})
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, &rpc.Unused{})
}

// BreakSlaves is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) BreakSlaves(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_BREAK_SLAVES, &rpc.Unused{}, &rpc.Unused{})
}

//
// Backup related methods
//

// Snapshot is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) Snapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs) (<-chan *logutil.LoggerEvent, tmclient.SnapshotReplyFunc, error) {
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return nil, nil, timeoutError(fmt.Errorf("timeout connecting to TabletManager.Snapshot on %v", tablet.Alias))
		}
	}
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), connectTimeout, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	rpcstream := make(chan *gorpcproto.SnapshotStreamingReply, 10)
	result := &actionnode.SnapshotReply{}

	c := rpcClient.StreamGo("TabletManager.Snapshot", sa, rpcstream)
	interrupted := false
	go func() {
		for {
			select {
			case <-ctx.Done():
				// context is done
				interrupted = true
				close(logstream)
				rpcClient.Close()
				return
			case ssr, ok := <-rpcstream:
				if !ok {
					close(logstream)
					rpcClient.Close()
					return
				}
				if ssr.Log != nil {
					logstream <- ssr.Log
				}
				if ssr.Result != nil {
					*result = *ssr.Result
				}
			}
		}
	}()
	return logstream, func() (*actionnode.SnapshotReply, error) {
		// this is only called after streaming is done
		if interrupted {
			return nil, fmt.Errorf("TabletManager.Snapshot interrupted by context")
		}
		return result, c.Error
	}, nil
}

// SnapshotSourceEnd is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) SnapshotSourceEnd(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SnapshotSourceEndArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, args, &rpc.Unused{})
}

// ReserveForRestore is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) ReserveForRestore(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.ReserveForRestoreArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, args, &rpc.Unused{})
}

// Restore is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) Restore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.RestoreArgs) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return nil, nil, timeoutError(fmt.Errorf("timeout connecting to TabletManager.Restore on %v", tablet.Alias))
		}
	}
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), connectTimeout, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	rpcstream := make(chan *logutil.LoggerEvent, 10)
	c := rpcClient.StreamGo("TabletManager.Restore", sa, rpcstream)
	interrupted := false
	go func() {
		for {
			select {
			case <-ctx.Done():
				// context is done
				interrupted = true
				close(logstream)
				rpcClient.Close()
				return
			case ssr, ok := <-rpcstream:
				if !ok {
					close(logstream)
					rpcClient.Close()
					return
				}
				logstream <- ssr
			}
		}
	}()
	return logstream, func() error {
		// this is only called after streaming is done
		if interrupted {
			return fmt.Errorf("TabletManager.Restore interrupted by context")
		}
		return c.Error
	}, nil
}

//
// RPC related methods
//

// IsTimeoutError is part of the tmclient.TabletManagerClient interface
func (client *GoRPCTabletManagerClient) IsTimeoutError(err error) bool {
	switch err.(type) {
	case timeoutError:
		return true
	default:
		return false
	}
}
