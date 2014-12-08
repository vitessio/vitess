// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmclient

import (
	"fmt"
	"time"

	"code.google.com/p/go.net/context"
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
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("bson", func() tmclient.TabletManagerClient {
		return &GoRpcTabletManagerClient{}
	})
}

// GoRpcTabletManagerClient implements tmclient.TabletManagerClient
type GoRpcTabletManagerClient struct{}

// rpcCallTablet wil execute the RPC on the remote server.
func (client *GoRpcTabletManagerClient) rpcCallTablet(ctx context.Context, tablet *topo.TabletInfo, name string, args, reply interface{}) error {
	// create the RPC client, using ctx.Deadline if set, or no timeout.
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return fmt.Errorf("Timeout connecting to TabletManager.%v on %v", name, tablet.Alias)
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
		return fmt.Errorf("Timeout waiting for TabletManager.%v to %v", name, tablet.Alias)
	case <-call.Done:
		if call.Error != nil {
			return fmt.Errorf("Remote error for %v: %v", tablet.Alias, call.Error.Error())
		} else {
			return nil
		}
	}
}

//
// Various read-only methods
//

func (client *GoRpcTabletManagerClient) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	var result string
	err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PING, "payload", &result)
	if err != nil {
		return err
	}
	if result != "payload" {
		return fmt.Errorf("Bad ping result: %v", result)
	}
	return nil
}

func (client *GoRpcTabletManagerClient) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLEEP, &duration, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	var hr hook.HookResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXECUTE_HOOK, hk, &hr); err != nil {
		return nil, err
	}
	return &hr, nil
}

func (client *GoRpcTabletManagerClient) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	var sd myproto.SchemaDefinition
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_SCHEMA, &gorpcproto.GetSchemaArgs{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}, &sd); err != nil {
		return nil, err
	}
	return &sd, nil
}

func (client *GoRpcTabletManagerClient) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*myproto.Permissions, error) {
	var p myproto.Permissions
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_PERMISSIONS, &rpc.Unused{}, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Various read-write methods
//

func (client *GoRpcTabletManagerClient) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SET_RDONLY, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SET_RDWR, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topo.TabletType) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_CHANGE_TYPE, &dbType, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) Scrap(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SCRAP, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_REFRESH_STATE, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topo.TabletType) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RUN_HEALTH_CHECK, &targetTabletType, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RELOAD_SCHEMA, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, change, &scr); err != nil {
		return nil, err
	}
	return &scr, nil
}

func (client *GoRpcTabletManagerClient) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_APPLY_SCHEMA, change, &scr); err != nil {
		return nil, err
	}
	return &scr, nil
}

func (client *GoRpcTabletManagerClient) ExecuteFetch(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXECUTE_FETCH, &gorpcproto.ExecuteFetchArgs{Query: query, MaxRows: maxRows, WantFields: wantFields, DisableBinlogs: disableBinlogs}, &qr); err != nil {
		return nil, err
	}
	return &qr, nil
}

//
// Replication related methods
//

func (client *GoRpcTabletManagerClient) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_STATUS, &rpc.Unused{}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) WaitSlavePosition(ctx context.Context, tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, &gorpcproto.WaitSlavePositionArgs{
		Position:    waitPos,
		WaitTimeout: waitTime,
	}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	var rp myproto.ReplicationPosition
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_MASTER_POSITION, &rpc.Unused{}, &rp); err != nil {
		return rp, err
	}
	return rp, nil
}

func (client *GoRpcTabletManagerClient) ReparentPosition(ctx context.Context, tablet *topo.TabletInfo, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_REPARENT_POSITION, rp, &rsd); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerClient) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_SLAVE, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, &gorpcproto.StopSlaveMinimumArgs{
		Position: minPos,
		WaitTime: waitTime,
	}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_START_SLAVE, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, &gorpcproto.TabletExternallyReparentedArgs{ExternalID: externalID}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	var sl gorpcproto.GetSlavesReply
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_GET_SLAVES, &rpc.Unused{}, &sl); err != nil {
		return nil, err
	}
	return sl.Addrs, nil
}

func (client *GoRpcTabletManagerClient) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, &gorpcproto.WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: waitTime,
	}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) StopBlp(ctx context.Context, tablet *topo.TabletInfo) (*blproto.BlpPositionList, error) {
	var bpl blproto.BlpPositionList
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_STOP_BLP, &rpc.Unused{}, &bpl); err != nil {
		return nil, err
	}
	return &bpl, nil
}

func (client *GoRpcTabletManagerClient) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_START_BLP, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
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

func (client *GoRpcTabletManagerClient) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_DEMOTE_MASTER, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_PROMOTE_SLAVE, &rpc.Unused{}, &rsd); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerClient) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, &rpc.Unused{}, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) RestartSlave(ctx context.Context, tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RESTART_SLAVE, rsd, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) BreakSlaves(ctx context.Context, tablet *topo.TabletInfo) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_BREAK_SLAVES, &rpc.Unused{}, &rpc.Unused{})
}

//
// Backup related methods
//

func (client *GoRpcTabletManagerClient) Snapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.SnapshotReplyFunc, error) {
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), waitTime, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	rpcstream := make(chan *gorpcproto.SnapshotStreamingReply, 10)
	result := &actionnode.SnapshotReply{}

	c := rpcClient.StreamGo("TabletManager.Snapshot", sa, rpcstream)
	go func() {
		for ssr := range rpcstream {
			if ssr.Log != nil {
				logstream <- ssr.Log
			}
			if ssr.Result != nil {
				*result = *ssr.Result
			}
		}
		close(logstream)
		rpcClient.Close()
	}()
	return logstream, func() (*actionnode.SnapshotReply, error) {
		return result, c.Error
	}, nil
}

func (client *GoRpcTabletManagerClient) SnapshotSourceEnd(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SnapshotSourceEndArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, args, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) ReserveForRestore(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.ReserveForRestoreArgs) error {
	return client.rpcCallTablet(ctx, tablet, actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, args, &rpc.Unused{})
}

func (client *GoRpcTabletManagerClient) Restore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.RestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), waitTime, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	c := rpcClient.StreamGo("TabletManager.Restore", sa, logstream)
	return logstream, func() error {
		rpcClient.Close()
		return c.Error
	}, nil
}

func (client *GoRpcTabletManagerClient) MultiSnapshot(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.MultiSnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.MultiSnapshotReplyFunc, error) {
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), waitTime, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	rpcstream := make(chan *gorpcproto.MultiSnapshotStreamingReply, 10)
	result := &actionnode.MultiSnapshotReply{}

	c := rpcClient.StreamGo("TabletManager.MultiSnapshot", sa, rpcstream)
	go func() {
		for ssr := range rpcstream {
			if ssr.Log != nil {
				logstream <- ssr.Log
			}
			if ssr.Result != nil {
				*result = *ssr.Result
			}
		}
		close(logstream)
		rpcClient.Close()
	}()
	return logstream, func() (*actionnode.MultiSnapshotReply, error) {
		return result, c.Error
	}, nil
}

func (client *GoRpcTabletManagerClient) MultiRestore(ctx context.Context, tablet *topo.TabletInfo, sa *actionnode.MultiRestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), waitTime, nil)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	c := rpcClient.StreamGo("TabletManager.MultiRestore", sa, logstream)
	return logstream, func() error {
		rpcClient.Close()
		return c.Error
	}, nil
}
