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
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("bson", func() tmclient.TabletManagerClient {
		return &GoRpcTabletManagerClient{}
	})
}

// GoRpcTabletManagerClient implements tmclient.TabletManagerClient
type GoRpcTabletManagerClient struct{}

func (client *GoRpcTabletManagerClient) rpcCallTablet(tablet *topo.TabletInfo, name string, args, reply interface{}, waitTime time.Duration) error {
	// create the RPC client, using waitTime as the connect
	// timeout, and starting the overall timeout as well
	tmr := time.NewTimer(waitTime)
	defer tmr.Stop()
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr(), waitTime, nil)
	if err != nil {
		return fmt.Errorf("RPC error for %v: %v", tablet.Alias, err.Error())
	}
	defer rpcClient.Close()

	// do the call in the remaining time
	call := rpcClient.Go("TabletManager."+name, args, reply, nil)
	select {
	case <-tmr.C:
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

func (client *GoRpcTabletManagerClient) Ping(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var result string
	err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_PING, "payload", &result, waitTime)
	if err != nil {
		return err
	}
	if result != "payload" {
		return fmt.Errorf("Bad ping result: %v", result)
	}
	return nil
}

func (client *GoRpcTabletManagerClient) Sleep(tablet *topo.TabletInfo, duration, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLEEP, &duration, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) ExecuteHook(tablet *topo.TabletInfo, hk *hook.Hook, waitTime time.Duration) (*hook.HookResult, error) {
	var hr hook.HookResult
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_EXECUTE_HOOK, hk, &hr, waitTime); err != nil {
		return nil, err
	}
	return &hr, nil
}

func (client *GoRpcTabletManagerClient) GetSchema(tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error) {
	var sd myproto.SchemaDefinition
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_SCHEMA, &gorpcproto.GetSchemaArgs{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}, &sd, waitTime); err != nil {
		return nil, err
	}
	return &sd, nil
}

func (client *GoRpcTabletManagerClient) GetPermissions(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.Permissions, error) {
	var p myproto.Permissions
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_PERMISSIONS, "", &p, waitTime); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Various read-write methods
//

func (client *GoRpcTabletManagerClient) SetReadOnly(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SET_RDONLY, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) SetReadWrite(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SET_RDWR, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_CHANGE_TYPE, &dbType, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) Scrap(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SCRAP, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) RefreshState(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_REFRESH_STATE, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) RunHealthCheck(tablet *topo.TabletInfo, targetTabletType topo.TabletType, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RUN_HEALTH_CHECK, &targetTabletType, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) ReloadSchema(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RELOAD_SCHEMA, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) PreflightSchema(tablet *topo.TabletInfo, change string, waitTime time.Duration) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, change, &scr, waitTime); err != nil {
		return nil, err
	}
	return &scr, nil
}

func (client *GoRpcTabletManagerClient) ApplySchema(tablet *topo.TabletInfo, change *myproto.SchemaChange, waitTime time.Duration) (*myproto.SchemaChangeResult, error) {
	var scr myproto.SchemaChangeResult
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_APPLY_SCHEMA, change, &scr, waitTime); err != nil {
		return nil, err
	}
	return &scr, nil
}

func (client *GoRpcTabletManagerClient) ExecuteFetch(tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool, waitTime time.Duration) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_EXECUTE_FETCH, &gorpcproto.ExecuteFetchArgs{Query: query, MaxRows: maxRows, WantFields: wantFields, DisableBinlogs: disableBinlogs}, &qr, waitTime); err != nil {
		return nil, err
	}
	return &qr, nil
}

//
// Replication related methods
//

func (client *GoRpcTabletManagerClient) SlaveStatus(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_STATUS, "", &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) WaitSlavePosition(tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, &gorpcproto.WaitSlavePositionArgs{
		Position:    waitPos,
		WaitTimeout: waitTime,
	}, &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	var rp myproto.ReplicationPosition
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_MASTER_POSITION, "", &rp, waitTime); err != nil {
		return rp, err
	}
	return rp, nil
}

func (client *GoRpcTabletManagerClient) ReparentPosition(tablet *topo.TabletInfo, rp *myproto.ReplicationPosition, waitTime time.Duration) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_REPARENT_POSITION, rp, &rsd, waitTime); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerClient) StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_SLAVE, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) StopSlaveMinimum(tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, &gorpcproto.StopSlaveMinimumArgs{
		Position: minPos,
		WaitTime: waitTime,
	}, &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerClient) StartSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_START_SLAVE, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) TabletExternallyReparented(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) ([]string, error) {
	var sl gorpcproto.GetSlavesReply
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_SLAVES, "", &sl, waitTime); err != nil {
		return nil, err
	}
	return sl.Addrs, nil
}

func (client *GoRpcTabletManagerClient) WaitBlpPosition(tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, &gorpcproto.WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: waitTime,
	}, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) StopBlp(tablet *topo.TabletInfo, waitTime time.Duration) (*blproto.BlpPositionList, error) {
	var bpl blproto.BlpPositionList
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_BLP, "", &bpl, waitTime); err != nil {
		return nil, err
	}
	return &bpl, nil
}

func (client *GoRpcTabletManagerClient) StartBlp(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_START_BLP, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) RunBlpUntil(tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	var pos myproto.ReplicationPosition
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RUN_BLP_UNTIL, &gorpcproto.RunBlpUntilArgs{
		BlpPositionList: positions,
		WaitTimeout:     waitTime,
	}, &pos, waitTime); err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return pos, nil
}

//
// Reparenting related functions
//

func (client *GoRpcTabletManagerClient) DemoteMaster(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_DEMOTE_MASTER, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) PromoteSlave(tablet *topo.TabletInfo, waitTime time.Duration) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_PROMOTE_SLAVE, "", &rsd, waitTime); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerClient) SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) RestartSlave(tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RESTART_SLAVE, rsd, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) SlaveWasRestarted(tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) BreakSlaves(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_BREAK_SLAVES, "", &noOutput, waitTime)
}

//
// Backup related methods
//

func (client *GoRpcTabletManagerClient) Snapshot(tablet *topo.TabletInfo, sa *actionnode.SnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.SnapshotReplyFunc, error) {
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

func (client *GoRpcTabletManagerClient) SnapshotSourceEnd(tablet *topo.TabletInfo, args *actionnode.SnapshotSourceEndArgs, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, args, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) ReserveForRestore(tablet *topo.TabletInfo, args *actionnode.ReserveForRestoreArgs, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, args, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerClient) Restore(tablet *topo.TabletInfo, sa *actionnode.RestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
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

func (client *GoRpcTabletManagerClient) MultiSnapshot(tablet *topo.TabletInfo, sa *actionnode.MultiSnapshotArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.MultiSnapshotReplyFunc, error) {
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

func (client *GoRpcTabletManagerClient) MultiRestore(tablet *topo.TabletInfo, sa *actionnode.MultiRestoreArgs, waitTime time.Duration) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
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
