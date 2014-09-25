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
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpcproto"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	initiator.RegisterTabletManagerConnFactory("bson", func(ts topo.Server) initiator.TabletManagerConn {
		return &GoRpcTabletManagerConn{ts}
	})
}

// GoRpcTabletManagerConn implements initiator.TabletManagerConn
type GoRpcTabletManagerConn struct {
	ts topo.Server
}

func (client *GoRpcTabletManagerConn) rpcCallTablet(tablet *topo.TabletInfo, name string, args, reply interface{}, waitTime time.Duration) error {
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

func (client *GoRpcTabletManagerConn) Ping(tablet *topo.TabletInfo, waitTime time.Duration) error {
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

func (client *GoRpcTabletManagerConn) Sleep(tablet *topo.TabletInfo, duration, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLEEP, &duration, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) GetSchema(tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error) {
	var sd myproto.SchemaDefinition
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_SCHEMA, &gorpcproto.GetSchemaArgs{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}, &sd, waitTime); err != nil {
		return nil, err
	}
	return &sd, nil
}

func (client *GoRpcTabletManagerConn) GetPermissions(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.Permissions, error) {
	var p myproto.Permissions
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_PERMISSIONS, "", &p, waitTime); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Various read-write methods
//

func (client *GoRpcTabletManagerConn) SetReadOnly(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SET_RDONLY, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) SetReadWrite(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SET_RDWR, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_CHANGE_TYPE, &dbType, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) Scrap(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SCRAP, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) ReloadSchema(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RELOAD_SCHEMA, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) ExecuteFetch(tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs bool, waitTime time.Duration) (*mproto.QueryResult, error) {
	var qr mproto.QueryResult
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_EXECUTE_FETCH, &gorpcproto.ExecuteFetchArgs{Query: query, MaxRows: maxRows, WantFields: wantFields, DisableBinlogs: disableBinlogs}, &qr, waitTime); err != nil {
		return nil, err
	}
	return &qr, nil
}

//
// Replication related methods
//

func (client *GoRpcTabletManagerConn) SlaveStatus(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_STATUS, "", &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerConn) WaitSlavePosition(tablet *topo.TabletInfo, waitPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, &gorpcproto.WaitSlavePositionArgs{
		Position:    waitPos,
		WaitTimeout: waitTime,
	}, &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerConn) MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	var rp myproto.ReplicationPosition
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_MASTER_POSITION, "", &rp, waitTime); err != nil {
		return rp, err
	}
	return rp, nil
}

func (client *GoRpcTabletManagerConn) ReparentPosition(tablet *topo.TabletInfo, rp *myproto.ReplicationPosition, waitTime time.Duration) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_REPARENT_POSITION, rp, &rsd, waitTime); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerConn) StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_SLAVE, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) StopSlaveMinimum(tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	var status myproto.ReplicationStatus
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, &gorpcproto.StopSlaveMinimumArgs{
		Position: minPos,
		WaitTime: waitTime,
	}, &status, waitTime); err != nil {
		return nil, err
	}
	return &status, nil
}

func (client *GoRpcTabletManagerConn) StartSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_START_SLAVE, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) TabletExternallyReparented(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) ([]string, error) {
	var sl gorpcproto.GetSlavesReply
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_GET_SLAVES, "", &sl, waitTime); err != nil {
		return nil, err
	}
	return sl.Addrs, nil
}

func (client *GoRpcTabletManagerConn) WaitBlpPosition(tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, &gorpcproto.WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: waitTime,
	}, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) StopBlp(tablet *topo.TabletInfo, waitTime time.Duration) (*blproto.BlpPositionList, error) {
	var bpl blproto.BlpPositionList
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_STOP_BLP, "", &bpl, waitTime); err != nil {
		return nil, err
	}
	return &bpl, nil
}

func (client *GoRpcTabletManagerConn) StartBlp(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_START_BLP, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) RunBlpUntil(tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
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

func (client *GoRpcTabletManagerConn) DemoteMaster(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_DEMOTE_MASTER, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) PromoteSlave(tablet *topo.TabletInfo, waitTime time.Duration) (*actionnode.RestartSlaveData, error) {
	var rsd actionnode.RestartSlaveData
	if err := client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_PROMOTE_SLAVE, "", &rsd, waitTime); err != nil {
		return nil, err
	}
	return &rsd, nil
}

func (client *GoRpcTabletManagerConn) SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, "", &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) RestartSlave(tablet *topo.TabletInfo, rsd *actionnode.RestartSlaveData, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_RESTART_SLAVE, rsd, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) SlaveWasRestarted(tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, &noOutput, waitTime)
}

func (client *GoRpcTabletManagerConn) BreakSlaves(tablet *topo.TabletInfo, waitTime time.Duration) error {
	var noOutput rpc.UnusedResponse
	return client.rpcCallTablet(tablet, actionnode.TABLET_ACTION_BREAK_SLAVES, "", &noOutput, waitTime)
}
