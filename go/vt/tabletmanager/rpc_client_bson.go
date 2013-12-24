// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	RegisterTabletManagerConnFactory("bson", func(ts topo.Server) TabletManagerConn {
		return &GoRpcTabletManagerConn{ts}
	})
}

type GoRpcTabletManagerConn struct {
	ts topo.Server
}

func (client *GoRpcTabletManagerConn) rpcCallTablet(tablet *topo.TabletInfo, name string, args, reply interface{}, waitTime time.Duration) error {

	// create the RPC client, using waitTime as the connect
	// timeout, and starting the overall timeout as well
	timer := time.After(waitTime)
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.GetAddr(), waitTime, nil)
	if err != nil {
		return fmt.Errorf("RPC error for %v: %v", tablet.Alias, err.Error())
	}
	defer rpcClient.Close()

	// do the call in the remaining time
	call := rpcClient.Go("TabletManager."+name, args, reply, nil)
	select {
	case <-timer:
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
	err := client.rpcCallTablet(tablet, TABLET_ACTION_PING, "payload", &result, waitTime)
	if err != nil {
		return err
	}
	if result != "payload" {
		return fmt.Errorf("Bad ping result: %v", result)
	}
	return nil
}

func (client *GoRpcTabletManagerConn) GetSchema(tablet *topo.TabletInfo, tables []string, includeViews bool, waitTime time.Duration) (*mysqlctl.SchemaDefinition, error) {
	var sd mysqlctl.SchemaDefinition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_GET_SCHEMA, &GetSchemaArgs{Tables: tables, IncludeViews: includeViews}, &sd, waitTime); err != nil {
		return nil, err
	}
	return &sd, nil
}

func (client *GoRpcTabletManagerConn) GetPermissions(tablet *topo.TabletInfo, waitTime time.Duration) (*mysqlctl.Permissions, error) {
	var p mysqlctl.Permissions
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_GET_PERMISSIONS, "", &p, waitTime); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Various read-write methods
//

func (client *GoRpcTabletManagerConn) ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_CHANGE_TYPE, &dbType, rpc.NilResponse, waitTime)
}

//
// Replication related methods
//

func (client *GoRpcTabletManagerConn) SlavePosition(tablet *topo.TabletInfo, waitTime time.Duration) (*mysqlctl.ReplicationPosition, error) {
	var rp mysqlctl.ReplicationPosition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_SLAVE_POSITION, "", &rp, waitTime); err != nil {
		return nil, err
	}
	return &rp, nil
}

func (client *GoRpcTabletManagerConn) WaitSlavePosition(tablet *topo.TabletInfo, replicationPosition *mysqlctl.ReplicationPosition, waitTime time.Duration) (*mysqlctl.ReplicationPosition, error) {
	var rp mysqlctl.ReplicationPosition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_WAIT_SLAVE_POSITION, &SlavePositionReq{
		ReplicationPosition: *replicationPosition,
		WaitTimeout:         int(waitTime / time.Second),
	}, &rp, waitTime); err != nil {
		return nil, err
	}
	return &rp, nil
}

func (client *GoRpcTabletManagerConn) MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (*mysqlctl.ReplicationPosition, error) {
	var rp mysqlctl.ReplicationPosition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_MASTER_POSITION, "", &rp, waitTime); err != nil {
		return nil, err
	}
	return &rp, nil
}

func (client *GoRpcTabletManagerConn) StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_STOP_SLAVE, "", rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) StopSlaveMinimum(tablet *topo.TabletInfo, groupId int64, waitTime time.Duration) (*mysqlctl.ReplicationPosition, error) {
	var pos mysqlctl.ReplicationPosition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_STOP_SLAVE_MINIMUM, &StopSlaveMinimumArgs{
		GroupdId: groupId,
		WaitTime: waitTime,
	}, &pos, waitTime); err != nil {
		return nil, err
	}
	return &pos, nil
}

func (client *GoRpcTabletManagerConn) StartSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_START_SLAVE, "", rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) (*SlaveList, error) {
	var sl SlaveList
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_GET_SLAVES, "", &sl, waitTime); err != nil {
		return nil, err
	}
	return &sl, nil
}

func (client *GoRpcTabletManagerConn) WaitBlpPosition(tablet *topo.TabletInfo, blpPosition mysqlctl.BlpPosition, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_WAIT_BLP_POSITION, &WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: int(waitTime / time.Second),
	}, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) StopBlp(tablet *topo.TabletInfo, waitTime time.Duration) (*mysqlctl.BlpPositionList, error) {
	var bpl mysqlctl.BlpPositionList
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_STOP_BLP, "", &bpl, waitTime); err != nil {
		return nil, err
	}
	return &bpl, nil
}

func (client *GoRpcTabletManagerConn) StartBlp(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_START_BLP, "", rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) RunBlpUntil(tablet *topo.TabletInfo, positions *mysqlctl.BlpPositionList, waitTime time.Duration) (*mysqlctl.ReplicationPosition, error) {
	var pos mysqlctl.ReplicationPosition
	if err := client.rpcCallTablet(tablet, TABLET_ACTION_RUN_BLP_UNTIL, &RunBlpUntilArgs{
		BlpPositionList: positions,
		WaitTimeout:     waitTime,
	}, &pos, waitTime); err != nil {
		return nil, err
	}
	return &pos, nil
}

//
// Reparenting related functions
//

func (client *GoRpcTabletManagerConn) SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_SLAVE_WAS_PROMOTED, rpc.NilRequest, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) SlaveWasRestarted(tablet *topo.TabletInfo, args *SlaveWasRestartedData, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_SLAVE_WAS_RESTARTED, args, rpc.NilResponse, waitTime)
}
