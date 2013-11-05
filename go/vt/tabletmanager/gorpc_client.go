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
	RegisterTabletManagerConnFactory("gorpc", func(ts topo.Server) TabletManagerConn {
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
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr, waitTime)
	if err != nil {
		return fmt.Errorf("RPC error for %v: %v", tablet.GetAlias(), err.Error())
	}
	defer rpcClient.Close()

	// do the call in the remaining time
	call := rpcClient.Go("TabletManager."+name, args, reply, nil)
	select {
	case <-timer:
		return fmt.Errorf("Timeout waiting for TabletManager.%v to %v", name, tablet.GetAlias())
	case <-call.Done:
		if call.Error != nil {
			return fmt.Errorf("Remote error for %v: %v", tablet.GetAlias(), call.Error.Error())
		} else {
			return nil
		}
	}
}

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

func (client *GoRpcTabletManagerConn) ChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_CHANGE_TYPE, &dbType, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) SlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_SLAVE_WAS_PROMOTED, rpc.NilRequest, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) SlaveWasRestarted(tablet *topo.TabletInfo, args *SlaveWasRestartedData, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_SLAVE_WAS_RESTARTED, args, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) WaitBlpPosition(tablet *topo.TabletInfo, blpPosition mysqlctl.BlpPosition, waitTime time.Duration) error {
	return client.rpcCallTablet(tablet, TABLET_ACTION_WAIT_BLP_POSITION, &WaitBlpPositionArgs{
		BlpPosition: blpPosition,
		WaitTimeout: int(waitTime / time.Second),
	}, rpc.NilResponse, waitTime)
}

func (client *GoRpcTabletManagerConn) GetSchemaTablet(tablet *topo.TabletInfo, tables []string, includeViews bool, waitTime time.Duration) (*mysqlctl.SchemaDefinition, error) {
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
