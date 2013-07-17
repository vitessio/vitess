// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
	rpcproto "code.google.com/p/vitess/go/rpcwrap/proto"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/rpc"
)

// we keep track of the agent so we can use its tabletAlias, ts, ...
type TabletManager struct {
	agent  *ActionAgent
	mysqld *mysqlctl.Mysqld
}

var TabletManagerRpcService *TabletManager

func (agent *ActionAgent) RegisterQueryService(mysqld *mysqlctl.Mysqld) {
	if TabletManagerRpcService != nil {
		relog.Warning("RPC service already up %v", TabletManagerRpcService)
		return
	}
	TabletManagerRpcService = &TabletManager{agent, mysqld}
	rpcwrap.RegisterAuthenticated(TabletManagerRpcService)
}

func (tm *TabletManager) wrapErr(context *rpcproto.Context, name string, args interface{}, reply interface{}, err error) error {
	if err != nil {
		relog.Warning("TabletManager.%v(%v)(from %v) error: %v", name, args, context.RemoteAddr, err.Error())
		return fmt.Errorf("%v (on %v)", err, tm.agent.tabletAlias)
	}
	relog.Info("TabletManager.%v(%v)(from %v): %v", name, args, context.RemoteAddr, reply)
	return nil
}

func (tm *TabletManager) Ping(context *rpcproto.Context, args, reply *string) error {
	*reply = *args
	return tm.wrapErr(context, TABLET_ACTION_PING, *args, *reply, nil)
}

func (tm *TabletManager) GetSchema(context *rpcproto.Context, args *GetSchemaArgs, reply *mysqlctl.SchemaDefinition) error {
	// read the tablet to get the dbname
	tablet, err := tm.agent.ts.GetTablet(tm.agent.tabletAlias)
	if err != nil {
		return tm.wrapErr(context, TABLET_ACTION_GET_SCHEMA, args, reply, err)
	}

	// and get the schema
	sd, err := tm.mysqld.GetSchema(tablet.DbName(), args.Tables, args.IncludeViews)
	if err == nil {
		*reply = *sd
	}
	return tm.wrapErr(context, TABLET_ACTION_GET_SCHEMA, args, reply, err)
}

func (tm *TabletManager) GetPermissions(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.Permissions) error {
	p, err := tm.mysqld.GetPermissions()
	if err == nil {
		*reply = *p
	}
	return tm.wrapErr(context, TABLET_ACTION_GET_PERMISSIONS, args, reply, err)
}

func (tm *TabletManager) SlavePosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) error {
	position, err := tm.mysqld.SlaveStatus()
	if err == nil {
		*reply = *position
	}
	return tm.wrapErr(context, TABLET_ACTION_SLAVE_POSITION, args, reply, err)
}

func (tm *TabletManager) WaitSlavePosition(context *rpcproto.Context, args *SlavePositionReq, reply *mysqlctl.ReplicationPosition) error {
	if err := tm.mysqld.WaitMasterPos(&args.ReplicationPosition, args.WaitTimeout); err != nil {
		return tm.wrapErr(context, TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, err)
	}

	position, err := tm.mysqld.SlaveStatus()
	if err != nil {
		return tm.wrapErr(context, TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, err)
	}
	*reply = *position
	return tm.wrapErr(context, TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, nil)
}

func (tm *TabletManager) MasterPosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) error {
	position, err := tm.mysqld.MasterStatus()
	if err == nil {
		*reply = *position
	}
	return tm.wrapErr(context, TABLET_ACTION_MASTER_POSITION, args, reply, err)
}

func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.wrapErr(context, TABLET_ACTION_STOP_SLAVE, args, reply, tm.mysqld.StopSlave())
}

func (tm *TabletManager) GetSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *SlaveList) (err error) {
	reply.Addrs, err = tm.mysqld.FindSlaves()
	return tm.wrapErr(context, TABLET_ACTION_GET_SLAVES, args, reply, err)
}
