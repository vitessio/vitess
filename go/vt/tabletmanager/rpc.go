// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/rpc"
)

// This file contains the RPC methods for the tablet manager.
// There are multiple kinds of actions:
// 1 - read-only actions that can be executed in parallel.
// 2 - read-write actions that change something, and need to take the
//     action lock (they should Lock and Unlock agent.actionMutex)
// 3 - read-write actions that also need to reload the tablet state.
//     (they should use both actionMutex lock and run
//     agent.afterAction() if the action is successful, using wrapErrForAction)
// All actions are type 1 unless otherwise noted.

// rpcTimeout is used for timing out the queries on the server in a
// reasonable amount of time. The actions are stored in the
// topo.Server, and if the client goes away, it cleans up the action
// node, and the server doesn't do the action. In the RPC case, if the
// client goes away (while waiting on the action mutex), the server
// won't know, and may still execute the RPC call at a later time.
// To prevent that, if it takes more than rpcTimeout to take the action mutex,
// we return an error to the caller.
const rpcTimeout = time.Second * 30

//
// Utility functions for RPC service
//

// we keep track of the agent so we can use its tabletAlias, ts, ...
type TabletManager struct {
	agent  *ActionAgent
	mysqld *mysqlctl.Mysqld
}

var TabletManagerRpcService *TabletManager

func (agent *ActionAgent) RegisterQueryService(mysqld *mysqlctl.Mysqld) {
	if TabletManagerRpcService != nil {
		log.Warningf("RPC service already up %v", TabletManagerRpcService)
		return
	}
	TabletManagerRpcService = &TabletManager{agent, mysqld}
	rpcwrap.RegisterAuthenticated(TabletManagerRpcService)
}

// wrapErr is to use with Type 1 and Type 2 actions.
func (tm *TabletManager) wrapErr(context *rpcproto.Context, name string, args interface{}, reply interface{}, err error) error {
	if err != nil {
		log.Warningf("TabletManager.%v(%v)(from %v) error: %v", name, args, context.RemoteAddr, err.Error())
		return fmt.Errorf("%v (on %v)", err, tm.agent.tabletAlias)
	}
	log.Infof("TabletManager.%v(%v)(from %v): %v", name, args, context.RemoteAddr, reply)
	return nil
}

// wrapErrForAction is to use with type 3 actions.
func (tm *TabletManager) wrapErrForAction(context *rpcproto.Context, name string, args interface{}, reply interface{}, reloadSchema bool, err error) error {
	err = tm.wrapErr(context, name, args, reply, err)
	if err == nil {
		tm.agent.afterAction("RPC("+name+")", reloadSchema)
	}
	return err
}

//
// Various read-only methods
//

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

//
// Replication related methods
//

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

// StopSlave is a Type 2 action: takes the action lock, but doesn't
// update tablet state
func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	beforeLock := time.Now()
	tm.agent.actionMutex.Lock()
	defer tm.agent.actionMutex.Unlock()
	if time.Now().Sub(beforeLock) > rpcTimeout {
		return fmt.Errorf("server timeout for StopSlave")
	}

	return tm.wrapErr(context, TABLET_ACTION_STOP_SLAVE, args, reply, tm.mysqld.StopSlave(map[string]string{"TABLET_ALIAS": tm.agent.tabletAlias.String()}))
}

func (tm *TabletManager) GetSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *SlaveList) (err error) {
	reply.Addrs, err = tm.mysqld.FindSlaves()
	return tm.wrapErr(context, TABLET_ACTION_GET_SLAVES, args, reply, err)
}

type WaitBlpPositionArgs struct {
	BlpPosition mysqlctl.BlpPosition
	WaitTimeout int
}

func (tm *TabletManager) WaitBlpPosition(context *rpcproto.Context, args *WaitBlpPositionArgs, reply *rpc.UnusedResponse) error {
	return tm.wrapErr(context, TABLET_ACTION_WAIT_BLP_POSITION, args, reply, tm.mysqld.WaitBlpPos(&args.BlpPosition, args.WaitTimeout))
}

//
// Reparenting related functions
//

// SlaveWasPromoted is a Type 3 action: takes the action lock, and
// update tablet state
func (tm *TabletManager) SlaveWasPromoted(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	beforeLock := time.Now()
	tm.agent.actionMutex.Lock()
	defer tm.agent.actionMutex.Unlock()
	if time.Now().Sub(beforeLock) > rpcTimeout {
		return fmt.Errorf("server timeout for SlaveWasPromoted")
	}

	return tm.wrapErrForAction(context, TABLET_ACTION_SLAVE_WAS_PROMOTED, args, reply, false /*reloadSchema*/, slaveWasPromoted(tm.agent.ts, tm.agent.tabletAlias))
}

// SlaveWasRestarted is a Type 3 action: takes the action lock, and
// update tablet state
func (tm *TabletManager) SlaveWasRestarted(context *rpcproto.Context, args *SlaveWasRestartedData, reply *rpc.UnusedResponse) error {
	beforeLock := time.Now()
	tm.agent.actionMutex.Lock()
	defer tm.agent.actionMutex.Unlock()
	if time.Now().Sub(beforeLock) > rpcTimeout {
		return fmt.Errorf("server timeout for SlaveWasRestarted")
	}

	return tm.wrapErrForAction(context, TABLET_ACTION_SLAVE_WAS_RESTARTED, args, reply, false /*reloadSchema*/, slaveWasRestarted(tm.agent.ts, tm.mysqld, tm.agent.tabletAlias, args))
}
