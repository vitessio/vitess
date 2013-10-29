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
	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the RPC methods for the tablet manager.

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

func (tm *TabletManager) rpcWrapper(context *rpcproto.Context, name string, args, reply interface{}, f func() error, lock, runAfterAction, reloadSchema bool) (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("TabletManager.%v(%v) panic: %v", name, args, x)
			err = fmt.Errorf("caught panic during %v: %v", name, x)
		}
	}()

	if lock {
		beforeLock := time.Now()
		tm.agent.actionMutex.Lock()
		defer tm.agent.actionMutex.Unlock()
		if time.Now().Sub(beforeLock) > rpcTimeout {
			return fmt.Errorf("server timeout for " + name)
		}
	}

	if err = f(); err != nil {
		log.Warningf("TabletManager.%v(%v)(from %v) error: %v", name, args, context.RemoteAddr, err.Error())
		return fmt.Errorf("TabletManager.%v on %v error: %v", name, tm.agent.tabletAlias, err)
	}
	log.Infof("TabletManager.%v(%v)(from %v): %v", name, args, context.RemoteAddr, reply)
	if runAfterAction {
		tm.agent.afterAction("RPC("+name+")", reloadSchema)
	}
	return
}

// There are multiple kinds of actions:
// 1 - read-only actions that can be executed in parallel.
// 2 - read-write actions that change something, and need to take the
//     action lock.
// 3 - read-write actions that need to take the action lock, and also
//     need to reload the tablet state.
// 4 - read-write actions that need to take the action lock, need to
//     reload the tablet state, and reload the schema afterwards.

func (tm *TabletManager) rpcWrap(context *rpcproto.Context, name string, args, reply interface{}, f func() error) error {
	return tm.rpcWrapper(context, name, args, reply, f,
		false /*lock*/, false /*runAfterAction*/, false /*reloadSchema*/)
}

func (tm *TabletManager) rpcWrapLock(context *rpcproto.Context, name string, args, reply interface{}, f func() error) error {
	return tm.rpcWrapper(context, name, args, reply, f,
		true /*lock*/, false /*runAfterAction*/, false /*reloadSchema*/)
}

func (tm *TabletManager) rpcWrapLockAction(context *rpcproto.Context, name string, args, reply interface{}, f func() error) error {
	return tm.rpcWrapper(context, name, args, reply, f,
		true /*lock*/, true /*runAfterAction*/, false /*reloadSchema*/)
}

func (tm *TabletManager) rpcWrapLockActionSchema(context *rpcproto.Context, name string, args, reply interface{}, f func() error) error {
	return tm.rpcWrapper(context, name, args, reply, f,
		true /*lock*/, true /*runAfterAction*/, true /*reloadSchema*/)
}

//
// Various read-only methods
//

func (tm *TabletManager) Ping(context *rpcproto.Context, args, reply *string) error {
	return tm.rpcWrap(context, TABLET_ACTION_PING, args, reply, func() error {
		*reply = *args
		return nil
	})
}

func (tm *TabletManager) GetSchema(context *rpcproto.Context, args *GetSchemaArgs, reply *mysqlctl.SchemaDefinition) error {
	return tm.rpcWrap(context, TABLET_ACTION_GET_SCHEMA, args, reply, func() error {
		// read the tablet to get the dbname
		tablet, err := tm.agent.ts.GetTablet(tm.agent.tabletAlias)
		if err != nil {
			return err
		}

		// and get the schema
		sd, err := tm.mysqld.GetSchema(tablet.DbName(), args.Tables, args.IncludeViews)
		if err == nil {
			*reply = *sd
		}
		return err
	})
}

func (tm *TabletManager) GetPermissions(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.Permissions) error {
	return tm.rpcWrap(context, TABLET_ACTION_GET_PERMISSIONS, args, reply, func() error {
		p, err := tm.mysqld.GetPermissions()
		if err == nil {
			*reply = *p
		}
		return err
	})
}

//
// Various read-write methods
//

func (tm *TabletManager) ChangeType(context *rpcproto.Context, args *topo.TabletType, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context, TABLET_ACTION_CHANGE_TYPE, args, reply, func() error {
		return ChangeType(tm.agent.ts, tm.agent.tabletAlias, *args, true /*runHooks*/)
	})
}

//
// Replication related methods
//

func (tm *TabletManager) SlavePosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) error {
	return tm.rpcWrap(context, TABLET_ACTION_SLAVE_POSITION, args, reply, func() error {
		position, err := tm.mysqld.SlaveStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

func (tm *TabletManager) WaitSlavePosition(context *rpcproto.Context, args *SlavePositionReq, reply *mysqlctl.ReplicationPosition) error {
	return tm.rpcWrap(context, TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, func() error {
		if err := tm.mysqld.WaitMasterPos(&args.ReplicationPosition, args.WaitTimeout); err != nil {
			return err
		}

		position, err := tm.mysqld.SlaveStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

func (tm *TabletManager) MasterPosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) error {
	return tm.rpcWrap(context, TABLET_ACTION_MASTER_POSITION, args, reply, func() error {
		position, err := tm.mysqld.MasterStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLock(context, TABLET_ACTION_STOP_SLAVE, args, reply, func() error {
		return tm.mysqld.StopSlave(map[string]string{"TABLET_ALIAS": tm.agent.tabletAlias.String()})
	})
}

func (tm *TabletManager) GetSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *SlaveList) error {
	return tm.rpcWrap(context, TABLET_ACTION_GET_SLAVES, args, reply, func() error {
		var err error
		reply.Addrs, err = tm.mysqld.FindSlaves()
		return err
	})
}

type WaitBlpPositionArgs struct {
	BlpPosition mysqlctl.BlpPosition
	WaitTimeout int
}

func (tm *TabletManager) WaitBlpPosition(context *rpcproto.Context, args *WaitBlpPositionArgs, reply *rpc.UnusedResponse) error {
	return tm.rpcWrap(context, TABLET_ACTION_WAIT_BLP_POSITION, args, reply, func() error {
		return tm.mysqld.WaitBlpPos(&args.BlpPosition, args.WaitTimeout)
	})
}

//
// Reparenting related functions
//

func (tm *TabletManager) SlaveWasPromoted(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context, TABLET_ACTION_SLAVE_WAS_PROMOTED, args, reply, func() error {
		return slaveWasPromoted(tm.agent.ts, tm.agent.tabletAlias)
	})
}

func (tm *TabletManager) SlaveWasRestarted(context *rpcproto.Context, args *SlaveWasRestartedData, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context, TABLET_ACTION_SLAVE_WAS_RESTARTED, args, reply, func() error {
		return slaveWasRestarted(tm.agent.ts, tm.mysqld, tm.agent.tabletAlias, args)
	})
}
