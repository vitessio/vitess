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
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletManager is the Go RPC implementation of the RPC service
type TabletManager struct {
	tabletManager
}

var TabletManagerRpcService *TabletManager

func init() {
	registerQueryServices = append(registerQueryServices, registerBsonQueryService)
}

func registerBsonQueryService(agent *ActionAgent, mysqld *mysqlctl.Mysqld) {
	if TabletManagerRpcService != nil {
		log.Warningf("RPC service already up %v", TabletManagerRpcService)
		return
	}
	TabletManagerRpcService = &TabletManager{tabletManager{agent, mysqld}}
	rpcwrap.RegisterAuthenticated(TabletManagerRpcService)
}

//
// Various read-only methods
//

func (tm *TabletManager) Ping(context *rpcproto.Context, args, reply *string) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_PING, args, reply, func() error {
		*reply = *args
		return nil
	})
}

type GetSchemaArgs struct {
	Tables       []string
	IncludeViews bool
}

func (tm *TabletManager) GetSchema(context *rpcproto.Context, args *GetSchemaArgs, reply *myproto.SchemaDefinition) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_GET_SCHEMA, args, reply, func() error {
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
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_GET_PERMISSIONS, args, reply, func() error {
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
	return tm.rpcWrapLockAction(context.RemoteAddr, actionnode.TABLET_ACTION_CHANGE_TYPE, args, reply, func() error {
		return ChangeType(tm.agent.ts, tm.agent.tabletAlias, *args, true /*runHooks*/)
	})
}

type SetBlacklistedTablesArgs struct {
	Tables []string
}

func (tm *TabletManager) SetBlacklistedTables(context *rpcproto.Context, args *SetBlacklistedTablesArgs, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context.RemoteAddr, actionnode.TABLET_ACTION_SET_BLACKLISTED_TABLES, args, reply, func() error {
		return SetBlacklistedTables(tm.agent.ts, tm.agent.tabletAlias, args.Tables)
	})
}

//
// Replication related methods
//

func (tm *TabletManager) SlavePosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *myproto.ReplicationPosition) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_SLAVE_POSITION, args, reply, func() error {
		position, err := tm.mysqld.SlaveStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

type SlavePositionReq struct {
	ReplicationPosition myproto.ReplicationPosition
	WaitTimeout         time.Duration // pass in zero to wait indefinitely
}

func (tm *TabletManager) WaitSlavePosition(context *rpcproto.Context, args *SlavePositionReq, reply *myproto.ReplicationPosition) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, func() error {
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

func (tm *TabletManager) MasterPosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *myproto.ReplicationPosition) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_MASTER_POSITION, args, reply, func() error {
		position, err := tm.mysqld.MasterStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_STOP_SLAVE, args, reply, func() error {
		return tm.mysqld.StopSlave(map[string]string{"TABLET_ALIAS": tm.agent.tabletAlias.String()})
	})
}

type StopSlaveMinimumArgs struct {
	GroupdId int64
	WaitTime time.Duration
}

func (tm *TabletManager) StopSlaveMinimum(context *rpcproto.Context, args *StopSlaveMinimumArgs, reply *myproto.ReplicationPosition) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, args, reply, func() error {
		if err := tm.mysqld.WaitForMinimumReplicationPosition(args.GroupdId, args.WaitTime); err != nil {
			return err
		}
		position, err := tm.mysqld.SlaveStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

func (tm *TabletManager) StartSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_START_SLAVE, args, reply, func() error {
		return tm.mysqld.StartSlave(map[string]string{"TABLET_ALIAS": tm.agent.tabletAlias.String()})
	})
}

func (tm *TabletManager) GetSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *SlaveList) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_GET_SLAVES, args, reply, func() error {
		var err error
		reply.Addrs, err = tm.mysqld.FindSlaves()
		return err
	})
}

type WaitBlpPositionArgs struct {
	BlpPosition mysqlctl.BlpPosition
	WaitTimeout time.Duration
}

func (tm *TabletManager) WaitBlpPosition(context *rpcproto.Context, args *WaitBlpPositionArgs, reply *rpc.UnusedResponse) error {
	return tm.rpcWrap(context.RemoteAddr, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, args, reply, func() error {
		return tm.mysqld.WaitBlpPos(&args.BlpPosition, args.WaitTimeout)
	})
}

func (tm *TabletManager) StopBlp(context *rpcproto.Context, args *rpc.UnusedRequest, reply *mysqlctl.BlpPositionList) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_STOP_BLP, args, reply, func() error {
		if tm.agent.BinlogPlayerMap == nil {
			return fmt.Errorf("No BinlogPlayerMap configured")
		}
		tm.agent.BinlogPlayerMap.Stop()
		positions, err := tm.agent.BinlogPlayerMap.BlpPositionList()
		if err != nil {
			return err
		}
		*reply = *positions
		return nil
	})
}

func (tm *TabletManager) StartBlp(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_START_BLP, args, reply, func() error {
		if tm.agent.BinlogPlayerMap == nil {
			return fmt.Errorf("No BinlogPlayerMap configured")
		}
		tm.agent.BinlogPlayerMap.Start()
		return nil
	})
}

type RunBlpUntilArgs struct {
	BlpPositionList *mysqlctl.BlpPositionList
	WaitTimeout     time.Duration
}

func (tm *TabletManager) RunBlpUntil(context *rpcproto.Context, args *RunBlpUntilArgs, reply *myproto.ReplicationPosition) error {
	return tm.rpcWrapLock(context.RemoteAddr, actionnode.TABLET_ACTION_RUN_BLP_UNTIL, args, reply, func() error {
		if tm.agent.BinlogPlayerMap == nil {
			return fmt.Errorf("No BinlogPlayerMap configured")
		}
		if err := tm.agent.BinlogPlayerMap.RunUntil(args.BlpPositionList, args.WaitTimeout); err != nil {
			return err
		}
		position, err := tm.mysqld.MasterStatus()
		if err == nil {
			*reply = *position
		}
		return err
	})
}

//
// Reparenting related functions
//

func (tm *TabletManager) SlaveWasPromoted(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context.RemoteAddr, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, args, reply, func() error {
		return slaveWasPromoted(tm.agent.ts, tm.mysqld, tm.agent.tabletAlias)
	})
}

func (tm *TabletManager) SlaveWasRestarted(context *rpcproto.Context, args *actionnode.SlaveWasRestartedArgs, reply *rpc.UnusedResponse) error {
	return tm.rpcWrapLockAction(context.RemoteAddr, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, reply, func() error {
		return slaveWasRestarted(tm.agent.ts, tm.mysqld, tm.agent.tabletAlias, args)
	})
}
