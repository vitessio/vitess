// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmserver

import (
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpcproto"
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletManager is the Go RPC implementation of the RPC service
type TabletManager struct {
	// implementation of the agent to call
	agent tabletmanager.RpcAgent
}

//
// Various read-only methods
//

func (tm *TabletManager) Ping(context *rpcproto.Context, args, reply *string) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_PING, args, reply, func() error {
		*reply = tm.agent.Ping(*args)
		return nil
	})
}

func (tm *TabletManager) Sleep(context *rpcproto.Context, args *time.Duration, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SLEEP, args, reply, true, func() error {
		tm.agent.Sleep(*args)
		return nil
	})
}

func (tm *TabletManager) ExecuteHook(context *rpcproto.Context, args *hook.Hook, reply *hook.HookResult) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_EXECUTE_HOOK, args, reply, true, func() error {
		*reply = *tm.agent.ExecuteHook(args)
		return nil
	})
}

func (tm *TabletManager) GetSchema(context *rpcproto.Context, args *gorpcproto.GetSchemaArgs, reply *myproto.SchemaDefinition) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_GET_SCHEMA, args, reply, func() error {
		sd, err := tm.agent.GetSchema(args.Tables, args.ExcludeTables, args.IncludeViews)
		if err == nil {
			*reply = *sd
		}
		return err
	})
}

func (tm *TabletManager) GetPermissions(context *rpcproto.Context, args *rpc.UnusedRequest, reply *myproto.Permissions) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_GET_PERMISSIONS, args, reply, func() error {
		p, err := tm.agent.GetPermissions()
		if err == nil {
			*reply = *p
		}
		return err
	})
}

//
// Various read-write methods
//

func (tm *TabletManager) SetReadOnly(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SET_RDONLY, args, reply, true, func() error {
		return tm.agent.SetReadOnly(true)
	})
}

func (tm *TabletManager) SetReadWrite(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SET_RDWR, args, reply, true, func() error {
		return tm.agent.SetReadOnly(false)
	})
}

func (tm *TabletManager) ChangeType(context *rpcproto.Context, args *topo.TabletType, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_CHANGE_TYPE, args, reply, true, func() error {
		return tm.agent.ChangeType(*args)
	})
}

func (tm *TabletManager) Scrap(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SCRAP, args, reply, true, func() error {
		return tm.agent.Scrap()
	})
}

func (tm *TabletManager) RefreshState(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_REFRESH_STATE, args, reply, true, func() error {
		tm.agent.RefreshState()
		return nil
	})
}

func (tm *TabletManager) RunHealthCheck(context *rpcproto.Context, args *topo.TabletType, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_RUN_HEALTH_CHECK, args, reply, func() error {
		tm.agent.RunHealthCheck(*args)
		return nil
	})
}

func (tm *TabletManager) ReloadSchema(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_RELOAD_SCHEMA, args, reply, true, func() error {
		tm.agent.ReloadSchema()
		return nil
	})
}

func (tm *TabletManager) PreflightSchema(context *rpcproto.Context, args *string, reply *myproto.SchemaChangeResult) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, args, reply, true, func() error {
		scr, err := tm.agent.PreflightSchema(*args)
		if err == nil {
			*reply = *scr
		}
		return err
	})
}

func (tm *TabletManager) ApplySchema(context *rpcproto.Context, args *myproto.SchemaChange, reply *myproto.SchemaChangeResult) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_APPLY_SCHEMA, args, reply, true, func() error {
		scr, err := tm.agent.ApplySchema(args)
		if err == nil {
			*reply = *scr
		}
		return err
	})
}

func (tm *TabletManager) ExecuteFetch(context *rpcproto.Context, args *gorpcproto.ExecuteFetchArgs, reply *mproto.QueryResult) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_EXECUTE_FETCH, args, reply, func() error {
		qr, err := tm.agent.ExecuteFetch(args.Query, args.MaxRows, args.WantFields, args.DisableBinlogs)
		if err == nil {
			*reply = *qr
		}
		return err
	})
}

//
// Replication related methods
//

func (tm *TabletManager) SlaveStatus(context *rpcproto.Context, args *rpc.UnusedRequest, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_SLAVE_STATUS, args, reply, func() error {
		status, err := tm.agent.SlaveStatus()
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) WaitSlavePosition(context *rpcproto.Context, args *gorpcproto.WaitSlavePositionArgs, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, true, func() error {
		status, err := tm.agent.WaitSlavePosition(args.Position, args.WaitTimeout)
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) MasterPosition(context *rpcproto.Context, args *rpc.UnusedRequest, reply *myproto.ReplicationPosition) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_MASTER_POSITION, args, reply, func() error {
		position, err := tm.agent.MasterPosition()
		if err == nil {
			*reply = position
		}
		return err
	})
}

func (tm *TabletManager) ReparentPosition(context *rpcproto.Context, args *myproto.ReplicationPosition, reply *actionnode.RestartSlaveData) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_REPARENT_POSITION, args, reply, func() error {
		rsd, err := tm.agent.ReparentPosition(args)
		if err == nil {
			*reply = *rsd
		}
		return err
	})
}

func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_STOP_SLAVE, args, reply, true, func() error {
		return tm.agent.StopSlave()
	})
}

func (tm *TabletManager) StopSlaveMinimum(context *rpcproto.Context, args *gorpcproto.StopSlaveMinimumArgs, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, args, reply, true, func() error {
		status, err := tm.agent.StopSlaveMinimum(args.Position, args.WaitTime)
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) StartSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_START_SLAVE, args, reply, true, func() error {
		return tm.agent.StartSlave()
	})
}

func (tm *TabletManager) TabletExternallyReparented(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	// TODO(alainjobart) we should forward the RPC deadline from
	// the original gorpc call. Until we support that, use a
	// reasonnable hard-coded value.
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, args, reply, false, func() error {
		return tm.agent.TabletExternallyReparented(30 * time.Second)
	})
}

func (tm *TabletManager) GetSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *gorpcproto.GetSlavesReply) error {
	return tm.agent.RpcWrap(context, actionnode.TABLET_ACTION_GET_SLAVES, args, reply, func() error {
		var err error
		reply.Addrs, err = tm.agent.GetSlaves()
		return err
	})
}

func (tm *TabletManager) WaitBlpPosition(context *rpcproto.Context, args *gorpcproto.WaitBlpPositionArgs, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, args, reply, true, func() error {
		return tm.agent.WaitBlpPosition(&args.BlpPosition, args.WaitTimeout)
	})
}

func (tm *TabletManager) StopBlp(context *rpcproto.Context, args *rpc.UnusedRequest, reply *blproto.BlpPositionList) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_STOP_BLP, args, reply, true, func() error {
		positions, err := tm.agent.StopBlp()
		if err == nil {
			*reply = *positions
		}
		return err
	})
}

func (tm *TabletManager) StartBlp(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_START_BLP, args, reply, true, func() error {
		return tm.agent.StartBlp()
	})
}

func (tm *TabletManager) RunBlpUntil(context *rpcproto.Context, args *gorpcproto.RunBlpUntilArgs, reply *myproto.ReplicationPosition) error {
	return tm.agent.RpcWrapLock(context, actionnode.TABLET_ACTION_RUN_BLP_UNTIL, args, reply, true, func() error {
		position, err := tm.agent.RunBlpUntil(args.BlpPositionList, args.WaitTimeout)
		if err == nil {
			*reply = *position
		}
		return err
	})
}

//
// Reparenting related functions
//

func (tm *TabletManager) DemoteMaster(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_DEMOTE_MASTER, args, reply, true, func() error {
		return tm.agent.DemoteMaster()
	})
}

func (tm *TabletManager) PromoteSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *actionnode.RestartSlaveData) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_PROMOTE_SLAVE, args, reply, true, func() error {
		rsd, err := tm.agent.PromoteSlave()
		if err == nil {
			*reply = *rsd
		}
		return err
	})
}

func (tm *TabletManager) SlaveWasPromoted(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, args, reply, true, func() error {
		return tm.agent.SlaveWasPromoted()
	})
}

func (tm *TabletManager) RestartSlave(context *rpcproto.Context, args *actionnode.RestartSlaveData, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_RESTART_SLAVE, args, reply, true, func() error {
		return tm.agent.RestartSlave(args)
	})
}

func (tm *TabletManager) SlaveWasRestarted(context *rpcproto.Context, args *actionnode.SlaveWasRestartedArgs, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, reply, true, func() error {
		return tm.agent.SlaveWasRestarted(args)
	})
}

func (tm *TabletManager) BreakSlaves(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_BREAK_SLAVES, args, reply, true, func() error {
		return tm.agent.BreakSlaves()
	})
}

// backup related methods

func (tm *TabletManager) Snapshot(context *rpcproto.Context, args *actionnode.SnapshotArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SNAPSHOT, args, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewChannelLogger(10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for e := range logger {
				ssr := &gorpcproto.SnapshotStreamingReply{
					Log: &e,
				}
				// Note we don't interrupt the loop here, as
				// we still need to flush and finish the
				// command, even if the channel to the client
				// has been broken. We'll just keep trying to send.
				sendReply(ssr)
			}
			wg.Done()
		}()

		sr, err := tm.agent.Snapshot(args, logger)
		close(logger)
		wg.Wait()
		if err != nil {
			return err
		}
		ssr := &gorpcproto.SnapshotStreamingReply{
			Result: sr,
		}
		sendReply(ssr)
		return nil
	})
}

func (tm *TabletManager) SnapshotSourceEnd(context *rpcproto.Context, args *actionnode.SnapshotSourceEndArgs, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, args, reply, true, func() error {
		return tm.agent.SnapshotSourceEnd(args)
	})
}

func (tm *TabletManager) ReserveForRestore(context *rpcproto.Context, args *actionnode.ReserveForRestoreArgs, reply *rpc.UnusedResponse) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, args, reply, true, func() error {
		return tm.agent.ReserveForRestore(args)
	})
}

func (tm *TabletManager) Restore(context *rpcproto.Context, args *actionnode.RestoreArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_RESTORE, args, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewChannelLogger(10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for e := range logger {
				// Note we don't interrupt the loop here, as
				// we still need to flush and finish the
				// command, even if the channel to the client
				// has been broken. We'll just keep trying to send.
				sendReply(&e)
			}
			wg.Done()
		}()

		err := tm.agent.Restore(args, logger)
		close(logger)
		wg.Wait()
		return err
	})
}

func (tm *TabletManager) MultiSnapshot(context *rpcproto.Context, args *actionnode.MultiSnapshotArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_MULTI_SNAPSHOT, args, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewChannelLogger(10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for e := range logger {
				ssr := &gorpcproto.MultiSnapshotStreamingReply{
					Log: &e,
				}
				// Note we don't interrupt the loop here, as
				// we still need to flush and finish the
				// command, even if the channel to the client
				// has been broken. We'll just keep trying to send.
				sendReply(ssr)
			}
			wg.Done()
		}()

		sr, err := tm.agent.MultiSnapshot(args, logger)
		close(logger)
		wg.Wait()
		if err != nil {
			return err
		}
		ssr := &gorpcproto.MultiSnapshotStreamingReply{
			Result: sr,
		}
		sendReply(ssr)
		return nil
	})
}

func (tm *TabletManager) MultiRestore(context *rpcproto.Context, args *actionnode.MultiRestoreArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(context, actionnode.TABLET_ACTION_MULTI_RESTORE, args, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewChannelLogger(10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for e := range logger {
				// Note we don't interrupt the loop here, as
				// we still need to flush and finish the
				// command, even if the channel to the client
				// has been broken. We'll just keep trying to send.
				sendReply(&e)
			}
			wg.Done()
		}()

		err := tm.agent.MultiRestore(args, logger)
		close(logger)
		wg.Wait()
		return err
	})
}

// registration glue

func init() {
	tabletmanager.RegisterQueryServices = append(tabletmanager.RegisterQueryServices, func(agent *tabletmanager.ActionAgent) {
		servenv.Register("tabletmanager", &TabletManager{agent})
	})
}

// RegisterForTest will register the RPC, to be used by test instances only
func RegisterForTest(server *rpcplus.Server, agent *tabletmanager.ActionAgent) {
	server.Register(&TabletManager{agent})
}
