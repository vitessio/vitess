// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmserver

import (
	"sync"
	"time"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
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

func (tm *TabletManager) Ping(ctx context.Context, args, reply *string) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_PING, args, reply, func() error {
		*reply = tm.agent.Ping(ctx, *args)
		return nil
	})
}

func (tm *TabletManager) Sleep(ctx context.Context, args *time.Duration, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SLEEP, args, reply, true, func() error {
		tm.agent.Sleep(ctx, *args)
		return nil
	})
}

func (tm *TabletManager) ExecuteHook(ctx context.Context, args *hook.Hook, reply *hook.HookResult) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_EXECUTE_HOOK, args, reply, true, func() error {
		*reply = *tm.agent.ExecuteHook(ctx, args)
		return nil
	})
}

func (tm *TabletManager) GetSchema(ctx context.Context, args *gorpcproto.GetSchemaArgs, reply *myproto.SchemaDefinition) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_GET_SCHEMA, args, reply, func() error {
		sd, err := tm.agent.GetSchema(ctx, args.Tables, args.ExcludeTables, args.IncludeViews)
		if err == nil {
			*reply = *sd
		}
		return err
	})
}

func (tm *TabletManager) GetPermissions(ctx context.Context, args *rpc.Unused, reply *myproto.Permissions) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_GET_PERMISSIONS, args, reply, func() error {
		p, err := tm.agent.GetPermissions(ctx)
		if err == nil {
			*reply = *p
		}
		return err
	})
}

//
// Various read-write methods
//

func (tm *TabletManager) SetReadOnly(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SET_RDONLY, args, reply, true, func() error {
		return tm.agent.SetReadOnly(ctx, true)
	})
}

func (tm *TabletManager) SetReadWrite(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SET_RDWR, args, reply, true, func() error {
		return tm.agent.SetReadOnly(ctx, false)
	})
}

func (tm *TabletManager) ChangeType(ctx context.Context, args *topo.TabletType, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_CHANGE_TYPE, args, reply, true, func() error {
		return tm.agent.ChangeType(ctx, *args)
	})
}

func (tm *TabletManager) Scrap(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SCRAP, args, reply, true, func() error {
		return tm.agent.Scrap(ctx)
	})
}

func (tm *TabletManager) RefreshState(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_REFRESH_STATE, args, reply, true, func() error {
		tm.agent.RefreshState(ctx)
		return nil
	})
}

func (tm *TabletManager) RunHealthCheck(ctx context.Context, args *topo.TabletType, reply *rpc.Unused) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_RUN_HEALTH_CHECK, args, reply, func() error {
		tm.agent.RunHealthCheck(ctx, *args)
		return nil
	})
}

func (tm *TabletManager) ReloadSchema(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_RELOAD_SCHEMA, args, reply, true, func() error {
		tm.agent.ReloadSchema(ctx)
		return nil
	})
}

func (tm *TabletManager) PreflightSchema(ctx context.Context, args *string, reply *myproto.SchemaChangeResult) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, args, reply, true, func() error {
		scr, err := tm.agent.PreflightSchema(ctx, *args)
		if err == nil {
			*reply = *scr
		}
		return err
	})
}

func (tm *TabletManager) ApplySchema(ctx context.Context, args *myproto.SchemaChange, reply *myproto.SchemaChangeResult) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_APPLY_SCHEMA, args, reply, true, func() error {
		scr, err := tm.agent.ApplySchema(ctx, args)
		if err == nil {
			*reply = *scr
		}
		return err
	})
}

func (tm *TabletManager) ExecuteFetch(ctx context.Context, args *gorpcproto.ExecuteFetchArgs, reply *mproto.QueryResult) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_EXECUTE_FETCH, args, reply, func() error {
		qr, err := tm.agent.ExecuteFetch(ctx, args.Query, args.MaxRows, args.WantFields, args.DisableBinlogs)
		if err == nil {
			*reply = *qr
		}
		return err
	})
}

//
// Replication related methods
//

func (tm *TabletManager) SlaveStatus(ctx context.Context, args *rpc.Unused, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_SLAVE_STATUS, args, reply, func() error {
		status, err := tm.agent.SlaveStatus(ctx)
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) WaitSlavePosition(ctx context.Context, args *gorpcproto.WaitSlavePositionArgs, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION, args, reply, true, func() error {
		status, err := tm.agent.WaitSlavePosition(ctx, args.Position, args.WaitTimeout)
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) MasterPosition(ctx context.Context, args *rpc.Unused, reply *myproto.ReplicationPosition) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_MASTER_POSITION, args, reply, func() error {
		position, err := tm.agent.MasterPosition(ctx)
		if err == nil {
			*reply = position
		}
		return err
	})
}

func (tm *TabletManager) ReparentPosition(ctx context.Context, args *myproto.ReplicationPosition, reply *actionnode.RestartSlaveData) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_REPARENT_POSITION, args, reply, func() error {
		rsd, err := tm.agent.ReparentPosition(ctx, args)
		if err == nil {
			*reply = *rsd
		}
		return err
	})
}

func (tm *TabletManager) StopSlave(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_STOP_SLAVE, args, reply, true, func() error {
		return tm.agent.StopSlave(ctx)
	})
}

func (tm *TabletManager) StopSlaveMinimum(ctx context.Context, args *gorpcproto.StopSlaveMinimumArgs, reply *myproto.ReplicationStatus) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM, args, reply, true, func() error {
		status, err := tm.agent.StopSlaveMinimum(ctx, args.Position, args.WaitTime)
		if err == nil {
			*reply = *status
		}
		return err
	})
}

func (tm *TabletManager) StartSlave(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_START_SLAVE, args, reply, true, func() error {
		return tm.agent.StartSlave(ctx)
	})
}

func (tm *TabletManager) TabletExternallyReparented(ctx context.Context, args *gorpcproto.TabletExternallyReparentedArgs, reply *rpc.Unused) error {
	// TODO(alainjobart) we should forward the RPC deadline from
	// the original gorpc call. Until we support that, use a
	// reasonable hard-coded value.
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED, args, reply, false, func() error {
		return tm.agent.TabletExternallyReparented(ctx, args.ExternalID)
	})
}

func (tm *TabletManager) GetSlaves(ctx context.Context, args *rpc.Unused, reply *gorpcproto.GetSlavesReply) error {
	return tm.agent.RpcWrap(ctx, actionnode.TABLET_ACTION_GET_SLAVES, args, reply, func() error {
		var err error
		reply.Addrs, err = tm.agent.GetSlaves(ctx)
		return err
	})
}

func (tm *TabletManager) WaitBlpPosition(ctx context.Context, args *gorpcproto.WaitBlpPositionArgs, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_WAIT_BLP_POSITION, args, reply, true, func() error {
		return tm.agent.WaitBlpPosition(ctx, &args.BlpPosition, args.WaitTimeout)
	})
}

func (tm *TabletManager) StopBlp(ctx context.Context, args *rpc.Unused, reply *blproto.BlpPositionList) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_STOP_BLP, args, reply, true, func() error {
		positions, err := tm.agent.StopBlp(ctx)
		if err == nil {
			*reply = *positions
		}
		return err
	})
}

func (tm *TabletManager) StartBlp(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_START_BLP, args, reply, true, func() error {
		return tm.agent.StartBlp(ctx)
	})
}

func (tm *TabletManager) RunBlpUntil(ctx context.Context, args *gorpcproto.RunBlpUntilArgs, reply *myproto.ReplicationPosition) error {
	return tm.agent.RpcWrapLock(ctx, actionnode.TABLET_ACTION_RUN_BLP_UNTIL, args, reply, true, func() error {
		position, err := tm.agent.RunBlpUntil(ctx, args.BlpPositionList, args.WaitTimeout)
		if err == nil {
			*reply = *position
		}
		return err
	})
}

//
// Reparenting related functions
//

func (tm *TabletManager) DemoteMaster(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_DEMOTE_MASTER, args, reply, true, func() error {
		return tm.agent.DemoteMaster(ctx)
	})
}

func (tm *TabletManager) PromoteSlave(ctx context.Context, args *rpc.Unused, reply *actionnode.RestartSlaveData) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_PROMOTE_SLAVE, args, reply, true, func() error {
		rsd, err := tm.agent.PromoteSlave(ctx)
		if err == nil {
			*reply = *rsd
		}
		return err
	})
}

func (tm *TabletManager) SlaveWasPromoted(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED, args, reply, true, func() error {
		return tm.agent.SlaveWasPromoted(ctx)
	})
}

func (tm *TabletManager) RestartSlave(ctx context.Context, args *actionnode.RestartSlaveData, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_RESTART_SLAVE, args, reply, true, func() error {
		return tm.agent.RestartSlave(ctx, args)
	})
}

func (tm *TabletManager) SlaveWasRestarted(ctx context.Context, args *actionnode.SlaveWasRestartedArgs, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, args, reply, true, func() error {
		return tm.agent.SlaveWasRestarted(ctx, args)
	})
}

func (tm *TabletManager) BreakSlaves(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_BREAK_SLAVES, args, reply, true, func() error {
		return tm.agent.BreakSlaves(ctx)
	})
}

// backup related methods

func (tm *TabletManager) Snapshot(ctx context.Context, args *actionnode.SnapshotArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SNAPSHOT, args, nil, true, func() error {
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

		sr, err := tm.agent.Snapshot(ctx, args, logger)
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

func (tm *TabletManager) SnapshotSourceEnd(ctx context.Context, args *actionnode.SnapshotSourceEndArgs, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, args, reply, true, func() error {
		return tm.agent.SnapshotSourceEnd(ctx, args)
	})
}

func (tm *TabletManager) ReserveForRestore(ctx context.Context, args *actionnode.ReserveForRestoreArgs, reply *rpc.Unused) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, args, reply, true, func() error {
		return tm.agent.ReserveForRestore(ctx, args)
	})
}

func (tm *TabletManager) Restore(ctx context.Context, args *actionnode.RestoreArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_RESTORE, args, nil, true, func() error {
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

		err := tm.agent.Restore(ctx, args, logger)
		close(logger)
		wg.Wait()
		return err
	})
}

func (tm *TabletManager) MultiSnapshot(ctx context.Context, args *actionnode.MultiSnapshotArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_MULTI_SNAPSHOT, args, nil, true, func() error {
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

		sr, err := tm.agent.MultiSnapshot(ctx, args, logger)
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

func (tm *TabletManager) MultiRestore(ctx context.Context, args *actionnode.MultiRestoreArgs, sendReply func(interface{}) error) error {
	return tm.agent.RpcWrapLockAction(ctx, actionnode.TABLET_ACTION_MULTI_RESTORE, args, nil, true, func() error {
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

		err := tm.agent.MultiRestore(ctx, args, logger)
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
