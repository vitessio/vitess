// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Actions modify the state of a tablet, shard or keyspace.
//
// They are stored in topology server and form a queue. Only the
// lowest action id should be executing at any given time.
//
// The creation, deletion and modifaction of an action node may be used as
// a signal to other components in the system.

package initiator

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// The actor applies individual commands to execute an action read from a node
// in topology server.
//
// The actor signals completion by removing the action node from the topology server.
//
// Errors are written to the action node and must (currently) be resolved by
// hand using zk tools.

var interrupted = make(chan struct{})
var once sync.Once

// In certain cases (vtctl most notably) having SIGINT manifest itself
// as an instant timeout lets us break out cleanly.
func SignalInterrupt() {
	close(interrupted)
}

type InitiatorError string

func (e InitiatorError) Error() string {
	return string(e)
}

type ActionInitiator struct {
	ts  topo.Server
	rpc TabletManagerConn
}

func NewActionInitiator(ts topo.Server, tabletManagerProtocol string) *ActionInitiator {
	f, ok := tabletManagerConnFactories[tabletManagerProtocol]
	if !ok {
		log.Fatalf("No TabletManagerProtocol registered with name %s", tabletManagerProtocol)
	}

	return &ActionInitiator{ts, f(ts)}
}

func (ai *ActionInitiator) writeTabletAction(tabletAlias topo.TabletAlias, node *actionnode.ActionNode) (actionPath string, err error) {
	data := node.SetGuid().ToJson()
	return ai.ts.WriteTabletAction(tabletAlias, data)
}

func (ai *ActionInitiator) Ping(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_PING})
}

func (ai *ActionInitiator) RpcPing(tabletAlias topo.TabletAlias, waitTime time.Duration) error {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return ai.rpc.Ping(tablet, waitTime)
}

func (ai *ActionInitiator) Sleep(tabletAlias topo.TabletAlias, duration time.Duration) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SLEEP, Args: &duration})
}

func (ai *ActionInitiator) ChangeType(tabletAlias topo.TabletAlias, dbType topo.TabletType) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_CHANGE_TYPE, Args: &dbType})
}

func (ai *ActionInitiator) RpcChangeType(tablet *topo.TabletInfo, dbType topo.TabletType, waitTime time.Duration) error {
	return ai.rpc.ChangeType(tablet, dbType, waitTime)
}

func (ai *ActionInitiator) SetBlacklistedTables(tablet *topo.TabletInfo, tables []string, waitTime time.Duration) error {
	return ai.rpc.SetBlacklistedTables(tablet, tables, waitTime)
}

func (ai *ActionInitiator) SetReadOnly(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SET_RDONLY})
}

func (ai *ActionInitiator) SetReadWrite(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SET_RDWR})
}

func (ai *ActionInitiator) DemoteMaster(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_DEMOTE_MASTER})
}

func (ai *ActionInitiator) Snapshot(tabletAlias topo.TabletAlias, args *actionnode.SnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SNAPSHOT, Args: args})
}

func (ai *ActionInitiator) SnapshotSourceEnd(tabletAlias topo.TabletAlias, args *actionnode.SnapshotSourceEndArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END, Args: args})
}

func (ai *ActionInitiator) MultiSnapshot(tabletAlias topo.TabletAlias, args *actionnode.MultiSnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_MULTI_SNAPSHOT, Args: args})
}

func (ai *ActionInitiator) MultiRestore(tabletAlias topo.TabletAlias, args *actionnode.MultiRestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_MULTI_RESTORE, Args: args})
}

func (ai *ActionInitiator) BreakSlaves(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_BREAK_SLAVES})
}

func (ai *ActionInitiator) PromoteSlave(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_PROMOTE_SLAVE})
}

func (ai *ActionInitiator) SlaveWasPromoted(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED})
}

func (ai *ActionInitiator) RpcSlaveWasPromoted(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return ai.rpc.SlaveWasPromoted(tablet, waitTime)
}

func (ai *ActionInitiator) RestartSlave(tabletAlias topo.TabletAlias, args *actionnode.RestartSlaveData) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_RESTART_SLAVE, Args: args})
}

func (ai *ActionInitiator) SlaveWasRestarted(tabletAlias topo.TabletAlias, args *actionnode.SlaveWasRestartedArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED, Args: args})
}

func (ai *ActionInitiator) RpcSlaveWasRestarted(tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs, waitTime time.Duration) error {
	return ai.rpc.SlaveWasRestarted(tablet, args, waitTime)
}

func (ai *ActionInitiator) ReparentPosition(tabletAlias topo.TabletAlias, slavePos *myproto.ReplicationPosition) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_REPARENT_POSITION, Args: slavePos})
}

func (ai *ActionInitiator) MasterPosition(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	return ai.rpc.MasterPosition(tablet, waitTime)
}

func (ai *ActionInitiator) SlavePosition(tablet *topo.TabletInfo, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	return ai.rpc.SlavePosition(tablet, waitTime)
}

func (ai *ActionInitiator) WaitSlavePosition(tablet *topo.TabletInfo, replicationPosition *myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	return ai.rpc.WaitSlavePosition(tablet, replicationPosition, waitTime)
}

func (ai *ActionInitiator) StopSlave(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return ai.rpc.StopSlave(tablet, waitTime)
}

func (ai *ActionInitiator) StopSlaveMinimum(tabletAlias topo.TabletAlias, groupId int64, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return ai.rpc.StopSlaveMinimum(tablet, groupId, waitTime)
}

func (ai *ActionInitiator) StartSlave(tabletAlias topo.TabletAlias, waitTime time.Duration) error {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return ai.rpc.StartSlave(tablet, waitTime)
}

func (ai *ActionInitiator) WaitBlpPosition(tabletAlias topo.TabletAlias, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return ai.rpc.WaitBlpPosition(tablet, blpPosition, waitTime)
}

func (ai *ActionInitiator) StopBlp(tabletAlias topo.TabletAlias, waitTime time.Duration) (*blproto.BlpPositionList, error) {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return ai.rpc.StopBlp(tablet, waitTime)
}

func (ai *ActionInitiator) StartBlp(tabletAlias topo.TabletAlias, waitTime time.Duration) error {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return ai.rpc.StartBlp(tablet, waitTime)
}

func (ai *ActionInitiator) RunBlpUntil(tabletAlias topo.TabletAlias, positions *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return ai.rpc.RunBlpUntil(tablet, positions, waitTime)
}

func (ai *ActionInitiator) ReserveForRestore(dstTabletAlias topo.TabletAlias, args *actionnode.ReserveForRestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(dstTabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE, Args: args})
}

func (ai *ActionInitiator) Restore(dstTabletAlias topo.TabletAlias, args *actionnode.RestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(dstTabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_RESTORE, Args: args})
}

func (ai *ActionInitiator) Scrap(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_SCRAP})
}

func (ai *ActionInitiator) GetSchema(tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool, waitTime time.Duration) (*myproto.SchemaDefinition, error) {
	return ai.rpc.GetSchema(tablet, tables, excludeTables, includeViews, waitTime)
}

func (ai *ActionInitiator) PreflightSchema(tabletAlias topo.TabletAlias, change string) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA, Args: &change})
}

func (ai *ActionInitiator) ApplySchema(tabletAlias topo.TabletAlias, sc *myproto.SchemaChange) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_APPLY_SCHEMA, Args: sc})
}

func (ai *ActionInitiator) ReloadSchema(tablet *topo.TabletInfo, waitTime time.Duration) error {
	return ai.rpc.ReloadSchema(tablet, waitTime)
}

func (ai *ActionInitiator) GetPermissions(tabletAlias topo.TabletAlias, waitTime time.Duration) (*myproto.Permissions, error) {
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return ai.rpc.GetPermissions(tablet, waitTime)
}

func (ai *ActionInitiator) ExecuteHook(tabletAlias topo.TabletAlias, _hook *hook.Hook) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &actionnode.ActionNode{Action: actionnode.TABLET_ACTION_EXECUTE_HOOK, Args: _hook})
}

func (ai *ActionInitiator) GetSlaves(tablet *topo.TabletInfo, waitTime time.Duration) ([]string, error) {
	return ai.rpc.GetSlaves(tablet, waitTime)
}

func (ai *ActionInitiator) WaitForCompletion(actionPath string, waitTime time.Duration) error {
	_, err := WaitForCompletion(ai.ts, actionPath, waitTime)
	return err
}

func (ai *ActionInitiator) WaitForCompletionReply(actionPath string, waitTime time.Duration) (interface{}, error) {
	return WaitForCompletion(ai.ts, actionPath, waitTime)
}

func WaitForCompletion(ts topo.Server, actionPath string, waitTime time.Duration) (interface{}, error) {
	// If there is no duration specified, block for a sufficiently long time
	if waitTime <= 0 {
		waitTime = 24 * time.Hour
	}

	data, err := ts.WaitForTabletAction(actionPath, waitTime, interrupted)
	if err != nil {
		return nil, err
	}

	// parse it
	actionNode, dataErr := actionnode.ActionNodeFromJson(data, "")
	if dataErr != nil {
		return nil, fmt.Errorf("action data error: %v %v %#v", actionPath, dataErr, data)
	} else if actionNode.Error != "" {
		return nil, fmt.Errorf("action failed: %v %v", actionPath, actionNode.Error)
	}

	return actionNode.Reply, nil
}
