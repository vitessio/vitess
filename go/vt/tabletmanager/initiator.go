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

package tabletmanager

import (
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/vt/hook"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/vt/zktopo"
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
	ts topo.Server
}

func NewActionInitiator(ts topo.Server) *ActionInitiator {
	return &ActionInitiator{ts}
}

func actionGuid() string {
	now := time.Now().Format(time.RFC3339)
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	hostname := "unknown"
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	return fmt.Sprintf("%v-%v-%v", now, username, hostname)
}

func (ai *ActionInitiator) writeTabletAction(tabletAlias topo.TabletAlias, node *ActionNode) (actionPath string, err error) {
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	return ai.ts.WriteTabletAction(tabletAlias, data)
}

func (ai *ActionInitiator) rpcCall(tabletAlias topo.TabletAlias, name string, args, reply interface{}, waitTime time.Duration) error {
	// read the tablet from ZK to get the address to connect to
	tablet, err := ai.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return ai.rpcCallTablet(tablet, name, args, reply, waitTime)
}

// TODO(alainjobart) keep a cache of rpcClient by tabletAlias
func (ai *ActionInitiator) rpcCallTablet(tablet *topo.TabletInfo, name string, args, reply interface{}, waitTime time.Duration) error {

	// create the RPC client, using waitTime as the connect
	// timeout, and starting the overall timeout as well
	timer := time.After(waitTime)
	rpcClient, err := bsonrpc.DialHTTP("tcp", tablet.Addr, waitTime)
	if err != nil {
		return fmt.Errorf("RPC error for %v: %v", tablet.Alias(), err.Error())
	}
	defer rpcClient.Close()

	// do the call in the remaining time
	call := rpcClient.Go("TabletManager."+name, args, reply, nil)
	select {
	case <-timer:
		return fmt.Errorf("Timeout waiting for TabletManager.%v to %v", name, tablet.Alias())
	case <-call.Done:
		if call.Error != nil {
			return fmt.Errorf("Remote error for %v: %v", tablet.Alias(), call.Error.Error())
		} else {
			return nil
		}
	}
}

func (ai *ActionInitiator) Ping(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_PING})
}

func (ai *ActionInitiator) RpcPing(tabletAlias topo.TabletAlias, waitTime time.Duration) error {
	var result string
	err := ai.rpcCall(tabletAlias, TABLET_ACTION_PING, "payload", &result, waitTime)
	if err != nil {
		return err
	}
	if result != "payload" {
		return fmt.Errorf("Bad ping result: %v", result)
	}
	return nil
}

func (ai *ActionInitiator) Sleep(tabletAlias topo.TabletAlias, duration time.Duration) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SLEEP, args: &duration})
}

func (ai *ActionInitiator) ChangeType(tabletAlias topo.TabletAlias, dbType topo.TabletType) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_CHANGE_TYPE, args: &dbType})
}

func (ai *ActionInitiator) SetReadOnly(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SET_RDONLY})
}

func (ai *ActionInitiator) SetReadWrite(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SET_RDWR})
}

func (ai *ActionInitiator) DemoteMaster(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_DEMOTE_MASTER})
}

type SnapshotArgs struct {
	Concurrency int
	ServerMode  bool
}

// used by both Snapshot and PartialSnapshot
type SnapshotReply struct {
	ZkParentPath string // XXX
	ParentAlias  topo.TabletAlias
	ManifestPath string

	// these two are only used for ServerMode=true full snapshot
	SlaveStartRequired bool
	ReadOnly           bool
}

type MultiSnapshotReply struct {
	ParentAlias   topo.TabletAlias
	ManifestPaths []string
}

func (ai *ActionInitiator) Snapshot(tabletAlias topo.TabletAlias, args *SnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SNAPSHOT, args: args})
}

type SnapshotSourceEndArgs struct {
	SlaveStartRequired bool
	ReadOnly           bool
}

func (ai *ActionInitiator) SnapshotSourceEnd(tabletAlias topo.TabletAlias, args *SnapshotSourceEndArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SNAPSHOT_SOURCE_END, args: args})
}

type PartialSnapshotArgs struct {
	KeyName     string
	StartKey    key.HexKeyspaceId
	EndKey      key.HexKeyspaceId
	Concurrency int
}

type MultiSnapshotArgs struct {
	KeyName          string
	KeyRanges        []key.KeyRange
	Tables           []string
	Concurrency      int
	SkipSlaveRestart bool
	MaximumFilesize  uint64
}

type MultiRestoreArgs struct {
	SrcTabletAliases       []topo.TabletAlias
	Concurrency            int
	FetchConcurrency       int
	InsertTableConcurrency int
	FetchRetryCount        int
	Strategy               string
}

func (ai *ActionInitiator) PartialSnapshot(tabletAlias topo.TabletAlias, args *PartialSnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_PARTIAL_SNAPSHOT, args: args})
}

func (ai *ActionInitiator) MultiSnapshot(tabletAlias topo.TabletAlias, args *MultiSnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_MULTI_SNAPSHOT, args: args})
}

func (ai *ActionInitiator) RestoreFromMultiSnapshot(tabletAlias topo.TabletAlias, args *MultiRestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_MULTI_RESTORE, args: args})
}

func (ai *ActionInitiator) BreakSlaves(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_BREAK_SLAVES})
}

func (ai *ActionInitiator) PromoteSlave(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_PROMOTE_SLAVE})
}

func (ai *ActionInitiator) SlaveWasPromoted(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SLAVE_WAS_PROMOTED})
}

func (ai *ActionInitiator) RestartSlave(tabletAlias topo.TabletAlias, args *RestartSlaveData) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_RESTART_SLAVE, args: args})
}

type SlaveWasRestartedData struct {
	Parent               topo.TabletAlias
	ExpectedMasterAddr   string
	ExpectedMasterIpAddr string
	ScrapStragglers      bool
}

func (ai *ActionInitiator) SlaveWasRestarted(tabletAlias topo.TabletAlias, args *SlaveWasRestartedData) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SLAVE_WAS_RESTARTED, args: args})
}

func (ai *ActionInitiator) ReparentPosition(tabletAlias topo.TabletAlias, slavePos *mysqlctl.ReplicationPosition) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_REPARENT_POSITION, args: slavePos})
}

func (ai *ActionInitiator) MasterPosition(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_MASTER_POSITION})
}

func (ai *ActionInitiator) SlavePosition(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SLAVE_POSITION})
}

type SlavePositionReq struct {
	ReplicationPosition mysqlctl.ReplicationPosition
	WaitTimeout         int // seconds, zero to wait indefinitely
}

func (ai *ActionInitiator) WaitSlavePosition(tabletAlias topo.TabletAlias, args *SlavePositionReq) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_WAIT_SLAVE_POSITION, args: args})
}

func (ai *ActionInitiator) StopSlave(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_STOP_SLAVE})
}

type ReserveForRestoreArgs struct {
	ZkSrcTabletPath string // XXX
	SrcTabletAlias  topo.TabletAlias
}

func (ai *ActionInitiator) ReserveForRestore(dstTabletAlias topo.TabletAlias, args *ReserveForRestoreArgs) (actionPath string, err error) {
	args.ZkSrcTabletPath = zktopo.TabletPathForAlias(args.SrcTabletAlias) // XXX
	return ai.writeTabletAction(dstTabletAlias, &ActionNode{Action: TABLET_ACTION_RESERVE_FOR_RESTORE, args: args})
}

// used for both Restore and PartialRestore
type RestoreArgs struct {
	ZkSrcTabletPath       string // XXX
	SrcTabletAlias        topo.TabletAlias
	SrcFilePath           string
	ZkParentPath          string // XXX
	ParentAlias           topo.TabletAlias
	FetchConcurrency      int
	FetchRetryCount       int
	WasReserved           bool
	DontWaitForSlaveStart bool
}

func (ai *ActionInitiator) Restore(dstTabletAlias topo.TabletAlias, args *RestoreArgs) (actionPath string, err error) {
	args.ZkSrcTabletPath = zktopo.TabletPathForAlias(args.SrcTabletAlias) // XXX
	args.ZkParentPath = zktopo.TabletPathForAlias(args.ParentAlias)       // XXX
	return ai.writeTabletAction(dstTabletAlias, &ActionNode{Action: TABLET_ACTION_RESTORE, args: args})
}

func (ai *ActionInitiator) PartialRestore(dstTabletAlias topo.TabletAlias, args *RestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(dstTabletAlias, &ActionNode{Action: TABLET_ACTION_PARTIAL_RESTORE, args: args})
}

func (ai *ActionInitiator) Scrap(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_SCRAP})
}

type GetSchemaArgs struct {
	Tables       []string
	IncludeViews bool
}

func (ai *ActionInitiator) GetSchema(tabletAlias topo.TabletAlias, tables []string, includeViews bool) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_GET_SCHEMA, args: &GetSchemaArgs{Tables: tables, IncludeViews: includeViews}})
}

func (ai *ActionInitiator) RpcGetSchemaTablet(tablet *topo.TabletInfo, tables []string, includeViews bool, waitTime time.Duration) (*mysqlctl.SchemaDefinition, error) {
	var sd mysqlctl.SchemaDefinition
	if err := ai.rpcCallTablet(tablet, TABLET_ACTION_GET_SCHEMA, &GetSchemaArgs{Tables: tables, IncludeViews: includeViews}, &sd, waitTime); err != nil {
		return nil, err
	}
	return &sd, nil
}

func (ai *ActionInitiator) PreflightSchema(tabletAlias topo.TabletAlias, change string) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_PREFLIGHT_SCHEMA, args: &change})
}

func (ai *ActionInitiator) ApplySchema(tabletAlias topo.TabletAlias, sc *mysqlctl.SchemaChange) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_APPLY_SCHEMA, args: sc})
}

func (ai *ActionInitiator) RpcGetPermissions(tabletAlias topo.TabletAlias, waitTime time.Duration) (*mysqlctl.Permissions, error) {
	var p mysqlctl.Permissions
	if err := ai.rpcCall(tabletAlias, TABLET_ACTION_GET_PERMISSIONS, "", &p, waitTime); err != nil {
		return nil, err
	}
	return &p, nil
}

func (ai *ActionInitiator) ExecuteHook(tabletAlias topo.TabletAlias, _hook *hook.Hook) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_EXECUTE_HOOK, args: _hook})
}

type SlaveList struct {
	Addrs []string
}

func (ai *ActionInitiator) GetSlaves(tabletAlias topo.TabletAlias) (actionPath string, err error) {
	return ai.writeTabletAction(tabletAlias, &ActionNode{Action: TABLET_ACTION_GET_SLAVES})
}

func (ai *ActionInitiator) ReparentShard(tabletAlias topo.TabletAlias) *ActionNode {
	return &ActionNode{
		Action:     SHARD_ACTION_REPARENT,
		ActionGuid: actionGuid(),
		args:       &tabletAlias,
	}
}

func (ai *ActionInitiator) ShardExternallyReparented(tabletAlias topo.TabletAlias) *ActionNode {
	return &ActionNode{
		Action:     SHARD_ACTION_EXTERNALLY_REPARENTED,
		ActionGuid: actionGuid(),
		args:       &tabletAlias,
	}
}

func (ai *ActionInitiator) RebuildShard() *ActionNode {
	return &ActionNode{
		Action:     SHARD_ACTION_REBUILD,
		ActionGuid: actionGuid(),
	}
}

func (ai *ActionInitiator) CheckShard() *ActionNode {
	return &ActionNode{
		Action:     SHARD_ACTION_CHECK,
		ActionGuid: actionGuid(),
	}
}

// parameters are stored for debug purposes
type ApplySchemaShardArgs struct {
	MasterTabletAlias topo.TabletAlias
	Change            string
	Simple            bool
}

func (ai *ActionInitiator) ApplySchemaShard(masterTabletAlias topo.TabletAlias, change string, simple bool) *ActionNode {
	return &ActionNode{
		Action:     SHARD_ACTION_APPLY_SCHEMA,
		ActionGuid: actionGuid(),
		args: &ApplySchemaShardArgs{
			MasterTabletAlias: masterTabletAlias,
			Change:            change,
			Simple:            simple,
		},
	}
}

func (ai *ActionInitiator) RebuildKeyspace() *ActionNode {
	return &ActionNode{
		Action:     KEYSPACE_ACTION_REBUILD,
		ActionGuid: actionGuid(),
	}
}

// parameters are stored for debug purposes
type ApplySchemaKeyspaceArgs struct {
	Change string
	Simple bool
}

func (ai *ActionInitiator) ApplySchemaKeyspace(change string, simple bool) *ActionNode {
	return &ActionNode{
		Action:     KEYSPACE_ACTION_APPLY_SCHEMA,
		ActionGuid: actionGuid(),
		args: &ApplySchemaKeyspaceArgs{
			Change: change,
			Simple: simple,
		},
	}
}

func (ai *ActionInitiator) WaitForCompletion(actionPath string, waitTime time.Duration) error {
	_, err := WaitForCompletion(ai.ts, actionPath, waitTime)
	return err
}

func (ai *ActionInitiator) WaitForCompletionReply(actionPath string, waitTime time.Duration) (interface{}, error) {
	return WaitForCompletion(ai.ts, actionPath, waitTime)
}

func WaitForCompletion(ts topo.Server, actionPath string, waitTime time.Duration) (interface{}, error) {
	// If there is no duration specified, block for a sufficiently long time.
	if waitTime <= 0 {
		waitTime = 24 * time.Hour
	}

	data, err := ts.WaitForTabletAction(actionPath, waitTime, interrupted)
	if err != nil {
		return nil, err
	}

	// parse it
	actionNode, dataErr := ActionNodeFromJson(data, "")
	if dataErr != nil {
		return nil, fmt.Errorf("action data error: %v %v %#v", actionPath, dataErr, data)
	} else if actionNode.Error != "" {
		return nil, fmt.Errorf("action failed: %v %v", actionPath, actionNode.Error)
	}

	return actionNode.reply, nil
}
