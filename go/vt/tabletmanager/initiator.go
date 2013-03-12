// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Actions modify the state of a tablet, shard or keyspace.
//
// They are stored in zookeeper below "action" nodes and form a queue. Only the
// lowest action id should be executing at any given time.
//
// The creation, deletion and modifaction of an action node may be used as
// a signal to other components in the system.

package tabletmanager

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"sort"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/hook"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// The actor applies individual commands to execute an action read from a node
// in zookeeper.
//
// The actor signals completion by removing the action node from zookeeper.
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
	zconn zk.Conn
}

func NewActionInitiator(zconn zk.Conn) *ActionInitiator {
	return &ActionInitiator{zconn}
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

func (ai *ActionInitiator) writeTabletAction(zkTabletPath string, node *ActionNode) (actionPath string, err error) {
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath, err = TabletActionPath(zkTabletPath)
	if err != nil {
		return
	}
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) writeShardAction(zkShardPath string, node *ActionNode) (actionPath string, err error) {
	if err := IsShardPath(zkShardPath); err != nil {
		return "", err
	}
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath, err = ShardActionPath(zkShardPath)
	if err != nil {
		return
	}
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) writeKeyspaceAction(zkKeyspacePath string, node *ActionNode) (actionPath string, err error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath, err = KeyspaceActionPath(zkKeyspacePath)
	if err != nil {
		return
	}
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) Ping(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PING})
}

func (ai *ActionInitiator) Sleep(zkTabletPath string, duration time.Duration) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SLEEP, args: &duration})
}

func (ai *ActionInitiator) ChangeType(zkTabletPath string, dbType TabletType) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_CHANGE_TYPE, args: &dbType})
}

func (ai *ActionInitiator) SetReadOnly(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SET_RDONLY})
}

func (ai *ActionInitiator) SetReadWrite(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SET_RDWR})
}

func (ai *ActionInitiator) DemoteMaster(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_DEMOTE_MASTER})
}

type SnapshotArgs struct {
	Concurrency int
	ServerMode  bool
}

// used by both Snapshot and PartialSnapshot
type SnapshotReply struct {
	ZkParentPath string
	ManifestPath string

	// these two are only used for ServerMode=true full snapshot
	SlaveStartRequired bool
	ReadOnly           bool
}

type MultiSnapshotReply struct {
	ZkParentPath  string
	ManifestPaths []string
}

func (ai *ActionInitiator) Snapshot(zkTabletPath string, args *SnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SNAPSHOT, args: args})
}

type SnapshotSourceEndArgs struct {
	SlaveStartRequired bool
	ReadOnly           bool
}

func (ai *ActionInitiator) SnapshotSourceEnd(zkTabletPath string, args *SnapshotSourceEndArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SNAPSHOT_SOURCE_END, args: args})
}

type PartialSnapshotArgs struct {
	KeyName     string
	StartKey    key.HexKeyspaceId
	EndKey      key.HexKeyspaceId
	Concurrency int
}

type MultiSnapshotArgs struct {
	KeyName     string
	KeyRanges   []key.KeyRange
	Tables      []string
	Concurrency int
}
type MultiRestoreArgs struct {
	ZkSrcTabletPaths []string
	Concurrency      int
	FetchConcurrency int
	FetchRetryCount  int
	Force            bool
	KeyRange         key.KeyRange
	DbName           string
}

func (ai *ActionInitiator) PartialSnapshot(zkTabletPath string, args *PartialSnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PARTIAL_SNAPSHOT, args: args})
}

func (ai *ActionInitiator) MultiSnapshot(zkTabletPath string, args *MultiSnapshotArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_MULTI_SNAPSHOT, args: args})
}

func (ai *ActionInitiator) RestoreFromMultiSnapshot(zkTabletPath string, args *MultiRestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_MULTI_RESTORE, args: args})
}

func (ai *ActionInitiator) BreakSlaves(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_BREAK_SLAVES})
}

func (ai *ActionInitiator) PromoteSlave(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PROMOTE_SLAVE})
}

func (ai *ActionInitiator) RestartSlave(zkTabletPath string, args *RestartSlaveData) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_RESTART_SLAVE, args: args})
}

func (ai *ActionInitiator) ReparentPosition(zkTabletPath string, slavePos *mysqlctl.ReplicationPosition) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_REPARENT_POSITION, args: slavePos})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) MasterPosition(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_MASTER_POSITION})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) SlavePosition(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SLAVE_POSITION})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) WaitSlavePosition(zkTabletPath string, args *SlavePositionReq) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_WAIT_SLAVE_POSITION, args: args})
}

func (ai *ActionInitiator) StopSlave(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_STOP_SLAVE})
}

type ReserveForRestoreArgs struct {
	ZkSrcTabletPath string
}

func (ai *ActionInitiator) ReserveForRestore(zkDstTabletPath string, args *ReserveForRestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_RESERVE_FOR_RESTORE, args: args})
}

// used for both Restore and PartialRestore
type RestoreArgs struct {
	ZkSrcTabletPath       string
	SrcFilePath           string
	ZkParentPath          string
	FetchConcurrency      int
	FetchRetryCount       int
	WasReserved           bool
	DontWaitForSlaveStart bool
}

func (ai *ActionInitiator) Restore(zkDstTabletPath string, args *RestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_RESTORE, args: args})
}

func (ai *ActionInitiator) PartialRestore(zkDstTabletPath string, args *RestoreArgs) (actionPath string, err error) {
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_PARTIAL_RESTORE, args: args})
}

func (ai *ActionInitiator) Scrap(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SCRAP})
}

type GetSchemaArgs struct {
	Tables       []string
	IncludeViews bool
}

func (ai *ActionInitiator) GetSchema(zkTabletPath string, tables []string, includeViews bool) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_GET_SCHEMA, args: &GetSchemaArgs{Tables: tables, IncludeViews: includeViews}})
}

func (ai *ActionInitiator) PreflightSchema(zkTabletPath, change string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PREFLIGHT_SCHEMA, args: &change})
}

func (ai *ActionInitiator) ApplySchema(zkTabletPath string, sc *mysqlctl.SchemaChange) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_APPLY_SCHEMA, args: sc})
}

func (ai *ActionInitiator) ExecuteHook(zkTabletPath string, _hook *hook.Hook) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_EXECUTE_HOOK, args: _hook})
}

func (ai *ActionInitiator) ReparentShard(zkShardPath, zkTabletPath string) (actionPath string, err error) {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return "", err
	}
	return ai.writeShardAction(zkShardPath, &ActionNode{Action: SHARD_ACTION_REPARENT, args: &zkTabletPath})
}

func (ai *ActionInitiator) RebuildShard(zkShardPath string) (actionPath string, err error) {
	if err := IsShardPath(zkShardPath); err != nil {
		return "", err
	}
	return ai.writeShardAction(zkShardPath, &ActionNode{Action: SHARD_ACTION_REBUILD})
}

func (ai *ActionInitiator) CheckShard(zkShardPath string) (actionPath string, err error) {
	return ai.writeShardAction(zkShardPath, &ActionNode{Action: SHARD_ACTION_CHECK})
}

// parameters are stored for debug purposes
type ApplySchemaShardArgs struct {
	ZkMasterTabletPath string
	Change             string
	Simple             bool
}

func (ai *ActionInitiator) ApplySchemaShard(zkShardPath, zkMasterTabletPath, change string, simple bool) (actionPath string, err error) {
	return ai.writeShardAction(zkShardPath, &ActionNode{Action: SHARD_ACTION_APPLY_SCHEMA, args: &ApplySchemaShardArgs{ZkMasterTabletPath: zkMasterTabletPath, Change: change, Simple: simple}})
}

func (ai *ActionInitiator) RebuildKeyspace(zkKeyspacePath string) (actionPath string, err error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	node := &ActionNode{Action: KEYSPACE_ACTION_REBUILD}
	return ai.writeKeyspaceAction(zkKeyspacePath, node)
}

// parameters are stored for debug purposes
type ApplySchemaKeyspaceArgs struct {
	Change string
	Simple bool
}

func (ai *ActionInitiator) ApplySchemaKeyspace(zkKeyspacePath, change string, simple bool) (actionPath string, err error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	return ai.writeKeyspaceAction(zkKeyspacePath, &ActionNode{Action: KEYSPACE_ACTION_APPLY_SCHEMA, args: &ApplySchemaKeyspaceArgs{Change: change, Simple: simple}})
}

func (ai *ActionInitiator) WaitForCompletion(actionPath string, waitTime time.Duration) error {
	_, err := WaitForCompletion(ai.zconn, actionPath, waitTime)
	return err
}

func (ai *ActionInitiator) WaitForCompletionReply(actionPath string, waitTime time.Duration) (interface{}, error) {
	return WaitForCompletion(ai.zconn, actionPath, waitTime)
}

func WaitForCompletion(zconn zk.Conn, actionPath string, waitTime time.Duration) (interface{}, error) {
	// If there is no duration specified, block for a sufficiently long time.
	if waitTime <= 0 {
		waitTime = 24 * time.Hour
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// see if the file exists or sets a watch
	// the loop is to resist zk disconnects while we're waiting
	actionLogPath := ActionToActionLogPath(actionPath)
wait:
	for {
		stat, watch, err := zconn.ExistsW(actionLogPath)
		if err != nil {
			return nil, fmt.Errorf("action err: %v %v", actionLogPath, err)
		}

		// file exists, go on
		if stat != nil {
			break wait
		}

		// if the file doesn't exist yet, wait for creation event.
		// On any other event we'll retry the ExistsW
		select {
		case actionEvent := <-watch:
			if actionEvent.Type == zookeeper.EVENT_CREATED {
				break wait
			} else {
				// Log unexpected events. Reconnects are
				// handled by zk.Conn, so calling ExistsW again
				// will handle a disconnect.
				relog.Warning("unexpected zk event: %v", actionEvent)
			}
		case <-timer.C:
			return nil, fmt.Errorf("action err: %v deadline exceeded %v", actionLogPath, waitTime)
		case <-interrupted:
			return nil, fmt.Errorf("action err: %v interrupted by signal", actionLogPath)
		}
	}

	// the node exists, read it
	data, _, err := zconn.Get(actionLogPath)
	if err != nil {
		return nil, fmt.Errorf("action err: %v %v", actionLogPath, err)
	}

	// parse it
	actionNode, dataErr := ActionNodeFromJson(data, actionLogPath)
	if dataErr != nil {
		return nil, fmt.Errorf("action data error: %v %v %#v", actionLogPath, dataErr, data)
	} else if actionNode.Error != "" {
		return nil, fmt.Errorf("action failed: %v %v", actionPath, actionNode.Error)
	}

	return actionNode.reply, nil
}

// Remove all queued actions, leaving the action node itself in place.
//
// This inherently breaks the locking mechanism of the action queue,
// so this is a rare cleaup action, not a normal part of the flow.
func PurgeActions(zconn zk.Conn, zkActionPath string) error {
	if path.Base(zkActionPath) != "action" {
		return fmt.Errorf("not action path: %v", zkActionPath)
	}

	children, _, err := zconn.Children(zkActionPath)
	if err != nil {
		return err
	}

	sort.Strings(children)
	// Purge newer items first so the action queues don't try to process something.
	for i := len(children) - 1; i >= 0; i-- {
		actionPath := path.Join(zkActionPath, children[i])
		data, _, err := zconn.Get(actionPath)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("purge action err: %v", err)
		}
		actionNode, err := ActionNodeFromJson(data, actionPath)
		if err != nil {
			relog.Warning("bad action data: %v %v %#v", actionPath, err, data)
		} else if actionNode.State == ACTION_STATE_RUNNING {
			relog.Info("cannot remove running action: %v %v %v", actionPath, actionNode.Action, actionNode.ActionGuid)
			continue
		}

		err = zk.DeleteRecursive(zconn, actionPath, -1)
		if err != nil {
			return fmt.Errorf("purge action err: %v", err)
		}
	}
	return nil
}

// Return a list of queued actions that have been sitting for more
// than some amount of time.
func StaleActions(zconn zk.Conn, zkActionPath string, maxStaleness time.Duration) ([]string, error) {
	if path.Base(zkActionPath) != "action" {
		return nil, fmt.Errorf("not action path: %v", zkActionPath)
	}

	children, _, err := zconn.Children(zkActionPath)
	if err != nil {
		return nil, err
	}

	staleActions := make([]string, 0, 16)
	// Purge newer items first so the action queues don't try to process something.
	sort.Strings(children)
	for i := 0; i < len(children); i++ {
		actionPath := path.Join(zkActionPath, children[i])
		data, stat, err := zconn.Get(actionPath)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, fmt.Errorf("stale action err: %v", err)
		}
		if stat == nil || time.Since(stat.MTime()) <= maxStaleness {
			continue
		}
		actionNode, err := ActionNodeFromJson(data, actionPath)
		if err != nil {
			relog.Warning("bad action data: %v %v %#v", actionPath, err, data)
		} else if actionNode.State != ACTION_STATE_RUNNING {
			staleActions = append(staleActions, actionPath)
		}
	}
	return staleActions, nil
}

// Prune old actionlog entries. Returns how many entries were purged
// (even if there was an error)
//
// There is a chance some processes might still be waiting for action
// results, but it is very very small.
func PruneActionLogs(zconn zk.Conn, zkActionLogPath string, keepCount int) (prunedCount int, err error) {
	if path.Base(zkActionLogPath) != "actionlog" {
		return 0, fmt.Errorf("not actionlog path: %v", zkActionLogPath)
	}

	// get sorted list of children
	children, _, err := zconn.Children(zkActionLogPath)
	if err != nil {
		return 0, err
	}
	sort.Strings(children)

	// see if nothing to do
	if len(children) <= keepCount {
		return 0, nil
	}

	for i := 0; i < len(children)-keepCount; i++ {
		actionPath := path.Join(zkActionLogPath, children[i])
		err = zk.DeleteRecursive(zconn, actionPath, -1)
		if err != nil {
			return prunedCount, fmt.Errorf("purge action err: %v", err)
		}
		prunedCount++
	}
	return prunedCount, nil
}
