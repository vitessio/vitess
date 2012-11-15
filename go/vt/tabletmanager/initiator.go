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
	"time"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
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
	actionPath = TabletActionPath(zkTabletPath)
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) writeShardAction(zkShardPath string, node *ActionNode) (actionPath string, err error) {
	MustBeShardPath(zkShardPath)
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath = ShardActionPath(zkShardPath)
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) writeKeyspaceAction(zkKeyspacePath string, node *ActionNode) (actionPath string, err error) {
	MustBeKeyspacePath(zkKeyspacePath)
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath = KeyspaceActionPath(zkKeyspacePath)
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (ai *ActionInitiator) Ping(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PING})
}

func (ai *ActionInitiator) Sleep(zkTabletPath string, duration time.Duration) (actionPath string, err error) {
	args := map[string]string{"Duration": duration.String()}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SLEEP, Args: args})
}

func (ai *ActionInitiator) ChangeType(zkTabletPath string, dbType TabletType) (actionPath string, err error) {
	args := map[string]string{"DbType": string(dbType)}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_CHANGE_TYPE, Args: args})
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

func (ai *ActionInitiator) Snapshot(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SNAPSHOT})
}

func (ai *ActionInitiator) PartialSnapshot(zkTabletPath, keyName string, startKey, endKey key.HexKeyspaceId) (actionPath string, err error) {
	args := map[string]string{
		"KeyName":  keyName,
		"StartKey": string(startKey),
		"EndKey":   string(endKey),
	}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PARTIAL_SNAPSHOT, Args: args})
}

func (ai *ActionInitiator) BreakSlaves(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_BREAK_SLAVES})
}

func (ai *ActionInitiator) PromoteSlave(zkTabletPath, zkShardActionPath string) (actionPath string, err error) {
	args := map[string]string{"ShardActionPath": zkShardActionPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PROMOTE_SLAVE, Args: args})
}

func (ai *ActionInitiator) RestartSlave(zkTabletPath, zkShardActionPath string, rsd *RestartSlaveData) (actionPath string, err error) {
	args := map[string]string{"ShardActionPath": zkShardActionPath}
	if rsd != nil {
		args["RestartSlaveData"] = jscfg.ToJson(rsd)
	}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_RESTART_SLAVE, Args: args})
}

func (ai *ActionInitiator) ReparentPosition(zkTabletPath string, slavePos *mysqlctl.ReplicationPosition) (actionPath string, err error) {
	args := map[string]string{"SlavePosition": jscfg.ToJson(slavePos)}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_REPARENT_POSITION, Args: args})
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
func (ai *ActionInitiator) WaitSlavePosition(zkTabletPath, zkArgsPath string) (actionPath string, err error) {
	args := map[string]string{"ArgsPath": zkArgsPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_WAIT_SLAVE_POSITION, Args: args})
}

func (ai *ActionInitiator) StopSlave(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_STOP_SLAVE})
}

func (ai *ActionInitiator) Restore(zkDstTabletPath, zkSrcTabletPath, srcFilePath, zkParentPath string) (actionPath string, err error) {
	args := map[string]string{
		"SrcTabletPath": zkSrcTabletPath,
		"SrcFilePath":   srcFilePath,
		"zkParentPath":  zkParentPath,
	}
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_RESTORE, Args: args})
}

func (ai *ActionInitiator) PartialRestore(zkDstTabletPath, zkSrcTabletPath, srcFilePath, zkParentPath string) (actionPath string, err error) {
	args := map[string]string{
		"SrcTabletPath": zkSrcTabletPath,
		"SrcFilePath":   srcFilePath,
		"zkParentPath":  zkParentPath,
	}
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_PARTIAL_RESTORE, Args: args})
}

func (ai *ActionInitiator) Scrap(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SCRAP})
}

func (ai *ActionInitiator) GetSchema(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_GET_SCHEMA})
}

func (ai *ActionInitiator) PreflightSchema(zkTabletPath, change string) (actionPath string, err error) {
	args := map[string]string{"Change": change}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PREFLIGHT_SCHEMA, Args: args})
}

func (ai *ActionInitiator) ApplySchema(zkTabletPath string, sc *mysqlctl.SchemaChange) (actionPath string, err error) {
	args := map[string]string{
		"SchemaChange": jscfg.ToJson(sc),
	}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_APPLY_SCHEMA, Args: args})
}

func (ai *ActionInitiator) ExecuteHook(zkTabletPath string, hook *Hook) (actionPath string, err error) {
	args := hook.Parameters
	args["HookName"] = hook.Name
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_EXECUTE_HOOK, Args: args})
}

func (ai *ActionInitiator) ReparentShard(zkShardPath, zkTabletPath string) (actionPath string, err error) {
	MustBeTabletPath(zkTabletPath)
	node := &ActionNode{Action: SHARD_ACTION_REPARENT}
	node.Args = map[string]string{"tabletPath": zkTabletPath}
	return ai.writeShardAction(zkShardPath, node)
}

func (ai *ActionInitiator) RebuildShard(zkShardPath string) (actionPath string, err error) {
	MustBeShardPath(zkShardPath)
	node := &ActionNode{Action: SHARD_ACTION_REBUILD}
	return ai.writeShardAction(zkShardPath, node)
}

func (ai *ActionInitiator) CheckShard(zkShardPath string) (actionPath string, err error) {
	node := &ActionNode{Action: SHARD_ACTION_CHECK}
	return ai.writeShardAction(zkShardPath, node)
}

func (ai *ActionInitiator) ApplySchemaShard(zkShardPath string) (actionPath string, err error) {
	node := &ActionNode{Action: SHARD_ACTION_APPLY_SCHEMA}
	return ai.writeShardAction(zkShardPath, node)
}

func (ai *ActionInitiator) RebuildKeyspace(zkKeyspacePath string) (actionPath string, err error) {
	MustBeKeyspacePath(zkKeyspacePath)
	node := &ActionNode{Action: KEYSPACE_ACTION_REBUILD}
	return ai.writeKeyspaceAction(zkKeyspacePath, node)
}

func (ai *ActionInitiator) WaitForCompletion(actionPath string, waitTime time.Duration) error {
	_, err := WaitForCompletion(ai.zconn, actionPath, waitTime)
	return err
}

func (ai *ActionInitiator) WaitForCompletionResult(actionPath string, waitTime time.Duration) (map[string]string, error) {
	return WaitForCompletion(ai.zconn, actionPath, waitTime)
}

func WaitForCompletion(zconn zk.Conn, actionPath string, waitTime time.Duration) (map[string]string, error) {
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

	return actionNode.Results, nil
}

// Remove all queued actions, regardless of their current state, leaving
// the action node in place.
//
// This inherently breaks the locking mechanism of the action queue,
// so this is a rare cleaup action, not a normal part of the flow.
func PurgeActions(zconn zk.Conn, zkActionPath string) error {
	if path.Base(zkActionPath) != "action" {
		panic(fmt.Errorf("not action path: %v", zkActionPath))
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
			relog.Warning("cannot remove running action: %v %v %v", actionPath, actionNode.Action, actionNode.ActionGuid)
			continue
		}

		err = zk.DeleteRecursive(zconn, actionPath, -1)
		if err != nil {
			return fmt.Errorf("purge action err: %v", err)
		}
	}
	return nil
}

// Prune old actionlog entries. Returns how many entries were purged
// (even if there was an error)
//
// There is a chance some processes might still be waiting for action
// results, but it is very very small.
func PruneActionLogs(zconn zk.Conn, zkActionLogPath string, keepCount int) (prunedCount int, err error) {
	if path.Base(zkActionLogPath) != "actionlog" {
		panic(fmt.Errorf("not actionlog path: %v", zkActionLogPath))
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
