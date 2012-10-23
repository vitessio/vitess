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
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
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
	now := time.Now().Unix()
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

func (ai *ActionInitiator) RestartSlave(zkTabletPath, zkShardActionPath string) (actionPath string, err error) {
	args := map[string]string{"ShardActionPath": zkShardActionPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_RESTART_SLAVE, Args: args})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) MasterPosition(zkTabletPath, zkReplyPath string) (actionPath string, err error) {
	args := map[string]string{"ReplyPath": zkReplyPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_MASTER_POSITION, Args: args})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) SlavePosition(zkTabletPath, zkReplyPath string) (actionPath string, err error) {
	args := map[string]string{"ReplyPath": zkReplyPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SLAVE_POSITION, Args: args})
}

// NOTE(msolomon) Also available as RPC.
func (ai *ActionInitiator) WaitSlavePosition(zkTabletPath, zkArgsPath, zkReplyPath string) (actionPath string, err error) {
	args := map[string]string{"ArgsPath": zkArgsPath, "ReplyPath": zkReplyPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_WAIT_SLAVE_POSITION, Args: args})
}

func (ai *ActionInitiator) StopSlave(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_STOP_SLAVE})
}

func (ai *ActionInitiator) Restore(zkDstTabletPath, zkSrcTabletPath string) (actionPath string, err error) {
	args := map[string]string{"SrcTabletPath": zkSrcTabletPath}
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_RESTORE, Args: args})
}

func (ai *ActionInitiator) PartialRestore(zkDstTabletPath, zkSrcTabletPath string) (actionPath string, err error) {
	args := map[string]string{"SrcTabletPath": zkSrcTabletPath}
	return ai.writeTabletAction(zkDstTabletPath, &ActionNode{Action: TABLET_ACTION_PARTIAL_RESTORE, Args: args})
}

func (ai *ActionInitiator) Scrap(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SCRAP})
}

func (ai *ActionInitiator) GetSchema(zkTabletPath, zkReplyPath string) (actionPath string, err error) {
	args := map[string]string{"ReplyPath": zkReplyPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_GET_SCHEMA, Args: args})
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

func (ai *ActionInitiator) RebuildKeyspace(zkKeyspacePath string) (actionPath string, err error) {
	MustBeKeyspacePath(zkKeyspacePath)
	node := &ActionNode{Action: KEYSPACE_ACTION_REBUILD}
	return ai.writeKeyspaceAction(zkKeyspacePath, node)
}

func (ai *ActionInitiator) WaitForCompletion(actionPath string, waitTime time.Duration) error {
	return WaitForCompletion(ai.zconn, actionPath, waitTime)
}

func WaitForCompletion(zconn zk.Conn, actionPath string, waitTime time.Duration) error {
	// If there is no duration specified, block for a sufficiently long time.
	if waitTime <= 0 {
		waitTime = 24 * time.Hour
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()
	for {
		data, _, watch, err := zconn.GetW(actionPath)
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNONODE) {
				return nil
			}
			return fmt.Errorf("action err: %v %v", actionPath, err)
		} else {
			actionNode, dataErr := ActionNodeFromJson(data, actionPath)
			if dataErr != nil {
				return fmt.Errorf("action data error: %v %v %#v", actionPath, dataErr, data)
			} else if actionNode.Error != "" {
				return fmt.Errorf("action failed: %v %v", actionPath, actionNode.Error)
			}
		}

		select {
		case actionEvent := <-watch:
			switch actionEvent.Type {
			case zookeeper.EVENT_CHANGED:
				// reload the node and try again
				continue
			case zookeeper.EVENT_DELETED:
				return nil
			default:
				// Log unexpected events. Reconnects are handled by zk.Conn, so
				// calling GetW again will handle a disconnect.
				relog.Warning("unexpected zk event: %v", actionEvent)
			}
		case <-timer.C:
			return fmt.Errorf("action err: %v deadline exceeded %v", actionPath, waitTime)
		}
	}
	panic("unreachable")
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
	for _, child := range children {
		err = zk.DeleteRecursive(zconn, path.Join(zkActionPath, child), -1)
		if err != nil {
			return err
		}
	}
	return nil
}
