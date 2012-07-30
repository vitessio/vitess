// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Actions modify the state of a tablet, shard or keyspace.
*/

package tabletmanager

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/relog"
	"launchpad.net/gozk/zookeeper"
)

/*
 The actor applies individual commands to execute an action read from a node
 in zookeeper.

 The actor signals completion by removing the action node from zookeeper.

 Errors are written to the action node and must (currently) be resolved by
 hand using zk tools.
*/

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

// FIXME(msolomon) do we care if the action is queued?
func (ai *ActionInitiator) writeTabletAction(zkTabletPath string, node *ActionNode) (actionPath string, err error) {
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath = TabletActionPath(zkTabletPath)
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	return ai.zconn.Create(actionPath+"/", data, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

// FIXME(msolomon) do we care if the action is queued?
func (ai *ActionInitiator) writeShardAction(zkShardPath string, node *ActionNode) (actionPath string, err error) {
	MustBeShardPath(zkShardPath)
	node.ActionGuid = actionGuid()
	data := ActionNodeToJson(node)
	actionPath = ShardActionPath(zkShardPath)
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

func (ai *ActionInitiator) PromoteSlave(zkTabletPath, zkShardActionPath string) (actionPath string, err error) {
	args := map[string]string{"ShardActionPath": zkShardActionPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_PROMOTE_SLAVE, Args: args})
}

func (ai *ActionInitiator) RestartSlave(zkTabletPath, zkShardActionPath string) (actionPath string, err error) {
	args := map[string]string{"ShardActionPath": zkShardActionPath}
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_RESTART_SLAVE, Args: args})
}

func (ai *ActionInitiator) Scrap(zkTabletPath string) (actionPath string, err error) {
	return ai.writeTabletAction(zkTabletPath, &ActionNode{Action: TABLET_ACTION_SCRAP})
}

func (ai *ActionInitiator) ReparentShard(zkShardPath, zkTabletPath string) (actionPath string, err error) {
	MustBeTabletPath(zkTabletPath)
	node := &ActionNode{Action: SHARD_ACTION_REPARENT}
	node.Args = map[string]string{"tabletPath": zkTabletPath}
	return ai.writeShardAction(zkShardPath, node)
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
				// FIXME(msolomon) handle zk disconnect
				relog.Warning("unexpected zk event: %v", actionEvent)
			}
		case <-timer.C:
			return fmt.Errorf("action err: %v deadline exceeded %v", actionPath, waitTime)
		}
	}
	panic("unreachable")
}
