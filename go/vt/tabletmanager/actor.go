// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/shard"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
The actor applies individual commands to execute an action read
from a node in zookeeper. Anything that modifies the state of the
table should be applied by this code.

The actor signals completion by removing the action node from zookeeper.

Errors are written to the action node and must (currently) be resolved
by hand using zk tools.
*/

type TabletActorError string

func (e TabletActorError) Error() string {
	return string(e)
}

type TabletActor struct {
	mysqld       *mysqlctl.Mysqld
	zconn        zk.Conn
	zkTabletPath string
	zkVtRoot     string
}

func NewTabletActor(mysqld *mysqlctl.Mysqld, zconn zk.Conn) *TabletActor {
	return &TabletActor{mysqld, zconn, "", ""}
}

// FIXME(msolomon) protect against unforeseen panics and classify errors as "fatal" or
// resolvable. For instance, if your zk connection fails, better to just fail. If data
// is corrupt, you can't fix it gracefully.
func (ta *TabletActor) HandleAction(actionPath, action, actionGuid string) error {
	data, stat, zkErr := ta.zconn.Get(actionPath)
	if zkErr != nil {
		relog.Error("HandleAction failed: %v", zkErr)
		return zkErr
	}

	actionNode, err := ActionNodeFromJson(data, actionPath)
	if err != nil {
		relog.Error("HandleAction failed unmarshaling %v: %v", actionPath, err)
		return err
	}

	switch actionNode.State {
	case ACTION_STATE_RUNNING:
		relog.Warning("HandleAction waiting for running action: %v", actionPath)
		return WaitForCompletion(ta.zconn, actionPath, 0)
	case ACTION_STATE_FAILED:
		return fmt.Errorf(actionNode.Error)
	}

	// Claim the action by this process.
	actionNode.State = ACTION_STATE_RUNNING
	newData := ActionNodeToJson(actionNode)
	_, zkErr = ta.zconn.Set(actionPath, newData, stat.Version())
	if zkErr != nil {
		if zookeeper.IsError(zkErr, zookeeper.ZBADVERSION) {
			// The action is schedule by another actor. Most likely
			// the tablet restarted during an action. Just wait for completion.
			relog.Warning("HandleAction waiting for scheduled action: %v", actionPath)
			return WaitForCompletion(ta.zconn, actionPath, 0)
		} else {
			return zkErr
		}
	}

	ta.zkTabletPath = TabletPathFromActionPath(actionPath)
	ta.zkVtRoot = VtRootFromTabletPath(ta.zkTabletPath)

	relog.Info("HandleAction: %v %v", actionPath, data)

	// validate actions, but don't write this back into zk
	if actionNode.Action != action || actionNode.ActionGuid != actionGuid {
		relog.Error("HandleAction validation failed %v: (%v,%v) (%v,%v)",
			actionPath, actionNode.Action, action, actionNode.ActionGuid, actionGuid)
		return TabletActorError("invalid action initiation: " + action + " " + actionGuid)
	}

	actionErr := ta.dispatchAction(actionNode)
	if actionErr != nil {
		// on failure, set an error field on the node - let other tools deal
		// with it.
		actionNode.Error = actionErr.Error()
		actionNode.State = ACTION_STATE_FAILED
	} else {
		actionNode.Error = ""
	}

	newData = ActionNodeToJson(actionNode)
	if newData != data {
		_, zkErr = ta.zconn.Set(actionPath, newData, -1)
		if zkErr != nil {
			relog.Error("HandleAction failed writing: %v", zkErr)
			return zkErr
		}
	}

	if actionErr != nil {
		return actionErr
	} else {
		// remove from zk on success
		zkErr = ta.zconn.Delete(actionPath, -1)
		if zkErr != nil {
			relog.Error("HandleAction failed deleting: %v", zkErr)
			return zkErr
		}
	}
	return nil
}

func (ta *TabletActor) dispatchAction(actionNode *ActionNode) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = x.(error)
		}
	}()

	switch actionNode.Action {
	case TABLET_ACTION_PING:
		// Just an end-to-end verification that we got the message.
		err = nil
	case TABLET_ACTION_SLEEP:
		err = ta.sleep(actionNode.Args)
	case TABLET_ACTION_SET_RDONLY:
		err = ta.setReadOnly(true)
	case TABLET_ACTION_SET_RDWR:
		err = ta.setReadOnly(false)
	case TABLET_ACTION_CHANGE_TYPE:
		err = ta.changeType(actionNode.Args)
	case TABLET_ACTION_DEMOTE_MASTER:
		err = ta.demoteMaster()
	case TABLET_ACTION_PROMOTE_SLAVE:
		err = ta.promoteSlave(actionNode.Args)
	case TABLET_ACTION_RESTART_SLAVE:
		err = ta.restartSlave(actionNode.Args)
	case TABLET_ACTION_BREAK_SLAVES:
		err = ta.mysqld.BreakSlaves()
	case TABLET_ACTION_SCRAP:
		err = ta.scrap()
	default:
		err = TabletActorError("invalid action: " + actionNode.Action)
	}

	return
}

func (ta *TabletActor) sleep(args map[string]string) error {
	duration, ok := args["Duration"]
	if !ok {
		return fmt.Errorf("missing Duration in args")
	}
	d, err := time.ParseDuration(duration)
	if err != nil {
		return err
	}
	time.Sleep(d)
	return nil
}

func (ta *TabletActor) setReadOnly(rdonly bool) error {
	err := ta.mysqld.SetReadOnly(rdonly)
	if err != nil {
		return err
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if rdonly {
		tablet.State = STATE_READ_ONLY
	} else {
		tablet.State = STATE_READ_WRITE
	}
	return UpdateTablet(ta.zconn, ta.zkTabletPath, tablet)
}

func (ta *TabletActor) changeType(args map[string]string) error {
	dbType, ok := args["DbType"]
	if !ok {
		return fmt.Errorf("missing DbType in args")
	}
	return ChangeType(ta.zconn, ta.zkTabletPath, TabletType(dbType))
}

func (ta *TabletActor) demoteMaster() error {
	_, err := ta.mysqld.DemoteMaster()
	if err != nil {
		return err
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	tablet.State = STATE_READ_ONLY
	// NOTE(msolomon) there is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will succeed. It will be
	// less noisy to simply leave the entry until well promote the master.
	return UpdateTablet(ta.zconn, ta.zkTabletPath, tablet)
}

type RestartSlaveData struct {
	ReplicationState *mysqlctl.ReplicationState
	WaitPosition     *mysqlctl.ReplicationPosition
	TimePromoted     int64 // used to verify replication - a row will be inserted with this timestamp
	Parent           TabletAlias
	Force            bool
}

func (ta *TabletActor) promoteSlave(args map[string]string) error {
	zkShardActionPath, ok := args["ShardActionPath"]
	if !ok {
		return fmt.Errorf("missing ShardActionPath in args")
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	zkRestartSlaveDataPath := path.Join(zkShardActionPath, "restart_slave_data.json")
	// The presence of this node indicates that the promote action succeeded.
	stat, err := ta.zconn.Exists(zkRestartSlaveDataPath)
	if stat != nil {
		err = fmt.Errorf("restart_slave_data.json already exists - suspicious")
	}
	if err != nil {
		return err
	}

	// No slave data, perform the action.
	alias := TabletAlias{tablet.Tablet.Cell, tablet.Tablet.Uid}
	rsd := &RestartSlaveData{Parent: alias, Force: (tablet.Parent.Uid == NO_TABLET)}
	rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted, err = ta.mysqld.PromoteSlave()
	if err != nil {
		return err
	}
	relog.Debug("PromoteSlave %#v", *rsd)
	// This data is valuable - commit it to zk first.
	_, err = ta.zconn.Create(zkRestartSlaveDataPath, toJson(rsd), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Remove tablet from the replication graph if this is not already the master.
	if tablet.Parent.Uid != NO_TABLET {
		oldReplicationPath := tablet.ReplicationPath()
		err = ta.zconn.Delete(oldReplicationPath, -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return err
		}
	}
	// Update tablet regardless - trend towards consistency.
	tablet.State = STATE_READ_WRITE
	tablet.Type = TYPE_MASTER
	tablet.Parent.Cell = ""
	tablet.Parent.Uid = NO_TABLET
	err = UpdateTablet(ta.zconn, ta.zkTabletPath, tablet)
	if err != nil {
		return err
	}
	// NOTE(msolomon) A serving graph update is required, but in order for the
	// shard to be consistent the master must be scrapped first. That is
	// externally coordinated by the wrangler reparent action.

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	newReplicationPath := tablet.ReplicationPath()
	_, err = ta.zconn.Create(newReplicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}

func (ta *TabletActor) restartSlave(args map[string]string) error {
	zkShardActionPath, ok := args["ShardActionPath"]
	if !ok {
		return fmt.Errorf("missing ShardActionPath in args")
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	zkRestartSlaveDataPath := path.Join(zkShardActionPath, "restart_slave_data.json")
	data, _, err := ta.zconn.Get(zkRestartSlaveDataPath)
	if err != nil {
		return err
	}

	rsd := new(RestartSlaveData)
	err = json.Unmarshal([]byte(data), rsd)
	if err != nil {
		return err
	}

	// If this check fails, we seem reparented. The only part that could have failed
	// is the insert in the replication graph. Do NOT try to reparent
	// again. That will either wedge replication to corrupt data.
	if tablet.Parent != rsd.Parent {
		relog.Debug("restart with new parent")
		// Remove tablet from the replication graph.
		oldReplicationPath := tablet.ReplicationPath()
		err = ta.zconn.Delete(oldReplicationPath, -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return err
		}

		err = ta.mysqld.RestartSlave(rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted)
		if err != nil {
			return err
		}
		// Once this action completes, update authoritive tablet node first.
		tablet.Parent = rsd.Parent
		err = UpdateTablet(ta.zconn, ta.zkTabletPath, tablet)
		if err != nil {
			return err
		}
	} else if rsd.Force {
		err = ta.mysqld.RestartSlave(rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted)
		if err != nil {
			return err
		}
	}

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	newReplicationPath := tablet.ReplicationPath()
	_, err = ta.zconn.Create(newReplicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}

func (ta *TabletActor) scrap() error {
	return Scrap(ta.zconn, ta.zkTabletPath, false)
}

// Make this external, since in needs to be forced from time to time.
func Scrap(zconn zk.Conn, zkTabletPath string, force bool) error {
	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	wasIdle := tablet.Type == TYPE_IDLE
	tablet.Type = TYPE_SCRAP
	tablet.Parent = TabletAlias{}
	// Update the tablet first, since that is canonical.
	err = UpdateTablet(zconn, zkTabletPath, tablet)
	if err != nil {
		return err
	}

	// Remove any pending actions. Presumably forcing a scrap means you don't
	// want the agent doing anything and the machine requires manual attention.
	if force {
		actionPath := TabletActionPath(zkTabletPath)
		err = PurgeActions(zconn, actionPath)
		if err != nil {
			relog.Warning("purge actions failed: %v %v", actionPath, err)
		}
	}

	if !wasIdle {
		err = zconn.Delete(tablet.ReplicationPath(), -1)
		if err != nil {
			switch err.(*zookeeper.Error).Code {
			case zookeeper.ZNONODE:
				relog.Debug("no replication path: %v", tablet.ReplicationPath())
				return nil
			case zookeeper.ZNOTEMPTY:
				// If you are forcing the scrapping of a master, you can't update the
				// replication graph yet, since other nodes are still under the impression
				// they are slaved to this tablet.
				// If the node was not empty, we can't do anything about it - the replication
				// graph needs to be fixed by reparenting. If the action was forced, assume
				// the user knows best and squelch the error.
				if tablet.Parent.Uid == NO_TABLET && force {
					return nil
				}
			default:
				return err
			}
		}
	}
	return nil
}

// Make this external, since these transitions need to be forced from time to time.
func ChangeType(zconn zk.Conn, zkTabletPath string, newType TabletType) error {
	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}
	if !IsTrivialTypeChange(tablet.Type, newType) {
		return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, zkTabletPath)
	}
	tablet.Type = newType
	if newType == TYPE_IDLE {
		if tablet.Parent.Uid == NO_TABLET {
			// With a master the node cannot be set to idle unless we have already removed all of
			// the derived paths. The global replication path is a good indication that this has
			// been resolved.
			stat, err := zconn.Exists(tablet.ReplicationPath())
			if err != nil {
				return err
			}
			if stat != nil && stat.NumChildren() != 0 {
				return fmt.Errorf("cannot change tablet type %v -> %v - reparent action has not finished %v", tablet.Type, newType, zkTabletPath)
			}
		}
		tablet.Parent = TabletAlias{}
		tablet.Keyspace = ""
		tablet.Shard = ""
		tablet.KeyRange = shard.KeyRange{}
	}
	return UpdateTablet(zconn, zkTabletPath, tablet)
}
