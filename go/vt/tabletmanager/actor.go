// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// The actor applies individual commands to execute an action read
// from a node in zookeeper. Anything that modifies the state of the
// table should be applied by this code.
//
// The actor signals completion by removing the action node from zookeeper.
//
// Errors are written to the action node and must (currently) be resolved
// by hand using zk tools.

const (
	restartSlaveDataFilename        = "restart_slave_data.json"
	restoreSlaveDataFilename        = "restore_slave_data.json"
	partialRestoreSlaveDataFilename = "partial_restore_slave_data.json"
)

type TabletActorError string

func (e TabletActorError) Error() string {
	return string(e)
}

type RestartSlaveData struct {
	ReplicationState *mysqlctl.ReplicationState
	WaitPosition     *mysqlctl.ReplicationPosition
	TimePromoted     int64 // used to verify replication - a row will be inserted with this timestamp
	Parent           TabletAlias
	Force            bool
}

// Enough data to restore a slave from a mysqlctl.ReplicaSource
type RestoreSlaveData struct {
	ReplicaSource *mysqlctl.ReplicaSource
	Parent        TabletAlias
}

// Enough data to restore a slave from a mysqlctl.SplitReplicaSource
type PartialRestoreSlaveData struct {
	SplitReplicaSource *mysqlctl.SplitReplicaSource
	Parent             TabletAlias
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
func (ta *TabletActor) HandleAction(actionPath, action, actionGuid string, forceRerun bool) error {
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
		if !forceRerun {
			relog.Warning("HandleAction waiting for running action: %v", actionPath)
			return WaitForCompletion(ta.zconn, actionPath, 0)
		}
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
			if panicErr, ok := x.(error); ok {
				err = panicErr
			} else {
				err = fmt.Errorf("dispatchAction panic: %v", x)
			}
			err = relog.NewPanicError(err)
		}
	}()

	switch actionNode.Action {
	case TABLET_ACTION_BREAK_SLAVES:
		err = ta.mysqld.BreakSlaves()
	case TABLET_ACTION_CHANGE_TYPE:
		err = ta.changeType(actionNode.Args)
	case TABLET_ACTION_DEMOTE_MASTER:
		err = ta.demoteMaster()
	case TABLET_ACTION_MASTER_POSITION:
		err = ta.masterPosition(actionNode.Args)
	case TABLET_ACTION_PARTIAL_RESTORE:
		err = ta.partialRestore(actionNode.Args)
	case TABLET_ACTION_PARTIAL_SNAPSHOT:
		_, err = ta.partialSnapshot(actionNode.Args)
	case TABLET_ACTION_PING:
		// Just an end-to-end verification that we got the message.
		err = nil
	case TABLET_ACTION_PROMOTE_SLAVE:
		err = ta.promoteSlave(actionNode.Args)
	case TABLET_ACTION_RESTART_SLAVE:
		err = ta.restartSlave(actionNode.Args)
	case TABLET_ACTION_RESTORE:
		err = ta.restore(actionNode.Args)
	case TABLET_ACTION_SCRAP:
		err = ta.scrap()
	case TABLET_ACTION_GET_SCHEMA:
		err = ta.getSchema(actionNode.Args)
	case TABLET_ACTION_EXECUTE_HOOK:
		err = ta.executeHook(actionNode.path, actionNode.Args)
	case TABLET_ACTION_SET_RDONLY:
		err = ta.setReadOnly(true)
	case TABLET_ACTION_SET_RDWR:
		err = ta.setReadOnly(false)
	case TABLET_ACTION_SLEEP:
		err = ta.sleep(actionNode.Args)
	case TABLET_ACTION_SLAVE_POSITION:
		err = ta.slavePosition(actionNode.Args)
	case TABLET_ACTION_SNAPSHOT:
		_, err = ta.snapshot()
	case TABLET_ACTION_STOP_SLAVE:
		err = ta.mysqld.StopSlave()
	case TABLET_ACTION_WAIT_SLAVE_POSITION:
		err = ta.waitSlavePosition(actionNode.Args)
	default:
		err = TabletActorError("invalid action: " + actionNode.Action)
	}

	return
}

// create the path for the reply if not absolute, and returns the full
// reply path
func (ta *TabletActor) createReplyPath(actionPath, replyPath string) (string, error) {
	// we're given an absolute path, trust the sender knows it's good
	if strings.HasPrefix(replyPath, "/") {
		return replyPath, nil
	}

	// create the path
	actionReplyPath := TabletActionToReplyPath(actionPath, "")

	// backward compatibility code: create the 'reply' path for the tablet
	// if it doesn't exist
	tabletReplyPath := path.Dir(actionReplyPath)
	if stat, err := ta.zconn.Exists(tabletReplyPath); stat == nil {
		_, err = ta.zconn.Create(tabletReplyPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			return "", err
		}
	}

	_, err := ta.zconn.Create(actionReplyPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return "", err
	}

	return path.Join(actionReplyPath, replyPath), nil
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

func (ta *TabletActor) promoteSlave(args map[string]string) error {
	zkShardActionPath, ok := args["ShardActionPath"]
	if !ok {
		return fmt.Errorf("missing ShardActionPath in args")
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	zkRestartSlaveDataPath := path.Join(zkShardActionPath, restartSlaveDataFilename)
	// The presence of this node indicates that the promote action succeeded.
	stat, err := ta.zconn.Exists(zkRestartSlaveDataPath)
	if stat != nil {
		err = fmt.Errorf("slave restart data already exists - suspicious: %v", zkRestartSlaveDataPath)
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
	_, err = ta.zconn.Create(zkRestartSlaveDataPath, jscfg.ToJson(rsd), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
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

func (ta *TabletActor) masterPosition(args map[string]string) error {
	zkReplyPath, ok := args["ReplyPath"]
	if !ok {
		return fmt.Errorf("missing ReplyPath in args")
	}

	position, err := ta.mysqld.MasterStatus()
	if err != nil {
		return err
	}
	relog.Debug("MasterPosition %#v %v", *position, zkReplyPath)
	_, err = ta.zconn.Create(zkReplyPath, jscfg.ToJson(position), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ta *TabletActor) slavePosition(args map[string]string) error {
	zkReplyPath, ok := args["ReplyPath"]
	if !ok {
		return fmt.Errorf("missing ReplyPath in args")
	}

	position, err := ta.mysqld.SlaveStatus()
	if err != nil {
		return err
	}
	relog.Debug("SlavePosition %#v %v", *position, zkReplyPath)
	_, err = ta.zconn.Create(zkReplyPath, jscfg.ToJson(position), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ta *TabletActor) waitSlavePosition(args map[string]string) error {
	zkArgsPath, ok := args["ArgsPath"]
	if !ok {
		return fmt.Errorf("missing ArgsPath in args")
	}

	data, _, err := ta.zconn.Get(zkArgsPath)
	if err != nil {
		return err
	}

	slavePos := new(SlavePositionReq)
	if err = json.Unmarshal([]byte(data), slavePos); err != nil {
		return err
	}

	relog.Debug("WaitSlavePosition %#v %v", *slavePos, zkArgsPath)
	err = ta.mysqld.WaitMasterPos(&slavePos.ReplicationPosition, slavePos.WaitTimeout)
	if err != nil {
		return err
	}

	return ta.slavePosition(args)
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

	zkRestartSlaveDataPath := path.Join(zkShardActionPath, restartSlaveDataFilename)
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

func (ta *TabletActor) getSchema(args map[string]string) error {
	// get where we put the response
	zkReplyPath, ok := args["ReplyPath"]
	if !ok {
		return fmt.Errorf("missing ReplyPath in args")
	}

	// read the tablet to get the dbname
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// and get the schema
	sd, err := ta.mysqld.GetSchema(tablet.DbName())
	if err != nil {
		return err
	}

	_, err = ta.zconn.Create(zkReplyPath, jscfg.ToJson(sd), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ta *TabletActor) executeHook(actionPath string, args map[string]string) (err error) {
	// get where we put the response
	zkReplyPath, ok := args["HookReplyPath"]
	if !ok {
		return fmt.Errorf("missing ReplyPath in args")
	}
	delete(args, "HookReplyPath")
	zkReplyPath, err = ta.createReplyPath(actionPath, zkReplyPath)
	if err != nil {
		return err
	}

	// reconstruct the Hook, execute it
	name := args["HookName"]
	delete(args, "HookName")
	hook := &Hook{Name: name, Parameters: args}
	hr := hook.Execute()

	_, err = ta.zconn.Create(zkReplyPath, jscfg.ToJson(hr), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

// Operate on a backup tablet. Shutdown mysqld and copy the data files aside.
func (ta *TabletActor) snapshot() (*RestoreSlaveData, error) {
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return nil, err
	}

	if tablet.Type != TYPE_BACKUP {
		return nil, fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	replicaSource, err := ta.mysqld.CreateSnapshot(tablet.DbName(), tablet.Addr, false)
	if err != nil {
		return nil, err
	}

	rsd := &RestoreSlaveData{ReplicaSource: replicaSource}
	if tablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doens't work in hierarchical replication.
		rsd.Parent = tablet.Alias()
	} else {
		rsd.Parent = tablet.Parent
	}

	rsdZkPath := path.Join(ta.zkTabletPath, restoreSlaveDataFilename)
	_, err = ta.zconn.Create(rsdZkPath, jscfg.ToJson(rsd), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return nil, err
	}

	return rsd, nil
}

// Operate on restore tablet.
// Check that the ReplicaSource is valid and the master has not changed.
// Shutdown mysqld.
// Load the snapshot from source tablet.
// Restart mysqld and replication.
// Put tablet into the replication graph as a spare.
func (ta *TabletActor) restore(args map[string]string) error {
	zkSrcTabletPath, ok := args["SrcTabletPath"]
	if !ok {
		return fmt.Errorf("missing SrcTabletPath in args")
	}

	data, _, err := ta.zconn.Get(path.Join(zkSrcTabletPath, restoreSlaveDataFilename))
	if err != nil {
		return err
	}

	rsd := new(RestoreSlaveData)
	if err := json.Unmarshal([]byte(data), rsd); err != nil {
		return err
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != TYPE_RESTORE {
		return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	parentPath := TabletPathForAlias(rsd.Parent)
	parentTablet, err := ReadTablet(ta.zconn, parentPath)
	if err != nil {
		return err
	}

	if parentTablet.Type != TYPE_MASTER {
		return fmt.Errorf("restore expected master parent: %v %v", parentTablet.Type, parentPath)
	}

	if err := ta.mysqld.RestoreFromSnapshot(rsd.ReplicaSource); err != nil {
		relog.Error("RestoreFromSnapshot failed: %v", err)
		return err
	}

	// Once this action completes, update authoritive tablet node first.
	tablet.Parent = rsd.Parent
	tablet.Keyspace = parentTablet.Keyspace
	tablet.Shard = parentTablet.Shard
	tablet.Type = TYPE_SPARE
	tablet.KeyRange = parentTablet.KeyRange

	if err := UpdateTablet(ta.zconn, ta.zkTabletPath, tablet); err != nil {
		return err
	}

	return CreateTabletReplicationPaths(ta.zconn, ta.zkTabletPath, tablet.Tablet)
}

// Operate on a backup tablet. Halt mysqld (read-only, lock tables)
// and dump the partial data files.
func (ta *TabletActor) partialSnapshot(args map[string]string) (*PartialRestoreSlaveData, error) {
	keyName, ok := args["KeyName"]
	if !ok {
		return nil, fmt.Errorf("missing KeyName in args")
	}
	startKey, ok := args["StartKey"]
	if !ok {
		return nil, fmt.Errorf("missing StartKey in args")
	}
	endKey, ok := args["EndKey"]
	if !ok {
		return nil, fmt.Errorf("missing EndKey in args")
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return nil, err
	}

	if tablet.Type != TYPE_BACKUP {
		return nil, fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	splitReplicaSource, err := ta.mysqld.CreateSplitReplicaSource(tablet.DbName(), keyName, key.HexKeyspaceId(startKey), key.HexKeyspaceId(endKey), tablet.Addr, false)
	if err != nil {
		return nil, err
	}

	rsd := &PartialRestoreSlaveData{SplitReplicaSource: splitReplicaSource}
	if tablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doens't work in hierarchical replication.
		rsd.Parent = tablet.Alias()
	} else {
		rsd.Parent = tablet.Parent
	}

	rsdZkPath := path.Join(ta.zkTabletPath, partialRestoreSlaveDataFilename)
	_, err = ta.zconn.Create(rsdZkPath, jscfg.ToJson(rsd), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return nil, err
	}

	return rsd, nil
}

// Operate on restore tablet.
// Check that the ReplicaSource is valid and the master has not changed.
// Put Mysql in read-only mode.
// Load the snapshot from source tablet.
// FIXME(alainjobart) which state should the tablet be in? it is a slave,
//   but with a much smaller keyspace. For now, do the same as snapshot,
//   but this is very dangerous, it cannot be used as a real slave
//   or promoted to master in the same shard!
// Put tablet into the replication graph as a spare.
func (ta *TabletActor) partialRestore(args map[string]string) error {
	zkSrcTabletPath, ok := args["SrcTabletPath"]
	if !ok {
		return fmt.Errorf("missing SrcTabletPath in args")
	}

	data, _, err := ta.zconn.Get(path.Join(zkSrcTabletPath, partialRestoreSlaveDataFilename))
	if err != nil {
		return err
	}

	rsd := new(PartialRestoreSlaveData)
	if err := json.Unmarshal([]byte(data), rsd); err != nil {
		return err
	}

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != TYPE_RESTORE {
		return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	parentPath := TabletPathForAlias(rsd.Parent)
	parentTablet, err := ReadTablet(ta.zconn, parentPath)
	if err != nil {
		return err
	}

	if parentTablet.Type != TYPE_MASTER {
		return fmt.Errorf("restore expected master parent: %v %v", parentTablet.Type, parentPath)
	}

	if err := ta.mysqld.RestoreFromPartialSnapshot(rsd.SplitReplicaSource); err != nil {
		relog.Error("RestoreFromPartialSnapshot failed: %v", err)
		return err
	}

	// Once this action completes, update authoritive tablet node first.
	tablet.Parent = rsd.Parent
	tablet.Keyspace = parentTablet.Keyspace
	tablet.Shard = parentTablet.Shard
	tablet.Type = TYPE_SPARE
	tablet.KeyRange = rsd.SplitReplicaSource.KeyRange

	if err := UpdateTablet(ta.zconn, ta.zkTabletPath, tablet); err != nil {
		return err
	}

	return CreateTabletReplicationPaths(ta.zconn, ta.zkTabletPath, tablet.Tablet)
}

// Make this external, since in needs to be forced from time to time.
func Scrap(zconn zk.Conn, zkTabletPath string, force bool) error {
	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	wasIdle := false
	replicationPath := ""
	if tablet.Type == TYPE_IDLE {
		wasIdle = true
	} else {
		replicationPath = tablet.ReplicationPath()
	}
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
		err = zconn.Delete(replicationPath, -1)
		if err != nil {
			switch err.(*zookeeper.Error).Code {
			case zookeeper.ZNONODE:
				relog.Debug("no replication path: %v", replicationPath)
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
		tablet.KeyRange = key.KeyRange{}
	}
	return UpdateTablet(zconn, zkTabletPath, tablet)
}
