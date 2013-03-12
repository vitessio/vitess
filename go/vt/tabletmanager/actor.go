// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/hook"
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

func (rsd *RestartSlaveData) String() string {
	return fmt.Sprintf("RestartSlaveData{ReplicationState:%#v WaitPosition:%#v TimePromoted:%v Parent:%v Force:%v}", rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted, rsd.Parent, rsd.Force)
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

// This function should be protected from unforseen panics, as
// dispatchAction will catch everything. The rest of the code in this
// function should not panic.
func (ta *TabletActor) HandleAction(actionPath, action, actionGuid string, forceRerun bool) error {
	var err error
	ta.zkTabletPath, err = TabletPathFromActionPath(actionPath)
	if err != nil {
		return err
	}
	ta.zkVtRoot, err = VtRootFromTabletPath(ta.zkTabletPath)
	if err != nil {
		return err
	}

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
		// see if the process is still running, and if so, wait for it
		if _, err := os.FindProcess(actionNode.Pid); err != nil {
			// process is dead, either clean up or re-run
			if !forceRerun {
				actionErr := fmt.Errorf("Previous vtaction process died")
				if err := StoreActionResponse(ta.zconn, actionNode, actionPath, actionErr); err != nil {
					relog.Error("Dead process detector failed to update actionNode: %v", err)
				}
				return actionErr
			}
		} else {
			relog.Warning("HandleAction waiting for running action: %v", actionPath)
			_, err := WaitForCompletion(ta.zconn, actionPath, 0)
			return err
		}
	case ACTION_STATE_FAILED:
		// this happens only in a couple cases:
		// - vtaction was killed by a signal and we caught it
		// - vtaction died unexpectedly, and the next vtaction run detected it
		return fmt.Errorf(actionNode.Error)
	case ACTION_STATE_DONE:
		// this is bad
		return fmt.Errorf("Unexpected finished ActionNode in action queue: %v", actionPath)
	}

	// Claim the action by this process.
	actionNode.State = ACTION_STATE_RUNNING
	actionNode.Pid = os.Getpid()
	newData := ActionNodeToJson(actionNode)
	_, zkErr = ta.zconn.Set(actionPath, newData, stat.Version())
	if zkErr != nil {
		if zookeeper.IsError(zkErr, zookeeper.ZBADVERSION) {
			// The action is schedule by another actor. Most likely
			// the tablet restarted during an action. Just wait for completion.
			relog.Warning("HandleAction waiting for scheduled action: %v", actionPath)
			_, err := WaitForCompletion(ta.zconn, actionPath, 0)
			return err
		} else {
			return zkErr
		}
	}

	// signal handler after we've signed up for the action
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for sig := range c {
			err := StoreActionResponse(ta.zconn, actionNode, actionPath, fmt.Errorf("vtaction interrupted by signal: %v", sig))
			if err != nil {
				relog.Error("Signal handler failed to update actionNode: %v", err)
				os.Exit(-2)
			}
			os.Exit(-1)
		}
	}()

	relog.Info("HandleAction: %v %v", actionPath, data)
	// validate actions, but don't write this back into zk
	if actionNode.Action != action || actionNode.ActionGuid != actionGuid {
		relog.Error("HandleAction validation failed %v: (%v,%v) (%v,%v)",
			actionPath, actionNode.Action, action, actionNode.ActionGuid, actionGuid)
		return TabletActorError("invalid action initiation: " + action + " " + actionGuid)
	}
	actionErr := ta.dispatchAction(actionNode)
	err = StoreActionResponse(ta.zconn, actionNode, actionPath, actionErr)
	if err != nil {
		return err
	}

	// remove from zk on completion
	zkErr = ta.zconn.Delete(actionPath, -1)
	if zkErr != nil {
		relog.Error("HandleAction failed deleting: %v", zkErr)
		return zkErr
	}
	return actionErr
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
		err = ta.changeType(actionNode)
	case TABLET_ACTION_DEMOTE_MASTER:
		err = ta.demoteMaster()
	case TABLET_ACTION_MASTER_POSITION:
		err = ta.masterPosition(actionNode)
	case TABLET_ACTION_PARTIAL_RESTORE:
		err = ta.partialRestore(actionNode)
	case TABLET_ACTION_PARTIAL_SNAPSHOT:
		err = ta.partialSnapshot(actionNode)
	case TABLET_ACTION_MULTI_SNAPSHOT:
		err = ta.multiSnapshot(actionNode)
	case TABLET_ACTION_MULTI_RESTORE:
		err = ta.multiRestore(actionNode)
	case TABLET_ACTION_PING:
		// Just an end-to-end verification that we got the message.
		err = nil
	case TABLET_ACTION_PROMOTE_SLAVE:
		err = ta.promoteSlave(actionNode)
	case TABLET_ACTION_RESTART_SLAVE:
		err = ta.restartSlave(actionNode)
	case TABLET_ACTION_RESERVE_FOR_RESTORE:
		err = ta.reserveForRestore(actionNode)
	case TABLET_ACTION_RESTORE:
		err = ta.restore(actionNode)
	case TABLET_ACTION_SCRAP:
		err = ta.scrap()
	case TABLET_ACTION_GET_SCHEMA:
		err = ta.getSchema(actionNode)
	case TABLET_ACTION_PREFLIGHT_SCHEMA:
		err = ta.preflightSchema(actionNode)
	case TABLET_ACTION_APPLY_SCHEMA:
		err = ta.applySchema(actionNode)
	case TABLET_ACTION_EXECUTE_HOOK:
		err = ta.executeHook(actionNode)
	case TABLET_ACTION_SET_RDONLY:
		err = ta.setReadOnly(true)
	case TABLET_ACTION_SET_RDWR:
		err = ta.setReadOnly(false)
	case TABLET_ACTION_SLEEP:
		err = ta.sleep(actionNode)
	case TABLET_ACTION_SLAVE_POSITION:
		err = ta.slavePosition(actionNode)
	case TABLET_ACTION_REPARENT_POSITION:
		err = ta.reparentPosition(actionNode)
	case TABLET_ACTION_SNAPSHOT:
		err = ta.snapshot(actionNode)
	case TABLET_ACTION_SNAPSHOT_SOURCE_END:
		err = ta.snapshotSourceEnd(actionNode)
	case TABLET_ACTION_STOP_SLAVE:
		err = ta.mysqld.StopSlave()
	case TABLET_ACTION_WAIT_SLAVE_POSITION:
		err = ta.waitSlavePosition(actionNode)
	default:
		err = TabletActorError("invalid action: " + actionNode.Action)
	}

	return
}

// Write the result of an action into zookeeper
func StoreActionResponse(zconn zk.Conn, actionNode *ActionNode, actionPath string, actionErr error) error {
	// change our state
	if actionErr != nil {
		// on failure, set an error field on the node
		actionNode.Error = actionErr.Error()
		actionNode.State = ACTION_STATE_FAILED
	} else {
		actionNode.Error = ""
		actionNode.State = ACTION_STATE_DONE
	}
	actionNode.Pid = 0

	// Write the data first to our action node, then to the log.
	// In the error case, this node will be left behind to debug.
	data := ActionNodeToJson(actionNode)
	_, err := zconn.Set(actionPath, data, -1)
	if err != nil {
		return err
	}
	actionLogPath := ActionToActionLogPath(actionPath)
	_, err = zk.CreateRecursive(zconn, actionLogPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ta *TabletActor) sleep(actionNode *ActionNode) error {
	duration := actionNode.args.(*time.Duration)
	time.Sleep(*duration)
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

func (ta *TabletActor) changeType(actionNode *ActionNode) error {
	dbType := actionNode.args.(*TabletType)
	return ChangeType(ta.zconn, ta.zkTabletPath, *dbType, true)
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

func (ta *TabletActor) promoteSlave(actionNode *ActionNode) error {
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// Perform the action.
	alias := TabletAlias{tablet.Tablet.Cell, tablet.Tablet.Uid}
	rsd := &RestartSlaveData{Parent: alias, Force: (tablet.Parent.Uid == NO_TABLET)}
	rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted, err = ta.mysqld.PromoteSlave(false)
	if err != nil {
		return err
	}
	relog.Info("PromoteSlave %v", rsd.String())
	actionNode.reply = rsd

	// Remove tablet from the replication graph if this is not already the master.
	if tablet.Parent.Uid != NO_TABLET {
		oldReplicationPath, err := tablet.ReplicationPath()
		if err != nil {
			return err
		}
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
	newReplicationPath, err := tablet.ReplicationPath()
	if err != nil {
		return err
	}
	_, err = ta.zconn.Create(newReplicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}

func (ta *TabletActor) masterPosition(actionNode *ActionNode) error {
	position, err := ta.mysqld.MasterStatus()
	if err != nil {
		return err
	}
	relog.Debug("MasterPosition %#v", *position)
	actionNode.reply = position
	return nil
}

func (ta *TabletActor) slavePosition(actionNode *ActionNode) error {
	position, err := ta.mysqld.SlaveStatus()
	if err != nil {
		return err
	}
	relog.Debug("SlavePosition %#v", *position)
	actionNode.reply = position
	return nil
}

func (ta *TabletActor) reparentPosition(actionNode *ActionNode) error {
	slavePos := actionNode.args.(*mysqlctl.ReplicationPosition)

	replicationState, waitPosition, timePromoted, err := ta.mysqld.ReparentPosition(slavePos)
	if err != nil {
		return err
	}
	rsd := new(RestartSlaveData)
	rsd.ReplicationState = replicationState
	rsd.TimePromoted = timePromoted
	rsd.WaitPosition = waitPosition
	parts := strings.Split(ta.zkTabletPath, "/")
	uid, err := ParseUid(parts[len(parts)-1])
	if err != nil {
		return err
	}
	rsd.Parent = TabletAlias{parts[2], uid}
	relog.Debug("reparentPosition %v", rsd.String())
	actionNode.reply = rsd
	return nil
}

func (ta *TabletActor) waitSlavePosition(actionNode *ActionNode) error {
	slavePos := actionNode.args.(*SlavePositionReq)
	relog.Debug("WaitSlavePosition %#v", *slavePos)
	if err := ta.mysqld.WaitMasterPos(&slavePos.ReplicationPosition, slavePos.WaitTimeout); err != nil {
		return err
	}

	return ta.slavePosition(actionNode)
}

func (ta *TabletActor) restartSlave(actionNode *ActionNode) error {
	rsd := actionNode.args.(*RestartSlaveData)

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// If this check fails, we seem reparented. The only part that could have failed
	// is the insert in the replication graph. Do NOT try to reparent
	// again. That will either wedge replication to corrupt data.
	if tablet.Parent != rsd.Parent {
		relog.Debug("restart with new parent")
		// Remove tablet from the replication graph.
		oldReplicationPath, err := tablet.ReplicationPath()
		if err != nil {
			return err
		}
		err = ta.zconn.Delete(oldReplicationPath, -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return err
		}

		// Move a lag slave into the orphan lag type so we can safely ignore
		// this reparenting until replication catches up.
		if tablet.Type == TYPE_LAG {
			tablet.Type = TYPE_LAG_ORPHAN
		} else {
			err = ta.mysqld.RestartSlave(rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted)
			if err != nil {
				return err
			}
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
		// Complete the special orphan accounting.
		if tablet.Type == TYPE_LAG_ORPHAN {
			tablet.Type = TYPE_LAG
			err = UpdateTablet(ta.zconn, ta.zkTabletPath, tablet)
			if err != nil {
				return err
			}
		}
	}

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	newReplicationPath, err := tablet.ReplicationPath()
	if err != nil {
		return err
	}
	_, err = ta.zconn.Create(newReplicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}

func (ta *TabletActor) scrap() error {
	return Scrap(ta.zconn, ta.zkTabletPath, false)
}

func (ta *TabletActor) getSchema(actionNode *ActionNode) error {
	gsa := actionNode.args.(*GetSchemaArgs)

	// read the tablet to get the dbname
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// and get the schema
	sd, err := ta.mysqld.GetSchema(tablet.DbName(), gsa.Tables, gsa.IncludeViews)
	if err != nil {
		return err
	}
	actionNode.reply = sd
	return nil
}

func (ta *TabletActor) preflightSchema(actionNode *ActionNode) error {
	change := actionNode.args.(*string)

	// read the tablet to get the dbname
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// and preflight the change
	scr, err := ta.mysqld.PreflightSchemaChange(tablet.DbName(), *change)
	if err != nil {
		return err
	}
	actionNode.reply = scr
	return nil
}

func (ta *TabletActor) applySchema(actionNode *ActionNode) error {
	sc := actionNode.args.(*mysqlctl.SchemaChange)

	// read the tablet to get the dbname
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	// and apply the change
	scr, err := ta.mysqld.ApplySchemaChange(tablet.DbName(), sc)
	if err != nil {
		return err
	}
	actionNode.reply = scr
	return nil
}

func (ta *TabletActor) executeHook(actionNode *ActionNode) (err error) {
	// FIXME(msolomon) should't the reply get distilled into an error?
	actionNode.reply = actionNode.args.(*hook.Hook).Execute()
	return nil
}

// Operate on a backup tablet. Shutdown mysqld and copy the data files aside.
func (ta *TabletActor) snapshot(actionNode *ActionNode) error {
	args := actionNode.args.(*SnapshotArgs)

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	if tablet.Type != TYPE_BACKUP {
		return fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	filename, slaveStartRequired, readOnly, err := ta.mysqld.CreateSnapshot(tablet.DbName(), tablet.Addr, false, args.Concurrency, args.ServerMode)
	if err != nil {
		return err
	}

	sr := &SnapshotReply{ManifestPath: filename, SlaveStartRequired: slaveStartRequired, ReadOnly: readOnly}
	if tablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		sr.ZkParentPath = tablet.Path()
	} else {
		sr.ZkParentPath = TabletPathForAlias(tablet.Parent)
	}
	actionNode.reply = sr
	return nil
}

func (ta *TabletActor) snapshotSourceEnd(actionNode *ActionNode) error {
	args := actionNode.args.(*SnapshotSourceEndArgs)

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	if tablet.Type != TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("expected snapshot_source type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	return ta.mysqld.SnapshotSourceEnd(args.SlaveStartRequired, args.ReadOnly, true)
}

// fetch a json file and parses it
func fetchAndParseJsonFile(addr, filename string, result interface{}) error {
	// read the manifest
	murl := "http://" + addr + filename
	resp, err := http.Get(murl)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Error fetching url %v: %v", murl, resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	// unpack it
	return json.Unmarshal(data, result)
}

// change a tablet type to RESTORE and set all the other arguments.
// from now on, we have to go to either SPARE or SCRAP
func (ta *TabletActor) changeTypeToRestore(tablet, sourceTablet *TabletInfo, parentAlias TabletAlias, keyRange key.KeyRange) error {
	// run the optional idle_server_check hook
	if err := hook.NewSimpleHook("idle_server_check").ExecuteOptional(); err != nil {
		return err
	}

	// change the type
	tablet.Parent = parentAlias
	tablet.Keyspace = sourceTablet.Keyspace
	tablet.Shard = sourceTablet.Shard
	tablet.Type = TYPE_RESTORE
	tablet.KeyRange = keyRange
	tablet.DbNameOverride = sourceTablet.DbNameOverride
	if err := UpdateTablet(ta.zconn, ta.zkTabletPath, tablet); err != nil {
		return err
	}

	// and create the replication graph items
	return CreateTabletReplicationPaths(ta.zconn, ta.zkTabletPath, tablet.Tablet)
}

// Reserve a tablet for restore.
// Can be called remotely
func (ta *TabletActor) reserveForRestore(actionNode *ActionNode) error {
	// first check mysql, no need to go further if we can't restore
	if err := ta.mysqld.ValidateCloneTarget(); err != nil {
		return err
	}
	args := actionNode.args.(*ReserveForRestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	// read the source tablet
	sourceTablet, err := ReadTablet(ta.zconn, args.ZkSrcTabletPath)
	if err != nil {
		return err
	}

	// find the parent tablet alias we will be using
	var parentAlias TabletAlias
	if sourceTablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		parentAlias = sourceTablet.Alias()
	} else {
		parentAlias = sourceTablet.Parent
	}

	return ta.changeTypeToRestore(tablet, sourceTablet, parentAlias, sourceTablet.KeyRange)
}

// Operate on restore tablet.
// Check that the SnapshotManifest is valid and the master has not changed.
// Shutdown mysqld.
// Load the snapshot from source tablet.
// Restart mysqld and replication.
// Put tablet into the replication graph as a spare.
func (ta *TabletActor) restore(actionNode *ActionNode) error {
	args := actionNode.args.(*RestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if args.WasReserved {
		if tablet.Type != TYPE_RESTORE {
			return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, ta.zkTabletPath)
		}
	} else {
		if tablet.Type != TYPE_IDLE {
			return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.zkTabletPath)
		}
	}

	// read the source tablet, compute args.SrcFilePath if default
	sourceTablet, err := ReadTablet(ta.zconn, args.ZkSrcTabletPath)
	if err != nil {
		return err
	}
	if strings.ToLower(args.SrcFilePath) == "default" {
		args.SrcFilePath = path.Join(mysqlctl.SnapshotURLPath, mysqlctl.SnapshotManifestFile)
	}

	// read the parent tablet, verify its state
	parentTablet, err := ReadTablet(ta.zconn, args.ZkParentPath)
	if err != nil {
		return err
	}
	if parentTablet.Type != TYPE_MASTER && parentTablet.Type != TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("restore expected master or snapshot_source parent: %v %v", parentTablet.Type, args.ZkParentPath)
	}

	// read & unpack the manifest
	sm := new(mysqlctl.SnapshotManifest)
	if err := fetchAndParseJsonFile(sourceTablet.Addr, args.SrcFilePath, sm); err != nil {
		return err
	}

	if !args.WasReserved {
		if err := ta.changeTypeToRestore(tablet, sourceTablet, parentTablet.Alias(), sourceTablet.KeyRange); err != nil {
			return err
		}
	}

	// do the work
	if err := ta.mysqld.RestoreFromSnapshot(sm, args.FetchConcurrency, args.FetchRetryCount, args.DontWaitForSlaveStart); err != nil {
		relog.Error("RestoreFromSnapshot failed (%v), scrapping", err)
		if err := Scrap(ta.zconn, ta.zkTabletPath, false); err != nil {
			relog.Error("Failed to Scrap after failed RestoreFromSnapshot: %v", err)
		}

		return err
	}

	// change to TYPE_SPARE, we're done!
	return ChangeType(ta.zconn, ta.zkTabletPath, TYPE_SPARE, true)
}

// Operate on a backup tablet. Halt mysqld (read-only, lock tables)
// and dump the partial data files.
func (ta *TabletActor) partialSnapshot(actionNode *ActionNode) error {
	args := actionNode.args.(*PartialSnapshotArgs)

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	if tablet.Type != TYPE_BACKUP {
		return fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	filename, err := ta.mysqld.CreateSplitSnapshot(tablet.DbName(), args.KeyName, args.StartKey, args.EndKey, tablet.Addr, false, args.Concurrency)
	if err != nil {
		return err
	}

	sr := &SnapshotReply{ManifestPath: filename}
	if tablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doens't work in hierarchical replication.
		sr.ZkParentPath = tablet.Path()
	} else {
		sr.ZkParentPath = TabletPathForAlias(tablet.Parent)
	}
	actionNode.reply = sr
	return nil
}

func (ta *TabletActor) multiSnapshot(actionNode *ActionNode) error {
	args := actionNode.args.(*MultiSnapshotArgs)

	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}

	if tablet.Type != TYPE_BACKUP {
		return fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	filenames, err := ta.mysqld.CreateMultiSnapshot(args.KeyRanges, tablet.DbName(), args.KeyName, tablet.Addr, false, args.Concurrency, args.Tables)
	if err != nil {
		return err
	}

	sr := &MultiSnapshotReply{ManifestPaths: filenames}
	if tablet.Parent.Uid == NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doens't work in hierarchical replication.
		sr.ZkParentPath = tablet.Path()
	} else {
		sr.ZkParentPath = TabletPathForAlias(tablet.Parent)
	}
	actionNode.reply = sr
	return nil
}

func (ta *TabletActor) multiRestore(actionNode *ActionNode) (err error) {
	args := actionNode.args.(*MultiRestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if !args.Force && tablet.Type != TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	// get source tablets addresses
	sourceAddrs := make([]*url.URL, len(args.ZkSrcTabletPaths))
	for i, path := range args.ZkSrcTabletPaths {
		t, e := ReadTablet(ta.zconn, path)
		if e != nil {
			return e
		}
		sourceAddrs[i] = &url.URL{Host: t.Addr, Path: "/" + t.DbName()}
	}
	// NOTE(szopa): This is a subset of what changeTypeToRestore
	// does. This guy is going to be a master, it should not
	// inherit the keyspace or db name from the sources.
	tablet.Type = TYPE_RESTORE
	tablet.KeyRange = args.KeyRange
	if err := UpdateTablet(ta.zconn, ta.zkTabletPath, tablet); err != nil {
		return err
	}

	if err := ta.mysqld.RestoreFromMultiSnapshot(args.DbName, args.KeyRange, sourceAddrs, args.Concurrency, args.FetchConcurrency, args.FetchRetryCount, args.Force); err != nil {
		if e := Scrap(ta.zconn, ta.zkTabletPath, false); e != nil {
			relog.Error("Failed to Scrap after failed RestoreFromMultiSnapshot: %v", e)
		}
		return err
	}

	return ChangeType(ta.zconn, ta.zkTabletPath, TYPE_SPARE, true)
}

// Operate on restore tablet.
// Check that the SnapshotManifest is valid and the master has not changed.
// Put Mysql in read-only mode.
// Load the snapshot from source tablet.
// FIXME(alainjobart) which state should the tablet be in? it is a slave,
//   but with a much smaller keyspace. For now, do the same as snapshot,
//   but this is very dangerous, it cannot be used as a real slave
//   or promoted to master in the same shard!
// Put tablet into the replication graph as a spare.
func (ta *TabletActor) partialRestore(actionNode *ActionNode) error {
	args := actionNode.args.(*RestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ReadTablet(ta.zconn, ta.zkTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.zkTabletPath)
	}

	// read the source tablet
	sourceTablet, err := ReadTablet(ta.zconn, args.ZkSrcTabletPath)
	if err != nil {
		return err
	}

	// read the parent tablet, verify its state
	parentTablet, err := ReadTablet(ta.zconn, args.ZkParentPath)
	if err != nil {
		return err
	}
	if parentTablet.Type != TYPE_MASTER {
		return fmt.Errorf("restore expected master parent: %v %v", parentTablet.Type, args.ZkParentPath)
	}

	// read & unpack the manifest
	ssm := new(mysqlctl.SplitSnapshotManifest)
	if err := fetchAndParseJsonFile(sourceTablet.Addr, args.SrcFilePath, ssm); err != nil {
		return err
	}

	// change our type to RESTORE and set all the other arguments.
	if err := ta.changeTypeToRestore(tablet, sourceTablet, parentTablet.Alias(), ssm.KeyRange); err != nil {
		return err
	}

	// do the work
	if err := ta.mysqld.RestoreFromPartialSnapshot(ssm, args.FetchConcurrency, args.FetchRetryCount); err != nil {
		relog.Error("RestoreFromPartialSnapshot failed: %v", err)
		if err := Scrap(ta.zconn, ta.zkTabletPath, false); err != nil {
			relog.Error("Failed to Scrap after failed RestoreFromPartialSnapshot: %v", err)
		}
		return err
	}

	// change to TYPE_SPARE, we're done!
	return ChangeType(ta.zconn, ta.zkTabletPath, TYPE_MASTER, true)
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
		replicationPath, err = tablet.ReplicationPath()
		if err != nil {
			return err
		}
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
		actionPath, err := TabletActionPath(zkTabletPath)
		if err != nil {
			relog.Warning("TabletActionPath(%v) failed: %v", zkTabletPath, err)
		} else {
			err = PurgeActions(zconn, actionPath)
			if err != nil {
				relog.Warning("purge actions failed: %v %v", actionPath, err)
			}
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

	// run a hook for final cleanup, only in non-force mode.
	// (force mode executes on the vtctl side, not on the vttablet side)
	if !force {
		if err := hook.NewSimpleHook("postflight_scrap").ExecuteOptional(); err != nil {
			// we don't want to return an error, the server
			// is already in bad shape probably.
			relog.Warning("Scrap: postflight_scrap failed: %v", err)
		}
	}

	return nil
}

// Make this external, since these transitions need to be forced from time to time.
func ChangeType(zconn zk.Conn, zkTabletPath string, newType TabletType, runHooks bool) error {
	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	if !IsTrivialTypeChange(tablet.Type, newType) || !IsValidTypeChange(tablet.Type, newType) {
		return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, zkTabletPath)
	}

	if runHooks {
		// Only run the idle_server_check hook when
		// transitioning from non-serving to serving.
		if !IsServingType(tablet.Type) && IsServingType(newType) {
			if err := hook.NewSimpleHook("idle_server_check").ExecuteOptional(); err != nil {
				return err
			}
		}

		// Run the live_server_check any time we transition to a serving type.
		if IsServingType(newType) {
			if err := hook.NewSimpleHook("live_server_check").ExecuteOptional(); err != nil {
				return err
			}
		}
	}

	tablet.Type = newType
	if newType == TYPE_IDLE {
		if tablet.Parent.Uid == NO_TABLET {
			// With a master the node cannot be set to idle unless we have already removed all of
			// the derived paths. The global replication path is a good indication that this has
			// been resolved.
			rp, err := tablet.ReplicationPath()
			if err != nil {
				return err
			}
			stat, err := zconn.Exists(rp)
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
