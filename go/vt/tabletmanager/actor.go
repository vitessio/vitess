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
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
	"github.com/youtube/vitess/go/vt/topo"
)

// The actor applies individual commands to execute an action read
// from a node in topology server. Anything that modifies the state of the
// table should be applied by this code.
//
// The actor signals completion by removing the action node from topology server.
//
// Errors are written to the action node and must (currently) be resolved
// by hand using topo.Server tools.

type TabletActorError string

func (e TabletActorError) Error() string {
	return string(e)
}

type TabletActor struct {
	mysqld      *mysqlctl.Mysqld
	mysqlDaemon mysqlctl.MysqlDaemon
	ts          topo.Server
	tabletAlias topo.TabletAlias
}

func NewTabletActor(mysqld *mysqlctl.Mysqld, mysqlDaemon mysqlctl.MysqlDaemon, topoServer topo.Server, tabletAlias topo.TabletAlias) *TabletActor {
	return &TabletActor{mysqld, mysqlDaemon, topoServer, tabletAlias}
}

// This function should be protected from unforseen panics, as
// dispatchAction will catch everything. The rest of the code in this
// function should not panic.
func (ta *TabletActor) HandleAction(actionPath, action, actionGuid string, forceRerun bool) error {
	tabletAlias, data, version, err := ta.ts.ReadTabletActionPath(actionPath)
	ta.tabletAlias = tabletAlias
	actionNode, err := actionnode.ActionNodeFromJson(data, actionPath)
	if err != nil {
		log.Errorf("HandleAction failed unmarshaling %v: %v", actionPath, err)
		return err
	}

	switch actionNode.State {
	case actionnode.ACTION_STATE_RUNNING:
		// see if the process is still running, and if so, wait for it
		proc, _ := os.FindProcess(actionNode.Pid)
		if proc.Signal(syscall.Signal(0)) == syscall.ESRCH {
			// process is dead, either clean up or re-run
			if !forceRerun {
				actionErr := fmt.Errorf("Previous vtaction process died")
				if err := StoreActionResponse(ta.ts, actionNode, actionPath, actionErr); err != nil {
					log.Errorf("Dead process detector failed to update actionNode: %v", err)
				}
				return actionErr
			}
		} else {
			log.Warningf("HandleAction waiting for running action: %v", actionPath)
			_, err := initiator.WaitForCompletion(ta.ts, actionPath, 0)
			return err
		}
	case actionnode.ACTION_STATE_FAILED:
		// this happens only in a couple cases:
		// - vtaction was killed by a signal and we caught it
		// - vtaction died unexpectedly, and the next vtaction run detected it
		return fmt.Errorf(actionNode.Error)
	case actionnode.ACTION_STATE_DONE:
		// this is bad
		return fmt.Errorf("Unexpected finished ActionNode in action queue: %v", actionPath)
	}

	// Claim the action by this process.
	actionNode.State = actionnode.ACTION_STATE_RUNNING
	actionNode.Pid = os.Getpid()
	newData := actionNode.ToJson()
	err = ta.ts.UpdateTabletAction(actionPath, newData, version)
	if err != nil {
		if err == topo.ErrBadVersion {
			// The action is schedule by another
			// actor. Most likely the tablet restarted
			// during an action. Just wait for completion.
			log.Warningf("HandleAction waiting for scheduled action: %v", actionPath)
			_, err = initiator.WaitForCompletion(ta.ts, actionPath, 0)
			return err
		} else {
			return err
		}
	}

	// signal handler after we've signed up for the action
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for sig := range c {
			err := StoreActionResponse(ta.ts, actionNode, actionPath, fmt.Errorf("vtaction interrupted by signal: %v", sig))
			if err != nil {
				log.Errorf("Signal handler failed to update actionNode: %v", err)
				os.Exit(-2)
			}
			os.Exit(-1)
		}
	}()

	log.Infof("HandleAction: %v %v", actionPath, data)
	// validate actions, but don't write this back into topo.Server
	if actionNode.Action != action || actionNode.ActionGuid != actionGuid {
		log.Errorf("HandleAction validation failed %v: (%v,%v) (%v,%v)",
			actionPath, actionNode.Action, action, actionNode.ActionGuid, actionGuid)
		return TabletActorError("invalid action initiation: " + action + " " + actionGuid)
	}
	actionErr := ta.dispatchAction(actionNode)
	if err := StoreActionResponse(ta.ts, actionNode, actionPath, actionErr); err != nil {
		return err
	}

	// unblock in topo.Server on completion
	if err := ta.ts.UnblockTabletAction(actionPath); err != nil {
		log.Errorf("HandleAction failed unblocking: %v", err)
		return err
	}
	return actionErr
}

func (ta *TabletActor) dispatchAction(actionNode *actionnode.ActionNode) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = tb.Errorf("dispatchAction panic %v", x)
		}
	}()

	switch actionNode.Action {
	case actionnode.TABLET_ACTION_BREAK_SLAVES:
		err = ta.mysqld.BreakSlaves()
	case actionnode.TABLET_ACTION_CHANGE_TYPE:
		err = ta.changeType(actionNode)
	case actionnode.TABLET_ACTION_DEMOTE_MASTER:
		err = ta.demoteMaster()
	case actionnode.TABLET_ACTION_MULTI_SNAPSHOT:
		err = ta.multiSnapshot(actionNode)
	case actionnode.TABLET_ACTION_MULTI_RESTORE:
		err = ta.multiRestore(actionNode)
	case actionnode.TABLET_ACTION_PING:
		// Just an end-to-end verification that we got the message.
		err = nil
	case actionnode.TABLET_ACTION_PROMOTE_SLAVE:
		err = ta.promoteSlave(ta.mysqlDaemon, actionNode)
	case actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED:
		err = SlaveWasPromoted(ta.ts, ta.mysqlDaemon, ta.tabletAlias)
	case actionnode.TABLET_ACTION_RESTART_SLAVE:
		err = ta.restartSlave(actionNode)
	case actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED:
		err = SlaveWasRestarted(ta.ts, ta.mysqlDaemon, ta.tabletAlias, actionNode.Args.(*actionnode.SlaveWasRestartedArgs))
	case actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE:
		err = ta.reserveForRestore(actionNode)
	case actionnode.TABLET_ACTION_RESTORE:
		err = ta.restore(actionNode)
	case actionnode.TABLET_ACTION_SCRAP:
		err = ta.scrap()
	case actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA:
		err = ta.preflightSchema(actionNode)
	case actionnode.TABLET_ACTION_APPLY_SCHEMA:
		err = ta.applySchema(actionNode)
	case actionnode.TABLET_ACTION_EXECUTE_HOOK:
		err = ta.executeHook(actionNode)
	case actionnode.TABLET_ACTION_SET_RDONLY:
		err = ta.setReadOnly(true)
	case actionnode.TABLET_ACTION_SET_RDWR:
		err = ta.setReadOnly(false)
	case actionnode.TABLET_ACTION_SLEEP:
		err = ta.sleep(actionNode)
	case actionnode.TABLET_ACTION_REPARENT_POSITION:
		err = ta.reparentPosition(actionNode)
	case actionnode.TABLET_ACTION_SNAPSHOT:
		err = ta.snapshot(actionNode)
	case actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END:
		err = ta.snapshotSourceEnd(actionNode)

	case actionnode.TABLET_ACTION_SET_BLACKLISTED_TABLES,
		actionnode.TABLET_ACTION_GET_SCHEMA,
		actionnode.TABLET_ACTION_RELOAD_SCHEMA,
		actionnode.TABLET_ACTION_GET_PERMISSIONS,
		actionnode.TABLET_ACTION_SLAVE_POSITION,
		actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION,
		actionnode.TABLET_ACTION_MASTER_POSITION,
		actionnode.TABLET_ACTION_STOP_SLAVE,
		actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM,
		actionnode.TABLET_ACTION_START_SLAVE,
		actionnode.TABLET_ACTION_GET_SLAVES,
		actionnode.TABLET_ACTION_WAIT_BLP_POSITION,
		actionnode.TABLET_ACTION_STOP_BLP,
		actionnode.TABLET_ACTION_START_BLP,
		actionnode.TABLET_ACTION_RUN_BLP_UNTIL:
		err = TabletActorError("Operation " + actionNode.Action + "  only supported as RPC")
	default:
		err = TabletActorError("invalid action: " + actionNode.Action)
	}

	return
}

// Write the result of an action into topology server
func StoreActionResponse(ts topo.Server, actionNode *actionnode.ActionNode, actionPath string, actionErr error) error {
	// change our state
	if actionErr != nil {
		// on failure, set an error field on the node
		actionNode.Error = actionErr.Error()
		actionNode.State = actionnode.ACTION_STATE_FAILED
	} else {
		actionNode.Error = ""
		actionNode.State = actionnode.ACTION_STATE_DONE
	}
	actionNode.Pid = 0

	// Write the data first to our action node, then to the log.
	// In the error case, this node will be left behind to debug.
	data := actionNode.ToJson()
	return ts.StoreTabletActionResponse(actionPath, data)
}

func (ta *TabletActor) sleep(actionNode *actionnode.ActionNode) error {
	duration := actionNode.Args.(*time.Duration)
	time.Sleep(*duration)
	return nil
}

func (ta *TabletActor) setReadOnly(rdonly bool) error {
	err := ta.mysqld.SetReadOnly(rdonly)
	if err != nil {
		return err
	}

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	if rdonly {
		tablet.State = topo.STATE_READ_ONLY
	} else {
		tablet.State = topo.STATE_READ_WRITE
	}
	return topo.UpdateTablet(ta.ts, tablet)
}

func (ta *TabletActor) changeType(actionNode *actionnode.ActionNode) error {
	dbType := actionNode.Args.(*topo.TabletType)
	return ChangeType(ta.ts, ta.tabletAlias, *dbType, true /*runHooks*/)
}

func (ta *TabletActor) demoteMaster() error {
	_, err := ta.mysqld.DemoteMaster()
	if err != nil {
		return err
	}

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	tablet.State = topo.STATE_READ_ONLY
	// NOTE(msolomon) there is no serving graph update - the master tablet will
	// be replaced. Even though writes may fail, reads will succeed. It will be
	// less noisy to simply leave the entry until well promote the master.
	return topo.UpdateTablet(ta.ts, tablet)
}

func (ta *TabletActor) promoteSlave(mysqlDaemon mysqlctl.MysqlDaemon, actionNode *actionnode.ActionNode) error {
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	// Perform the action.
	rsd := &actionnode.RestartSlaveData{Parent: tablet.Alias, Force: (tablet.Parent.Uid == topo.NO_TABLET)}
	rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted, err = ta.mysqld.PromoteSlave(false, ta.hookExtraEnv())
	if err != nil {
		return err
	}
	log.Infof("PromoteSlave %v", rsd.String())
	actionNode.Reply = rsd

	return updateReplicationGraphForPromotedSlave(ta.ts, mysqlDaemon, tablet)
}

func SlaveWasPromoted(ts topo.Server, mysqlDaemon mysqlctl.MysqlDaemon, tabletAlias topo.TabletAlias) error {
	// We first check we don't have a master any more.
	// If we do, it probably means we're not *the* master, and something
	// is really wrong.
	masterAddr, err := mysqlDaemon.GetMasterAddr()
	if err != mysqlctl.ErrNotSlave {
		return fmt.Errorf("new master is a slave: %v %v", masterAddr, err)
	}

	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return updateReplicationGraphForPromotedSlave(ts, mysqlDaemon, tablet)
}

func updateReplicationGraphForPromotedSlave(ts topo.Server, mysqlDaemon mysqlctl.MysqlDaemon, tablet *topo.TabletInfo) error {
	// Remove tablet from the replication graph if this is not already the master.
	if tablet.Parent.Uid != topo.NO_TABLET {
		if err := topo.DeleteTabletReplicationData(ts, tablet.Tablet); err != nil && err != topo.ErrNoNode {
			return err
		}
	}

	// Update tablet regardless - trend towards consistency.
	tablet.State = topo.STATE_READ_WRITE
	tablet.Type = topo.TYPE_MASTER
	tablet.Parent.Cell = ""
	tablet.Parent.Uid = topo.NO_TABLET
	err := topo.UpdateTablet(ts, tablet)
	if err != nil {
		return err
	}
	// NOTE(msolomon) A serving graph update is required, but in
	// order for the shard to be consistent the old master must be
	// scrapped first. That is externally coordinated by the
	// wrangler reparent action.

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(ts, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

func (ta *TabletActor) reparentPosition(actionNode *actionnode.ActionNode) error {
	slavePos := actionNode.Args.(*myproto.ReplicationPosition)

	replicationState, waitPosition, timePromoted, err := ta.mysqld.ReparentPosition(slavePos)
	if err != nil {
		return err
	}
	rsd := new(actionnode.RestartSlaveData)
	rsd.ReplicationState = replicationState
	rsd.TimePromoted = timePromoted
	rsd.WaitPosition = waitPosition
	rsd.Parent = ta.tabletAlias
	log.V(6).Infof("reparentPosition %v", rsd.String())
	actionNode.Reply = rsd
	return nil
}

func (ta *TabletActor) restartSlave(actionNode *actionnode.ActionNode) error {
	rsd := actionNode.Args.(*actionnode.RestartSlaveData)

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	// If this check fails, we seem reparented. The only part that
	// could have failed is the insert in the replication
	// graph. Do NOT try to reparent again. That will either wedge
	// replication or corrupt data.
	if tablet.Parent != rsd.Parent {
		log.V(6).Infof("restart with new parent")
		// Remove tablet from the replication graph.
		if err = topo.DeleteTabletReplicationData(ta.ts, tablet.Tablet); err != nil && err != topo.ErrNoNode {
			return err
		}

		// Move a lag slave into the orphan lag type so we can safely ignore
		// this reparenting until replication catches up.
		if tablet.Type == topo.TYPE_LAG {
			tablet.Type = topo.TYPE_LAG_ORPHAN
		} else {
			err = ta.mysqld.RestartSlave(rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted)
			if err != nil {
				return err
			}
		}
		// Once this action completes, update authoritive tablet node first.
		tablet.Parent = rsd.Parent
		err = topo.UpdateTablet(ta.ts, tablet)
		if err != nil {
			return err
		}
	} else if rsd.Force {
		err = ta.mysqld.RestartSlave(rsd.ReplicationState, rsd.WaitPosition, rsd.TimePromoted)
		if err != nil {
			return err
		}
		// Complete the special orphan accounting.
		if tablet.Type == topo.TYPE_LAG_ORPHAN {
			tablet.Type = topo.TYPE_LAG
			err = topo.UpdateTablet(ta.ts, tablet)
			if err != nil {
				return err
			}
		}
	} else {
		// There is nothing to safely reparent, so check replication. If
		// either replication thread is not running, report an error.
		replicationPos, err := ta.mysqld.SlaveStatus()
		if err != nil {
			return fmt.Errorf("cannot verify replication for slave: %v", err)
		}
		if replicationPos.SecondsBehindMaster == myproto.InvalidLagSeconds {
			return fmt.Errorf("replication not running for slave")
		}
	}

	// Insert the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(ta.ts, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

func SlaveWasRestarted(ts topo.Server, mysqlDaemon mysqlctl.MysqlDaemon, tabletAlias topo.TabletAlias, swrd *actionnode.SlaveWasRestartedArgs) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	// check the reparent actually worked
	masterAddr, err := mysqlDaemon.GetMasterAddr()
	if err != nil {
		return err
	}
	if masterAddr != swrd.ExpectedMasterAddr && masterAddr != swrd.ExpectedMasterIpAddr {
		log.Errorf("SlaveWasRestarted found unexpected master %v for %v (was expecting %v or %v)", masterAddr, tabletAlias, swrd.ExpectedMasterAddr, swrd.ExpectedMasterIpAddr)
		// Disabled for now
		// if swrd.ContinueOnUnexpectedMaster {
		//	log.Errorf("ContinueOnUnexpectedMaster is set, we keep going anyway")
		// } else
		if swrd.ScrapStragglers {
			return Scrap(ts, tablet.Alias, false)
		} else {
			return fmt.Errorf("Unexpected master %v for %v (was expecting %v or %v)", masterAddr, tabletAlias, swrd.ExpectedMasterAddr, swrd.ExpectedMasterIpAddr)
		}
	}

	// Once this action completes, update authoritive tablet node first.
	tablet.Parent = swrd.Parent
	if tablet.Type == topo.TYPE_MASTER {
		tablet.Type = topo.TYPE_SPARE
		tablet.State = topo.STATE_READ_ONLY
	}
	err = topo.UpdateTablet(ts, tablet)
	if err != nil {
		return err
	}

	// Update the new tablet location in the replication graph now that
	// we've updated the tablet.
	err = topo.CreateTabletReplicationData(ts, tablet.Tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	return nil
}

func (ta *TabletActor) scrap() error {
	return Scrap(ta.ts, ta.tabletAlias, false)
}

func (ta *TabletActor) preflightSchema(actionNode *actionnode.ActionNode) error {
	change := actionNode.Args.(*string)

	// read the tablet to get the dbname
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	// and preflight the change
	scr, err := ta.mysqld.PreflightSchemaChange(tablet.DbName(), *change)
	if err != nil {
		return err
	}
	actionNode.Reply = scr
	return nil
}

func (ta *TabletActor) applySchema(actionNode *actionnode.ActionNode) error {
	sc := actionNode.Args.(*myproto.SchemaChange)

	// read the tablet to get the dbname
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	// and apply the change
	scr, err := ta.mysqld.ApplySchemaChange(tablet.DbName(), sc)
	if err != nil {
		return err
	}
	actionNode.Reply = scr
	return nil
}

// add TABLET_ALIAS to environment
func configureTabletHook(hk *hook.Hook, tabletAlias topo.TabletAlias) {
	if hk.ExtraEnv == nil {
		hk.ExtraEnv = make(map[string]string, 1)
	}
	hk.ExtraEnv["TABLET_ALIAS"] = tabletAlias.String()
}

func (ta *TabletActor) executeHook(actionNode *actionnode.ActionNode) (err error) {
	// FIXME(msolomon) should't the reply get distilled into an error?
	h := actionNode.Args.(*hook.Hook)
	configureTabletHook(h, ta.tabletAlias)
	actionNode.Reply = h.Execute()
	return nil
}

func (ta *TabletActor) hookExtraEnv() map[string]string {
	return map[string]string{"TABLET_ALIAS": ta.tabletAlias.String()}
}

// Operate on a backup tablet. Shutdown mysqld and copy the data files aside.
func (ta *TabletActor) snapshot(actionNode *actionnode.ActionNode) error {
	args := actionNode.Args.(*actionnode.SnapshotArgs)

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	if tablet.Type != topo.TYPE_BACKUP {
		return fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.tabletAlias)
	}

	filename, slaveStartRequired, readOnly, err := ta.mysqld.CreateSnapshot(tablet.DbName(), tablet.GetAddr(), false, args.Concurrency, args.ServerMode, ta.hookExtraEnv())
	if err != nil {
		return err
	}

	sr := &actionnode.SnapshotReply{ManifestPath: filename, SlaveStartRequired: slaveStartRequired, ReadOnly: readOnly}
	if tablet.Parent.Uid == topo.NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		sr.ParentAlias = tablet.Alias
	} else {
		sr.ParentAlias = tablet.Parent
	}
	actionNode.Reply = sr
	return nil
}

func (ta *TabletActor) snapshotSourceEnd(actionNode *actionnode.ActionNode) error {
	args := actionNode.Args.(*actionnode.SnapshotSourceEndArgs)

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}

	if tablet.Type != topo.TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("expected snapshot_source type, not %v: %v", tablet.Type, ta.tabletAlias)
	}

	return ta.mysqld.SnapshotSourceEnd(args.SlaveStartRequired, args.ReadOnly, true, ta.hookExtraEnv())
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
// from now on, we can go to:
// - back to IDLE if we don't use the tablet at all (after for instance
//   a successful ReserveForRestore but a failed Snapshot)
// - to SCRAP if something in the process on the target host fails
// - to SPARE if the clone works
func (ta *TabletActor) changeTypeToRestore(tablet, sourceTablet *topo.TabletInfo, parentAlias topo.TabletAlias, keyRange key.KeyRange) error {
	// run the optional preflight_assigned hook
	hk := hook.NewSimpleHook("preflight_assigned")
	configureTabletHook(hk, ta.tabletAlias)
	if err := hk.ExecuteOptional(); err != nil {
		return err
	}

	// change the type
	tablet.Parent = parentAlias
	tablet.Keyspace = sourceTablet.Keyspace
	tablet.Shard = sourceTablet.Shard
	tablet.Type = topo.TYPE_RESTORE
	tablet.KeyRange = keyRange
	tablet.DbNameOverride = sourceTablet.DbNameOverride
	if err := topo.UpdateTablet(ta.ts, tablet); err != nil {
		return err
	}

	// and create the replication graph items
	return topo.CreateTabletReplicationData(ta.ts, tablet.Tablet)
}

// Reserve a tablet for restore.
// Can be called remotely
func (ta *TabletActor) reserveForRestore(actionNode *actionnode.ActionNode) error {
	// first check mysql, no need to go further if we can't restore
	if err := ta.mysqld.ValidateCloneTarget(ta.hookExtraEnv()); err != nil {
		return err
	}
	args := actionNode.Args.(*actionnode.ReserveForRestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.tabletAlias)
	}

	// read the source tablet
	sourceTablet, err := ta.ts.GetTablet(args.SrcTabletAlias)
	if err != nil {
		return err
	}

	// find the parent tablet alias we will be using
	var parentAlias topo.TabletAlias
	if sourceTablet.Parent.Uid == topo.NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doesn't work in hierarchical replication.
		parentAlias = sourceTablet.Alias
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
func (ta *TabletActor) restore(actionNode *actionnode.ActionNode) error {
	args := actionNode.Args.(*actionnode.RestoreArgs)

	// read our current tablet, verify its state
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	if args.WasReserved {
		if tablet.Type != topo.TYPE_RESTORE {
			return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, ta.tabletAlias)
		}
	} else {
		if tablet.Type != topo.TYPE_IDLE {
			return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, ta.tabletAlias)
		}
	}

	// read the source tablet, compute args.SrcFilePath if default
	sourceTablet, err := ta.ts.GetTablet(args.SrcTabletAlias)
	if err != nil {
		return err
	}
	if strings.ToLower(args.SrcFilePath) == "default" {
		args.SrcFilePath = path.Join(mysqlctl.SnapshotURLPath, mysqlctl.SnapshotManifestFile)
	}

	// read the parent tablet, verify its state
	parentTablet, err := ta.ts.GetTablet(args.ParentAlias)
	if err != nil {
		return err
	}
	if parentTablet.Type != topo.TYPE_MASTER && parentTablet.Type != topo.TYPE_SNAPSHOT_SOURCE {
		return fmt.Errorf("restore expected master or snapshot_source parent: %v %v", parentTablet.Type, args.ParentAlias)
	}

	// read & unpack the manifest
	sm := new(mysqlctl.SnapshotManifest)
	if err := fetchAndParseJsonFile(sourceTablet.GetAddr(), args.SrcFilePath, sm); err != nil {
		return err
	}

	if !args.WasReserved {
		if err := ta.changeTypeToRestore(tablet, sourceTablet, parentTablet.Alias, sourceTablet.KeyRange); err != nil {
			return err
		}
	}

	// do the work
	if err := ta.mysqld.RestoreFromSnapshot(sm, args.FetchConcurrency, args.FetchRetryCount, args.DontWaitForSlaveStart, ta.hookExtraEnv()); err != nil {
		log.Errorf("RestoreFromSnapshot failed (%v), scrapping", err)
		if err := Scrap(ta.ts, ta.tabletAlias, false); err != nil {
			log.Errorf("Failed to Scrap after failed RestoreFromSnapshot: %v", err)
		}

		return err
	}

	// change to TYPE_SPARE, we're done!
	return ChangeType(ta.ts, ta.tabletAlias, topo.TYPE_SPARE, true)
}

func (ta *TabletActor) multiSnapshot(actionNode *actionnode.ActionNode) error {
	args := actionNode.Args.(*actionnode.MultiSnapshotArgs)

	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	ki, err := ta.ts.GetKeyspace(tablet.Keyspace)
	if err != nil {
		return err
	}

	if tablet.Type != topo.TYPE_BACKUP {
		return fmt.Errorf("expected backup type, not %v: %v", tablet.Type, ta.tabletAlias)
	}

	filenames, err := ta.mysqld.CreateMultiSnapshot(args.KeyRanges, tablet.DbName(), ki.ShardingColumnName, ki.ShardingColumnType, tablet.GetAddr(), false, args.Concurrency, args.Tables, args.SkipSlaveRestart, args.MaximumFilesize, ta.hookExtraEnv())
	if err != nil {
		return err
	}

	sr := &actionnode.MultiSnapshotReply{ManifestPaths: filenames}
	if tablet.Parent.Uid == topo.NO_TABLET {
		// If this is a master, this will be the new parent.
		// FIXME(msolomon) this doens't work in hierarchical replication.
		sr.ParentAlias = tablet.Alias
	} else {
		sr.ParentAlias = tablet.Parent
	}
	actionNode.Reply = sr
	return nil
}

func (ta *TabletActor) multiRestore(actionNode *actionnode.ActionNode) (err error) {
	args := actionNode.Args.(*actionnode.MultiRestoreArgs)

	// read our current tablet, verify its state
	// we only support restoring to the master or spare replicas
	tablet, err := ta.ts.GetTablet(ta.tabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_MASTER && tablet.Type != topo.TYPE_SPARE && tablet.Type != topo.TYPE_REPLICA && tablet.Type != topo.TYPE_RDONLY {
		return fmt.Errorf("expected master, spare replica or rdonly type, not %v: %v", tablet.Type, ta.tabletAlias)
	}

	// get source tablets addresses
	sourceAddrs := make([]*url.URL, len(args.SrcTabletAliases))
	keyRanges := make([]key.KeyRange, len(args.SrcTabletAliases))
	for i, alias := range args.SrcTabletAliases {
		t, e := ta.ts.GetTablet(alias)
		if e != nil {
			return e
		}
		sourceAddrs[i] = &url.URL{Host: t.GetAddr(), Path: "/" + t.DbName()}
		keyRanges[i], e = key.KeyRangesOverlap(tablet.KeyRange, t.KeyRange)
		if e != nil {
			return e
		}
	}

	// change type to restore, no change to replication graph
	originalType := tablet.Type
	tablet.Type = topo.TYPE_RESTORE
	err = topo.UpdateTablet(ta.ts, tablet)
	if err != nil {
		return err
	}

	// run the action, scrap if it fails
	if err := ta.mysqld.MultiRestore(tablet.DbName(), keyRanges, sourceAddrs, args.Concurrency, args.FetchConcurrency, args.InsertTableConcurrency, args.FetchRetryCount, args.Strategy); err != nil {
		if e := Scrap(ta.ts, ta.tabletAlias, false); e != nil {
			log.Errorf("Failed to Scrap after failed RestoreFromMultiSnapshot: %v", e)
		}
		return err
	}

	// restore type back
	tablet.Type = originalType
	return topo.UpdateTablet(ta.ts, tablet)
}

// Make this external, since in needs to be forced from time to time.
func Scrap(ts topo.Server, tabletAlias topo.TabletAlias, force bool) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	// If you are already scrap, skip updating replication data. It won't
	// be there anyway.
	wasAssigned := tablet.IsAssigned()
	tablet.Type = topo.TYPE_SCRAP
	tablet.Parent = topo.TabletAlias{}
	// Update the tablet first, since that is canonical.
	err = topo.UpdateTablet(ts, tablet)
	if err != nil {
		return err
	}

	// Remove any pending actions. Presumably forcing a scrap means you don't
	// want the agent doing anything and the machine requires manual attention.
	if force {
		err := ts.PurgeTabletActions(tabletAlias, actionnode.ActionNodeCanBePurged)
		if err != nil {
			log.Warningf("purge actions failed: %v", err)
		}
	}

	if wasAssigned {
		err = topo.DeleteTabletReplicationData(ts, tablet.Tablet)
		if err != nil {
			if err == topo.ErrNoNode {
				log.V(6).Infof("no ShardReplication object for cell %v", tablet.Alias.Cell)
				err = nil
			}
			if err != nil {
				log.Warningf("remove replication data for %v failed: %v", tablet.Alias, err)
			}
		}
	}

	// run a hook for final cleanup, only in non-force mode.
	// (force mode executes on the vtctl side, not on the vttablet side)
	if !force {
		hk := hook.NewSimpleHook("postflight_scrap")
		configureTabletHook(hk, tablet.Alias)
		if hookErr := hk.ExecuteOptional(); hookErr != nil {
			// we don't want to return an error, the server
			// is already in bad shape probably.
			log.Warningf("Scrap: postflight_scrap failed: %v", hookErr)
		}
	}

	return nil
}

// Make this external, since these transitions need to be forced from time to time.
func ChangeType(ts topo.Server, tabletAlias topo.TabletAlias, newType topo.TabletType, runHooks bool) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	if !topo.IsTrivialTypeChange(tablet.Type, newType) || !topo.IsValidTypeChange(tablet.Type, newType) {
		return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, tabletAlias)
	}

	if runHooks {
		// Only run the preflight_serving_type hook when
		// transitioning from non-serving to serving.
		if !topo.IsInServingGraph(tablet.Type) && topo.IsInServingGraph(newType) {
			if err := hook.NewSimpleHook("preflight_serving_type").ExecuteOptional(); err != nil {
				return err
			}
		}
	}

	tablet.Type = newType
	if newType == topo.TYPE_IDLE {
		if tablet.Parent.IsZero() {
			si, err := ts.GetShard(tablet.Keyspace, tablet.Shard)
			if err != nil {
				return err
			}
			rec := concurrency.AllErrorRecorder{}
			wg := sync.WaitGroup{}
			for _, cell := range si.Cells {
				wg.Add(1)
				go func(cell string) {
					defer wg.Done()
					sri, err := ts.GetShardReplication(cell, tablet.Keyspace, tablet.Shard)
					if err != nil {
						log.Warningf("Cannot check cell %v for extra replication paths, assuming it's good", cell)
						return
					}
					for _, rl := range sri.ReplicationLinks {
						if rl.Parent == tabletAlias {
							rec.RecordError(fmt.Errorf("Still have a ReplicationLink in cell %v", cell))
						}
					}
				}(cell)
			}
			wg.Wait()
			if rec.HasErrors() {
				return rec.Error()
			}
		}
		tablet.Parent = topo.TabletAlias{}
		tablet.Keyspace = ""
		tablet.Shard = ""
		tablet.KeyRange = key.KeyRange{}
	}
	return topo.UpdateTablet(ts, tablet)
}

// Make this external, since these transitions need to be forced from time to time.
func SetBlacklistedTables(ts topo.Server, tabletAlias topo.TabletAlias, tables []string) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	tablet.BlacklistedTables = tables
	return topo.UpdateTablet(ts, tablet)
}
