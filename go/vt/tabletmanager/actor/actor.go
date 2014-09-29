// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package actor contains the code for all the actions executed
remotely on a tablet. These actions can be executed as:
- RPCs: called directly from vttablet
- ActionNodes: executed from within vtaction
*/
package actor

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/mysqlctl"
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

// TabletActor is the main object for this package.
type TabletActor struct {
	mysqld      *mysqlctl.Mysqld
	mysqlDaemon mysqlctl.MysqlDaemon
	ts          topo.Server
	tabletAlias topo.TabletAlias
}

// NewTabletActor creates a new TabletActor object.
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
					return actionErr
				}
				if err := ta.ts.UnblockTabletAction(actionPath); err != nil {
					log.Errorf("Dead process detector failed unblocking: %v", err)
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
	case actionnode.TABLET_ACTION_PING:
		// Just an end-to-end verification that we got the message.
		err = nil

	case actionnode.TABLET_ACTION_EXECUTE_HOOK,
		actionnode.TABLET_ACTION_SET_RDONLY,
		actionnode.TABLET_ACTION_SET_RDWR,
		actionnode.TABLET_ACTION_CHANGE_TYPE,
		actionnode.TABLET_ACTION_SCRAP,
		actionnode.TABLET_ACTION_SLEEP,
		actionnode.TABLET_ACTION_GET_SCHEMA,
		actionnode.TABLET_ACTION_RELOAD_SCHEMA,
		actionnode.TABLET_ACTION_PREFLIGHT_SCHEMA,
		actionnode.TABLET_ACTION_APPLY_SCHEMA,
		actionnode.TABLET_ACTION_GET_PERMISSIONS,
		actionnode.TABLET_ACTION_SLAVE_STATUS,
		actionnode.TABLET_ACTION_WAIT_SLAVE_POSITION,
		actionnode.TABLET_ACTION_MASTER_POSITION,
		actionnode.TABLET_ACTION_REPARENT_POSITION,
		actionnode.TABLET_ACTION_DEMOTE_MASTER,
		actionnode.TABLET_ACTION_PROMOTE_SLAVE,
		actionnode.TABLET_ACTION_SLAVE_WAS_PROMOTED,
		actionnode.TABLET_ACTION_RESTART_SLAVE,
		actionnode.TABLET_ACTION_SLAVE_WAS_RESTARTED,
		actionnode.TABLET_ACTION_BREAK_SLAVES,
		actionnode.TABLET_ACTION_STOP_SLAVE,
		actionnode.TABLET_ACTION_STOP_SLAVE_MINIMUM,
		actionnode.TABLET_ACTION_START_SLAVE,
		actionnode.TABLET_ACTION_EXTERNALLY_REPARENTED,
		actionnode.TABLET_ACTION_GET_SLAVES,
		actionnode.TABLET_ACTION_WAIT_BLP_POSITION,
		actionnode.TABLET_ACTION_STOP_BLP,
		actionnode.TABLET_ACTION_START_BLP,
		actionnode.TABLET_ACTION_RUN_BLP_UNTIL,
		actionnode.TABLET_ACTION_SNAPSHOT,
		actionnode.TABLET_ACTION_SNAPSHOT_SOURCE_END,
		actionnode.TABLET_ACTION_RESERVE_FOR_RESTORE,
		actionnode.TABLET_ACTION_RESTORE,
		actionnode.TABLET_ACTION_MULTI_SNAPSHOT,
		actionnode.TABLET_ACTION_MULTI_RESTORE:
		err = TabletActorError("Operation " + actionNode.Action + "  only supported as RPC")
	default:
		err = TabletActorError("invalid action: " + actionNode.Action)
	}

	return
}

// StoreActionResponse writes the result of an action into topology server
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

// ChecktabletMysqlPort will check the mysql port for the tablet is good,
// and if not will try to update it
func CheckTabletMysqlPort(ts topo.Server, mysqlDaemon mysqlctl.MysqlDaemon, tablet *topo.TabletInfo) *topo.TabletInfo {
	mport, err := mysqlDaemon.GetMysqlPort()
	if err != nil {
		log.Warningf("Cannot get current mysql port, not checking it: %v", err)
		return nil
	}

	if mport == tablet.Portmap["mysql"] {
		return nil
	}

	log.Warningf("MySQL port has changed from %v to %v, updating it in tablet record", tablet.Portmap["mysql"], mport)
	tablet.Portmap["mysql"] = mport
	if err := topo.UpdateTablet(ts, tablet); err != nil {
		log.Warningf("Failed to update tablet record, may use old mysql port")
		return nil
	}

	return tablet
}
