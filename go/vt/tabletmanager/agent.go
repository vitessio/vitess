// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The agent handles local execution of actions triggered remotely.
It has two execution models:

- listening on an action path for ActionNode objects.  When receiving
  an action, it will forward it to vtaction to perform it (vtaction
  uses the actor code). We usually use this model for long-running
  queries where an RPC would time out.

  All vtaction calls lock the actionMutex.

  After executing vtaction, we always call agent.changeCallback.
  Additionnally, for TABLET_ACTION_APPLY_SCHEMA, we will force a schema
  reload.

- listening as an RPC server. The agent performs the action itself,
  calling the actor code directly. We use this for short lived actions.

  Most RPC calls lock the actionMutex, except the easy read-donly ones.

  We will not call changeCallback for all actions, just for the ones
  that are relevant. Same for schema reload.

  See rpc_server.go for all cases, and which action takes the actionMutex,
  runs changeCallback, and reloads the schema.

*/

package tabletmanager

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"reflect"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	vtactionBinaryPath = flag.String("vtaction_binary_path", "", "Full path (including filename) to vtaction binary. If not set, tries VTROOT/bin/vtaction.")

	historyLength = 16
)

type tabletChangeItem struct {
	oldTablet  topo.Tablet
	newTablet  topo.Tablet
	context    string
	queuedTime time.Time
}

// HealthRecord records one run of the health checker
type HealthRecord struct {
	Error  error
	Result map[string]string
	Time   time.Time
}

// This returns a readable one word version of the health
func (r *HealthRecord) Class() string {
	switch {
	case r.Error != nil:
		return "unhealthy"
	case len(r.Result) > 0:
		return "unhappy"
	default:
		return "healthy"
	}
}

// IsDuplicate implements history.Deduplicable
func (r *HealthRecord) IsDuplicate(other interface{}) bool {
	rother, ok := other.(HealthRecord)
	if !ok {
		return false
	}
	return reflect.DeepEqual(r.Error, rother.Error) && reflect.DeepEqual(r.Result, rother.Result)
}

// ActionAgent is the main class for the agent.
type ActionAgent struct {
	// The following fields are set during creation
	TopoServer      topo.Server
	TabletAlias     topo.TabletAlias
	Mysqld          *mysqlctl.Mysqld
	DBConfigs       *dbconfigs.DBConfigs
	SchemaOverrides []tabletserver.SchemaOverride

	// BinlogPlayerMap is optional
	BinlogPlayerMap *BinlogPlayerMap

	// Internal variables
	vtActionBinFile string        // path to vtaction binary
	done            chan struct{} // closed when we are done.

	// This is the History of the health checks, public so status
	// pages can display it
	History *history.History

	// actionMutex is there to run only one action at a time. If
	// both agent.actionMutex and agent.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex // to run only one action at a time

	// mutex is protecting the rest of the members
	mutex       sync.Mutex
	changeItems chan tabletChangeItem
	_tablet     *topo.TabletInfo
}

func (agent *ActionAgent) runChangeCallback(oldTablet *topo.Tablet, context string) {
	agent.mutex.Lock()
	// Access directly since we have the lock.
	newTablet := agent._tablet.Tablet
	agent.changeItems <- tabletChangeItem{oldTablet: *oldTablet, newTablet: *newTablet, context: context, queuedTime: time.Now()}
	log.Infof("Queued tablet callback: %v", context)
	agent.mutex.Unlock()
}

func (agent *ActionAgent) executeCallbacksLoop() {
	for {
		select {
		case changeItem := <-agent.changeItems:
			wg := sync.WaitGroup{}
			agent.mutex.Lock()
			log.Infof("Running tablet callback after %v: %v", time.Now().Sub(changeItem.queuedTime), changeItem.context)
			wg.Add(1)
			go func() {
				defer wg.Done()
				agent.changeCallback(changeItem.oldTablet, changeItem.newTablet)
			}()
			agent.mutex.Unlock()
			wg.Wait()
		case <-agent.done:
			return
		}
	}
}

func (agent *ActionAgent) readTablet() error {
	tablet, err := agent.TopoServer.GetTablet(agent.TabletAlias)
	if err != nil {
		return err
	}
	agent.mutex.Lock()
	agent._tablet = tablet
	agent.mutex.Unlock()
	return nil
}

func (agent *ActionAgent) Tablet() *topo.TabletInfo {
	agent.mutex.Lock()
	tablet := agent._tablet
	agent.mutex.Unlock()
	return tablet
}

func (agent *ActionAgent) resolvePaths() error {
	var p string
	if *vtactionBinaryPath != "" {
		p = *vtactionBinaryPath
	} else {
		vtroot, err := env.VtRoot()
		if err != nil {
			return err
		}
		p = path.Join(vtroot, "bin/vtaction")
	}
	if _, err := os.Stat(p); err != nil {
		return fmt.Errorf("vtaction binary %s not found: %v", p, err)
	}
	agent.vtActionBinFile = p
	return nil
}

// A non-nil return signals that event processing should stop.
func (agent *ActionAgent) dispatchAction(actionPath, data string) error {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	log.Infof("action dispatch %v", actionPath)
	actionNode, err := actionnode.ActionNodeFromJson(data, actionPath)
	if err != nil {
		log.Errorf("action decode failed: %v %v", actionPath, err)
		return nil
	}

	cmd := []string{
		agent.vtActionBinFile,
		"-action", actionNode.Action,
		"-action-node", actionPath,
		"-action-guid", actionNode.ActionGuid,
		"-mycnf-file", agent.Mysqld.MycnfPath(),
	}
	cmd = append(cmd, logutil.GetSubprocessFlags()...)
	cmd = append(cmd, topo.GetSubprocessFlags()...)
	cmd = append(cmd, dbconfigs.GetSubprocessFlags()...)
	log.Infof("action launch %v", cmd)
	vtActionCmd := exec.Command(cmd[0], cmd[1:]...)

	stdOut, vtActionErr := vtActionCmd.CombinedOutput()
	if vtActionErr != nil {
		log.Errorf("agent action failed: %v %v\n%s", actionPath, vtActionErr, stdOut)
		// If the action failed, preserve single execution path semantics.
		return vtActionErr
	}

	log.Infof("Agent action completed %v %s", actionPath, stdOut)
	agent.afterAction(actionPath, actionNode.Action == actionnode.TABLET_ACTION_APPLY_SCHEMA)
	return nil
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

// afterAction needs to be run after an action may have changed the current
// state of the tablet.
func (agent *ActionAgent) afterAction(context string, reloadSchema bool) {
	log.Infof("Executing post-action change callbacks")

	// Save the old tablet so callbacks can have a better idea of
	// the precise nature of the transition.
	oldTablet := agent.Tablet().Tablet

	// Actions should have side effects on the tablet, so reload the data.
	if err := agent.readTablet(); err != nil {
		log.Warningf("Failed rereading tablet after %v - services may be inconsistent: %v", context, err)
	} else {
		if updatedTablet := CheckTabletMysqlPort(agent.TopoServer, agent.Mysqld, agent.Tablet()); updatedTablet != nil {
			agent.mutex.Lock()
			agent._tablet = updatedTablet
			agent.mutex.Unlock()
		}

		agent.runChangeCallback(oldTablet, context)
	}

	// Maybe invalidate the schema.
	// This adds a dependency between tabletmanager and tabletserver,
	// so it's not ideal. But I (alainjobart) think it's better
	// to have up to date schema in vtocc.
	if reloadSchema {
		tabletserver.ReloadSchema()
	}
	log.Infof("Done with post-action change callbacks")
}

func (agent *ActionAgent) verifyTopology() error {
	tablet := agent.Tablet()
	if tablet == nil {
		return fmt.Errorf("agent._tablet is nil")
	}

	if err := topo.Validate(agent.TopoServer, agent.TabletAlias); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		log.Warningf("tablet validate failed: %v %v", agent.TabletAlias, err)
	}

	return agent.TopoServer.ValidateTabletActions(agent.TabletAlias)
}

func (agent *ActionAgent) verifyServingAddrs() error {
	if !agent.Tablet().IsRunningQueryService() {
		return nil
	}

	// Check to see our address is registered in the right place.
	addr, err := agent.Tablet().Tablet.EndPoint()
	if err != nil {
		return err
	}
	return agent.TopoServer.UpdateTabletEndpoint(agent.Tablet().Tablet.Alias.Cell, agent.Tablet().Keyspace, agent.Tablet().Shard, agent.Tablet().Type, addr)
}

// bindAddr: the address for the query service advertised by this agent
func (agent *ActionAgent) Start(mysqlPort, vtPort, vtsPort int) error {
	var err error
	if err = agent.readTablet(); err != nil {
		return err
	}

	if err = agent.resolvePaths(); err != nil {
		return err
	}

	// find our hostname as fully qualified, and IP
	hostname, err := netutil.FullyQualifiedHostname()
	if err != nil {
		return err
	}
	ipAddrs, err := net.LookupHost(hostname)
	if err != nil {
		return err
	}
	ipAddr := ipAddrs[0]

	// Update bind addr for mysql and query service in the tablet node.
	f := func(tablet *topo.Tablet) error {
		// the first four values are for backward compatibility
		tablet.Addr = fmt.Sprintf("%v:%v", hostname, vtPort)
		if vtsPort != 0 {
			tablet.SecureAddr = fmt.Sprintf("%v:%v", hostname, vtsPort)
		}
		tablet.MysqlAddr = fmt.Sprintf("%v:%v", hostname, mysqlPort)
		tablet.MysqlIpAddr = fmt.Sprintf("%v:%v", ipAddr, mysqlPort)

		// new values
		tablet.Hostname = hostname
		tablet.IPAddr = ipAddr
		if tablet.Portmap == nil {
			tablet.Portmap = make(map[string]int)
		}
		tablet.Portmap["mysql"] = mysqlPort
		tablet.Portmap["vt"] = vtPort
		if vtsPort != 0 {
			tablet.Portmap["vts"] = vtsPort
		} else {
			delete(tablet.Portmap, "vts")
		}
		return nil
	}
	if err := agent.TopoServer.UpdateTabletFields(agent.Tablet().Alias, f); err != nil {
		return err
	}

	// Reread to get the changes we just made
	if err := agent.readTablet(); err != nil {
		return err
	}

	data := fmt.Sprintf("host:%v\npid:%v\n", hostname, os.Getpid())

	if err := agent.TopoServer.CreateTabletPidNode(agent.TabletAlias, data, agent.done); err != nil {
		return err
	}

	if err = agent.verifyTopology(); err != nil {
		return err
	}

	if err = agent.verifyServingAddrs(); err != nil {
		return err
	}

	oldTablet := &topo.Tablet{}
	agent.runChangeCallback(oldTablet, "Start")

	go agent.actionEventLoop()
	go agent.executeCallbacksLoop()
	return nil
}

func (agent *ActionAgent) Stop() {
	close(agent.done)
	if agent.BinlogPlayerMap != nil {
		agent.BinlogPlayerMap.StopAllPlayersAndReset()
	}
}

func (agent *ActionAgent) actionEventLoop() {
	f := func(actionPath, data string) error {
		return agent.dispatchAction(actionPath, data)
	}
	agent.TopoServer.ActionEventLoop(agent.TabletAlias, f, agent.done)
}
