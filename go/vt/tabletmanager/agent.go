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

  After executing vtaction, we always call the ChangeCallbacks.
  Additionnally, for TABLET_ACTION_APPLY_SCHEMA, we will force a schema
  reload.

- listening as an RPC server. The agent performs the action itself,
  calling the actor code directly. We use this for short lived actions.

  Most RPC calls lock the actionMutex, except the easy read-donly ones.

  We will not call the ChangeCallbacks for all actions, just for the ones
  that are relevant. Same for schema reload.

  See rpc_server.go for all cases, and which action takes the actionMutex,
  runs the ChangeCallbacks, and reloads the schema.

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
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var vtactionBinaryPath = flag.String("vtaction_binary_path", "", "Full path (including filename) to vtaction binary. If not set, tries VTROOT/bin/vtaction.")

// Each TabletChangeCallback must be idempotent and "threadsafe".  The
// agent will execute these in a new goroutine each time a change is
// triggered. We won't run two in parallel.
type TabletChangeCallback func(oldTablet, newTablet topo.Tablet)

type tabletChangeItem struct {
	oldTablet  topo.Tablet
	newTablet  topo.Tablet
	context    string
	queuedTime time.Time
}

type ActionAgent struct {
	TopoServer      topo.Server
	TabletAlias     topo.TabletAlias
	vtActionBinFile string // path to vtaction binary
	Mysqld          *mysqlctl.Mysqld
	BinlogPlayerMap *BinlogPlayerMap // optional

	done chan struct{} // closed when we are done.

	// actionMutex is there to run only one action at a time. If
	// both agent.actionMutex and agent.mutex needs to be taken,
	// take actionMutex first.
	actionMutex sync.Mutex // to run only one action at a time

	// mutex is protecting the rest of the members
	mutex           sync.Mutex
	changeCallbacks []TabletChangeCallback
	changeItems     chan tabletChangeItem
	_tablet         *topo.TabletInfo
}

func NewActionAgent(topoServer topo.Server, tabletAlias topo.TabletAlias, mysqld *mysqlctl.Mysqld) (*ActionAgent, error) {
	return &ActionAgent{
		TopoServer:      topoServer,
		TabletAlias:     tabletAlias,
		Mysqld:          mysqld,
		done:            make(chan struct{}),
		changeCallbacks: make([]TabletChangeCallback, 0, 8),
		changeItems:     make(chan tabletChangeItem, 100),
	}, nil
}

func (agent *ActionAgent) AddChangeCallback(f TabletChangeCallback) {
	agent.mutex.Lock()
	agent.changeCallbacks = append(agent.changeCallbacks, f)
	agent.mutex.Unlock()
}

func (agent *ActionAgent) runChangeCallbacks(oldTablet *topo.Tablet, context string) {
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
			var wg sync.WaitGroup
			agent.mutex.Lock()
			for _, f := range agent.changeCallbacks {
				log.Infof("Running tablet callback after %v: %v %v", time.Now().Sub(changeItem.queuedTime), changeItem.context, f)
				wg.Add(1)
				go func(f TabletChangeCallback, oldTablet, newTablet topo.Tablet) {
					defer wg.Done()
					f(oldTablet, newTablet)
				}(f, changeItem.oldTablet, changeItem.newTablet)
			}
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

		agent.runChangeCallbacks(oldTablet, context)
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
	agent.runChangeCallbacks(oldTablet, "Start")

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

// RunHealthCheck takes the action mutex, runs the health check,
// and if we need to change our state, do it.
// If we are the master, we don't change our type, healthy or not.
// If we are not the master, we change to spare if not healthy,
// or to the passed in targetTabletType if healthy.
//
// Note we only update the topo record if we need to, that is if our type or
// health details changed.
func (agent *ActionAgent) RunHealthCheck(targetTabletType topo.TabletType) {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	// read the current tablet record
	agent.mutex.Lock()
	tablet := agent._tablet
	agent.mutex.Unlock()

	// run the health check
	typeForHealthCheck := targetTabletType
	if tablet.Type == topo.TYPE_MASTER {
		typeForHealthCheck = topo.TYPE_MASTER
	}

	// TODO: link in with health module
	// healthDetails, err := checkHealth(typeForHealthCheck)
	log.Infof("Would call checkHealth(%v)", typeForHealthCheck)
	healthDetails := map[string]string{} // XXX
	var err error                        // XXX

	newTabletType := targetTabletType
	if err != nil {
		switch tablet.Type {
		case topo.TYPE_SPARE:
			// we are not healthy, already spare, we're good
			log.Infof("Tablet not healthy, staying as spare: %v", err)
			return
		case topo.TYPE_MASTER:
			// we are master, not healthy, what can we do about it?
			log.Infof("Tablet not healthy, but is the master, so keeping it as master")
			return
		}

		log.Infof("Tablet not healthy, converting to spare: %v", err)
		newTabletType = topo.TYPE_SPARE
	} else {
		// we are healthy, maybe with healthDetails, see if we
		// need to update the record
		if tablet.Type == topo.TYPE_MASTER {
			newTabletType = topo.TYPE_MASTER
		}
		if tablet.Type == newTabletType && reflect.DeepEqual(healthDetails, tablet.Health) {
			// no change in health, not logging anything,
			// and we're done
			return
		}

		// we need to update our state
		log.Infof("Updating tablet record as healthy type %v with health details %v", newTabletType, healthDetails)
	}

	// TODO: add healthDetails to ChangeType
	if err := ChangeType(agent.TopoServer, tablet.Alias, newTabletType, true /*runHooks*/); err != nil {
		log.Infof("Error updating tablet record: %v", err)
		return
	}

	// TODO: rebuild the serving graph in our cell.
	// That's tough, since that is part of wrangler... So need to
	// move the logic to topo, as it's more topo related anyway.
}
