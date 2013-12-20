// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The agent listens on an action node for new actions to perform.

It passes them off to a separate action process. Even though some
actions could be completed inline very quickly, the external process
makes it easy to track and interrupt complex actions that may wedge
due to external circumstances.
*/

package tabletmanager

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

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
	ts                topo.Server
	tabletAlias       topo.TabletAlias
	vtActionBinFile   string           // path to vtaction binary
	MycnfFile         string           // my.cnf file
	DbCredentialsFile string           // File that contains db credentials
	BinlogPlayerMap   *BinlogPlayerMap // optional

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

func NewActionAgent(topoServer topo.Server, tabletAlias topo.TabletAlias, mycnfFile, dbCredentialsFile string) (*ActionAgent, error) {
	return &ActionAgent{
		ts:                topoServer,
		tabletAlias:       tabletAlias,
		MycnfFile:         mycnfFile,
		DbCredentialsFile: dbCredentialsFile,
		done:              make(chan struct{}),
		changeCallbacks:   make([]TabletChangeCallback, 0, 8),
		changeItems:       make(chan tabletChangeItem, 100),
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
	tablet, err := agent.ts.GetTablet(agent.tabletAlias)
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
	vtroot, err := env.VtRoot()
	if err != nil {
		return err
	}
	path := path.Join(vtroot, "bin/vtaction")
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("vtaction binary %s not found: %v", path, err)
	}
	agent.vtActionBinFile = path
	return nil
}

// A non-nil return signals that event processing should stop.
func (agent *ActionAgent) dispatchAction(actionPath, data string) error {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	log.Infof("action dispatch %v", actionPath)
	actionNode, err := ActionNodeFromJson(data, actionPath)
	if err != nil {
		log.Errorf("action decode failed: %v %v", actionPath, err)
		return nil
	}

	cmd := []string{
		agent.vtActionBinFile,
		"-action", actionNode.Action,
		"-action-node", actionPath,
		"-action-guid", actionNode.ActionGuid,
		"-mycnf-file", agent.MycnfFile,
	}
	cmd = append(cmd, logutil.GetSubprocessFlags()...)
	cmd = append(cmd, topo.GetSubprocessFlags()...)
	cmd = append(cmd, dbconfigs.GetSubprocessFlags()...)
	if agent.DbCredentialsFile != "" {
		cmd = append(cmd, "-db-credentials-file", agent.DbCredentialsFile)
	}
	log.Infof("action launch %v", cmd)
	vtActionCmd := exec.Command(cmd[0], cmd[1:]...)

	stdOut, vtActionErr := vtActionCmd.CombinedOutput()
	if vtActionErr != nil {
		log.Errorf("agent action failed: %v %v\n%s", actionPath, vtActionErr, stdOut)
		// If the action failed, preserve single execution path semantics.
		return vtActionErr
	}

	log.Infof("Agent action completed %v %s", actionPath, stdOut)
	agent.afterAction(actionPath, actionNode.Action == TABLET_ACTION_APPLY_SCHEMA)
	return nil
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

	if err := topo.Validate(agent.ts, agent.tabletAlias); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		log.Warningf("tablet validate failed: %v %v", agent.tabletAlias, err)
	}

	return agent.ts.ValidateTabletActions(agent.tabletAlias)
}

func (agent *ActionAgent) verifyServingAddrs() error {
	if !agent.Tablet().IsRunningQueryService() {
		return nil
	}

	// Check to see our address is registered in the right place.
	addr, err := EndPointForTablet(agent.Tablet().Tablet)
	if err != nil {
		return err
	}
	return agent.ts.UpdateTabletEndpoint(agent.Tablet().Tablet.Alias.Cell, agent.Tablet().Keyspace, agent.Tablet().Shard, agent.Tablet().Type, addr)
}

func EndPointForTablet(tablet *topo.Tablet) (*topo.EndPoint, error) {
	entry := topo.NewAddr(tablet.Alias.Uid, tablet.Hostname)
	if err := tablet.ValidatePortmap(); err != nil {
		return nil, err
	}

	// TODO(szopa): Rename _vtocc to vt.
	entry.NamedPortMap = map[string]int{
		"_vtocc": tablet.Portmap["vt"],
		"_mysql": tablet.Portmap["mysql"],
	}
	if port, ok := tablet.Portmap["vts"]; ok {
		entry.NamedPortMap["_vts"] = port
	}
	return entry, nil
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
	if err := agent.ts.UpdateTabletFields(agent.Tablet().Alias, f); err != nil {
		return err
	}

	// Reread to get the changes we just made
	if err := agent.readTablet(); err != nil {
		return err
	}

	data := fmt.Sprintf("host:%v\npid:%v\n", hostname, os.Getpid())

	if err := agent.ts.CreateTabletPidNode(agent.tabletAlias, data, agent.done); err != nil {
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
	agent.ts.ActionEventLoop(agent.tabletAlias, f, agent.done)
}
