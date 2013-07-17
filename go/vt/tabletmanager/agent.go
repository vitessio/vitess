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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/env"
	"code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/vt/topo"
)

// Each TabletChangeCallback must be idempotent and "threadsafe".  The
// agent will execute these in a new goroutine each time a change is
// triggered.
type TabletChangeCallback func(oldTablet, newTablet topo.Tablet)

type ActionAgent struct {
	ts                topo.Server
	tabletAlias       topo.TabletAlias
	vtActionBinFile   string // path to vtaction binary
	MycnfFile         string // my.cnf file
	DbConfigsFile     string // File that contains db connection configs
	DbCredentialsFile string // File that contains db connection configs

	changeCallbacks []TabletChangeCallback

	mutex   sync.Mutex
	_tablet *topo.TabletInfo // must be accessed with lock - TabletInfo objects are not synchronized.
	done    chan struct{}    // closed when we are done.
}

func NewActionAgent(topoServer topo.Server, tabletAlias topo.TabletAlias, mycnfFile, dbConfigsFile, dbCredentialsFile string) (*ActionAgent, error) {
	return &ActionAgent{
		ts:                topoServer,
		tabletAlias:       tabletAlias,
		MycnfFile:         mycnfFile,
		DbConfigsFile:     dbConfigsFile,
		DbCredentialsFile: dbCredentialsFile,
		changeCallbacks:   make([]TabletChangeCallback, 0, 8),
		done:              make(chan struct{}),
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
	for _, f := range agent.changeCallbacks {
		relog.Info("running tablet callback: %v %v", context, f)
		go f(*oldTablet, *newTablet)
	}
	agent.mutex.Unlock()
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
	relog.Info("action dispatch %v", actionPath)
	actionNode, err := ActionNodeFromJson(data, actionPath)
	if err != nil {
		relog.Error("action decode failed: %v %v", actionPath, err)
		return nil
	}

	logfile := flag.Lookup("logfile").Value.String()
	if !strings.HasPrefix(logfile, "/dev") {
		logfile = path.Join(path.Dir(logfile), "vtaction.log")
	}
	cmd := []string{
		agent.vtActionBinFile,
		"-action", actionNode.Action,
		"-action-node", actionPath,
		"-action-guid", actionNode.ActionGuid,
		"-mycnf-file", agent.MycnfFile,
		"-logfile", logfile,
	}
	cmd = append(cmd, agent.ts.GetSubprocessFlags()...)
	if agent.DbConfigsFile != "" {
		cmd = append(cmd, "-db-configs-file", agent.DbConfigsFile)
	}
	if agent.DbCredentialsFile != "" {
		cmd = append(cmd, "-db-credentials-file", agent.DbCredentialsFile)
	}
	relog.Info("action launch %v", cmd)
	vtActionCmd := exec.Command(cmd[0], cmd[1:]...)

	stdOut, vtActionErr := vtActionCmd.CombinedOutput()
	if vtActionErr != nil {
		relog.Error("agent action failed: %v %v\n%s", actionPath, vtActionErr, stdOut)
		// If the action failed, preserve single execution path semantics.
		return vtActionErr
	}

	relog.Info("agent action completed %v %s", actionPath, stdOut)

	// Save the old tablet so callbacks can have a better idea of the precise
	// nature of the transition.
	oldTablet := agent.Tablet().Tablet

	// Actions should have side effects on the tablet, so reload the data.
	if err := agent.readTablet(); err != nil {
		relog.Warning("failed rereading tablet after action - services may be inconsistent: %v %v", actionPath, err)
	} else {
		agent.runChangeCallbacks(oldTablet, actionPath)
	}

	// Maybe invalidate the schema.
	// This adds a dependency between tabletmanager and tabletserver,
	// so it's not ideal. But I (alainjobart) think it's better
	// to have up to date schema in vtocc.
	if actionNode.Action == TABLET_ACTION_APPLY_SCHEMA {
		tabletserver.ReloadSchema()
	}

	return nil
}

func (agent *ActionAgent) verifyTopology() error {
	tablet := agent.Tablet()
	if tablet == nil {
		return fmt.Errorf("agent._tablet is nil")
	}

	if err := topo.Validate(agent.ts, agent.tabletAlias, ""); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		relog.Warning("tablet validate failed: %v %v", agent.tabletAlias, err)
	}

	return agent.ts.ValidateTabletActions(agent.tabletAlias)
}

func (agent *ActionAgent) verifyServingAddrs() error {
	if !agent.Tablet().IsServingType() {
		return nil
	}

	// Load the shard and see if we are supposed to be serving. We might be a serving type,
	// but we might be in a transitional state. Only once the shard info is updated do we
	// put ourselves in the client serving graph.
	shardInfo, err := agent.ts.GetShard(agent.Tablet().Keyspace, agent.Tablet().Shard)
	if err != nil {
		return err
	}

	if !shardInfo.Contains(agent.Tablet().Tablet) {
		return nil
	}

	// Check to see our address is registered in the right place.
	addr, err := VtnsAddrForTablet(agent.Tablet().Tablet)
	if err != nil {
		return err
	}
	return agent.ts.UpdateTabletEndpoint(agent.Tablet().Tablet.Cell, agent.Tablet().Keyspace, agent.Tablet().Shard, agent.Tablet().Type, addr)
}

func VtnsAddrForTablet(tablet *topo.Tablet) (*topo.VtnsAddr, error) {
	host, port, err := netutil.SplitHostPort(tablet.Addr)
	if err != nil {
		return nil, err
	}
	entry := topo.NewAddr(tablet.Uid, host, 0)
	entry.NamedPortMap["_vtocc"] = port
	if tablet.SecureAddr != "" {
		host, port, err = netutil.SplitHostPort(tablet.SecureAddr)
		if err != nil {
			return nil, err
		}
		entry.NamedPortMap["_vts"] = port
	}
	host, port, err = netutil.SplitHostPort(tablet.MysqlAddr)
	if err != nil {
		return nil, err
	}
	entry.NamedPortMap["_mysql"] = port
	return entry, nil
}

// bindAddr: the address for the query service advertised by this agent
func (agent *ActionAgent) Start(bindAddr, secureAddr, mysqlAddr string) error {
	var err error
	if err = agent.readTablet(); err != nil {
		return err
	}

	if err = agent.resolvePaths(); err != nil {
		return err
	}

	bindAddr, err = netutil.ResolveAddr(bindAddr)
	if err != nil {
		return err
	}
	if secureAddr != "" {
		secureAddr, err = netutil.ResolveAddr(secureAddr)
		if err != nil {
			return err
		}
	}
	mysqlAddr, err = netutil.ResolveAddr(mysqlAddr)
	if err != nil {
		return err
	}
	mysqlIpAddr, err := netutil.ResolveIpAddr(mysqlAddr)
	if err != nil {
		return err
	}

	// Update bind addr for mysql and query service in the tablet node.
	f := func(tablet *topo.Tablet) error {
		tablet.Addr = bindAddr
		tablet.SecureAddr = secureAddr
		tablet.MysqlAddr = mysqlAddr
		tablet.MysqlIpAddr = mysqlIpAddr
		return nil
	}
	if err := agent.ts.UpdateTabletFields(agent.Tablet().Alias(), f); err != nil {
		return err
	}

	// Reread in case there were changes
	if err := agent.readTablet(); err != nil {
		return err
	}

	if err := agent.ts.CreateTabletPidNode(agent.tabletAlias, agent.done); err != nil {
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
	return nil
}

func (agent *ActionAgent) Stop() {
	close(agent.done)
}

func (agent *ActionAgent) actionEventLoop() {
	f := func(actionPath, data string) error {
		return agent.dispatchAction(actionPath, data)
	}
	agent.ts.ActionEventLoop(agent.tabletAlias, f, agent.done)
}
