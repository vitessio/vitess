// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The agent listens on a zk node for new actions to perform.

It passes them off to a separate action process. Even though some
actions could be completed inline very quickly, the external process
makes it easy to track and interrupt complex actions that may wedge
due to external circumstances.
*/

package tabletmanager

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/env"
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/vt/zktopo" // FIXME(alainjobart) to be removed
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// Each TabletChangeCallback must be idempotent and "threadsafe".  The
// agent will execute these in a new goroutine each time a change is
// triggered.
type TabletChangeCallback func(oldTablet, newTablet Tablet)

type ActionAgent struct {
	ts                naming.TopologyServer
	zconn             zk.Conn // FIXME(alainjobart) will be removed eventually
	zkTabletPath      string  // FIXME(alainjobart) will be removed eventually
	tabletAlias       naming.TabletAlias
	zkActionPath      string
	vtActionBinFile   string // path to vtaction binary
	MycnfFile         string // my.cnf file
	DbConfigsFile     string // File that contains db connection configs
	DbCredentialsFile string // File that contains db connection configs

	changeCallbacks []TabletChangeCallback

	mutex   sync.Mutex
	_tablet *TabletInfo   // must be accessed with lock - TabletInfo objects are not synchronized.
	done    chan struct{} // closed when we are done.
}

func NewActionAgent(topoServer naming.TopologyServer, tabletAlias naming.TabletAlias, mycnfFile, dbConfigsFile, dbCredentialsFile string) (*ActionAgent, error) {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	actionPath, err := TabletActionPath(zkTabletPath)
	if err != nil {
		return nil, err
	}
	// FIXME(alainjobart) violates encapsulation until conversion is done
	zconn := topoServer.(*zktopo.ZkTopologyServer).GetZConn()
	return &ActionAgent{
		ts:                topoServer,
		zconn:             zconn,
		zkTabletPath:      zkTabletPath,
		tabletAlias:       tabletAlias,
		zkActionPath:      actionPath,
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

func (agent *ActionAgent) runChangeCallbacks(oldTablet *Tablet, context string) {
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
	tablet, err := ReadTablet(agent.ts, agent.tabletAlias)
	if err != nil {
		return err
	}
	agent.mutex.Lock()
	agent._tablet = tablet
	agent.mutex.Unlock()
	return nil
}

func (agent *ActionAgent) Tablet() *TabletInfo {
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
func (agent *ActionAgent) dispatchAction(actionPath string) error {
	relog.Info("action dispatch %v", actionPath)
	data, _, err := agent.zconn.Get(actionPath)
	if err != nil {
		relog.Error("action dispatch failed: %v", err)
		return nil
	}

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
	cmd = append(cmd, zk.GetZkSubprocessFlags()...)
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

func (agent *ActionAgent) handleActionQueue() (<-chan zookeeper.Event, error) {
	// This read may seem a bit pedantic, but it makes it easier for the system
	// to trend towards consistency if an action fails or somehow the action
	// queue gets mangled by an errant process.
	children, _, watch, err := agent.zconn.ChildrenW(agent.zkActionPath)
	if err != nil {
		return watch, err
	}
	if len(children) > 0 {
		sort.Strings(children)
		for _, child := range children {
			actionPath := agent.zkActionPath + "/" + child
			if _, err := strconv.ParseUint(child, 10, 64); err != nil {
				// This is handy if you want to restart a stuck queue.
				// FIXME(msolomon) could listen on the queue node for a change
				// generated by a "touch", but listening on two things is a bit
				// more complex.
				relog.Warning("remove invalid event from action queue: %v", child)
				agent.zconn.Delete(actionPath, -1)
			}
			if err := agent.dispatchAction(actionPath); err != nil {
				break
			}
		}
	}
	return watch, nil
}

func (agent *ActionAgent) verifyZkPaths() error {
	tablet := agent.Tablet()
	if tablet == nil {
		return fmt.Errorf("agent._tablet is nil")
	}

	if err := Validate(agent.ts, agent.tabletAlias, ""); err != nil {
		// Don't stop, it's not serious enough, this is likely transient.
		relog.Warning("tablet validate failed: %v %v", agent.tabletAlias, err)
	}

	// Ensure that the action node is there. There is no conflict creating
	// this node.
	_, err := agent.zconn.Create(agent.zkActionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}
	return nil
}

func (agent *ActionAgent) verifyServingAddrs() error {
	if !agent.Tablet().IsServingType() {
		return nil
	}

	// Load the shard and see if we are supposed to be serving. We might be a serving type,
	// but we might be in a transitional state. Only once the shard info is updated do we
	// put ourselves in the client serving graph.
	shardInfo, err := ReadShard(agent.ts, agent.Tablet().Keyspace, agent.Tablet().Shard)
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

func splitHostPort(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	p, err := strconv.ParseInt(port, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(p), nil
}

func fqdn() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	cname, err := net.LookupCNAME(hostname)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(cname, "."), nil
}

// Resolve an address where the host has been left blank, like ":3306"
func resolveAddr(addr string) (string, error) {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host, err = fqdn()
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%v:%v", host, port), nil
}

func resolveIpAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	ipAddrs, err := net.LookupHost(host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(ipAddrs[0], port), nil
}

func VtnsAddrForTablet(tablet *Tablet) (*naming.VtnsAddr, error) {
	host, port, err := splitHostPort(tablet.Addr)
	if err != nil {
		return nil, err
	}
	entry := naming.NewAddr(tablet.Uid, host, 0)
	entry.NamedPortMap["_vtocc"] = port
	if tablet.SecureAddr != "" {
		host, port, err = splitHostPort(tablet.SecureAddr)
		if err != nil {
			return nil, err
		}
		entry.NamedPortMap["_vts"] = port
	}
	host, port, err = splitHostPort(tablet.MysqlAddr)
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

	bindAddr, err = resolveAddr(bindAddr)
	if err != nil {
		return err
	}
	if secureAddr != "" {
		secureAddr, err = resolveAddr(secureAddr)
		if err != nil {
			return err
		}
	}
	mysqlAddr, err = resolveAddr(mysqlAddr)
	if err != nil {
		return err
	}
	mysqlIpAddr, err := resolveIpAddr(mysqlAddr)
	if err != nil {
		return err
	}

	// Update bind addr for mysql and query service in the tablet node.
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		if oldValue == "" {
			return "", fmt.Errorf("no data for tablet addr update: %v", agent.tabletAlias)
		}

		tablet, err := tabletFromJson(oldValue)
		if err != nil {
			return "", err
		}
		tablet.Addr = bindAddr
		tablet.SecureAddr = secureAddr
		tablet.MysqlAddr = mysqlAddr
		tablet.MysqlIpAddr = mysqlIpAddr
		return jscfg.ToJson(tablet), nil
	}
	err = agent.zconn.RetryChange(agent.Tablet().Path(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		return err
	}

	// Reread in case there were changes
	if err := agent.readTablet(); err != nil {
		return err
	}

	if err := zk.CreatePidNode(agent.zconn, agent.Tablet().PidPath(), agent.done); err != nil {
		return err
	}

	if err = agent.verifyZkPaths(); err != nil {
		return err
	}

	if err = agent.verifyServingAddrs(); err != nil {
		return err
	}

	oldTablet := &Tablet{}
	agent.runChangeCallbacks(oldTablet, "Start")

	go agent.actionEventLoop()
	return nil
}

func (agent *ActionAgent) Stop() {
	close(agent.done)
}

func (agent *ActionAgent) actionEventLoop() {
	for {
		// Process any pending actions when we startup, before we start listening
		// for events.
		watch, err := agent.handleActionQueue()
		if err != nil {
			relog.Warning("action queue failed: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// FIXME(msolomon) Add a skewing timer here to guarantee we wakeup
		// periodically even if events are missed?
		select {
		case event := <-watch:
			if !event.Ok() {
				// NOTE(msolomon) The zk meta conn will reconnect automatically, or
				// error out. At this point, there isn't much to do.
				relog.Warning("zookeeper not OK: %v", event)
				time.Sleep(5 * time.Second)
			}
			// Otherwise, just handle the queue above.
		case <-agent.done:
			return
		}
	}
}
