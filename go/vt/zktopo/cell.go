// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"math/rand"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the per-cell methods of ZkTopologyServer
*/

func tabletPathForAlias(alias naming.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, alias.TabletUidStr())
}

func TabletActionPathForAlias(alias naming.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v/action", alias.Cell, alias.TabletUidStr())
}

func tabletDirectoryForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/tablets", cell)
}

//
// Tablet management
//

func (zkts *ZkTopologyServer) CreateTablet(alias naming.TabletAlias, contents string) error {
	zkTabletPath := tabletPathForAlias(alias)

	// Create /zk/<cell>/vt/tablets/<uid>
	_, err := zk.CreateRecursive(zkts.zconn, zkTabletPath, contents, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = naming.ErrNodeExists
		}
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>/action
	tap := path.Join(zkTabletPath, "action")
	_, err = zkts.zconn.Create(tap, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>/actionlog
	talp := path.Join(zkTabletPath, "actionlog")
	_, err = zkts.zconn.Create(talp, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	return nil
}

func (zkts *ZkTopologyServer) UpdateTablet(alias naming.TabletAlias, contents string, existingVersion int) (int, error) {
	zkTabletPath := tabletPathForAlias(alias)
	stat, err := zkts.zconn.Set(zkTabletPath, contents, existingVersion)
	if err != nil {
		return 0, err
	}
	return stat.Version(), nil
}

func (zkts *ZkTopologyServer) DeleteTablet(alias naming.TabletAlias) error {
	zkTabletPath := tabletPathForAlias(alias)
	return zk.DeleteRecursive(zkts.zconn, zkTabletPath, -1)
}

func (zkts *ZkTopologyServer) ValidateTablet(alias naming.TabletAlias) error {
	zkTabletPath := tabletPathForAlias(alias)
	zkPaths := []string{
		path.Join(zkTabletPath, "action"),
		path.Join(zkTabletPath, "actionlog"),
	}

	for _, zkPath := range zkPaths {
		_, _, err := zkts.zconn.Get(zkPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zkts *ZkTopologyServer) GetTablet(alias naming.TabletAlias) (string, int, error) {
	zkTabletPath := tabletPathForAlias(alias)
	data, stat, err := zkts.zconn.Get(zkTabletPath)
	if err != nil {
		return "", 0, err
	}
	return data, stat.Version(), nil
}

func (zkts *ZkTopologyServer) GetTabletsByCell(cell string) ([]naming.TabletAlias, error) {
	zkTabletsPath := tabletDirectoryForCell(cell)
	children, _, err := zkts.zconn.Children(zkTabletsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	result := make([]naming.TabletAlias, len(children))
	for i, child := range children {
		result[i].Cell = cell
		result[i].Uid, err = naming.ParseUid(child)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

//
// Serving Graph management
//
func zkPathForVtKeyspace(cell, keyspace string) string {
	return fmt.Sprintf("/zk/%v/vt/ns/%v", cell, keyspace)
}

func zkPathForVtShard(cell, keyspace, shard string) string {
	return path.Join(zkPathForVtKeyspace(cell, keyspace), shard)
}

func zkPathForVtName(cell, keyspace, shard string, tabletType naming.TabletType) string {
	return path.Join(zkPathForVtShard(cell, keyspace, shard), string(tabletType))
}

func (zkts *ZkTopologyServer) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]naming.TabletType, error) {
	zkSgShardPath := zkPathForVtShard(cell, keyspace, shard)
	children, _, err := zkts.zconn.Children(zkSgShardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = naming.ErrNoNode
		}
		return nil, err
	}
	result := make([]naming.TabletType, len(children))
	for i, tt := range children {
		result[i] = naming.TabletType(tt)
	}
	return result, nil
}

func (zkts *ZkTopologyServer) UpdateSrvTabletType(cell, keyspace, shard string, tabletType naming.TabletType, addrs *naming.VtnsAddrs) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data := jscfg.ToJson(addrs)
	_, err := zk.CreateRecursive(zkts.zconn, path, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			// Node already exists - just stomp away. Multiple writers shouldn't be here.
			// We use RetryChange here because it won't update the node unnecessarily.
			f := func(oldValue string, oldStat zk.Stat) (string, error) {
				return data, nil
			}
			err = zkts.zconn.RetryChange(path, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
		}
	}
	return err
}

func (zkts *ZkTopologyServer) GetSrvTabletType(cell, keyspace, shard string, tabletType naming.TabletType) (*naming.VtnsAddrs, error) {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = naming.ErrNoNode
		}
		return nil, err
	}
	return naming.NewVtnsAddrs(data, stat.Version())
}

func (zkts *ZkTopologyServer) DeleteSrvTabletType(cell, keyspace, shard string, tabletType naming.TabletType) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	return zkts.zconn.Delete(path, -1)
}

func (zkts *ZkTopologyServer) UpdateSrvShard(cell, keyspace, shard string, srvShard *naming.SrvShard) error {
	path := zkPathForVtShard(cell, keyspace, shard)
	data := jscfg.ToJson(srvShard)
	_, err := zkts.zconn.Set(path, data, -1)
	return err
}

func (zkts *ZkTopologyServer) GetSrvShard(cell, keyspace, shard string) (*naming.SrvShard, error) {
	path := zkPathForVtShard(cell, keyspace, shard)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = naming.ErrNoNode
		}
		return nil, err
	}
	return naming.NewSrvShard(data, stat.Version())
}

func (zkts *ZkTopologyServer) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *naming.SrvKeyspace) error {
	path := zkPathForVtKeyspace(cell, keyspace)
	data := jscfg.ToJson(srvKeyspace)
	_, err := zkts.zconn.Set(path, data, -1)
	return err
}

func (zkts *ZkTopologyServer) GetSrvKeyspace(cell, keyspace string) (*naming.SrvKeyspace, error) {
	path := zkPathForVtKeyspace(cell, keyspace)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = naming.ErrNoNode
		}
		return nil, err
	}
	return naming.NewSrvKeyspace(data, stat.Version())
}

var skipUpdateErr = fmt.Errorf("skip update")

func (zkts *ZkTopologyServer) updateTabletEndpoint(oldValue string, oldStat zk.Stat, addr *naming.VtnsAddr) (newValue string, err error) {
	if oldStat == nil {
		// The incoming object doesn't exist - we haven't been placed in the serving
		// graph yet, so don't update. Assume the next process that rebuilds the graph
		// will get the updated tablet location.
		return "", skipUpdateErr
	}

	var addrs *naming.VtnsAddrs
	if oldValue != "" {
		addrs, err = naming.NewVtnsAddrs(oldValue, oldStat.Version())
		if err != nil {
			return
		}

		foundTablet := false
		for i, entry := range addrs.Entries {
			if entry.Uid == addr.Uid {
				foundTablet = true
				if !naming.VtnsAddrEquality(&entry, addr) {
					addrs.Entries[i] = *addr
				}
				break
			}
		}

		if !foundTablet {
			addrs.Entries = append(addrs.Entries, *addr)
		}
	} else {
		addrs = naming.NewAddrs()
		addrs.Entries = append(addrs.Entries, *addr)
	}
	return jscfg.ToJson(addrs), nil
}

func (zkts *ZkTopologyServer) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType naming.TabletType, addr *naming.VtnsAddr) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		return zkts.updateTabletEndpoint(oldValue, oldStat, addr)
	}
	err := zkts.zconn.RetryChange(path, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err == skipUpdateErr {
		err = nil
	}
	return err
}

//
// Remote Tablet Actions
//

func (zkts *ZkTopologyServer) WriteTabletAction(tabletAlias naming.TabletAlias, contents string) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionPath := TabletActionPathForAlias(tabletAlias) + "/"
	return zkts.zconn.Create(actionPath, contents, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (zkts *ZkTopologyServer) WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error) {
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// see if the file exists or sets a watch
	// the loop is to resist zk disconnects while we're waiting
	actionLogPath := strings.Replace(actionPath, "/action/", "/actionlog/", 1)
wait:
	for {
		var retryDelay <-chan time.Time
		stat, watch, err := zkts.zconn.ExistsW(actionLogPath)
		if err != nil {
			delay := 5*time.Second + time.Duration(rand.Int63n(55e9))
			relog.Warning("unexpected zk error, delay retry %v: %v", delay, err)
			// No one likes a thundering herd.
			retryDelay = time.After(delay)
		} else if stat != nil {
			// file exists, go on
			break wait
		}

		// if the file doesn't exist yet, wait for creation event.
		// On any other event we'll retry the ExistsW
		select {
		case actionEvent := <-watch:
			if actionEvent.Type == zookeeper.EVENT_CREATED {
				break wait
			} else {
				// Log unexpected events. Reconnects are
				// handled by zk.Conn, so calling ExistsW again
				// will handle a disconnect.
				relog.Warning("unexpected zk event: %v", actionEvent)
			}
		case <-retryDelay:
			continue wait
		case <-timer.C:
			return "", naming.ErrTimeout
		case <-interrupted:
			return "", naming.ErrInterrupted
		}
	}

	// the node exists, read it
	data, _, err := zkts.zconn.Get(actionLogPath)
	if err != nil {
		return "", fmt.Errorf("action err: %v %v", actionLogPath, err)
	}

	return data, nil
}

func (zkts *ZkTopologyServer) PurgeTabletActions(tabletAlias naming.TabletAlias, canBePurged func(data string) bool) error {
	actionPath := TabletActionPathForAlias(tabletAlias)
	return zkts.PurgeActions(actionPath, canBePurged)
}

//
// Supporting the local agent process.
//

func (zkts *ZkTopologyServer) ValidateTabletActions(tabletAlias naming.TabletAlias) error {
	actionPath := TabletActionPathForAlias(tabletAlias)

	// Ensure that the action node is there. There is no conflict creating
	// this node.
	_, err := zkts.zconn.Create(actionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}
	return nil
}

func (zkts *ZkTopologyServer) CreateTabletPidNode(tabletAlias naming.TabletAlias, done chan struct{}) error {
	zkTabletPath := tabletPathForAlias(tabletAlias)
	path := path.Join(zkTabletPath, "pid")
	return zk.CreatePidNode(zkts.zconn, path, done)
}

func (zkts *ZkTopologyServer) GetSubprocessFlags() []string {
	return zk.GetZkSubprocessFlags()
}

func (zkts *ZkTopologyServer) handleActionQueue(tabletAlias naming.TabletAlias, dispatchAction func(actionPath, data string) error) (<-chan zookeeper.Event, error) {
	zkActionPath := TabletActionPathForAlias(tabletAlias)

	// This read may seem a bit pedantic, but it makes it easier
	// for the system to trend towards consistency if an action
	// fails or somehow the action queue gets mangled by an errant
	// process.
	children, _, watch, err := zkts.zconn.ChildrenW(zkActionPath)
	if err != nil {
		return watch, err
	}
	if len(children) > 0 {
		sort.Strings(children)
		for _, child := range children {
			actionPath := zkActionPath + "/" + child
			if _, err := strconv.ParseUint(child, 10, 64); err != nil {
				// This is handy if you want to restart a stuck queue.
				// FIXME(msolomon) could listen on the queue node for a change
				// generated by a "touch", but listening on two things is a bit
				// more complex.
				relog.Warning("remove invalid event from action queue: %v", child)
				zkts.zconn.Delete(actionPath, -1)
			}

			data, _, err := zkts.zconn.Get(actionPath)
			if err != nil {
				relog.Error("cannot read action %v from zk: %v", actionPath, err)
				break
			}

			if err := dispatchAction(actionPath, data); err != nil {
				break
			}
		}
	}
	return watch, nil
}

func (zkts *ZkTopologyServer) ActionEventLoop(tabletAlias naming.TabletAlias, dispatchAction func(actionPath, data string) error, done chan struct{}) {
	for {
		// Process any pending actions when we startup, before we start listening
		// for events.
		watch, err := zkts.handleActionQueue(tabletAlias, dispatchAction)
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
		case <-done:
			return
		}
	}
}

// actionPathToTabletAlias parses an actionPath back
// zkActionPath is /zk/<cell>/vt/tablets/<uid>/action/<number>
func actionPathToTabletAlias(actionPath string) (naming.TabletAlias, error) {
	pathParts := strings.Split(actionPath, "/")
	if len(pathParts) != 8 || pathParts[0] != "" || pathParts[1] != "zk" || pathParts[3] != "vt" || pathParts[4] != "tablets" || pathParts[6] != "action" {
		return naming.TabletAlias{}, fmt.Errorf("invalid action path: %v", actionPath)
	}
	return naming.ParseTabletAliasString(pathParts[2] + "-" + pathParts[5])
}

func (zkts *ZkTopologyServer) ReadTabletActionPath(actionPath string) (naming.TabletAlias, string, int, error) {
	tabletAlias, err := actionPathToTabletAlias(actionPath)
	if err != nil {
		return naming.TabletAlias{}, "", 0, err
	}

	data, stat, err := zkts.zconn.Get(actionPath)
	if err != nil {
		return naming.TabletAlias{}, "", 0, err
	}

	return tabletAlias, data, stat.Version(), nil
}

func (zkts *ZkTopologyServer) UpdateTabletAction(actionPath, data string, version int) error {
	_, err := zkts.zconn.Set(actionPath, data, version)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = naming.ErrBadVersion
		}
		return err
	}
	return nil
}

// StoreTabletActionResponse stores the data both in action and actionlog
func (zkts *ZkTopologyServer) StoreTabletActionResponse(actionPath, data string) error {
	_, err := zkts.zconn.Set(actionPath, data, -1)
	if err != nil {
		return err
	}

	actionLogPath := strings.Replace(actionPath, "/action/", "/actionlog/", 1)
	_, err = zk.CreateRecursive(zkts.zconn, actionLogPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (zkts *ZkTopologyServer) UnblockTabletAction(actionPath string) error {
	return zkts.zconn.Delete(actionPath, -1)
}
