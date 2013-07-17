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
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the per-cell methods of zktopo.Server
*/

func TabletPathForAlias(alias topo.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, alias.TabletUidStr())
}

func TabletActionPathForAlias(alias topo.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v/action", alias.Cell, alias.TabletUidStr())
}

func tabletDirectoryForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/tablets", cell)
}

//
// Tablet management
//

func (zkts *Server) CreateTablet(tablet *topo.Tablet) error {
	zkTabletPath := TabletPathForAlias(tablet.Alias())

	// Create /zk/<cell>/vt/tablets/<uid>
	_, err := zk.CreateRecursive(zkts.zconn, zkTabletPath, tablet.Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
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

func (zkts *Server) UpdateTablet(tablet *topo.TabletInfo, existingVersion int) (int, error) {
	zkTabletPath := TabletPathForAlias(tablet.Alias())
	stat, err := zkts.zconn.Set(zkTabletPath, tablet.Json(), existingVersion)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = topo.ErrBadVersion
		} else if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}

		return 0, err
	}
	return stat.Version(), nil
}

func (zkts *Server) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		if oldValue == "" {
			return "", fmt.Errorf("no data for tablet addr update: %v", tabletAlias)
		}

		tablet, err := topo.TabletFromJson(oldValue)
		if err != nil {
			return "", err
		}
		if err := update(tablet); err != nil {
			return "", err
		}
		return jscfg.ToJson(tablet), nil
	}
	err := zkts.zconn.RetryChange(zkTabletPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

func (zkts *Server) DeleteTablet(alias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	err := zk.DeleteRecursive(zkts.zconn, zkTabletPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
	}
	return err
}

func (zkts *Server) ValidateTablet(alias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	zkPaths := []string{
		path.Join(zkTabletPath, "action"),
		path.Join(zkTabletPath, "actionlog"),
	}

	for _, zkPath := range zkPaths {
		if _, _, err := zkts.zconn.Get(zkPath); err != nil {
			return err
		}
	}
	return nil
}

func (zkts *Server) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	zkTabletPath := TabletPathForAlias(alias)
	data, stat, err := zkts.zconn.Get(zkTabletPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return topo.TabletInfoFromJson(data, stat.Version())
}

func (zkts *Server) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	zkTabletsPath := tabletDirectoryForCell(cell)
	children, _, err := zkts.zconn.Children(zkTabletsPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	sort.Strings(children)
	result := make([]topo.TabletAlias, len(children))
	for i, child := range children {
		result[i].Cell = cell
		result[i].Uid, err = topo.ParseUid(child)
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

func zkPathForVtName(cell, keyspace, shard string, tabletType topo.TabletType) string {
	return path.Join(zkPathForVtShard(cell, keyspace, shard), string(tabletType))
}

func (zkts *Server) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	zkSgShardPath := zkPathForVtShard(cell, keyspace, shard)
	children, _, err := zkts.zconn.Children(zkSgShardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	result := make([]topo.TabletType, len(children))
	for i, tt := range children {
		result[i] = topo.TabletType(tt)
	}
	return result, nil
}

func (zkts *Server) UpdateSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.VtnsAddrs) error {
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

func (zkts *Server) GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error) {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return topo.NewVtnsAddrs(data, stat.Version())
}

func (zkts *Server) DeleteSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	return zkts.zconn.Delete(path, -1)
}

func (zkts *Server) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	path := zkPathForVtShard(cell, keyspace, shard)
	data := jscfg.ToJson(srvShard)
	_, err := zkts.zconn.Set(path, data, -1)
	return err
}

func (zkts *Server) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	path := zkPathForVtShard(cell, keyspace, shard)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return topo.NewSrvShard(data, stat.Version())
}

func (zkts *Server) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	path := zkPathForVtKeyspace(cell, keyspace)
	data := jscfg.ToJson(srvKeyspace)
	_, err := zkts.zconn.Set(path, data, -1)
	return err
}

func (zkts *Server) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	path := zkPathForVtKeyspace(cell, keyspace)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return topo.NewSrvKeyspace(data, stat.Version())
}

var skipUpdateErr = fmt.Errorf("skip update")

func (zkts *Server) updateTabletEndpoint(oldValue string, oldStat zk.Stat, addr *topo.VtnsAddr) (newValue string, err error) {
	if oldStat == nil {
		// The incoming object doesn't exist - we haven't been placed in the serving
		// graph yet, so don't update. Assume the next process that rebuilds the graph
		// will get the updated tablet location.
		return "", skipUpdateErr
	}

	var addrs *topo.VtnsAddrs
	if oldValue != "" {
		addrs, err = topo.NewVtnsAddrs(oldValue, oldStat.Version())
		if err != nil {
			return
		}

		foundTablet := false
		for i, entry := range addrs.Entries {
			if entry.Uid == addr.Uid {
				foundTablet = true
				if !topo.VtnsAddrEquality(&entry, addr) {
					addrs.Entries[i] = *addr
				}
				break
			}
		}

		if !foundTablet {
			addrs.Entries = append(addrs.Entries, *addr)
		}
	} else {
		addrs = topo.NewAddrs()
		addrs.Entries = append(addrs.Entries, *addr)
	}
	return jscfg.ToJson(addrs), nil
}

func (zkts *Server) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.VtnsAddr) error {
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

func (zkts *Server) WriteTabletAction(tabletAlias topo.TabletAlias, contents string) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionPath := TabletActionPathForAlias(tabletAlias) + "/"
	return zkts.zconn.Create(actionPath, contents, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (zkts *Server) WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error) {
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
			return "", topo.ErrTimeout
		case <-interrupted:
			return "", topo.ErrInterrupted
		}
	}

	// the node exists, read it
	data, _, err := zkts.zconn.Get(actionLogPath)
	if err != nil {
		return "", fmt.Errorf("action err: %v %v", actionLogPath, err)
	}

	return data, nil
}

func (zkts *Server) PurgeTabletActions(tabletAlias topo.TabletAlias, canBePurged func(data string) bool) error {
	actionPath := TabletActionPathForAlias(tabletAlias)
	return zkts.PurgeActions(actionPath, canBePurged)
}

//
// Supporting the local agent process.
//

func (zkts *Server) ValidateTabletActions(tabletAlias topo.TabletAlias) error {
	actionPath := TabletActionPathForAlias(tabletAlias)

	// Ensure that the action node is there. There is no conflict creating
	// this node.
	_, err := zkts.zconn.Create(actionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}
	return nil
}

func (zkts *Server) CreateTabletPidNode(tabletAlias topo.TabletAlias, done chan struct{}) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	path := path.Join(zkTabletPath, "pid")
	return zk.CreatePidNode(zkts.zconn, path, done)
}

func (zkts *Server) ValidateTabletPidNode(tabletAlias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	path := path.Join(zkTabletPath, "pid")
	_, _, err := zkts.zconn.Get(path)
	return err
}

func (zkts *Server) GetSubprocessFlags() []string {
	return zk.GetZkSubprocessFlags()
}

func (zkts *Server) handleActionQueue(tabletAlias topo.TabletAlias, dispatchAction func(actionPath, data string) error) (<-chan zookeeper.Event, error) {
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

func (zkts *Server) ActionEventLoop(tabletAlias topo.TabletAlias, dispatchAction func(actionPath, data string) error, done chan struct{}) {
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
func actionPathToTabletAlias(actionPath string) (topo.TabletAlias, error) {
	pathParts := strings.Split(actionPath, "/")
	if len(pathParts) != 8 || pathParts[0] != "" || pathParts[1] != "zk" || pathParts[3] != "vt" || pathParts[4] != "tablets" || pathParts[6] != "action" {
		return topo.TabletAlias{}, fmt.Errorf("invalid action path: %v", actionPath)
	}
	return topo.ParseTabletAliasString(pathParts[2] + "-" + pathParts[5])
}

func (zkts *Server) ReadTabletActionPath(actionPath string) (topo.TabletAlias, string, int, error) {
	tabletAlias, err := actionPathToTabletAlias(actionPath)
	if err != nil {
		return topo.TabletAlias{}, "", 0, err
	}

	data, stat, err := zkts.zconn.Get(actionPath)
	if err != nil {
		return topo.TabletAlias{}, "", 0, err
	}

	return tabletAlias, data, stat.Version(), nil
}

func (zkts *Server) UpdateTabletAction(actionPath, data string, version int) error {
	_, err := zkts.zconn.Set(actionPath, data, version)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = topo.ErrBadVersion
		}
		return err
	}
	return nil
}

// StoreTabletActionResponse stores the data both in action and actionlog
func (zkts *Server) StoreTabletActionResponse(actionPath, data string) error {
	_, err := zkts.zconn.Set(actionPath, data, -1)
	if err != nil {
		return err
	}

	actionLogPath := strings.Replace(actionPath, "/action/", "/actionlog/", 1)
	_, err = zk.CreateRecursive(zkts.zconn, actionLogPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (zkts *Server) UnblockTabletAction(actionPath string) error {
	return zkts.zconn.Delete(actionPath, -1)
}
