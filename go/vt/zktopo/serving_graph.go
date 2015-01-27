// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// WatchSleepDuration is how many seconds interval to poll for in case
// the directory that contains a file to watch doesn't exist, or a watch
// is broken. It is exported so individual test and main programs
// can change it.
var WatchSleepDuration = 30 * time.Second

/*
This file contains the serving graph management code of zktopo.Server
*/
func zkPathForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/ns", cell)
}

func zkPathForVtKeyspace(cell, keyspace string) string {
	return path.Join(zkPathForCell(cell), keyspace)
}

func zkPathForVtShard(cell, keyspace, shard string) string {
	return path.Join(zkPathForVtKeyspace(cell, keyspace), shard)
}

func zkPathForVtName(cell, keyspace, shard string, tabletType topo.TabletType) string {
	return path.Join(zkPathForVtShard(cell, keyspace, shard), string(tabletType))
}

// GetSrvTabletTypesPerShard is part of the topo.Server interface
func (zkts *Server) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	zkSgShardPath := zkPathForVtShard(cell, keyspace, shard)
	children, _, err := zkts.zconn.Children(zkSgShardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	result := make([]topo.TabletType, 0, len(children))
	for _, tt := range children {
		// these two are used for locking
		if tt == "action" || tt == "actionlog" {
			continue
		}
		result = append(result, topo.TabletType(tt))
	}
	return result, nil
}

// UpdateEndPoints is part of the topo.Server interface
func (zkts *Server) UpdateEndPoints(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
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

// GetEndPoints is part of the topo.Server interface
func (zkts *Server) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	result := &topo.EndPoints{}
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), result); err != nil {
			return nil, fmt.Errorf("EndPoints unmarshal failed: %v %v", data, err)
		}
	}
	return result, nil
}

// DeleteEndPoints is part of the topo.Server interface
func (zkts *Server) DeleteEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	err := zkts.zconn.Delete(path, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

// UpdateSrvShard is part of the topo.Server interface
func (zkts *Server) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	path := zkPathForVtShard(cell, keyspace, shard)
	data := jscfg.ToJson(srvShard)
	_, err := zkts.zconn.Set(path, data, -1)
	return err
}

// GetSrvShard is part of the topo.Server interface
func (zkts *Server) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	path := zkPathForVtShard(cell, keyspace, shard)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	srvShard := topo.NewSrvShard(int64(stat.Version()))
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), srvShard); err != nil {
			return nil, fmt.Errorf("SrvShard unmarshal failed: %v %v", data, err)
		}
	}
	return srvShard, nil
}

// DeleteSrvShard is part of the topo.Server interface
func (zkts *Server) DeleteSrvShard(cell, keyspace, shard string) error {
	path := zkPathForVtShard(cell, keyspace, shard)
	err := zkts.zconn.Delete(path, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

// UpdateSrvKeyspace is part of the topo.Server interface
func (zkts *Server) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	path := zkPathForVtKeyspace(cell, keyspace)
	data := jscfg.ToJson(srvKeyspace)
	_, err := zkts.zconn.Set(path, data, -1)
	if zookeeper.IsError(err, zookeeper.ZNONODE) {
		_, err = zk.CreateRecursive(zkts.zconn, path, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	}
	return err
}

// GetSrvKeyspace is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	path := zkPathForVtKeyspace(cell, keyspace)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	srvKeyspace := topo.NewSrvKeyspace(int64(stat.Version()))
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), srvKeyspace); err != nil {
			return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
		}
	}
	return srvKeyspace, nil
}

// GetSrvKeyspaceNames is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspaceNames(cell string) ([]string, error) {
	children, _, err := zkts.zconn.Children(zkPathForCell(cell))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, nil
		}
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

var errSkipUpdate = fmt.Errorf("skip update")

func (zkts *Server) updateTabletEndpoint(oldValue string, oldStat zk.Stat, addr *topo.EndPoint) (newValue string, err error) {
	if oldStat == nil {
		// The incoming object doesn't exist - we haven't been placed in the serving
		// graph yet, so don't update. Assume the next process that rebuilds the graph
		// will get the updated tablet location.
		return "", errSkipUpdate
	}

	var addrs *topo.EndPoints
	if oldValue != "" {
		addrs = &topo.EndPoints{}
		if len(oldValue) > 0 {
			if err := json.Unmarshal([]byte(oldValue), addrs); err != nil {
				return "", fmt.Errorf("EndPoints unmarshal failed: %v %v", oldValue, err)
			}
		}

		foundTablet := false
		for i, entry := range addrs.Entries {
			if entry.Uid == addr.Uid {
				foundTablet = true
				if !topo.EndPointEquality(&entry, addr) {
					addrs.Entries[i] = *addr
				}
				break
			}
		}

		if !foundTablet {
			addrs.Entries = append(addrs.Entries, *addr)
		}
	} else {
		addrs = topo.NewEndPoints()
		addrs.Entries = append(addrs.Entries, *addr)
	}
	return jscfg.ToJson(addrs), nil
}

// UpdateTabletEndpoint is part of the topo.Server interface
func (zkts *Server) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		return zkts.updateTabletEndpoint(oldValue, oldStat, addr)
	}
	err := zkts.zconn.RetryChange(path, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err == errSkipUpdate || zookeeper.IsError(err, zookeeper.ZNONODE) {
		err = nil
	}
	return err
}

// WatchEndPoints is part of the topo.Server interface
func (zkts *Server) WatchEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)

	notifications := make(chan *topo.EndPoints, 10)
	stopWatching := make(chan struct{})

	// waitOrInterrupted will return true if stopWatching is triggered
	waitOrInterrupted := func() bool {
		timer := time.After(WatchSleepDuration)
		select {
		case <-stopWatching:
			close(notifications)
			return true
		case <-timer:
		}
		return false
	}

	go func() {
		for {
			// set the watch
			data, _, watch, err := zkts.zconn.GetW(path)
			if err != nil {
				if zookeeper.IsError(err, zookeeper.ZNONODE) {
					// the parent directory doesn't exist
					notifications <- nil
				}

				log.Errorf("Cannot set watch on %v, waiting for %s to retry: %v", path, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send nil if no
			// data
			var ep *topo.EndPoints
			sendIt := true
			if len(data) > 0 {
				ep = &topo.EndPoints{}
				if err := json.Unmarshal([]byte(data), ep); err != nil {
					log.Errorf("EndPoints unmarshal failed: %v %v", data, err)
					sendIt = false
				}
			}
			if sendIt {
				notifications <- ep
			}

			// now act on the watch
			select {
			case event, ok := <-watch:
				if !ok {
					log.Warningf("watch on %v was closed, waiting for %s to retry", path, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
					continue
				}

				if !event.Ok() {
					log.Warningf("received a non-OK event, waiting for %s to retry", path, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
				}
			case <-stopWatching:
				// user is not interested any more
				close(notifications)
				return
			}
		}
	}()

	return notifications, stopWatching, nil

}
