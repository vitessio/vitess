// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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

func zkPathForVtName(cell, keyspace, shard string, tabletType topodatapb.TabletType) string {
	return path.Join(zkPathForVtShard(cell, keyspace, shard), strings.ToLower(tabletType.String()))
}

// GetSrvTabletTypesPerShard is part of the topo.Server interface
func (zkts *Server) GetSrvTabletTypesPerShard(ctx context.Context, cell, keyspace, shard string) ([]topodatapb.TabletType, error) {
	zkSgShardPath := zkPathForVtShard(cell, keyspace, shard)
	children, _, err := zkts.zconn.Children(zkSgShardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	result := make([]topodatapb.TabletType, 0, len(children))
	for _, tt := range children {
		// these two are used for locking
		if tt == "action" || tt == "actionlog" {
			continue
		}
		if ptt, err := topoproto.ParseTabletType(tt); err == nil {
			result = append(result, ptt)
		}
	}
	return result, nil
}

// CreateEndPoints is part of the topo.Server interface
func (zkts *Server) CreateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, addrs *topodatapb.EndPoints) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, err := json.MarshalIndent(addrs, "", "  ")
	if err != nil {
		return err
	}

	// Create only if it doesn't exist.
	_, err = zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		err = topo.ErrNodeExists
	}
	return err
}

// UpdateEndPoints is part of the topo.Server interface
func (zkts *Server) UpdateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, addrs *topodatapb.EndPoints, existingVersion int64) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, err := json.MarshalIndent(addrs, "", "  ")
	if err != nil {
		return err
	}

	if existingVersion == -1 {
		// Update or create unconditionally.
		_, err := zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				// Node already exists - just stomp away. Multiple writers shouldn't be here.
				// We use RetryChange here because it won't update the node unnecessarily.
				f := func(oldValue string, oldStat zk.Stat) (string, error) {
					return string(data), nil
				}
				err = zkts.zconn.RetryChange(path, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
			}
		}
		return err
	}

	// Compare And Set
	if _, err = zkts.zconn.Set(path, string(data), int(existingVersion)); err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = topo.ErrBadVersion
		} else if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
	}
	return err
}

// GetEndPoints is part of the topo.Server interface
func (zkts *Server) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) (*topodatapb.EndPoints, int64, error) {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	data, stat, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, 0, err
	}
	result := &topodatapb.EndPoints{}
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), result); err != nil {
			return nil, 0, fmt.Errorf("EndPoints unmarshal failed: %v %v", data, err)
		}
	}
	return result, int64(stat.Version()), nil
}

// DeleteEndPoints is part of the topo.Server interface
func (zkts *Server) DeleteEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, existingVersion int64) error {
	path := zkPathForVtName(cell, keyspace, shard, tabletType)
	if err := zkts.zconn.Delete(path, int(existingVersion)); err != nil {
		switch {
		case zookeeper.IsError(err, zookeeper.ZNONODE):
			err = topo.ErrNoNode
		case zookeeper.IsError(err, zookeeper.ZBADVERSION):
			err = topo.ErrBadVersion
		}
		return err
	}
	return nil
}

// UpdateSrvShard is part of the topo.Server interface
func (zkts *Server) UpdateSrvShard(ctx context.Context, cell, keyspace, shard string, srvShard *topodatapb.SrvShard) error {
	path := zkPathForVtShard(cell, keyspace, shard)
	data, err := json.MarshalIndent(srvShard, "", "  ")
	if err != nil {
		return err
	}

	// Update or create unconditionally.
	if _, err = zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			// Node already exists - just stomp away. Multiple writers shouldn't be here.
			// We use RetryChange here because it won't update the node unnecessarily.
			f := func(oldValue string, oldStat zk.Stat) (string, error) {
				return string(data), nil
			}
			err = zkts.zconn.RetryChange(path, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
		}
	}
	return err
}

// GetSrvShard is part of the topo.Server interface
func (zkts *Server) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	path := zkPathForVtShard(cell, keyspace, shard)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	srvShard := &topodatapb.SrvShard{}
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), srvShard); err != nil {
			return nil, fmt.Errorf("SrvShard unmarshal failed: %v %v", data, err)
		}
	}
	return srvShard, nil
}

// DeleteSrvShard is part of the topo.Server interface
func (zkts *Server) DeleteSrvShard(ctx context.Context, cell, keyspace, shard string) error {
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
func (zkts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	path := zkPathForVtKeyspace(cell, keyspace)
	data, err := json.MarshalIndent(srvKeyspace, "", "  ")
	if err != nil {
		return err
	}
	_, err = zkts.zconn.Set(path, string(data), -1)
	if zookeeper.IsError(err, zookeeper.ZNONODE) {
		_, err = zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	}
	return err
}

// DeleteSrvKeyspace is part of the topo.Server interface
func (zkts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	path := zkPathForVtKeyspace(cell, keyspace)
	err := zkts.zconn.Delete(path, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

// GetSrvKeyspace is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	path := zkPathForVtKeyspace(cell, keyspace)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, topo.ErrNoNode
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := json.Unmarshal([]byte(data), srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// GetSrvKeyspaceNames is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
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

func (zkts *Server) updateTabletEndpoint(oldValue string, oldStat zk.Stat, addr *topodatapb.EndPoint) (newValue string, err error) {
	if oldStat == nil {
		// The incoming object doesn't exist - we haven't been placed in the serving
		// graph yet, so don't update. Assume the next process that rebuilds the graph
		// will get the updated tablet location.
		return "", errSkipUpdate
	}

	var addrs *topodatapb.EndPoints
	if oldValue != "" {
		addrs = &topodatapb.EndPoints{}
		if len(oldValue) > 0 {
			if err := json.Unmarshal([]byte(oldValue), addrs); err != nil {
				return "", fmt.Errorf("EndPoints unmarshal failed: %v %v", oldValue, err)
			}
		}

		foundTablet := false
		for i, entry := range addrs.Entries {
			if entry.Uid == addr.Uid {
				foundTablet = true
				if !topo.EndPointEquality(entry, addr) {
					addrs.Entries[i] = addr
				}
				break
			}
		}

		if !foundTablet {
			addrs.Entries = append(addrs.Entries, addr)
		}
	} else {
		addrs = topo.NewEndPoints()
		addrs.Entries = append(addrs.Entries, addr)
	}
	data, err := json.MarshalIndent(addrs, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// WatchSrvKeyspace is part of the topo.Server interface
func (zkts *Server) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (<-chan *topodatapb.SrvKeyspace, error) {
	filePath := zkPathForVtKeyspace(cell, keyspace)

	notifications := make(chan *topodatapb.SrvKeyspace, 10)

	// waitOrInterrupted will return true if context.Done() is triggered
	waitOrInterrupted := func() bool {
		timer := time.After(WatchSleepDuration)
		select {
		case <-ctx.Done():
			close(notifications)
			return true
		case <-timer:
		}
		return false
	}

	go func() {
		for {
			// set the watch
			data, _, watch, err := zkts.zconn.GetW(filePath)
			if err != nil {
				if zookeeper.IsError(err, zookeeper.ZNONODE) {
					// the parent directory doesn't exist
					notifications <- nil
				}

				log.Errorf("Cannot set watch on %v, waiting for %v to retry: %v", filePath, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send nil if no
			// data
			var srvKeyspace *topodatapb.SrvKeyspace
			sendIt := true
			if len(data) > 0 {
				srvKeyspace = &topodatapb.SrvKeyspace{}
				if err := json.Unmarshal([]byte(data), srvKeyspace); err != nil {
					log.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
					sendIt = false
				}
			}
			if sendIt {
				notifications <- srvKeyspace
			}

			// now act on the watch
			select {
			case event, ok := <-watch:
				if !ok {
					log.Warningf("watch on %v was closed, waiting for %v to retry", filePath, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
					continue
				}

				if !event.Ok() {
					log.Warningf("received a non-OK event for %v, waiting for %v to retry", filePath, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
				}
			case <-ctx.Done():
				// user is not interested any more
				close(notifications)
				return
			}
		}
	}()

	return notifications, nil
}
