// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"

	"code.google.com/p/vitess/go/jscfg"
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
