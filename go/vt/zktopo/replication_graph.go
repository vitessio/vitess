// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"path"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the replication graph management code for zktopo.Server
*/

func (zkts *Server) GetReplicationPaths(keyspace, shard, repPath string) ([]topo.TabletAlias, error) {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	children, _, err := zkts.zconn.Children(replicationPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	result := make([]topo.TabletAlias, 0, len(children))
	for _, child := range children {
		// 'action' and 'actionlog' can only be present at the toplevel
		if (repPath == "" || repPath == "/") && (child == "action" || child == "actionlog") {
			continue
		}
		alias, err := topo.ParseTabletAliasString(child)
		if err != nil {
			return nil, err
		}
		result = append(result, alias)
	}
	return result, nil
}

func (zkts *Server) CreateReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	_, err := zk.CreateRecursive(zkts.zconn, replicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
		}
		return err
	}
	return nil
}

func (zkts *Server) DeleteReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	err := zkts.zconn.Delete(replicationPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		} else if zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
			err = topo.ErrNotEmpty
		}
		return err
	}
	return nil
}
