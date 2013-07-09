// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the global (not per-cell) methods of ZkTopologyServer
*/
const (
	globalKeyspacesPath = "/zk/global/vt/keyspaces"
)

//
// Keyspace management
//

func (zkts *ZkTopologyServer) CreateKeyspace(keyspace string) error {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
	pathList := []string{
		keyspacePath,
		path.Join(keyspacePath, "action"),
		path.Join(keyspacePath, "actionlog"),
		path.Join(keyspacePath, "shards"),
	}

	alreadyExists := false
	for _, zkPath := range pathList {
		_, err := zk.CreateRecursive(zkts.Zconn, zkPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				alreadyExists = true
			} else {
				return fmt.Errorf("error creating keyspace: %v %v", zkPath, err)
			}
		}
	}
	if alreadyExists {
		return naming.ErrNodeExists
	}
	return nil
}

func (zkts *ZkTopologyServer) GetKeyspaces() ([]string, error) {
	children, _, err := zkts.Zconn.Children(globalKeyspacesPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

//
// Shard Management
//

func (zkts *ZkTopologyServer) CreateShard(keyspace, shard, contents string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	pathList := []string{
		shardPath,
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}

	alreadyExists := false
	for i, zkPath := range pathList {
		c := ""
		if i == 0 {
			c = contents
		}
		_, err := zk.CreateRecursive(zkts.Zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				alreadyExists = true
			} else {
				return fmt.Errorf("error creating shard: %v %v", zkPath, err)
			}
		}
	}
	if alreadyExists {
		return naming.ErrNodeExists
	}
	return nil
}

func (zkts *ZkTopologyServer) UpdateShard(keyspace, shard, contents string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	_, err := zkts.Zconn.Set(shardPath, contents, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return naming.ErrNoNode
		}
	}
	return err
}

func (zkts *ZkTopologyServer) ValidateShard(keyspace, shard string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	zkPaths := []string{
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}
	for _, zkPath := range zkPaths {
		_, _, err := zkts.Zconn.Get(zkPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zkts *ZkTopologyServer) GetShard(keyspace, shard string) (string, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, _, err := zkts.Zconn.Get(shardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return "", naming.ErrNoNode
		}
		return "", err
	}
	return data, nil

}

func (zkts *ZkTopologyServer) GetShardNames(keyspace string) ([]string, error) {
	shardsPath := path.Join(globalKeyspacesPath, keyspace, "shards")
	children, _, err := zkts.Zconn.Children(shardsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

//
// Replication graph management
//

func (zkts *ZkTopologyServer) GetReplicationPaths(keyspace, shard, repPath string) ([]naming.TabletAlias, error) {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	children, _, err := zkts.Zconn.Children(replicationPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, naming.ErrNoNode
		}
		return nil, err
	}

	result := make([]naming.TabletAlias, 0, len(children))
	for _, child := range children {
		// 'action' and 'actionlog' can only be present at the toplevel
		if (repPath == "" || repPath == "/") && (child == "action" || child == "actionlog") {
			continue
		}
		alias, err := naming.ParseTabletAliasString(child)
		if err != nil {
			return nil, err
		}
		result = append(result, alias)
	}
	return result, nil
}

func (zkts *ZkTopologyServer) CreateReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	_, err := zk.CreateRecursive(zkts.Zconn, replicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			return naming.ErrNodeExists
		}
		return err
	}
	return nil
}

func (zkts *ZkTopologyServer) DeleteReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	err := zkts.Zconn.Delete(replicationPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return naming.ErrNoNode
		} else if zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
			return naming.ErrNotEmpty
		}
		return err
	}
	return nil
}
