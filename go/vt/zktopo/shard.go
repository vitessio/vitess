// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the shard management code for zktopo.Server
*/

func (zkts *Server) CreateShard(keyspace, shard string, value *topo.Shard) error {
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
			c = jscfg.ToJson(value)
		}
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				alreadyExists = true
			} else {
				return fmt.Errorf("error creating shard: %v %v", zkPath, err)
			}
		}
	}
	if alreadyExists {
		return topo.ErrNodeExists
	}
	return nil
}

func (zkts *Server) UpdateShard(si *topo.ShardInfo) error {
	shardPath := path.Join(globalKeyspacesPath, si.Keyspace(), "shards", si.ShardName())
	_, err := zkts.zconn.Set(shardPath, jscfg.ToJson(si.Shard), -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
	}
	return err
}

func (zkts *Server) ValidateShard(keyspace, shard string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	zkPaths := []string{
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}
	for _, zkPath := range zkPaths {
		_, _, err := zkts.zconn.Get(zkPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zkts *Server) GetShard(keyspace, shard string) (*topo.ShardInfo, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, _, err := zkts.zconn.Get(shardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	s := &topo.Shard{}
	if err = json.Unmarshal([]byte(data), s); err != nil {
		return nil, fmt.Errorf("bad shard data %v", err)
	}

	return topo.NewShardInfo(keyspace, shard, s), nil
}

func (zkts *Server) GetShardCritical(keyspace, shard string) (*topo.ShardInfo, error) {
	return zkts.GetShard(keyspace, shard)
}

func (zkts *Server) GetShardNames(keyspace string) ([]string, error) {
	shardsPath := path.Join(globalKeyspacesPath, keyspace, "shards")
	children, _, err := zkts.zconn.Children(shardsPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}
