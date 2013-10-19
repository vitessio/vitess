// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the replication graph management code for zktopo.Server
*/

func shardReplicationPath(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "vt", "replication", keyspace, shard)
}

func (zkts *Server) CreateShardReplication(cell, keyspace, shard string, sr *topo.ShardReplication) error {
	data := jscfg.ToJson(sr)
	zkPath := shardReplicationPath(cell, keyspace, shard)
	_, err := zk.CreateRecursive(zkts.zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
		}
		return err
	}
	return nil
}

func (zkts *Server) UpdateShardReplicationFields(cell, keyspace, shard string, update func(*topo.ShardReplication) error) error {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		sr := &topo.ShardReplication{}
		if oldValue != "" {
			if err := json.Unmarshal([]byte(oldValue), sr); err != nil {
				return "", err
			}
		}

		if err := update(sr); err != nil {
			return "", err
		}
		return jscfg.ToJson(sr), nil
	}
	err := zkts.zconn.RetryChange(zkPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

func (zkts *Server) GetShardReplication(cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	data, _, err := zkts.zconn.Get(zkPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	sr := &topo.ShardReplication{}
	if err = json.Unmarshal([]byte(data), sr); err != nil {
		return nil, fmt.Errorf("bad ShardReplication data %v", err)
	}

	return topo.NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

func (zkts *Server) DeleteShardReplication(cell, keyspace, shard string) error {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	err := zkts.zconn.Delete(zkPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}
