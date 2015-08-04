// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/events"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the shard management code for zktopo.Server
*/

// CreateShard is part of the topo.Server interface
func (zkts *Server) CreateShard(ctx context.Context, keyspace, shard string, value *pb.Shard) error {
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
			c = jscfg.ToJSON(value)
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

	event.Dispatch(&events.ShardChange{
		ShardInfo: *topo.NewShardInfo(keyspace, shard, value, -1),
		Status:    "created",
	})
	return nil
}

// UpdateShard is part of the topo.Server interface
func (zkts *Server) UpdateShard(ctx context.Context, si *topo.ShardInfo, existingVersion int64) (int64, error) {
	shardPath := path.Join(globalKeyspacesPath, si.Keyspace(), "shards", si.ShardName())
	stat, err := zkts.zconn.Set(shardPath, jscfg.ToJSON(si.Shard), int(existingVersion))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return -1, err
	}

	event.Dispatch(&events.ShardChange{
		ShardInfo: *si,
		Status:    "updated",
	})
	return int64(stat.Version()), nil
}

// ValidateShard is part of the topo.Server interface
func (zkts *Server) ValidateShard(ctx context.Context, keyspace, shard string) error {
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

// GetShard is part of the topo.Server interface
func (zkts *Server) GetShard(ctx context.Context, keyspace, shard string) (*topo.ShardInfo, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, stat, err := zkts.zconn.Get(shardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	s := &pb.Shard{}
	if err = json.Unmarshal([]byte(data), s); err != nil {
		return nil, fmt.Errorf("bad shard data %v", err)
	}

	return topo.NewShardInfo(keyspace, shard, s, int64(stat.Version())), nil
}

// GetShardNames is part of the topo.Server interface
func (zkts *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
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

// DeleteShard is part of the topo.Server interface
func (zkts *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	err := zk.DeleteRecursive(zkts.zconn, shardPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}

	event.Dispatch(&events.ShardChange{
		ShardInfo: *topo.NewShardInfo(keyspace, shard, nil, -1),
		Status:    "deleted",
	})
	return nil
}
