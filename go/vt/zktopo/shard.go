// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the shard management code for zktopo.Server
*/

// CreateShard is part of the topo.Server interface
func (zkts *Server) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	pathList := []string{
		shardPath,
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	alreadyExists := false
	for i, zkPath := range pathList {
		c := ""
		if i == 0 {
			c = string(data)
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

// UpdateShard is part of the topo.Server interface
func (zkts *Server) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}
	stat, err := zkts.zconn.Set(shardPath, string(data), int(existingVersion))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return -1, err
	}
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
func (zkts *Server) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, stat, err := zkts.zconn.Get(shardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, 0, err
	}

	s := &topodatapb.Shard{}
	if err = json.Unmarshal([]byte(data), s); err != nil {
		return nil, 0, fmt.Errorf("bad shard data %v", err)
	}

	return s, int64(stat.Version()), nil
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
	return nil
}
