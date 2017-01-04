// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the shard management code for zktopo.Server
*/

// CreateShard is part of the topo.Server interface
func (zkts *Server) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	shardPath := path.Join(GlobalKeyspacesPath, keyspace, "shards", shard)
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
		var c []byte
		if i == 0 {
			c = data
		}
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PermAll))
		switch err {
		case nil:
			// nothing to do
		case zookeeper.ErrNodeExists:
			alreadyExists = true
		default:
			return convertError(err)
		}
	}
	if alreadyExists {
		return topo.ErrNodeExists
	}
	return nil
}

// UpdateShard is part of the topo.Server interface
func (zkts *Server) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	shardPath := path.Join(GlobalKeyspacesPath, keyspace, "shards", shard)
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}
	stat, err := zkts.zconn.Set(shardPath, data, int32(existingVersion))
	if err != nil {
		return -1, convertError(err)
	}
	return int64(stat.Version), nil
}

// GetShard is part of the topo.Server interface
func (zkts *Server) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	shardPath := path.Join(GlobalKeyspacesPath, keyspace, "shards", shard)
	data, stat, err := zkts.zconn.Get(shardPath)
	if err != nil {
		return nil, 0, convertError(err)
	}

	s := &topodatapb.Shard{}
	if err = json.Unmarshal(data, s); err != nil {
		return nil, 0, fmt.Errorf("bad shard data %v", err)
	}

	return s, int64(stat.Version), nil
}

// GetShardNames is part of the topo.Server interface
func (zkts *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	shardsPath := path.Join(GlobalKeyspacesPath, keyspace, "shards")
	children, _, err := zkts.zconn.Children(shardsPath)
	if err != nil {
		return nil, convertError(err)
	}

	sort.Strings(children)
	return children, nil
}

// DeleteShard is part of the topo.Server interface
func (zkts *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	shardPath := path.Join(GlobalKeyspacesPath, keyspace, "shards", shard)
	err := zk.DeleteRecursive(zkts.zconn, shardPath, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}
