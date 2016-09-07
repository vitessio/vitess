// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the replication graph management code for zktopo.Server
*/

func keyspaceReplicationPath(cell, keyspace string) string {
	return path.Join("/zk", cell, "vt", "replication", keyspace)
}

func shardReplicationPath(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "vt", "replication", keyspace, shard)
}

// UpdateShardReplicationFields is part of the topo.Server interface
func (zkts *Server) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	// create the parent directory to be sure it's here
	zkDir := path.Join("/zk", cell, "vt", "replication", keyspace)
	if _, err := zk.CreateRecursive(zkts.zconn, zkDir, nil, 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil && err != zookeeper.ErrNodeExists {
		return convertError(err)
	}

	// now update the data
	zkPath := shardReplicationPath(cell, keyspace, shard)
	for {
		data, stat, err := zkts.zconn.Get(zkPath)
		var version int32 = -1
		sr := &topodatapb.ShardReplication{}
		switch err {
		case zookeeper.ErrNoNode:
			// empty node, version is 0
		case nil:
			version = stat.Version
			if len(data) > 0 {
				if err = json.Unmarshal(data, sr); err != nil {
					return fmt.Errorf("bad ShardReplication data %v", err)
				}
			}
		default:
			return convertError(err)
		}

		err = update(sr)
		switch err {
		case topo.ErrNoUpdateNeeded:
			return nil
		case nil:
			// keep going
		default:
			return err
		}

		// marshall and save
		d, err := json.MarshalIndent(sr, "", "  ")
		if err != nil {
			return err
		}
		if version == -1 {
			_, err = zkts.zconn.Create(zkPath, d, 0, zookeeper.WorldACL(zookeeper.PermAll))
			if err != zookeeper.ErrNodeExists {
				return convertError(err)
			}
		} else {
			_, err = zkts.zconn.Set(zkPath, d, version)
			if err != zookeeper.ErrBadVersion {
				return convertError(err)
			}
		}
	}
}

// GetShardReplication is part of the topo.Server interface
func (zkts *Server) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	data, _, err := zkts.zconn.Get(zkPath)
	if err != nil {
		return nil, convertError(err)
	}

	sr := &topodatapb.ShardReplication{}
	if err = json.Unmarshal(data, sr); err != nil {
		return nil, fmt.Errorf("bad ShardReplication data %v", err)
	}

	return topo.NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

// DeleteShardReplication is part of the topo.Server interface
func (zkts *Server) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	err := zkts.zconn.Delete(zkPath, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// DeleteKeyspaceReplication is part of the topo.Server interface
func (zkts *Server) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	zkPath := keyspaceReplicationPath(cell, keyspace)
	err := zkts.zconn.Delete(zkPath, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}
