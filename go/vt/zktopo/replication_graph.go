// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

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
	if _, err := zk.CreateRecursive(zkts.zconn, zkDir, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return convertError(err)
	}

	// now update the data
	zkPath := shardReplicationPath(cell, keyspace, shard)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		sr := &topodatapb.ShardReplication{}
		if oldValue != "" {
			if err := json.Unmarshal([]byte(oldValue), sr); err != nil {
				return "", err
			}
		}

		if err := update(sr); err != nil {
			return "", err
		}
		data, err := json.MarshalIndent(sr, "", "  ")
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	err := zkts.zconn.RetryChange(zkPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetShardReplication is part of the topo.Server interface
func (zkts *Server) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	zkPath := shardReplicationPath(cell, keyspace, shard)
	data, _, err := zkts.zconn.Get(zkPath)
	if err != nil {
		return nil, convertError(err)
	}

	sr := &topodatapb.ShardReplication{}
	if err = json.Unmarshal([]byte(data), sr); err != nil {
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
