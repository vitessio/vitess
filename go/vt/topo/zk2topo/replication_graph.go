/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zk2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the replication graph management code for zktopo.Server
// Eventually, this code will move to go/vt/topo.

// UpdateShardReplicationFields is part of the topo.Server interface
func (zs *Server) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	zkPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)

	for {
		data, version, err := zs.Get(ctx, cell, zkPath)
		sr := &topodatapb.ShardReplication{}
		switch err {
		case topo.ErrNoNode:
			// Empty node, version is nil
		case nil:
			// Use any data we got.
			if err = proto.Unmarshal(data, sr); err != nil {
				return fmt.Errorf("bad ShardReplication data %v", err)
			}
		default:
			return err
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
		data, err = proto.Marshal(sr)
		if err != nil {
			return err
		}
		if version == nil {
			// We have to create, and we catch ErrNodeExists.
			_, err = zs.Create(ctx, cell, zkPath, data)
			if err == topo.ErrNodeExists {
				// Node was created by another process, try
				// again.
				continue
			}
			return err
		}

		// We have to update, and we catch ErrBadVersion.
		_, err = zs.Update(ctx, cell, zkPath, data, version)
		if err == topo.ErrBadVersion {
			// Node was updated by another process, try again.
			continue
		}
		return err
	}
}

// GetShardReplication is part of the topo.Server interface
func (zs *Server) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	zkPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)
	data, _, err := zs.Get(ctx, cell, zkPath)
	if err != nil {
		return nil, err
	}

	sr := &topodatapb.ShardReplication{}
	if err = proto.Unmarshal(data, sr); err != nil {
		return nil, fmt.Errorf("bad ShardReplication data %v", err)
	}

	return topo.NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

// DeleteShardReplication is part of the topo.Server interface
func (zs *Server) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	zkPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)
	return zs.Delete(ctx, cell, zkPath, nil)
}

// DeleteKeyspaceReplication is part of the topo.Server interface
func (zs *Server) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	zkPath := path.Join(keyspacesPath, keyspace)
	return zs.Delete(ctx, cell, zkPath, nil)
}
