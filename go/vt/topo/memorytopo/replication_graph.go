/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memorytopo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateShardReplicationFields implements topo.Impl.UpdateShardReplicationFields
func (mt *MemoryTopo) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	p := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)

	for {
		data, version, err := mt.Get(ctx, cell, p)
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
			_, err = mt.Create(ctx, cell, p, data)
			if err != topo.ErrNodeExists {
				return err
			}
		} else {
			// We have to update, and we catch ErrBadVersion.
			_, err = mt.Update(ctx, cell, p, data, version)
			if err != topo.ErrBadVersion {
				return err
			}
		}
	}
}

// GetShardReplication implements topo.Impl.GetShardReplication
func (mt *MemoryTopo) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	p := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)
	data, _, err := mt.Get(ctx, cell, p)
	if err != nil {
		return nil, err
	}

	sr := &topodatapb.ShardReplication{}
	if err = proto.Unmarshal(data, sr); err != nil {
		return nil, fmt.Errorf("bad ShardReplication data %v", err)
	}

	return topo.NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

// DeleteShardReplication implements topo.Impl.DeleteShardReplication
func (mt *MemoryTopo) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	p := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardReplicationFile)
	return mt.Delete(ctx, cell, p, nil)
}

// DeleteKeyspaceReplication implements topo.Impl.DeleteKeyspaceReplication
func (mt *MemoryTopo) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	p := path.Join(keyspacesPath, keyspace)
	return mt.Delete(ctx, cell, p, nil)
}
