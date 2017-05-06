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

package consultopo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard implements topo.Server.
func (s *Server) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	_, err = s.Create(ctx, topo.GlobalCell, shardPath, data)
	return err
}

// UpdateShard implements topo.Server.
func (s *Server) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(value)
	if err != nil {
		return -1, err
	}

	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	version, err := s.Update(ctx, topo.GlobalCell, shardPath, data, VersionFromInt(existingVersion))
	if err != nil {
		return -1, err
	}
	return int64(version.(ConsulVersion)), nil
}

// GetShard implements topo.Server.
func (s *Server) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	data, version, err := s.Get(ctx, topo.GlobalCell, shardPath)
	if err != nil {
		return nil, 0, err
	}

	sh := &topodatapb.Shard{}
	if err = proto.Unmarshal(data, sh); err != nil {
		return nil, 0, fmt.Errorf("bad shard data: %v", err)
	}

	return sh, int64(version.(ConsulVersion)), nil
}

// GetShardNames implements topo.Server.
func (s *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	shardsPath := path.Join(keyspacesPath, keyspace, shardsPath)
	children, err := s.ListDir(ctx, topo.GlobalCell, shardsPath)
	if err == topo.ErrNoNode {
		// The directory doesn't exist, let's see if the keyspace
		// is here or not.
		_, _, kerr := s.GetKeyspace(ctx, keyspace)
		if kerr == nil {
			// Keyspace is here, means no shards.
			return nil, nil
		}
		return nil, err
	}
	return children, err
}

// DeleteShard implements topo.Server.
func (s *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	return s.Delete(ctx, topo.GlobalCell, shardPath, nil)
}
