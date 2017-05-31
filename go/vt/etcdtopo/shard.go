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

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"golang.org/x/net/context"

	"github.com/coreos/go-etcd/etcd"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard implements topo.Server.
func (s *Server) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	global := s.getGlobal()

	if _, err = global.Create(shardFilePath(keyspace, shard), string(data), 0 /* ttl */); err != nil {
		return convertError(err)
	}
	return nil
}

// UpdateShard implements topo.Server.
func (s *Server) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}

	var resp *etcd.Response
	if existingVersion == -1 {
		// Set unconditionally.
		resp, err = s.getGlobal().Set(shardFilePath(keyspace, shard), string(data), 0 /* ttl */)
	} else {
		resp, err = s.getGlobal().CompareAndSwap(shardFilePath(keyspace, shard),
			string(data), 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
	}
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}
	return int64(resp.Node.ModifiedIndex), nil
}

// GetShard implements topo.Server.
func (s *Server) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	resp, err := s.getGlobal().Get(shardFilePath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, 0, convertError(err)
	}
	if resp.Node == nil {
		return nil, 0, ErrBadResponse
	}

	value := &topodatapb.Shard{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, 0, fmt.Errorf("bad shard data (%v): %q", err, resp.Node.Value)
	}

	return value, int64(resp.Node.ModifiedIndex), nil
}

// GetShardNames implements topo.Server.
func (s *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := s.getGlobal().Get(shardsDirPath(keyspace), true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	return getNodeNames(resp)
}

// DeleteShard implements topo.Server.
func (s *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	_, err := s.getGlobal().Delete(shardDirPath(keyspace, shard), true /* recursive */)
	if err != nil {
		return convertError(err)
	}
	return nil
}
