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

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateShardReplicationFields implements topo.Server.
func (s *Server) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, updateFunc func(*topodatapb.ShardReplication) error) error {
	var sri *topo.ShardReplicationInfo
	var version int64
	var err error

	for {
		if sri, version, err = s.getShardReplication(cell, keyspace, shard); err != nil {
			if err == topo.ErrNoNode {
				// Pass an empty struct to the update func, as specified in topo.Server.
				sri = topo.NewShardReplicationInfo(&topodatapb.ShardReplication{}, cell, keyspace, shard)
				version = -1
			} else {
				return err
			}
		}
		if err = updateFunc(sri.ShardReplication); err != nil {
			if err == topo.ErrNoUpdateNeeded {
				return nil
			}
			return err
		}
		if version == -1 {
			if _, err = s.createShardReplication(sri); err != topo.ErrNodeExists {
				return err
			}
		} else {
			if _, err = s.updateShardReplication(sri, version); err != topo.ErrBadVersion {
				return err
			}
		}
	}
}

func (s *Server) updateShardReplication(sri *topo.ShardReplicationInfo, existingVersion int64) (int64, error) {
	cell, err := s.getCell(sri.Cell())
	if err != nil {
		return -1, err
	}

	data, err := json.MarshalIndent(sri.ShardReplication, "", "  ")
	if err != nil {
		return -1, err
	}

	resp, err := cell.CompareAndSwap(shardReplicationFilePath(sri.Keyspace(), sri.Shard()),
		string(data), 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}

	return int64(resp.Node.ModifiedIndex), nil
}

func (s *Server) createShardReplication(sri *topo.ShardReplicationInfo) (int64, error) {
	cell, err := s.getCell(sri.Cell())
	if err != nil {
		return -1, err
	}

	data, err := json.MarshalIndent(sri.ShardReplication, "", "  ")
	if err != nil {
		return -1, err
	}
	resp, err := cell.Create(shardReplicationFilePath(sri.Keyspace(), sri.Shard()),
		string(data), 0 /* ttl */)
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}

	return int64(resp.Node.ModifiedIndex), nil
}

// GetShardReplication implements topo.Server.
func (s *Server) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	sri, _, err := s.getShardReplication(cell, keyspace, shard)
	return sri, err
}

func (s *Server) getShardReplication(cellName, keyspace, shard string) (*topo.ShardReplicationInfo, int64, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, -1, err
	}

	resp, err := cell.Get(shardReplicationFilePath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, -1, convertError(err)
	}
	if resp.Node == nil {
		return nil, -1, ErrBadResponse
	}

	value := &topodatapb.ShardReplication{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, -1, fmt.Errorf("bad shard replication data (%v): %q", err, resp.Node.Value)
	}

	return topo.NewShardReplicationInfo(value, cellName, keyspace, shard), int64(resp.Node.ModifiedIndex), nil
}

// DeleteShardReplication implements topo.Server.
func (s *Server) DeleteShardReplication(ctx context.Context, cellName, keyspace, shard string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(shardReplicationDirPath(keyspace, shard), true /* recursive */)
	return convertError(err)
}

// DeleteKeyspaceReplication implements topo.Server.
func (s *Server) DeleteKeyspaceReplication(ctx context.Context, cellName, keyspace string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(keyspaceReplicationDirPath(keyspace), true /* recursive */)
	return convertError(err)
}
