// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
)

// UpdateShardReplicationFields implements topo.Server.
func (s *Server) UpdateShardReplicationFields(cell, keyspace, shard string, updateFunc func(*topo.ShardReplication) error) error {
	var sri *topo.ShardReplicationInfo
	var version int64
	var err error

	for {
		if sri, version, err = s.getShardReplication(cell, keyspace, shard); err != nil {
			if err == topo.ErrNoNode {
				// Pass an empty struct to the update func, as specified in topo.Server.
				sri = topo.NewShardReplicationInfo(&topo.ShardReplication{}, cell, keyspace, shard)
				version = -1
			} else {
				return err
			}
		}
		if err = updateFunc(sri.ShardReplication); err != nil {
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

	data := jscfg.ToJson(sri.ShardReplication)
	resp, err := cell.CompareAndSwap(shardReplicationFilePath(sri.Keyspace(), sri.Shard()),
		data, 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
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

	data := jscfg.ToJson(sri.ShardReplication)
	resp, err := cell.Create(shardReplicationFilePath(sri.Keyspace(), sri.Shard()),
		data, 0 /* ttl */)
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}

	return int64(resp.Node.ModifiedIndex), nil
}

// GetShardReplication implements topo.Server.
func (s *Server) GetShardReplication(cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
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

	value := &topo.ShardReplication{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, -1, fmt.Errorf("bad shard replication data (%v): %q", err, resp.Node.Value)
	}

	return topo.NewShardReplicationInfo(value, cellName, keyspace, shard), int64(resp.Node.ModifiedIndex), nil
}

// DeleteShardReplication implements topo.Server.
func (s *Server) DeleteShardReplication(cellName, keyspace, shard string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(shardReplicationDirPath(keyspace, shard), true /* recursive */)
	return convertError(err)
}
