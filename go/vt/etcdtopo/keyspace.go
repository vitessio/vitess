// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateKeyspace implements topo.Server.
func (s *Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	global := s.getGlobal()

	if _, err = global.Create(keyspaceFilePath(keyspace), string(data), 0 /* ttl */); err != nil {
		return convertError(err)
	}
	return nil
}

// UpdateKeyspace implements topo.Server.
func (s *Server) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}

	var resp *etcd.Response
	if existingVersion == -1 {
		// Set unconditionally.
		resp, err = s.getGlobal().Set(keyspaceFilePath(keyspace), string(data), 0 /* ttl */)
	} else {
		resp, err = s.getGlobal().CompareAndSwap(keyspaceFilePath(keyspace),
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

// GetKeyspace implements topo.Server.
func (s *Server) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	resp, err := s.getGlobal().Get(keyspaceFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, 0, convertError(err)
	}
	if resp.Node == nil {
		return nil, 0, ErrBadResponse
	}

	value := &topodatapb.Keyspace{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, 0, fmt.Errorf("bad keyspace data (%v): %q", err, resp.Node.Value)
	}

	return value, int64(resp.Node.ModifiedIndex), nil
}

// GetKeyspaces implements topo.Server.
func (s *Server) GetKeyspaces(ctx context.Context) ([]string, error) {
	resp, err := s.getGlobal().Get(keyspacesDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}
	return getNodeNames(resp)
}

// DeleteKeyspaceShards implements topo.Server.
func (s *Server) DeleteKeyspaceShards(ctx context.Context, keyspace string) error {
	shards, err := s.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	global := s.getGlobal()
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			_, err := global.Delete(shardDirPath(keyspace, shard), true /* recursive */)
			rec.RecordError(convertError(err))
		}(shard)
	}
	wg.Wait()

	if err = rec.Error(); err != nil {
		return err
	}
	return nil
}

// DeleteKeyspace implements topo.Server.
func (s *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	_, err := s.getGlobal().Delete(keyspaceDirPath(keyspace), true /* recursive */)
	if err != nil {
		return convertError(err)
	}
	return nil
}
