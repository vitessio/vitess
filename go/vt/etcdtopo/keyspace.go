// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/events"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateKeyspace implements topo.Server.
func (s *Server) CreateKeyspace(ctx context.Context, keyspace string, value *pb.Keyspace) error {
	data := jscfg.ToJSON(value)
	global := s.getGlobal()

	resp, err := global.Create(keyspaceFilePath(keyspace), data, 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	if err := initLockFile(global, keyspaceDirPath(keyspace)); err != nil {
		return err
	}

	// We don't return ErrBadResponse in this case because the Create() suceeeded
	// and we don't really need the version to satisfy our contract - we're only
	// logging it.
	version := int64(-1)
	if resp.Node != nil {
		version = int64(resp.Node.ModifiedIndex)
	}
	event.Dispatch(&events.KeyspaceChange{
		KeyspaceInfo: *topo.NewKeyspaceInfo(keyspace, value, version),
		Status:       "created",
	})
	return nil
}

// UpdateKeyspace implements topo.Server.
func (s *Server) UpdateKeyspace(ctx context.Context, ki *topo.KeyspaceInfo, existingVersion int64) (int64, error) {
	data := jscfg.ToJSON(ki.Keyspace)

	resp, err := s.getGlobal().CompareAndSwap(keyspaceFilePath(ki.KeyspaceName()),
		data, 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceInfo: *ki,
		Status:       "updated",
	})
	return int64(resp.Node.ModifiedIndex), nil
}

// GetKeyspace implements topo.Server.
func (s *Server) GetKeyspace(ctx context.Context, keyspace string) (*topo.KeyspaceInfo, error) {
	resp, err := s.getGlobal().Get(keyspaceFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := &pb.Keyspace{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad keyspace data (%v): %q", err, resp.Node.Value)
	}

	return topo.NewKeyspaceInfo(keyspace, value, int64(resp.Node.ModifiedIndex)), nil
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

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceInfo: *topo.NewKeyspaceInfo(keyspace, nil, -1),
		Status:       "deleted all shards",
	})
	return nil
}

// DeleteKeyspace implements topo.Server.
func (s *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	_, err := s.getGlobal().Delete(keyspaceDirPath(keyspace), true /* recursive */)
	if err != nil {
		return convertError(err)
	}

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceInfo: *topo.NewKeyspaceInfo(keyspace, nil, -1),
		Status:       "deleted",
	})
	return nil
}
