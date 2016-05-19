// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

// RebuildKeyspaceGraph rebuilds the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(ctx context.Context, keyspace string, cells []string) error {
	return topotools.RebuildKeyspace(ctx, wr.logger, wr.ts, keyspace, cells)
}

func strInList(sl []string, s string) bool {
	for _, x := range sl {
		if x == s {
			return true
		}
	}
	return false
}

// RebuildReplicationGraph is a quick and dirty tool to resurrect the TopologyServer data from the
// canonical data stored in the tablet nodes.
//
// cells: local vt cells to scan for all tablets
// keyspaces: list of keyspaces to rebuild
func (wr *Wrangler) RebuildReplicationGraph(ctx context.Context, cells []string, keyspaces []string) error {
	if cells == nil || len(cells) == 0 {
		return fmt.Errorf("must specify cells to rebuild replication graph")
	}
	if keyspaces == nil || len(keyspaces) == 0 {
		return fmt.Errorf("must specify keyspaces to rebuild replication graph")
	}

	allTablets := make([]*topo.TabletInfo, 0, 1024)
	for _, cell := range cells {
		tablets, err := topotools.GetAllTablets(ctx, wr.ts, cell)
		if err != nil {
			return err
		}
		allTablets = append(allTablets, tablets...)
	}

	for _, keyspace := range keyspaces {
		wr.logger.Infof("delete keyspace shards: %v", keyspace)
		if err := wr.ts.DeleteKeyspaceShards(ctx, keyspace); err != nil {
			return err
		}
	}

	keyspacesToRebuild := make(map[string]bool)
	shardsCreated := make(map[string]bool)
	hasErr := false
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, ti := range allTablets {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			defer wg.Done()
			if !strInList(keyspaces, ti.Keyspace) {
				return
			}
			mu.Lock()
			keyspacesToRebuild[ti.Keyspace] = true
			shardPath := ti.Keyspace + "/" + ti.Shard
			if !shardsCreated[shardPath] {
				if err := wr.ts.CreateShard(ctx, ti.Keyspace, ti.Shard); err != nil && err != topo.ErrNodeExists {
					wr.logger.Warningf("failed re-creating shard %v: %v", shardPath, err)
					hasErr = true
				} else {
					shardsCreated[shardPath] = true
				}
			}
			mu.Unlock()
			err := topo.UpdateTabletReplicationData(ctx, wr.ts, ti.Tablet)
			if err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				wr.logger.Warningf("failed updating replication data: %v", err)
			}
		}(ti)
	}
	wg.Wait()

	for keyspace := range keyspacesToRebuild {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := wr.RebuildKeyspaceGraph(ctx, keyspace, nil); err != nil {
				mu.Lock()
				hasErr = true
				mu.Unlock()
				wr.logger.Warningf("RebuildKeyspaceGraph(%v) failed: %v", keyspace, err)
				return
			}
		}(keyspace)
	}
	wg.Wait()

	if hasErr {
		return fmt.Errorf("some errors occurred rebuilding replication graph, consult log")
	}
	return nil
}
