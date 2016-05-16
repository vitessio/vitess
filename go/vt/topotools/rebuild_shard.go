// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"sync"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RebuildShard updates the SrvShard objects and underlying serving graph.
//
// Re-read from TopologyServer to make sure we are using the side
// effects of all actions.
//
// This function will start each cell over from the beginning on ErrBadVersion,
// so it doesn't need a lock on the shard.
func RebuildShard(ctx context.Context, log logutil.Logger, ts topo.Server, keyspace, shard string, cells []string) (*topo.ShardInfo, error) {
	log.Infof("RebuildShard %v/%v", keyspace, shard)

	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topotools.RebuildShard")
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, err
	}

	// rebuild all cells in parallel
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range shardInfo.Cells {
		// skip this cell if we shouldn't rebuild it
		if !topo.InCellList(cell, cells) {
			continue
		}

		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			rec.RecordError(rebuildCellSrvShard(ctx, log, ts, shardInfo, cell))
		}(cell)
	}
	wg.Wait()

	return shardInfo, rec.Error()
}

// rebuildCellSrvShard computes and writes the serving graph data to a
// single cell
func rebuildCellSrvShard(ctx context.Context, log logutil.Logger, ts topo.Server, si *topo.ShardInfo, cell string) (err error) {
	log.Infof("rebuildCellSrvShard %v/%v in cell %v", si.Keyspace(), si.ShardName(), cell)
	return UpdateSrvShard(ctx, ts, cell, si)
}

// UpdateSrvShard creates the SrvShard object based on the global ShardInfo,
// and writes it to the given cell.
func UpdateSrvShard(ctx context.Context, ts topo.Server, cell string, si *topo.ShardInfo) error {
	srvShard := &topodatapb.SrvShard{
		Name:     si.ShardName(),
		KeyRange: si.KeyRange,
	}
	if si.MasterAlias != nil {
		srvShard.MasterCell = si.MasterAlias.Cell
	}
	return ts.UpdateSrvShard(ctx, cell, si.Keyspace(), si.ShardName(), srvShard)
}

// UpdateAllSrvShards calls UpdateSrvShard for all cells concurrently.
func UpdateAllSrvShards(ctx context.Context, ts topo.Server, si *topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	errs := concurrency.AllErrorRecorder{}

	for _, cell := range si.Cells {
		wg.Add(1)
		go func(cell string) {
			errs.RecordError(UpdateSrvShard(ctx, ts, cell, si))
			wg.Done()
		}(cell)
	}
	wg.Wait()
	return errs.Error()
}
