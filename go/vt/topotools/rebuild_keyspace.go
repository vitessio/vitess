/*
Copyright 2019 The Vitess Authors.

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

package topotools

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// RebuildKeyspace rebuilds the serving graph data while locking out other changes.
func RebuildKeyspace(ctx context.Context, log logutil.Logger, ts *topo.Server, keyspace string, cells []string, allowPartial bool) (err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "RebuildKeyspace")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	return RebuildKeyspaceLocked(ctx, log, ts, keyspace, cells, allowPartial)
}

// RebuildKeyspaceLocked should only be used with an action lock on the keyspace
// - otherwise the consistency of the serving graph data can't be
// guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func RebuildKeyspaceLocked(ctx context.Context, log logutil.Logger, ts *topo.Server, keyspace string, cells []string, allowPartial bool) error {
	if err := topo.CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return err
	}

	ki, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	shards, err := ts.FindAllShardsInKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	// This is safe to rebuild as long there are not srvKeyspaces with tablet controls set.
	// Build the list of cells to work on: we get the union
	// of all the Cells of all the Shards, limited to the provided cells.
	//
	// srvKeyspaceMap is a map:
	//   key: cell
	//   value: topo.SrvKeyspace object being built
	srvKeyspaceMap := make(map[string]*topodatapb.SrvKeyspace)
	for _, cell := range cells {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		switch {
		case err == nil:
			for _, partition := range srvKeyspace.GetPartitions() {
				for _, shardTabletControl := range partition.GetShardTabletControls() {
					if shardTabletControl.QueryServiceDisabled {
						return fmt.Errorf("can't rebuild serving keyspace while a migration is on going. TabletControls is set for partition %v", partition)
					}
				}
			}
		case topo.IsErrType(err, topo.NoNode):
			// NOOP
		default:
			return err
		}
		srvKeyspaceMap[cell] = &topodatapb.SrvKeyspace{
			ServedFrom: ki.ComputeCellServedFrom(cell),
		}
		srvKeyspaceMap[cell].ThrottlerConfig = ki.ThrottlerConfig
	}

	servedTypes := []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}

	// for each entry in the srvKeyspaceMap map, we do the following:
	// - get the Shard structures for each shard / cell
	// - if not present, build an empty one from global Shard
	// - sort the shards in the list by range
	// - check the ranges are compatible (no hole, covers everything)
	for cell, srvKeyspace := range srvKeyspaceMap {
		for _, si := range shards {
			// We rebuild keyspace iff shard primary is in a serving state.
			if !si.GetIsPrimaryServing() {
				continue
			}
			// for each type this shard is supposed to serve,
			// add it to srvKeyspace.Partitions
			for _, tabletType := range servedTypes {
				partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
				if partition == nil {
					partition = &topodatapb.SrvKeyspace_KeyspacePartition{
						ServedType: tabletType,
					}
					srvKeyspace.Partitions = append(srvKeyspace.Partitions, partition)
				}
				partition.ShardReferences = append(partition.ShardReferences, &topodatapb.ShardReference{
					Name:     si.ShardName(),
					KeyRange: si.KeyRange,
				})
			}
		}

		if !(ki.KeyspaceType == topodatapb.KeyspaceType_SNAPSHOT && allowPartial) {
			// skip this check for SNAPSHOT keyspaces so that incomplete keyspaces can still serve
			if err := topo.OrderAndCheckPartitions(cell, srvKeyspace); err != nil {
				return err
			}
		}

	}
	// And then finally save the keyspace objects, in parallel.
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for cell, srvKeyspace := range srvKeyspaceMap {
		wg.Add(1)
		go func(cell string, srvKeyspace *topodatapb.SrvKeyspace) {
			defer wg.Done()
			if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
				rec.RecordError(fmt.Errorf("writing serving data failed: %v", err))
			}
		}(cell, srvKeyspace)
	}
	wg.Wait()
	return rec.Error()
}
