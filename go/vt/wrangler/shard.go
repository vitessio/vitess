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

package wrangler

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// shard related methods for Wrangler

// UpdateSrvKeyspacePartitions changes the SrvKeyspaceGraph
// for a shard.  It updates serving graph
//
// This takes the keyspace lock as to not interfere with resharding operations.
func (wr *Wrangler) UpdateSrvKeyspacePartitions(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, cells []string, remove bool) (err error) {
	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "UpdateSrvKeyspacePartitions")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	if remove {
		return wr.ts.DeleteSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{si}, tabletType, cells)
	}
	return wr.ts.AddSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{si}, tabletType, cells)
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard.
func (wr *Wrangler) DeleteShard(ctx context.Context, keyspace, shard string, recursive, evenIfServing bool) error {
	// Read the Shard object. If it's not there, try to clean up
	// the topology anyway.
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Infof("Shard %v/%v doesn't seem to exist, cleaning up any potential leftover", keyspace, shard)
			return wr.ts.DeleteShard(ctx, keyspace, shard)
		}
		return err
	}

	servingCells, err := wr.ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		return err
	}
	// Check the Serving map for the shard, we don't want to
	// remove a serving shard if not absolutely sure.
	if !evenIfServing && len(servingCells) > 0 {
		return fmt.Errorf("shard %v/%v is still serving, cannot delete it, use even_if_serving flag if needed", keyspace, shard)
	}

	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		return err
	}

	// Go through all the cells.
	for _, cell := range cells {
		var aliases []*topodatapb.TabletAlias

		// Get the ShardReplication object for that cell. Try
		// to find all tablets that may belong to our shard.
		sri, err := wr.ts.GetShardReplication(ctx, cell, keyspace, shard)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// No ShardReplication object. It means the
			// topo is inconsistent. Let's read all the
			// tablets for that cell, and if we find any
			// in our keyspace / shard, either abort or
			// try to delete them.
			aliases, err = wr.ts.GetTabletAliasesByCell(ctx, cell)
			if err != nil {
				return fmt.Errorf("GetTabletsByCell(%v) failed: %v", cell, err)
			}
		case err == nil:
			// We found a ShardReplication object. We
			// trust it to have all tablet records.
			aliases = make([]*topodatapb.TabletAlias, len(sri.Nodes))
			for i, n := range sri.Nodes {
				aliases[i] = n.TabletAlias
			}
		default:
			return fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
		}

		// Get the corresponding Tablet records. Note
		// GetTabletMap ignores ErrNoNode, and it's good for
		// our purpose, it means a tablet was deleted but is
		// still referenced.
		tabletMap, err := wr.ts.GetTabletMap(ctx, aliases)
		if err != nil {
			return fmt.Errorf("GetTabletMap() failed: %v", err)
		}

		// Remove the tablets that don't belong to our
		// keyspace/shard from the map.
		for a, ti := range tabletMap {
			if ti.Keyspace != keyspace || ti.Shard != shard {
				delete(tabletMap, a)
			}
		}

		// Now see if we need to DeleteTablet, and if we can, do it.
		if len(tabletMap) > 0 {
			if !recursive {
				return fmt.Errorf("shard %v/%v still has %v tablets in cell %v; use -recursive or remove them manually", keyspace, shard, len(tabletMap), cell)
			}

			wr.Logger().Infof("Deleting all tablets in shard %v/%v cell %v", keyspace, shard, cell)
			for tabletAlias, tabletInfo := range tabletMap {
				// We don't care about scrapping or updating the replication graph,
				// because we're about to delete the entire replication graph.
				wr.Logger().Infof("Deleting tablet %v", tabletAlias)
				if err := wr.TopoServer().DeleteTablet(ctx, tabletInfo.Alias); err != nil && !topo.IsErrType(err, topo.NoNode) {
					// We don't want to continue if a DeleteTablet fails for
					// any good reason (other than missing tablet, in which
					// case it's just a topology server inconsistency we can
					// ignore). If we continue and delete the replication
					// graph, the tablet record will be orphaned, since
					// we'll no longer know it belongs to this shard.
					//
					// If the problem is temporary, or resolved externally, re-running
					// DeleteShard will skip over tablets that were already deleted.
					return fmt.Errorf("can't delete tablet %v: %v", tabletAlias, err)
				}
			}
		}
	}

	// Try to remove the replication graph and serving graph in each cell,
	// regardless of its existence.
	for _, cell := range cells {
		if err := wr.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && !topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}
	}

	return wr.ts.DeleteShard(ctx, keyspace, shard)
}

// SourceShardDelete will delete a SourceShard inside a shard, by index.
//
// This takes the keyspace lock as not to interfere with resharding operations.
func (wr *Wrangler) SourceShardDelete(ctx context.Context, keyspace, shard string, uid int32) (err error) {
	resp, err := wr.VtctldServer().SourceShardDelete(ctx, &vtctldatapb.SourceShardDeleteRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Uid:      uid,
	})
	if err != nil {
		return err
	}

	if resp.Shard == nil {
		return fmt.Errorf("no SourceShard with uid %v", uid)
	}

	return nil
}

// SourceShardAdd will add a new SourceShard inside a shard.
func (wr *Wrangler) SourceShardAdd(ctx context.Context, keyspace, shard string, uid int32, skeyspace, sshard string, keyRange *topodatapb.KeyRange, tables []string) (err error) {
	resp, err := wr.VtctldServer().SourceShardAdd(ctx, &vtctldatapb.SourceShardAddRequest{
		Keyspace:       keyspace,
		Shard:          shard,
		Uid:            uid,
		SourceKeyspace: skeyspace,
		SourceShard:    sshard,
		KeyRange:       keyRange,
		Tables:         tables,
	})
	if err != nil {
		return err
	}

	if resp.Shard == nil {
		return fmt.Errorf("uid %v is already in use", uid)
	}

	return nil
}
