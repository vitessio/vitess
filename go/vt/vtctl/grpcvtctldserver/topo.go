/*
Copyright 2021 The Vitess Authors.

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

package grpcvtctldserver

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

func deleteShard(ctx context.Context, ts *topo.Server, keyspace string, shard string, recursive bool, evenIfServing bool) error {
	// Read the Shard object. If it's not in the topo, try to clean up the topo
	// anyway.
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			log.Infof("Shard %v/%v doesn't seem to exist; cleaning up any potential leftover topo data", keyspace, shard)

			return ts.DeleteShard(ctx, keyspace, shard)
		}

		return err
	}

	servingCells, err := ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		return err
	}

	// We never want to remove a potentially serving shard unless someone
	// explicitly requested it.
	if len(servingCells) > 0 && !evenIfServing {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v is still serving; cannot delete it; use EvenIfServing = true to delete anyway", keyspace, shard)
	}

	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return err
	}

	for _, cell := range cells {
		if err := deleteShardCell(ctx, ts, keyspace, shard, cell, recursive); err != nil {
			return err
		}
	}

	// Try to remove the replication and serving graphs from each cell,
	// regardless of whether they exist.
	for _, cell := range cells {
		if err := ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && !topo.IsErrType(err, topo.NoNode) {
			log.Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %w", cell, keyspace, shard, err)
		}
	}

	return ts.DeleteShard(ctx, keyspace, shard)
}

// deleteShardCell is the per-cell helper function for deleteShard, and is
// distinct from the RemoveShardCell rpc. Despite having similar names, they are
// **not** the same!
func deleteShardCell(ctx context.Context, ts *topo.Server, keyspace string, shard string, cell string, recursive bool) error {
	var aliases []*topodatapb.TabletAlias

	// Get the ShardReplication object for the cell. Collect all the tablets
	// that belong to the shard.
	sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
	switch {
	case topo.IsErrType(err, topo.NoNode):
		// No ShardReplication object means that the topo is inconsistent.
		// Therefore we read all the tablets for that cell, and if we find any
		// in our shard, we'll either abort or try to delete them, depending on
		// whether recursive=true.
		aliases, err = ts.GetTabletsByCell(ctx, cell)
		if err != nil {
			return fmt.Errorf("GetTabletsByCell(%v) failed: %w", cell, err)
		}
	case err == nil:
		// If a ShardReplication object exists, we trust it to have all the
		// tablet records for the shard in that cell.
		aliases = make([]*topodatapb.TabletAlias, len(sri.Nodes))

		for i, node := range sri.Nodes {
			aliases[i] = node.TabletAlias
		}
	default:
		return fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %w", cell, keyspace, shard, err)
	}

	// Get all the tablet records for the aliases we've collected. Note that
	// GetTabletMap ignores ErrNoNode, which is convenient for our purpose; it
	// means a tablet was deleted but is still referenced.
	tabletMap, err := ts.GetTabletMap(ctx, aliases)
	if err != nil {
		return fmt.Errorf("GetTabletMap() failed: %w", err)
	}

	// In the case where no ShardReplication object exists, we collect the
	// aliases of every tablet in the cell, so we'll need to filter
	// out anything not in our shard.
	for alias, ti := range tabletMap {
		if !(ti.Keyspace == keyspace && ti.Shard == shard) {
			delete(tabletMap, alias)
		}
	}

	// If there are any tablets in the shard in the cell, delete them.
	if len(tabletMap) > 0 {
		if !recursive {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Shard %v/%v still hase %v tablets in cell %v; use Recursive = true or remove them manually", keyspace, shard, len(tabletMap), cell)
		}

		log.Infof("Deleting all %d tablets in shard %v/%v cell %v", len(tabletMap), keyspace, shard, cell)
		for alias, tablet := range tabletMap {
			// We don't care about updating the ShardReplication object, because
			// later we're going to delete the entire object.
			log.Infof("Deleting tablet %v", alias)
			if err := ts.DeleteTablet(ctx, tablet.Alias); err != nil && !topo.IsErrType(err, topo.NoNode) {
				// We don't want to continue if a DeleteTablet fails for any
				// reason other than a missing tablet (in which case it's just
				// topo server inconsistency, which we can ignore). If we were
				// to continue and delete the replication graph, the tablet
				// record would become orphaned, since we'd no longer know that
				// it belongs to this shard.
				//
				// If the problem is temporary, or resolved externally,
				// re-running DeleteShard will skip over tablets that were
				// already deleted.
				return fmt.Errorf("cannot delete tablet %v: %w", alias, err)
			}
		}
	}

	return nil
}

func deleteTablet(ctx context.Context, ts *topo.Server, alias *topodatapb.TabletAlias, allowPrimary bool) (err error) {
	tablet, err := ts.GetTablet(ctx, alias)
	if err != nil {
		return err
	}

	isPrimary, err := topotools.IsPrimaryTablet(ctx, ts, tablet)
	if err != nil {
		return err
	}

	if isPrimary && !allowPrimary {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot delete tablet %v as it is a master, pass AllowPrimary = true", topoproto.TabletAliasString(alias))
	}

	// Update the Shard object if the master was scrapped. We do this before
	// calling DeleteTablet so that the operation can be retried in case of
	// failure.
	if isPrimary {
		lockCtx, unlock, lockErr := ts.LockShard(ctx, tablet.Keyspace, tablet.Shard, fmt.Sprintf("DeleteTablet(%v)", topoproto.TabletAliasString(alias)))
		if lockErr != nil {
			return lockErr
		}

		defer unlock(&err)

		if _, err := ts.UpdateShardFields(lockCtx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
			if !topoproto.TabletAliasEqual(si.MasterAlias, alias) {
				log.Warningf(
					"Deleting master %v from shard %v/%v but master in Shard object was %v",
					topoproto.TabletAliasString(alias), tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(si.MasterAlias),
				)

				return topo.NewError(topo.NoUpdateNeeded, si.Keyspace()+"/"+si.ShardName())
			}

			si.MasterAlias = nil

			return nil
		}); err != nil {
			return err
		}
	}

	// Remove the tablet record and its replication graph entry.
	if err := topotools.DeleteTablet(ctx, ts, tablet.Tablet); err != nil {
		return err
	}

	// Return any error from unlocking the keyspace.
	return err
}

func removeShardCell(ctx context.Context, ts *topo.Server, cell string, keyspace string, shardName string, recursive bool, force bool) error {
	shard, err := ts.GetShard(ctx, keyspace, shardName)
	if err != nil {
		return err
	}

	servingCells, err := ts.GetShardServingCells(ctx, shard)
	if err != nil {
		return err
	}

	if !topo.InCellList(cell, servingCells) {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v does not have serving cell %v", keyspace, shardName, cell)
	}

	if shard.MasterAlias != nil && shard.MasterAlias.Cell == cell {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot remove cell %v; shard master %v is in cell", cell, topoproto.TabletAliasString(shard.MasterAlias))
	}

	replication, err := ts.GetShardReplication(ctx, cell, keyspace, shardName)
	switch {
	case err == nil:
		// We have tablets in the shard in this cell.
		if recursive {
			log.Infof("Deleting all tablets in cell %v in shard %v/%v", cell, keyspace, shardName)
			for _, node := range replication.Nodes {
				// We don't care about scraping our updating the replication
				// graph, because we're about to delete the entire replication
				// graph.
				log.Infof("Deleting tablet %v", topoproto.TabletAliasString(node.TabletAlias))
				if err := ts.DeleteTablet(ctx, node.TabletAlias); err != nil && !topo.IsErrType(err, topo.NoNode) {
					return fmt.Errorf("cannot delete tablet %v: %w", topoproto.TabletAliasString(node.TabletAlias), err)
				}
			}
		} else if len(replication.Nodes) > 0 {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cell %v has %v possible tablets in replication graph", cell, len(replication.Nodes))
		}

		// Remove the empty replication graph.
		if err := ts.DeleteShardReplication(ctx, cell, keyspace, shardName); err != nil && !topo.IsErrType(err, topo.NoNode) {
			return fmt.Errorf("error deleting ShardReplication object in cell %v: %w", cell, err)
		}
	case topo.IsErrType(err, topo.NoNode):
		// No ShardReplication object. This is the expected path when there are
		// no tablets in the shard in that cell.
		err = nil
	default:
		// If we can't get the replication object out of the local topo, we
		// assume the topo server is down in that cell, so we'll only continue
		// if Force was specified.
		if !force {
			return err
		}

		log.Warningf("Cannot get ShardReplication from cell %v; assuming cell topo server is down and forcing removal", cell)
	}

	// Finally, update the shard.

	log.Infof("Removing cell %v from SrvKeyspace %v/%v", cell, keyspace, shardName)

	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "Locking keyspace to remove shard from SrvKeyspace")
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	if err := ts.DeleteSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shard}, topodatapb.TabletType_RDONLY, []string{cell}); err != nil {
		return err
	}

	if err := ts.DeleteSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shard}, topodatapb.TabletType_REPLICA, []string{cell}); err != nil {
		return err
	}

	if err := ts.DeleteSrvKeyspacePartitions(ctx, keyspace, []*topo.ShardInfo{shard}, topodatapb.TabletType_MASTER, []string{cell}); err != nil {
		return err
	}

	return err
}
