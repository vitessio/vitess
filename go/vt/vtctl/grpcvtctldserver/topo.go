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
