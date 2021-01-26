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
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

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

// IsMasterTablet is a helper function to determine whether the current tablet
// is a master before we allow its tablet record to be deleted. The canonical
// way to determine the only true master in a shard is to list all the tablets
// and find the one with the highest MasterTermStartTime among the ones that
// claim to be master.
//
// We err on the side of caution here, i.e. we should never return false for
// a true master tablet, but it is okay to return true for a tablet that isn't
// the true master. This can occur if someone issues a DeleteTablet while
// the system is in transition (a reparenting event is in progress and parts of
// the topo have not yet been updated).
func IsMasterTablet(ctx context.Context, ts *topo.Server, ti *topo.TabletInfo) (bool, error) {
	// Tablet record claims to be non-master, we believe it
	if ti.Type != topodatapb.TabletType_MASTER {
		return false, nil
	}

	si, err := ts.GetShard(ctx, ti.Keyspace, ti.Shard)
	if err != nil {
		// strictly speaking it isn't correct to return false here, the tablet status is unknown
		return false, err
	}

	// Tablet record claims to be master, and shard record matches
	if topoproto.TabletAliasEqual(si.MasterAlias, ti.Tablet.Alias) {
		return true, nil
	}

	// Shard record has another tablet as master, so check MasterTermStartTime
	// If tablet record's MasterTermStartTime is later than the one in the shard
	// record, then the tablet is master
	tabletMTST := ti.GetMasterTermStartTime()
	shardMTST := si.GetMasterTermStartTime()

	return tabletMTST.After(shardMTST), nil
}
