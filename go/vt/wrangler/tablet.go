// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Tablet related methods for wrangler

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a slave type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// allowUpdate is true, and a tablet with the same ID exists, just update it.
// If a tablet already exists, and has a different keyspace / shard,
// allowDifferentShard must be set to accept the update.
// If a tablet is created as master, and there is already a different
// master in the shard, allowMasterOverride must be set.
func (wr *Wrangler) InitTablet(ctx context.Context, tablet *pb.Tablet, allowMasterOverride, allowDifferentShard, createShardAndKeyspace, allowUpdate bool) error {
	if err := topo.TabletComplete(tablet); err != nil {
		return err
	}

	// get the shard, possibly creating it
	var err error
	var si *topo.ShardInfo

	if createShardAndKeyspace {
		// create the parent keyspace and shard if needed
		si, err = topotools.GetOrCreateShard(ctx, wr.ts, tablet.Keyspace, tablet.Shard)
	} else {
		si, err = wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
		if err == topo.ErrNoNode {
			return fmt.Errorf("missing parent shard, use -parent option to create it, or CreateKeyspace / CreateShard")
		}
	}

	// get the shard, checks a couple things
	if err != nil {
		return fmt.Errorf("cannot get (or create) shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
	}
	if !key.KeyRangeEqual(si.KeyRange, tablet.KeyRange) {
		return fmt.Errorf("shard %v/%v has a different KeyRange: %v != %v", tablet.Keyspace, tablet.Shard, si.KeyRange, tablet.KeyRange)
	}
	if tablet.Type == pb.TabletType_MASTER && si.HasMaster() && !topoproto.TabletAliasEqual(si.MasterAlias, tablet.Alias) && !allowMasterOverride {
		return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v, use allow_master_override flag", topoproto.TabletAliasString(si.MasterAlias), tablet.Keyspace, tablet.Shard)
	}

	// update the shard record if needed
	if err := wr.updateShardCellsAndMaster(ctx, si, tablet.Alias, tablet.Type, allowMasterOverride); err != nil {
		return err
	}

	err = wr.ts.CreateTablet(ctx, tablet)
	if err == topo.ErrNodeExists && allowUpdate {
		// Try to update then
		oldTablet, err := wr.ts.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return fmt.Errorf("failed reading existing tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}

		// Check we have the same keyspace / shard, and if not,
		// require the allowDifferentShard flag.
		if (oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard) && !allowDifferentShard {
			return fmt.Errorf("old tablet has shard %v/%v, cannot override with shard %v/%v unless allow_different_shard is set", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}

		*(oldTablet.Tablet) = *tablet
		if err := wr.ts.UpdateTablet(ctx, oldTablet); err != nil {
			return fmt.Errorf("failed updating tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}
	}

	// now update the replication data
	if err := topo.UpdateTabletReplicationData(ctx, wr.ts, tablet); err != nil {
		return fmt.Errorf("failed updating tablet replication data for %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
	}
	return nil
}

// DeleteTablet removes a tablet from a shard.
// - if allowMaster is set, we can Delete a master tablet (and clear
// its record from the Shard record if it was the master).
// - if skipRebuild is set, we do not rebuild the serving graph.
func (wr *Wrangler) DeleteTablet(ctx context.Context, tabletAlias *pb.TabletAlias, allowMaster, skipRebuild bool) error {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.IsInServingGraph()
	wasMaster := ti.Type == pb.TabletType_MASTER
	if wasMaster && !allowMaster {
		return fmt.Errorf("cannot delete tablet %v as it is a master, use allow_master flag", topoproto.TabletAliasString(tabletAlias))
	}

	// remove the record and its replication graph entry
	if err := topotools.DeleteTablet(ctx, wr.ts, ti.Tablet); err != nil {
		return err
	}

	// update the Shard object if the master was scrapped
	if wasMaster {
		actionNode := actionnode.UpdateShard()
		lockPath, err := wr.lockShard(ctx, ti.Keyspace, ti.Shard, actionNode)
		if err != nil {
			return err
		}

		// read the shard with the lock
		si, err := wr.ts.GetShard(ctx, ti.Keyspace, ti.Shard)
		if err != nil {
			return wr.unlockShard(ctx, ti.Keyspace, ti.Shard, actionNode, lockPath, err)
		}

		// update it if the right alias is there
		if topoproto.TabletAliasEqual(si.MasterAlias, tabletAlias) {
			si.MasterAlias = nil

			// write it back
			if err := wr.ts.UpdateShard(ctx, si); err != nil {
				return wr.unlockShard(ctx, ti.Keyspace, ti.Shard, actionNode, lockPath, err)
			}
		} else {
			wr.Logger().Warningf("Deleting master %v from shard %v/%v but master in Shard object was %v", topoproto.TabletAliasString(tabletAlias), ti.Keyspace, ti.Shard, si.MasterAlias)
		}

		// and unlock
		if err := wr.unlockShard(ctx, ti.Keyspace, ti.Shard, actionNode, lockPath, err); err != nil {
			return err
		}
	}

	// and rebuild the original shard if needed
	if !rebuildRequired {
		wr.Logger().Infof("Rebuild not required")
		return nil
	}
	if skipRebuild {
		wr.Logger().Warningf("Rebuild required, but skipping it")
		return nil
	}
	_, err = wr.RebuildShardGraph(ctx, ti.Keyspace, ti.Shard, []string{ti.Alias.Cell})
	return err
}

// ChangeType changes the type of tablet and recompute all necessary derived paths in the
// serving graph. If force is true, it will bypass the RPC action
// system and make the data change directly, and not run the remote
// hooks.
//
// Note we don't update the master record in the Shard here, as we
// can't ChangeType from and out of master anyway.
func (wr *Wrangler) ChangeType(ctx context.Context, tabletAlias *pb.TabletAlias, tabletType pb.TabletType, force bool) error {
	rebuildRequired, cell, keyspace, shard, err := wr.ChangeTypeNoRebuild(ctx, tabletAlias, tabletType, force)
	if err != nil {
		return err
	}
	if rebuildRequired {
		_, err = wr.RebuildShardGraph(ctx, keyspace, shard, []string{cell})
		return err
	}
	return nil
}

// ChangeTypeNoRebuild changes a tablet's type, and returns whether
// there's a shard that should be rebuilt, along with its cell,
// keyspace, and shard. If force is true, it will bypass the RPC action
// system and make the data change directly, and not run the remote
// hooks.
//
// Note we don't update the master record in the Shard here, as we
// can't ChangeType from and out of master anyway.
func (wr *Wrangler) ChangeTypeNoRebuild(ctx context.Context, tabletAlias *pb.TabletAlias, tabletType pb.TabletType, force bool) (rebuildRequired bool, cell, keyspace, shard string, err error) {
	// Load tablet to find keyspace and shard assignment.
	// Don't load after the ChangeType which might have unassigned
	// the tablet.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return false, "", "", "", err
	}

	if force {
		if err := topotools.ChangeType(ctx, wr.ts, tabletAlias, tabletType, nil); err != nil {
			return false, "", "", "", err
		}
	} else {
		if err := wr.tmc.ChangeType(ctx, ti, tabletType); err != nil {
			return false, "", "", "", err
		}
	}

	if !ti.IsInServingGraph() {
		// re-read the tablet, see if we become serving
		ti, err = wr.ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return false, "", "", "", err
		}
		if !ti.IsInServingGraph() {
			return false, "", "", "", nil
		}
	}
	return true, ti.Alias.Cell, ti.Keyspace, ti.Shard, nil

}

// same as ChangeType, but assume we already have the shard lock,
// and do not have the option to force anything.
func (wr *Wrangler) changeTypeInternal(ctx context.Context, tabletAlias *pb.TabletAlias, dbType pb.TabletType) error {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.IsInServingGraph()

	// change the type
	if err := wr.tmc.ChangeType(ctx, ti, dbType); err != nil {
		return err
	}

	// rebuild if necessary
	if rebuildRequired {
		_, err = wr.RebuildShardGraph(ctx, ti.Keyspace, ti.Shard, []string{ti.Alias.Cell})
		if err != nil {
			return err
		}
	}
	return nil
}

// ExecuteFetchAsDba executes a query remotely using the DBA pool
func (wr *Wrangler) ExecuteFetchAsDba(ctx context.Context, tabletAlias *pb.TabletAlias, query string, maxRows int, wantFields, disableBinlogs bool, reloadSchema bool) (*mproto.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.ExecuteFetchAsDba(ctx, ti, query, maxRows, wantFields, disableBinlogs, reloadSchema)
}
