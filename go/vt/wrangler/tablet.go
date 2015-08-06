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
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

// Tablet related methods for wrangler

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a slave type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// update is true, and a tablet with the same ID exists, update it.
// If Force is true, and a tablet with the same ID already exists, it
// will be scrapped and deleted, and then recreated.
func (wr *Wrangler) InitTablet(ctx context.Context, tablet *topo.Tablet, force, createShardAndKeyspace, update bool) error {
	if err := tablet.Complete(); err != nil {
		return err
	}

	if tablet.IsInReplicationGraph() {
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
		if key.ProtoToKeyRange(si.KeyRange) != tablet.KeyRange {
			return fmt.Errorf("shard %v/%v has a different KeyRange: %v != %v", tablet.Keyspace, tablet.Shard, si.KeyRange, tablet.KeyRange)
		}
		if tablet.Type == topo.TYPE_MASTER && !topo.TabletAliasIsZero(si.MasterAlias) && topo.ProtoToTabletAlias(si.MasterAlias) != tablet.Alias && !force {
			return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", si.MasterAlias, tablet.Keyspace, tablet.Shard)
		}

		// update the shard record if needed
		if err := wr.updateShardCellsAndMaster(ctx, si, topo.TabletAliasToProto(tablet.Alias), topo.TabletTypeToProto(tablet.Type), force); err != nil {
			return err
		}
	}

	err := topo.CreateTablet(ctx, wr.ts, tablet)
	if err != nil && err == topo.ErrNodeExists {
		// Try to update nicely, but if it fails fall back to force behavior.
		if update || force {
			oldTablet, err := wr.ts.GetTablet(ctx, tablet.Alias)
			if err != nil {
				wr.Logger().Warningf("failed reading tablet %v: %v", tablet.Alias, err)
			} else {
				if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
					*(oldTablet.Tablet) = *tablet
					if err := topo.UpdateTablet(ctx, wr.ts, oldTablet); err != nil {
						wr.Logger().Warningf("failed updating tablet %v: %v", tablet.Alias, err)
						// now fall through the Scrap case
					} else {
						if !tablet.IsInReplicationGraph() {
							return nil
						}

						if err := topo.UpdateTabletReplicationData(ctx, wr.ts, tablet); err != nil {
							wr.Logger().Warningf("failed updating tablet replication data for %v: %v", tablet.Alias, err)
							// now fall through the Scrap case
						} else {
							return nil
						}
					}
				}
			}
		}
		if force {
			if err = wr.Scrap(ctx, tablet.Alias, force, false); err != nil {
				wr.Logger().Errorf("failed scrapping tablet %v: %v", tablet.Alias, err)
				return err
			}
			if err := wr.ts.DeleteTablet(ctx, tablet.Alias); err != nil {
				// we ignore this
				wr.Logger().Errorf("failed deleting tablet %v: %v", tablet.Alias, err)
			}
			return topo.CreateTablet(ctx, wr.ts, tablet)
		}
	}
	return err
}

// Scrap a tablet. If force is used, we write to topo.Server
// directly and don't remote-execute the command.
//
// If we scrap the master for a shard, we will clear its record
// from the Shard object (only if that was the right master)
func (wr *Wrangler) Scrap(ctx context.Context, tabletAlias topo.TabletAlias, force, skipRebuild bool) error {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsInServingGraph()
	wasMaster := ti.Type == topo.TYPE_MASTER

	if force {
		err = topotools.Scrap(ctx, wr.ts, ti.Alias, force)
	} else {
		err = wr.tmc.Scrap(ctx, ti)
	}
	if err != nil {
		return err
	}

	if !rebuildRequired {
		wr.Logger().Infof("Rebuild not required")
		return nil
	}
	if skipRebuild {
		wr.Logger().Warningf("Rebuild required, but skipping it")
		return nil
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
		if topo.TabletAliasEqual(si.MasterAlias, topo.TabletAliasToProto(tabletAlias)) {
			si.MasterAlias = nil

			// write it back
			if err := topo.UpdateShard(ctx, wr.ts, si); err != nil {
				return wr.unlockShard(ctx, ti.Keyspace, ti.Shard, actionNode, lockPath, err)
			}
		} else {
			wr.Logger().Warningf("Scrapping master %v from shard %v/%v but master in Shard object was %v", tabletAlias, ti.Keyspace, ti.Shard, si.MasterAlias)
		}

		// and unlock
		if err := wr.unlockShard(ctx, ti.Keyspace, ti.Shard, actionNode, lockPath, err); err != nil {
			return err
		}
	}

	// and rebuild the original shard
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
func (wr *Wrangler) ChangeType(ctx context.Context, tabletAlias topo.TabletAlias, tabletType topo.TabletType, force bool) error {
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
func (wr *Wrangler) ChangeTypeNoRebuild(ctx context.Context, tabletAlias topo.TabletAlias, tabletType topo.TabletType, force bool) (rebuildRequired bool, cell, keyspace, shard string, err error) {
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

	if !ti.Tablet.IsInServingGraph() {
		// re-read the tablet, see if we become serving
		ti, err = wr.ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return false, "", "", "", err
		}
		if !ti.Tablet.IsInServingGraph() {
			return false, "", "", "", nil
		}
	}
	return true, ti.Alias.Cell, ti.Keyspace, ti.Shard, nil

}

// same as ChangeType, but assume we already have the shard lock,
// and do not have the option to force anything.
func (wr *Wrangler) changeTypeInternal(ctx context.Context, tabletAlias topo.TabletAlias, dbType topo.TabletType) error {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsInServingGraph()

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

// DeleteTablet will get the tablet record, and if it's scrapped, will
// delete the record from the topology.
func (wr *Wrangler) DeleteTablet(ctx context.Context, tabletAlias topo.TabletAlias) error {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	// refuse to delete tablets that are not scrapped
	if ti.Type != topo.TYPE_SCRAP {
		return fmt.Errorf("Can only delete scrapped tablets")
	}
	return wr.TopoServer().DeleteTablet(ctx, tabletAlias)
}

// ExecuteFetchAsDba executes a query remotely using the DBA pool
func (wr *Wrangler) ExecuteFetchAsDba(ctx context.Context, tabletAlias topo.TabletAlias, query string, maxRows int, wantFields, disableBinlogs bool, reloadSchema bool) (*mproto.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.ExecuteFetchAsDba(ctx, ti, query, maxRows, wantFields, disableBinlogs, reloadSchema)
}
