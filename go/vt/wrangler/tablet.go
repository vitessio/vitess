// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

// Tablet related methods for wrangler

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a slave type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// update is true, and a tablet with the same ID exists, update it.
// If Force is true, and a tablet with the same ID already exists, it
// will be scrapped and deleted, and then recreated.
func (wr *Wrangler) InitTablet(tablet *topo.Tablet, force, createShardAndKeyspace, update bool) error {
	if err := tablet.Complete(); err != nil {
		return err
	}

	if tablet.IsInReplicationGraph() {
		// create the parent keyspace and shard if needed
		if createShardAndKeyspace {
			if err := wr.ts.CreateKeyspace(tablet.Keyspace, &topo.Keyspace{}); err != nil && err != topo.ErrNodeExists {
				return err
			}

			if err := topo.CreateShard(wr.ts, tablet.Keyspace, tablet.Shard); err != nil && err != topo.ErrNodeExists {
				return err
			}
		}

		// get the shard, checks a couple things
		si, err := wr.ts.GetShard(tablet.Keyspace, tablet.Shard)
		if err != nil {
			return fmt.Errorf("missing parent shard, use -parent option to create it, or CreateKeyspace / CreateShard")
		}
		if si.KeyRange != tablet.KeyRange {
			return fmt.Errorf("shard %v/%v has a different KeyRange: %v != %v", tablet.Keyspace, tablet.Shard, si.KeyRange, tablet.KeyRange)
		}
		if tablet.Type == topo.TYPE_MASTER && !si.MasterAlias.IsZero() && si.MasterAlias != tablet.Alias && !force {
			return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", si.MasterAlias, tablet.Keyspace, tablet.Shard)
		}

		// see if we specified a parent, otherwise get it from the shard
		if tablet.Parent.IsZero() && tablet.Type.IsSlaveType() {
			if si.MasterAlias.IsZero() {
				return fmt.Errorf("trying to create tablet %v in shard %v/%v without a master", tablet.Alias, tablet.Keyspace, tablet.Shard)
			}
			tablet.Parent = si.MasterAlias
		}

		// update the shard record if needed
		if err := wr.updateShardCellsAndMaster(si, tablet.Alias, tablet.Type, force); err != nil {
			return err
		}
	}

	err := topo.CreateTablet(wr.ts, tablet)
	if err != nil && err == topo.ErrNodeExists {
		// Try to update nicely, but if it fails fall back to force behavior.
		if update || force {
			oldTablet, err := wr.ts.GetTablet(tablet.Alias)
			if err != nil {
				log.Warningf("failed reading tablet %v: %v", tablet.Alias, err)
			} else {
				if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
					*(oldTablet.Tablet) = *tablet
					if err := topo.UpdateTablet(wr.ts, oldTablet); err != nil {
						log.Warningf("failed updating tablet %v: %v", tablet.Alias, err)
						// now fall through the Scrap case
					} else {
						if !tablet.IsInReplicationGraph() {
							return nil
						}

						if err := topo.CreateTabletReplicationData(wr.ts, tablet); err != nil {
							log.Warningf("failed updating tablet replication data for %v: %v", tablet.Alias, err)
							// now fall through the Scrap case
						} else {
							return nil
						}
					}
				}
			}
		}
		if force {
			if err = wr.Scrap(tablet.Alias, force, false); err != nil {
				log.Errorf("failed scrapping tablet %v: %v", tablet.Alias, err)
				return err
			}
			if err := wr.ts.DeleteTablet(tablet.Alias); err != nil {
				// we ignore this
				log.Errorf("failed deleting tablet %v: %v", tablet.Alias, err)
			}
			return topo.CreateTablet(wr.ts, tablet)
		}
	}
	return err
}

// Scrap a tablet. If force is used, we write to topo.Server
// directly and don't remote-execute the command.
//
// If we scrap the master for a shard, we will clear its record
// from the Shard object (only if that was the right master)
func (wr *Wrangler) Scrap(tabletAlias topo.TabletAlias, force, skipRebuild bool) error {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsInServingGraph()
	wasMaster := ti.Type == topo.TYPE_MASTER

	if force {
		err = topotools.Scrap(wr.ts, ti.Alias, force)
	} else {
		err = wr.ai.Scrap(ti, wr.ActionTimeout())
	}
	if err != nil {
		return err
	}

	if !rebuildRequired {
		log.Infof("Rebuild not required")
		return nil
	}
	if skipRebuild {
		log.Warningf("Rebuild required, but skipping it")
		return nil
	}

	// update the Shard object if the master was scrapped
	if wasMaster {
		actionNode := actionnode.UpdateShard()
		lockPath, err := wr.lockShard(ti.Keyspace, ti.Shard, actionNode)
		if err != nil {
			return err
		}

		// read the shard with the lock
		si, err := wr.ts.GetShard(ti.Keyspace, ti.Shard)
		if err != nil {
			return wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err)
		}

		// update it if the right alias is there
		if si.MasterAlias == tabletAlias {
			si.MasterAlias = topo.TabletAlias{}

			// write it back
			if err := topo.UpdateShard(wr.ts, si); err != nil {
				return wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err)
			}
		} else {
			log.Warningf("Scrapping master %v from shard %v/%v but master in Shard object was %v", tabletAlias, ti.Keyspace, ti.Shard, si.MasterAlias)
		}

		// and unlock
		if err := wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err); err != nil {
			return err
		}
	}

	// and rebuild the original shard / keyspace
	return wr.RebuildShardGraph(ti.Keyspace, ti.Shard, []string{ti.Alias.Cell})
}

// Change the type of tablet and recompute all necessary derived paths in the
// serving graph. If force is true, it will bypass the RPC action
// system and make the data change directly, and not run the remote
// hooks.
//
// Note we don't update the master record in the Shard here, as we
// can't ChangeType from and out of master anyway.
func (wr *Wrangler) ChangeType(tabletAlias topo.TabletAlias, tabletType topo.TabletType, force bool) error {
	rebuildRequired, cell, keyspace, shard, err := wr.ChangeTypeNoRebuild(tabletAlias, tabletType, force)
	if err != nil {
		return err
	}
	if rebuildRequired {
		return wr.RebuildShardGraph(keyspace, shard, []string{cell})
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
func (wr *Wrangler) ChangeTypeNoRebuild(tabletAlias topo.TabletAlias, tabletType topo.TabletType, force bool) (rebuildRequired bool, cell, keyspace, shard string, err error) {
	// Load tablet to find keyspace and shard assignment.
	// Don't load after the ChangeType which might have unassigned
	// the tablet.
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return false, "", "", "", err
	}

	if force {
		if err := topotools.ChangeType(wr.ts, tabletAlias, tabletType, nil, false); err != nil {
			return false, "", "", "", err
		}
	} else {
		if err := wr.ai.ChangeType(ti, tabletType, wr.ActionTimeout()); err != nil {
			return false, "", "", "", err
		}
	}

	if !ti.Tablet.IsInServingGraph() {
		// re-read the tablet, see if we become serving
		ti, err = wr.ts.GetTablet(tabletAlias)
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
func (wr *Wrangler) changeTypeInternal(tabletAlias topo.TabletAlias, dbType topo.TabletType) error {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsInServingGraph()

	// change the type
	if err := wr.ai.ChangeType(ti, dbType, wr.ActionTimeout()); err != nil {
		return err
	}

	// rebuild if necessary
	if rebuildRequired {
		err = topotools.RebuildShard(wr.logger, wr.ts, ti.Keyspace, ti.Shard, []string{ti.Alias.Cell}, wr.lockTimeout, interrupted)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteTablet will get the tablet record, and if it's scrapped, will
// delete the record from the topology.
func (wr *Wrangler) DeleteTablet(tabletAlias topo.TabletAlias) error {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	// refuse to delete tablets that are not scrapped
	if ti.Type != topo.TYPE_SCRAP {
		return fmt.Errorf("Can only delete scrapped tablets")
	}
	return wr.TopoServer().DeleteTablet(tabletAlias)
}

// ExecuteFetch will get data from a remote tablet
func (wr *Wrangler) ExecuteFetch(tabletAlias topo.TabletAlias, query string, maxRows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.ai.ExecuteFetch(ti, query, maxRows, wantFields, disableBinlogs, wr.ActionTimeout())
}
