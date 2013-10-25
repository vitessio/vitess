// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	log "github.com/golang/glog"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
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
			if err := wr.ts.CreateKeyspace(tablet.Keyspace); err != nil && err != topo.ErrNodeExists {
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
		if tablet.Type == topo.TYPE_MASTER && !si.MasterAlias.IsZero() && si.MasterAlias != tablet.GetAlias() && !force {
			return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", si.MasterAlias, tablet.Keyspace, tablet.Shard)
		}

		// see if we specified a parent, otherwise get it from the shard
		if tablet.Parent.IsZero() && tablet.Type.IsSlaveType() {
			if si.MasterAlias.IsZero() {
				return fmt.Errorf("trying to create tablet %v in shard %v/%v without a master", tablet.GetAlias(), tablet.Keyspace, tablet.Shard)
			}
			tablet.Parent = si.MasterAlias
		}

		// See if we need to update the Shard:
		// - add the tablet's cell to the shard's Cells if needed
		// - change the master if needed
		shardUpdateRequired := false
		if !si.HasCell(tablet.Cell) {
			shardUpdateRequired = true
		}
		if tablet.Type == topo.TYPE_MASTER && si.MasterAlias != tablet.GetAlias() {
			shardUpdateRequired = true
		}

		if shardUpdateRequired {
			actionNode := wr.ai.UpdateShard()
			lockPath, err := wr.lockShard(tablet.Keyspace, tablet.Shard, actionNode)
			if err != nil {
				return err
			}

			// re-read the shard with the lock
			si, err = wr.ts.GetShard(tablet.Keyspace, tablet.Shard)
			if err != nil {
				return wr.unlockShard(tablet.Keyspace, tablet.Shard, actionNode, lockPath, err)
			}

			// update it
			wasUpdated := false
			if !si.HasCell(tablet.Cell) {
				si.Cells = append(si.Cells, tablet.Cell)
				wasUpdated = true
			}
			if tablet.Type == topo.TYPE_MASTER && si.MasterAlias != tablet.GetAlias() {
				if !si.MasterAlias.IsZero() && !force {
					return wr.unlockShard(tablet.Keyspace, tablet.Shard, actionNode, lockPath, fmt.Errorf("creating this tablet would override old master %v in shard %v/%v", si.MasterAlias, tablet.Keyspace, tablet.Shard))
				}
				si.MasterAlias = tablet.GetAlias()
				wasUpdated = true
			}

			if wasUpdated {
				// write it back
				if err := wr.ts.UpdateShard(si); err != nil {
					return wr.unlockShard(tablet.Keyspace, tablet.Shard, actionNode, lockPath, err)
				}
			}

			// and unlock
			if err := wr.unlockShard(tablet.Keyspace, tablet.Shard, actionNode, lockPath, err); err != nil {
				return err
			}

			// also create the cell's ShardReplication
			if err := wr.ts.CreateShardReplication(tablet.Cell, tablet.Keyspace, tablet.Shard, &topo.ShardReplication{}); err != nil && err != topo.ErrNodeExists {
				return err
			}
		}
	}

	err := topo.CreateTablet(wr.ts, tablet)
	if err != nil && err == topo.ErrNodeExists {
		// Try to update nicely, but if it fails fall back to force behavior.
		if update || force {
			oldTablet, err := wr.ts.GetTablet(tablet.GetAlias())
			if err != nil {
				log.Warningf("failed reading tablet %v: %v", tablet.GetAlias(), err)
			} else {
				if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
					*(oldTablet.Tablet) = *tablet
					if err := topo.UpdateTablet(wr.ts, oldTablet); err != nil {
						log.Warningf("failed updating tablet %v: %v", tablet.GetAlias(), err)
						// now fall through the Scrap case
					} else {
						if !tablet.IsInReplicationGraph() {
							return nil
						}

						if err := topo.CreateTabletReplicationData(wr.ts, tablet); err != nil {
							log.Warningf("failed updating tablet replication data for %v: %v", tablet.GetAlias(), err)
							// now fall through the Scrap case
						} else {
							return nil
						}
					}
				}
			}
		}
		if force {
			if _, err = wr.Scrap(tablet.GetAlias(), force, false); err != nil {
				log.Errorf("failed scrapping tablet %v: %v", tablet.GetAlias(), err)
				return err
			}
			if err := wr.ts.DeleteTablet(tablet.GetAlias()); err != nil {
				// we ignore this
				log.Errorf("failed deleting tablet %v: %v", tablet.GetAlias(), err)
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
func (wr *Wrangler) Scrap(tabletAlias topo.TabletAlias, force, skipRebuild bool) (actionPath string, err error) {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return "", err
	}
	rebuildRequired := ti.Tablet.IsServingType()
	wasMaster := ti.Type == topo.TYPE_MASTER

	if force {
		err = tm.Scrap(wr.ts, ti.GetAlias(), force)
	} else {
		actionPath, err = wr.ai.Scrap(ti.GetAlias())
	}
	if err != nil {
		return "", err
	}

	if !rebuildRequired {
		log.Infof("Rebuild not required")
		return
	}
	if skipRebuild {
		log.Warningf("Rebuild required, but skipping it")
		return
	}

	// wait for the remote Scrap if necessary
	if actionPath != "" {
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		if err != nil {
			return "", err
		}
	}

	// update the Shard object if the master was scrapped
	if wasMaster {
		actionNode := wr.ai.UpdateShard()
		lockPath, err := wr.lockShard(ti.Keyspace, ti.Shard, actionNode)
		if err != nil {
			return "", err
		}

		// read the shard with the lock
		si, err := wr.ts.GetShard(ti.Keyspace, ti.Shard)
		if err != nil {
			return "", wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err)
		}

		// update it if the right alias is there
		if si.MasterAlias == tabletAlias {
			si.MasterAlias = topo.TabletAlias{}

			// write it back
			if err := wr.ts.UpdateShard(si); err != nil {
				return "", wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err)
			}
		} else {
			log.Warningf("Scrapping master %v from shard %v/%v but master in Shard object was %v", tabletAlias, ti.Keyspace, ti.Shard, si.MasterAlias)
		}

		// and unlock
		if err := wr.unlockShard(ti.Keyspace, ti.Shard, actionNode, lockPath, err); err != nil {
			return "", err
		}
	}

	// and rebuild the original shard / keyspace
	return "", wr.RebuildShardGraph(ti.Keyspace, ti.Shard, []string{ti.Cell})
}

// TODO(alainjobart) remove this flag and keep the
// useRpcChangeType=true code path once the server has been deployed
// everywhere.  Tests pass both with useRpcChangeType=false and
// useRpcChangeType=true.
var useRpcChangeType = false

// Change the type of tablet and recompute all necessary derived paths in the
// serving graph.
// force: Bypass the vtaction system and make the data change directly, and
// do not run the remote hooks.
//
// Note we don't update the master record in the Shard here, as we can't
// ChangeType from and out of master anyway.
func (wr *Wrangler) ChangeType(tabletAlias topo.TabletAlias, dbType topo.TabletType, force bool) error {
	// Load tablet to find keyspace and shard assignment.
	// Don't load after the ChangeType which might have unassigned
	// the tablet.
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	if force {
		// with --force, we do not run any hook
		err = tm.ChangeType(wr.ts, tabletAlias, dbType, false)
	} else {
		if useRpcChangeType {
			err = wr.ai.RpcChangeType(ti, dbType, wr.actionTimeout())
		} else {
			// the remote action will run the hooks
			var actionPath string
			actionPath, err = wr.ai.ChangeType(tabletAlias, dbType)
			// You don't have a choice - you must wait for
			// completion before rebuilding.
			if err == nil {
				err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
			}
		}
	}

	if err != nil {
		return err
	}

	// we rebuild if the tablet was serving, or if it is now
	var keyspaceToRebuild string
	var shardToRebuild string
	var cellToRebuild string
	if rebuildRequired {
		keyspaceToRebuild = ti.Keyspace
		shardToRebuild = ti.Shard
		cellToRebuild = ti.Cell
	} else {
		// re-read the tablet, see if we become serving
		ti, err := wr.ts.GetTablet(tabletAlias)
		if err != nil {
			return err
		}
		if ti.Tablet.IsServingType() {
			rebuildRequired = true
			keyspaceToRebuild = ti.Keyspace
			shardToRebuild = ti.Shard
			cellToRebuild = ti.Cell
		}
	}

	if rebuildRequired {
		if err := wr.RebuildShardGraph(keyspaceToRebuild, shardToRebuild, []string{cellToRebuild}); err != nil {
			return err
		}
	}
	return nil
}

// same as ChangeType, but assume we already have the shard lock,
// and do not have the option to force anything.
func (wr *Wrangler) changeTypeInternal(tabletAlias topo.TabletAlias, dbType topo.TabletType) error {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	// change the type
	if useRpcChangeType {
		if err := wr.ai.RpcChangeType(ti, dbType, wr.actionTimeout()); err != nil {
			return err
		}
	} else {
		actionPath, err := wr.ai.ChangeType(ti.GetAlias(), dbType)
		if err != nil {
			return err
		}
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		if err != nil {
			return err
		}
	}

	// rebuild if necessary
	if rebuildRequired {
		err = wr.rebuildShard(ti.Keyspace, ti.Shard, []string{ti.Cell})
		if err != nil {
			return err
		}
	}
	return nil
}
