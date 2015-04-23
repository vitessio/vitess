// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

/*
This file handles the reparenting operations.

FIXME(alainjobart) a lot of this code is being replaced now.
The new code starts at InitShardMaster.
*/

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/concurrency"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"
)

const (
	initShardMasterOperation            = "InitShardMaster"
	plannedReparentShardMasterOperation = "PlannedReparentShardMaster"
)

// ReparentShard creates the reparenting action and launches a goroutine
// to coordinate the procedure.
//
//
// leaveMasterReadOnly: leave the master in read-only mode, even
//   though all the other necessary updates have been made.
// forceReparentToCurrentMaster: mostly for test setups, this can
//   cause data loss.
func (wr *Wrangler) ReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster bool, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard("", masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.reparentShardLocked(ctx, keyspace, shard, masterElectTabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster, waitSlaveTimeout)

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) reparentShardLocked(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster bool, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShard(ctx, wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	slaveTabletMap, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	if shardInfo.MasterAlias == masterElectTabletAlias && !forceReparentToCurrentMaster {
		return fmt.Errorf("master-elect tablet %v is already master - specify -force to override", masterElectTabletAlias)
	}

	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, topotools.MapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		ShardInfo: *shardInfo,
		NewMaster: *masterElectTablet.Tablet,
	}

	if oldMasterTablet, ok := tabletMap[shardInfo.MasterAlias]; ok {
		ev.OldMaster = *oldMasterTablet.Tablet
	}

	if !shardInfo.MasterAlias.IsZero() && !forceReparentToCurrentMaster {
		err = wr.reparentShardGraceful(ctx, ev, shardInfo, slaveTabletMap, masterTabletMap, masterElectTablet, leaveMasterReadOnly, waitSlaveTimeout)
	} else {
		err = wr.reparentShardBrutal(ctx, ev, shardInfo, slaveTabletMap, masterTabletMap, masterElectTablet, leaveMasterReadOnly, forceReparentToCurrentMaster, waitSlaveTimeout)
	}

	if err == nil {
		// only log if it works, if it fails we'll show the error
		wr.Logger().Infof("reparentShard finished")
	}
	return err
}

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*myproto.ReplicationStatus, error) {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return nil, nil, err
	}

	// lock the shard
	actionNode := actionnode.CheckShard()
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return nil, nil, err
	}

	tabletMap, posMap, err := wr.shardReplicationStatuses(ctx, shardInfo)
	return tabletMap, posMap, wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) shardReplicationStatuses(ctx context.Context, shardInfo *topo.ShardInfo) ([]*topo.TabletInfo, []*myproto.ReplicationStatus, error) {
	// FIXME(msolomon) this assumes no hierarchical replication, which is currently the case.
	tabletMap, err := topo.GetTabletMapForShard(ctx, wr.ts, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)
	stats, err := wr.tabletReplicationStatuses(ctx, tablets)
	return tablets, stats, err
}

// ReparentTablet attempts to reparent this tablet to the current
// master, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(ctx context.Context, tabletAlias topo.TabletAlias) error {
	// Get specified tablet.
	// Get current shard master tablet.
	// Sanity check they are in the same keyspace/shard.
	// Get slave position for specified tablet.
	// Get reparent position from master for the given slave position.
	// Issue a restart slave on the specified tablet.

	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	shardInfo, err := wr.ts.GetShard(ti.Keyspace, ti.Shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias.IsZero() {
		return fmt.Errorf("no master tablet for shard %v/%v", ti.Keyspace, ti.Shard)
	}

	masterTi, err := wr.ts.GetTablet(shardInfo.MasterAlias)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if masterTi.Type != topo.TYPE_MASTER {
		return fmt.Errorf("TopologyServer has inconsistent state for shard master %v", shardInfo.MasterAlias)
	}
	if masterTi.Keyspace != ti.Keyspace || masterTi.Shard != ti.Shard {
		return fmt.Errorf("master %v and potential slave not in same keyspace/shard", shardInfo.MasterAlias)
	}

	status, err := wr.tmc.SlaveStatus(ctx, ti)
	if err != nil {
		return err
	}
	wr.Logger().Infof("slave tablet position: %v %v %v", tabletAlias, ti.MysqlAddr(), status.Position)

	rsd, err := wr.tmc.ReparentPosition(ctx, masterTi, &status.Position)
	if err != nil {
		return err
	}

	wr.Logger().Infof("master tablet position: %v %v %v", shardInfo.MasterAlias, masterTi.MysqlAddr(), rsd.ReplicationStatus.Position)
	// An orphan is already in the replication graph but it is
	// disconnected, hence we have to force this action.
	rsd.Force = ti.Type == topo.TYPE_LAG_ORPHAN
	return wr.tmc.RestartSlave(ctx, ti, rsd)
}

// InitShardMaster will make the provided tablet the master for the shard.
func (wr *Wrangler) InitShardMaster(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, force bool, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(initShardMasterOperation, masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.initShardMasterLocked(ctx, keyspace, shard, masterElectTabletAlias, force, waitSlaveTimeout)

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) initShardMasterLocked(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, force bool, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShard(ctx, wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	// Check the master elect is in tabletMap
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", masterElectTabletAlias)
	}

	// Check the master is the only master is the shard, or -force was used.
	_, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	if shardInfo.MasterAlias != masterElectTabletAlias {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not the shard master, use -force to proceed anyway", masterElectTabletAlias)
		}
		wr.logger.Warningf("master-elect tablet %v is not the shard master, proceeding anyway as -force was used", masterElectTabletAlias)
	}
	if _, ok := masterTabletMap[masterElectTabletAlias]; !ok {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not a master in the shard, use -force to proceed anyway", masterElectTabletAlias)
		}
		wr.logger.Warningf("master-elect tablet %v is not a master in the shard, proceeding anyway as -force was used", masterElectTabletAlias)
	}
	haveOtherMaster := false
	for alias, ti := range masterTabletMap {
		if alias != masterElectTabletAlias && ti.Type != topo.TYPE_SCRAP {
			haveOtherMaster = true
		}
	}
	if haveOtherMaster {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not the only master in the shard, use -force to proceed anyway", masterElectTabletAlias)
		}
		wr.logger.Warningf("master-elect tablet %v is not the only master in the shard, proceeding anyway as -force was used", masterElectTabletAlias)
	}

	// First phase: reset replication on all tablets. If anyone fails,
	// we stop. It is probably because it is unreachable, and may leave
	// an unstable database process in the mix, with a database daemon
	// at a wrong replication spot.
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("resetting replication on tablet %v", alias)
			if err := wr.TabletManagerClient().ResetReplication(ctx, tabletInfo); err != nil {
				rec.RecordError(fmt.Errorf("Tablet %v ResetReplication failed (either fix it, or Scrap it): %v", alias, err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Tell the new master to break its slaves, return its replication
	// position
	wr.logger.Infof("initializing master on %v", masterElectTabletAlias)
	rp, err := wr.TabletManagerClient().InitMaster(ctx, masterElectTabletInfo)
	if err != nil {
		return err
	}

	// Now tell the new master to insert the reparent_journal row,
	// and tell everybody else to become a slave of the new master,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the master to be able to commit its row in the
	// reparent_journal table, it needs connected slaves.
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if alias == masterElectTabletAlias {
			wgMaster.Add(1)
			go func(alias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = wr.TabletManagerClient().PopulateReparentJournal(ctx, tabletInfo, now, initShardMasterOperation, alias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("initializing slave %v", alias)
				if err := wr.TabletManagerClient().InitSlave(ctx, tabletInfo, masterElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("Tablet %v InitSlave failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one slave is done)
	wgMaster.Wait()
	if masterErr != nil {
		wgSlaves.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	if shardInfo.MasterAlias != masterElectTabletAlias {
		shardInfo.MasterAlias = masterElectTabletAlias
		if err := topo.UpdateShard(ctx, wr.ts, shardInfo); err != nil {
			wgSlaves.Wait()
			return fmt.Errorf("failed to update shard master record: %v", err)
		}
	}

	// Wait for the slaves to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgSlaves.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Then we rebuild the entire serving graph for the shard,
	// to account for all changes.
	_, err = wr.RebuildShardGraph(ctx, keyspace, shard, nil)
	return err
}

// PlannedReparentShard will make the provided tablet the master for the shard,
// when both the current and new master are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(plannedReparentShardMasterOperation, masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.plannedReparentShardLocked(ctx, keyspace, shard, masterElectTabletAlias, waitSlaveTimeout)

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, keyspace, shard string, masterElectTabletAlias topo.TabletAlias, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShard(ctx, wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	// Check corner cases we're going to depend on
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", masterElectTabletAlias)
	}
	if shardInfo.MasterAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already the master", masterElectTabletAlias)
	}
	oldMasterTabletInfo, ok := tabletMap[shardInfo.MasterAlias]
	if !ok {
		return fmt.Errorf("old master tablet %v is not in the shard", shardInfo.MasterAlias)
	}

	// Demote the current master, get its replication position
	wr.logger.Infof("demote current master %v", shardInfo.MasterAlias)
	rp, err := wr.tmc.DemoteMaster(ctx, oldMasterTabletInfo)
	if err != nil {
		return fmt.Errorf("old master tablet %v DemoteMaster failed: %v", shardInfo.MasterAlias, err)
	}

	// Wait on the master-elect tablet until it reaches that position,
	// then promote it
	wr.logger.Infof("promote slave %v", masterElectTabletAlias)
	rp, err = wr.tmc.PromoteSlaveWhenCaughtUp(ctx, masterElectTabletInfo, rp)
	if err != nil {
		return fmt.Errorf("master-elect tablet %v failed to catch up with replication or be upgraded to master: %v", masterElectTabletAlias, err)
	}

	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if alias == masterElectTabletAlias {
			wgMaster.Add(1)
			go func(alias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = wr.TabletManagerClient().PopulateReparentJournal(ctx, tabletInfo, now, plannedReparentShardMasterOperation, alias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("setting new master on slave %v", alias)
				if err := wr.TabletManagerClient().SetMaster(ctx, tabletInfo, masterElectTabletAlias, now); err != nil {
					rec.RecordError(fmt.Errorf("Tablet %v SetMaster failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one slave is done)
	wgMaster.Wait()
	if masterErr != nil {
		wgSlaves.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	wr.logger.Infof("updating shard record with new master %v", masterElectTabletAlias)
	shardInfo.MasterAlias = masterElectTabletAlias
	if err := topo.UpdateShard(ctx, wr.ts, shardInfo); err != nil {
		wgSlaves.Wait()
		return fmt.Errorf("failed to update shard master record: %v", err)
	}

	// Wait for the slaves to complete. If some of them fail, we
	// will rebuild the shard serving graph anyway
	wgSlaves.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf("Some slaves failed to reparent: %v", err)
		return err
	}

	// Then we rebuild the entire serving graph for the shard,
	// to account for all changes.
	wr.logger.Infof("rebuilding shard graph")
	_, err = wr.RebuildShardGraph(ctx, keyspace, shard, nil)
	return err

}
