// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

/*
This file handles the reparenting operations.
*/

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"

	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	initShardMasterOperation        = "InitShardMaster"
	plannedReparentShardOperation   = "PlannedReparentShard"
	emergencyReparentShardOperation = "EmergencyReparentShard"
)

// FIXME(alainjobart) rework this ShardReplicationStatuses function,
// it's clumpsy

// helper struct to queue up results
type rpcContext struct {
	tablet *topo.TabletInfo
	status *replicationdatapb.Status
	err    error
}

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
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

func (wr *Wrangler) shardReplicationStatuses(ctx context.Context, shardInfo *topo.ShardInfo) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	// FIXME(msolomon) this assumes no hierarchical replication, which is currently the case.
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)
	stats, err := wr.tabletReplicationStatuses(ctx, tablets)
	return tablets, stats, err
}

// tabletReplicationStatuses returns the ReplicationStatus of each tablet in
// tablets.
func (wr *Wrangler) tabletReplicationStatuses(ctx context.Context, tablets []*topo.TabletInfo) ([]*replicationdatapb.Status, error) {
	wr.logger.Infof("tabletReplicationStatuses: %v", tablets)
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	result := make([]*replicationdatapb.Status, len(tablets))

	for i, ti := range tablets {
		// Don't scan tablets that won't return something
		// useful. Otherwise, you'll end up waiting for a timeout.
		if ti.Type == topodatapb.TabletType_MASTER {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				pos, err := wr.tmc.MasterPosition(ctx, ti)
				if err != nil {
					rec.RecordError(fmt.Errorf("MasterPosition(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = &replicationdatapb.Status{
					Position: pos,
				}
			}(i, ti)
		} else if ti.IsSlaveType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := wr.tmc.SlaveStatus(ctx, ti)
				if err != nil {
					rec.RecordError(fmt.Errorf("SlaveStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return result, rec.Error()
}

// ReparentTablet tells a tablet to reparent this tablet to the current
// master, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	// Get specified tablet.
	// Get current shard master tablet.
	// Sanity check they are in the same keyspace/shard.
	// Issue a SetMaster to the tablet.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	shardInfo, err := wr.ts.GetShard(ctx, ti.Keyspace, ti.Shard)
	if err != nil {
		return err
	}
	if !shardInfo.HasMaster() {
		return fmt.Errorf("no master tablet for shard %v/%v", ti.Keyspace, ti.Shard)
	}

	masterTi, err := wr.ts.GetTablet(ctx, shardInfo.MasterAlias)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if masterTi.Type != topodatapb.TabletType_MASTER {
		return fmt.Errorf("TopologyServer has inconsistent state for shard master %v", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}
	if masterTi.Keyspace != ti.Keyspace || masterTi.Shard != ti.Shard {
		return fmt.Errorf("master %v and potential slave not in same keyspace/shard", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}

	// and do the remote command
	return wr.TabletManagerClient().SetMaster(ctx, ti, shardInfo.MasterAlias, 0, false)
}

// InitShardMaster will make the provided tablet the master for the shard.
func (wr *Wrangler) InitShardMaster(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(initShardMasterOperation, masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.initShardMasterLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, force, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMaster: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMaster")
	}

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) initShardMasterLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check the master elect is in tabletMap
	masterElectTabletInfo, ok := tabletMap[*masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet

	// Check the master is the only master is the shard, or -force was used.
	_, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not the shard master, use -force to proceed anyway", topoproto.TabletAliasString(masterElectTabletAlias))
		}
		wr.logger.Warningf("master-elect tablet %v is not the shard master, proceeding anyway as -force was used", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	if _, ok := masterTabletMap[*masterElectTabletAlias]; !ok {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not a master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(masterElectTabletAlias))
		}
		wr.logger.Warningf("master-elect tablet %v is not a master in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	haveOtherMaster := false
	for alias := range masterTabletMap {
		if !topoproto.TabletAliasEqual(&alias, masterElectTabletAlias) {
			haveOtherMaster = true
		}
	}
	if haveOtherMaster {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not the only master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(masterElectTabletAlias))
		}
		wr.logger.Warningf("master-elect tablet %v is not the only master in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(masterElectTabletAlias))
	}

	// First phase: reset replication on all tablets. If anyone fails,
	// we stop. It is probably because it is unreachable, and may leave
	// an unstable database process in the mix, with a database daemon
	// at a wrong replication spot.
	event.DispatchUpdate(ev, "resetting replication on all tablets")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("resetting replication on tablet %v", topoproto.TabletAliasString(&alias))
			if err := wr.TabletManagerClient().ResetReplication(ctx, tabletInfo); err != nil {
				rec.RecordError(fmt.Errorf("Tablet %v ResetReplication failed (either fix it, or Scrap it): %v", topoproto.TabletAliasString(&alias), err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Tell the new master to break its slaves, return its replication
	// position
	wr.logger.Infof("initializing master on %v", topoproto.TabletAliasString(masterElectTabletAlias))
	event.DispatchUpdate(ev, "initializing master")
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
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if topoproto.TabletAliasEqual(&alias, masterElectTabletAlias) {
			wgMaster.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", topoproto.TabletAliasString(&alias))
				masterErr = wr.TabletManagerClient().PopulateReparentJournal(ctx, tabletInfo, now, initShardMasterOperation, &alias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("initializing slave %v", topoproto.TabletAliasString(&alias))
				if err := wr.TabletManagerClient().InitSlave(ctx, tabletInfo, masterElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("Tablet %v InitSlave failed: %v", topoproto.TabletAliasString(&alias), err))
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
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(s *topodatapb.Shard) error {
			s.MasterAlias = masterElectTabletAlias
			return nil
		}); err != nil {
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

	// Create database if necessary on the master. Slaves will get it too through
	// replication. Since the user called InitShardMaster, they've told us to
	// assume that whatever data is on all the slaves is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", topoproto.TabletDbName(masterElectTabletInfo.Tablet))
	if _, err := wr.TabletManagerClient().ExecuteFetchAsDba(ctx, masterElectTabletInfo, createDB, 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	// Then we rebuild the entire serving graph for the shard,
	// to account for all changes.
	event.DispatchUpdate(ev, "rebuilding shard graph")
	_, err = wr.RebuildShardGraph(ctx, keyspace, shard, nil)
	return err
}

// PlannedReparentShard will make the provided tablet the master for the shard,
// when both the current and new master are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(plannedReparentShardOperation, masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.plannedReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished PlannedReparentShard")
	}

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check corner cases we're going to depend on
	masterElectTabletInfo, ok := tabletMap[*masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is already the master", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	oldMasterTabletInfo, ok := tabletMap[*shardInfo.MasterAlias]
	if !ok {
		return fmt.Errorf("old master tablet %v is not in the shard", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}
	ev.OldMaster = *oldMasterTabletInfo.Tablet

	// Demote the current master, get its replication position
	wr.logger.Infof("demote current master %v", shardInfo.MasterAlias)
	event.DispatchUpdate(ev, "demoting old master")
	rp, err := wr.tmc.DemoteMaster(ctx, oldMasterTabletInfo)
	if err != nil {
		return fmt.Errorf("old master tablet %v DemoteMaster failed: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
	}

	// Wait on the master-elect tablet until it reaches that position,
	// then promote it
	wr.logger.Infof("promote slave %v", topoproto.TabletAliasString(masterElectTabletAlias))
	event.DispatchUpdate(ev, "promoting slave")
	rp, err = wr.tmc.PromoteSlaveWhenCaughtUp(ctx, masterElectTabletInfo, rp)
	if err != nil {
		return fmt.Errorf("master-elect tablet %v failed to catch up with replication or be upgraded to master: %v", topoproto.TabletAliasString(masterElectTabletAlias), err)
	}

	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if topoproto.TabletAliasEqual(&alias, masterElectTabletAlias) {
			wgMaster.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", topoproto.TabletAliasString(&alias))
				masterErr = wr.TabletManagerClient().PopulateReparentJournal(ctx, tabletInfo, now, plannedReparentShardOperation, &alias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("setting new master on slave %v", topoproto.TabletAliasString(&alias))
				// also restart replication on old master
				forceStartSlave := topoproto.TabletAliasEqual(&alias, oldMasterTabletInfo.Alias)
				if err := wr.TabletManagerClient().SetMaster(ctx, tabletInfo, masterElectTabletAlias, now, forceStartSlave); err != nil {
					rec.RecordError(fmt.Errorf("Tablet %v SetMaster failed: %v", topoproto.TabletAliasString(&alias), err))
					return
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
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(s *topodatapb.Shard) error {
		s.MasterAlias = masterElectTabletAlias
		return nil
	}); err != nil {
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
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	_, err = wr.RebuildShardGraph(ctx, keyspace, shard, nil)
	return err
}

// EmergencyReparentShard will make the provided tablet the master for
// the shard, when the old master is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(emergencyReparentShardOperation, masterElectTabletAlias)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.emergencyReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished EmergencyReparentShard")
	}

	// and unlock
	return wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) emergencyReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check corner cases we're going to depend on
	masterElectTabletInfo, ok := tabletMap[*masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is already the master", topoproto.TabletAliasString(masterElectTabletAlias))
	}

	// Deal with the old master: try to remote-scrap it, if it's
	// truely dead we force-scrap it. Remove it from our map in any case.
	if shardInfo.HasMaster() {
		deleteOldMaster := true
		oldMasterTabletInfo, ok := tabletMap[*shardInfo.MasterAlias]
		if ok {
			delete(tabletMap, *shardInfo.MasterAlias)
		} else {
			oldMasterTabletInfo, err = wr.ts.GetTablet(ctx, shardInfo.MasterAlias)
			if err != nil {
				wr.logger.Warningf("cannot read old master tablet %v, won't touch it: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
				deleteOldMaster = false
			}
		}

		if deleteOldMaster {
			ev.OldMaster = *oldMasterTabletInfo.Tablet
			wr.logger.Infof("deleting old master %v", topoproto.TabletAliasString(shardInfo.MasterAlias))

			ctx, cancel := context.WithTimeout(ctx, waitSlaveTimeout)
			defer cancel()

			if err := topotools.DeleteTablet(ctx, wr.ts, oldMasterTabletInfo.Tablet); err != nil {
				wr.logger.Warningf("failed to delete old master tablet %v: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
			}
		}
	}

	// Stop replication on all slaves, get their current
	// replication position
	event.DispatchUpdate(ev, "stop replication on all slaves")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	statusMap := make(map[topodatapb.TabletAlias]*replicationdatapb.Status)
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(&alias))
			ctx, cancel := context.WithTimeout(ctx, waitSlaveTimeout)
			defer cancel()
			rp, err := wr.TabletManagerClient().StopReplicationAndGetStatus(ctx, tabletInfo)
			if err != nil {
				wr.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(&alias), err)
				return
			}
			mu.Lock()
			statusMap[alias] = rp
			mu.Unlock()
		}(alias, tabletInfo)
	}
	wg.Wait()

	// Verify masterElect is alive and has the most advanced position
	masterElectStatus, ok := statusMap[*masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("couldn't get master elect %v replication position", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	masterElectPos, err := replication.DecodePosition(masterElectStatus.Position)
	if err != nil {
		return fmt.Errorf("cannot decode master elect position %v: %v", masterElectStatus.Position, err)
	}
	for alias, status := range statusMap {
		if topoproto.TabletAliasEqual(&alias, masterElectTabletAlias) {
			continue
		}
		pos, err := replication.DecodePosition(status.Position)
		if err != nil {
			return fmt.Errorf("cannot decode slave %v position %v: %v", topoproto.TabletAliasString(&alias), status.Position, err)
		}
		if !masterElectPos.AtLeast(pos) {
			return fmt.Errorf("tablet %v is more advanced than master elect tablet %v: %v > %v", topoproto.TabletAliasString(&alias), topoproto.TabletAliasString(masterElectTabletAlias), status.Position, masterElectStatus)
		}
	}

	// Promote the masterElect
	wr.logger.Infof("promote slave %v", topoproto.TabletAliasString(masterElectTabletAlias))
	event.DispatchUpdate(ev, "promoting slave")
	rp, err := wr.tmc.PromoteSlave(ctx, masterElectTabletInfo)
	if err != nil {
		return fmt.Errorf("master-elect tablet %v failed to be upgraded to master: %v", topoproto.TabletAliasString(masterElectTabletAlias), err)
	}

	// Reset replication on all slaves to point to the new master, and
	// insert test row in the new master.
	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if topoproto.TabletAliasEqual(&alias, masterElectTabletAlias) {
			wgMaster.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", topoproto.TabletAliasString(&alias))
				masterErr = wr.TabletManagerClient().PopulateReparentJournal(ctx, tabletInfo, now, emergencyReparentShardOperation, &alias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias topodatapb.TabletAlias, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("setting new master on slave %v", topoproto.TabletAliasString(&alias))
				forceStartSlave := false
				if status, ok := statusMap[alias]; ok {
					forceStartSlave = status.SlaveIoRunning || status.SlaveSqlRunning
				}
				if err := wr.TabletManagerClient().SetMaster(ctx, tabletInfo, masterElectTabletAlias, now, forceStartSlave); err != nil {
					rec.RecordError(fmt.Errorf("Tablet %v SetMaster failed: %v", topoproto.TabletAliasString(&alias), err))
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
	wr.logger.Infof("updating shard record with new master %v", topoproto.TabletAliasString(masterElectTabletAlias))
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(s *topodatapb.Shard) error {
		s.MasterAlias = masterElectTabletAlias
		return nil
	}); err != nil {
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
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	_, err = wr.RebuildShardGraph(ctx, keyspace, shard, nil)
	return err
}
