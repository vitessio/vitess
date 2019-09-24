/*
Copyright 2017 Google Inc.

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

/*
This file handles the reparenting operations.
*/

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	initShardMasterOperation            = "InitShardMaster"
	plannedReparentShardOperation       = "PlannedReparentShard"
	emergencyReparentShardOperation     = "EmergencyReparentShard"
	tabletExternallyReparentedOperation = "TabletExternallyReparented"
)

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)

	wr.logger.Infof("Gathering tablet replication status for: %v", tablets)
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
				pos, err := wr.tmc.MasterPosition(ctx, ti.Tablet)
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
				status, err := wr.tmc.SlaveStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("SlaveStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return tablets, result, rec.Error()
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
	return wr.tmc.SetMaster(ctx, ti.Tablet, shardInfo.MasterAlias, 0, false)
}

// InitShardMaster will make the provided tablet the master for the shard.
func (wr *Wrangler) InitShardMaster(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("InitShardMaster(%v)", topoproto.TabletAliasString(masterElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.initShardMasterLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, force, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMaster: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMaster")
	}
	return err
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

	// Check the master elect is in tabletMap.
	masterElectTabletAliasStr := topoproto.TabletAliasString(masterElectTabletAlias)
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAliasStr]
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
	if _, ok := masterTabletMap[masterElectTabletAliasStr]; !ok {
		if !force {
			return fmt.Errorf("master-elect tablet %v is not a master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(masterElectTabletAlias))
		}
		wr.logger.Warningf("master-elect tablet %v is not a master in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	haveOtherMaster := false
	for alias := range masterTabletMap {
		if masterElectTabletAliasStr != alias {
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

	// Create a context for the following RPCs that respects waitSlaveTimeout
	resetCtx, resetCancel := context.WithTimeout(ctx, waitSlaveTimeout)
	defer resetCancel()

	event.DispatchUpdate(ev, "resetting replication on all tablets")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("resetting replication on tablet %v", alias)
			if err := wr.tmc.ResetReplication(resetCtx, tabletInfo.Tablet); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v ResetReplication failed (either fix it, or Scrap it): %v", alias, err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		// if any of the slaves failed
		return err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new master to break its slaves, return its replication
	// position
	wr.logger.Infof("initializing master on %v", topoproto.TabletAliasString(masterElectTabletAlias))
	event.DispatchUpdate(ev, "initializing master")
	rp, err := wr.tmc.InitMaster(ctx, masterElectTabletInfo.Tablet)
	if err != nil {
		return err
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitSlaveTimeout)
	defer replCancel()

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
		if alias == masterElectTabletAliasStr {
			wgMaster.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, initShardMasterOperation, masterElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("initializing slave %v", alias)
				if err := wr.tmc.InitSlave(replCtx, tabletInfo.Tablet, masterElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitSlave failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one slave is done).
	wgMaster.Wait()
	if masterErr != nil {
		// The master failed, there is no way the
		// slaves will work.  So we cancel them all.
		wr.logger.Warningf("master failed to PopulateReparentJournal, canceling slaves")
		replCancel()
		wgSlaves.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = masterElectTabletAlias
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
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(masterElectTabletInfo.Tablet)))
	if _, err := wr.tmc.ExecuteFetchAsDba(ctx, masterElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	return nil
}

// PlannedReparentShard will make the provided tablet the master for the shard,
// when both the current and new master are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias, avoidMasterAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) (err error) {
	// lock the shard
	lockAction := fmt.Sprintf(
		"PlannedReparentShard(%v, avoid_master=%v)",
		topoproto.TabletAliasString(masterElectTabletAlias),
		topoproto.TabletAliasString(avoidMasterAlias))
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, lockAction)
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// Attempt to set avoidMasterAlias if not provided by parameters
	if masterElectTabletAlias == nil && avoidMasterAlias == nil {
		shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return err
		}
		avoidMasterAlias = shardInfo.MasterAlias
	}

	// do the work
	err = wr.plannedReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, avoidMasterAlias, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished PlannedReparentShard")
	}
	return err
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias, avoidMasterTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) error {
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
	if topoproto.TabletAliasEqual(masterElectTabletAlias, avoidMasterTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is the same as the tablet to avoid", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	if masterElectTabletAlias == nil {
		if !topoproto.TabletAliasEqual(avoidMasterTabletAlias, shardInfo.MasterAlias) {
			event.DispatchUpdate(ev, "current master is different than -avoid_master, nothing to do")
			return nil
		}
		event.DispatchUpdate(ev, "searching for master candidate")
		masterElectTabletAlias, err = wr.chooseNewMaster(ctx, shardInfo, tabletMap, avoidMasterTabletAlias, waitSlaveTimeout)
		if err != nil {
			return err
		}
		if masterElectTabletAlias == nil {
			return fmt.Errorf("cannot find a tablet to reparent to")
		}
		wr.logger.Infof("elected new master candidate %v", topoproto.TabletAliasString(masterElectTabletAlias))
		event.DispatchUpdate(ev, "elected new master candidate")
	}
	masterElectTabletAliasStr := topoproto.TabletAliasString(masterElectTabletAlias)
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", masterElectTabletAliasStr)
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is already the master", masterElectTabletAliasStr)
	}
	if topoproto.TabletAliasIsZero(shardInfo.MasterAlias) {
		return fmt.Errorf("the shard has no master, use EmergencyReparentShard")
	}
	oldMasterTabletInfo, ok := tabletMap[topoproto.TabletAliasString(shardInfo.MasterAlias)]
	if !ok {
		return fmt.Errorf("old master tablet %v is not in the shard", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}
	ev.OldMaster = *oldMasterTabletInfo.Tablet

	// create a new context for the short running remote operations
	remoteCtx, remoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer remoteCancel()

	// Demote the current master, get its replication position
	wr.logger.Infof("demote current master %v", shardInfo.MasterAlias)
	event.DispatchUpdate(ev, "demoting old master")
	rp, err := wr.tmc.DemoteMaster(remoteCtx, oldMasterTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("old master tablet %v DemoteMaster failed: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
	}

	remoteCtx, remoteCancel = context.WithTimeout(ctx, waitSlaveTimeout)
	defer remoteCancel()

	// Wait on the master-elect tablet until it reaches that position,
	// then promote it
	wr.logger.Infof("promote slave %v", masterElectTabletAliasStr)
	event.DispatchUpdate(ev, "promoting slave")
	rp, err = wr.tmc.PromoteSlaveWhenCaughtUp(remoteCtx, masterElectTabletInfo.Tablet, rp)
	if err != nil || (ctx.Err() != nil && ctx.Err() == context.DeadlineExceeded) {
		remoteCancel()
		// if this fails it is not enough to return an error. we should rollback all the changes made by DemoteMaster
		remoteCtx, remoteCancel = context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer remoteCancel()
		if err1 := wr.tmc.UndoDemoteMaster(remoteCtx, oldMasterTabletInfo.Tablet); err1 != nil {
			log.Warningf("Encountered error %v while trying to undo DemoteMaster", err1)
		}
		return fmt.Errorf("master-elect tablet %v failed to catch up with replication or be upgraded to master: %v", masterElectTabletAliasStr, err)
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitSlaveTimeout)
	defer replCancel()

	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgSlaves := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var masterErr error
	oldMasterTabletInfoAliasStr := topoproto.TabletAliasString(oldMasterTabletInfo.Alias)
	for alias, tabletInfo := range tabletMap {
		if alias == masterElectTabletAliasStr {
			wgMaster.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, plannedReparentShardOperation, masterElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("setting new master on slave %v", alias)
				// also restart replication on old master
				forceStartSlave := alias == oldMasterTabletInfoAliasStr
				if err := wr.tmc.SetMaster(replCtx, tabletInfo.Tablet, masterElectTabletAlias, now, forceStartSlave); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v SetMaster failed: %v", alias, err))
					return
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one slave is done)
	wgMaster.Wait()
	if masterErr != nil {
		// The master failed, there is no way the
		// slaves will work.  So we cancel them all.
		wr.logger.Warningf("master failed to PopulateReparentJournal, canceling slaves")
		replCancel()
		wgSlaves.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	wr.logger.Infof("updating shard record with new master %v", masterElectTabletAlias)
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = masterElectTabletAlias
		return nil
	}); err != nil {
		wgSlaves.Wait()
		return fmt.Errorf("failed to update shard master record: %v", err)
	}

	// Wait for the slaves to complete.
	wgSlaves.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some slaves failed to reparent")
		return err
	}

	return nil
}

// maxReplPosSearch is a struct helping to search for a tablet with the largest replication
// position querying status from all tablets in parallel.
type maxReplPosSearch struct {
	wrangler         *Wrangler
	ctx              context.Context
	waitSlaveTimeout time.Duration
	waitGroup        sync.WaitGroup
	maxPosLock       sync.Mutex
	maxPos           mysql.Position
	maxPosTablet     *topodatapb.Tablet
}

func (maxPosSearch *maxReplPosSearch) processTablet(tablet *topodatapb.Tablet) {
	defer maxPosSearch.waitGroup.Done()
	maxPosSearch.wrangler.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	slaveStatusCtx, cancelSlaveStatus := context.WithTimeout(maxPosSearch.ctx, maxPosSearch.waitSlaveTimeout)
	defer cancelSlaveStatus()

	status, err := maxPosSearch.wrangler.tmc.SlaveStatus(slaveStatusCtx, tablet)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return
	}
	replPos, err := mysql.DecodePosition(status.Position)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("cannot decode slave %v position %v: %v", topoproto.TabletAliasString(tablet.Alias), status.Position, err)
		return
	}

	maxPosSearch.maxPosLock.Lock()
	if maxPosSearch.maxPosTablet == nil || !maxPosSearch.maxPos.AtLeast(replPos) {
		maxPosSearch.maxPos = replPos
		maxPosSearch.maxPosTablet = tablet
	}
	maxPosSearch.maxPosLock.Unlock()
}

// chooseNewMaster finds a tablet that is going to become master after reparent. The criteria
// for the new master-elect are (preferably) to be in the same cell as the current master, and
// to be different from avoidMasterTabletAlias. The tablet with the largest replication
// position is chosen to minimize the time of catching up with the master. Note that the search
// for largest replication position will race with transactions being executed on the master at
// the same time, so when all tablets are roughly at the same position then the choice of the
// new master-elect will be somewhat unpredictable.
func (wr *Wrangler) chooseNewMaster(
	ctx context.Context,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidMasterTabletAlias *topodatapb.TabletAlias,
	waitSlaveTimeout time.Duration) (*topodatapb.TabletAlias, error) {

	if avoidMasterTabletAlias == nil {
		return nil, fmt.Errorf("tablet to avoid for reparent is not provided, cannot choose new master")
	}
	var masterCell string
	if shardInfo.MasterAlias != nil {
		masterCell = shardInfo.MasterAlias.Cell
	}

	maxPosSearch := maxReplPosSearch{
		wrangler:         wr,
		ctx:              ctx,
		waitSlaveTimeout: waitSlaveTimeout,
		waitGroup:        sync.WaitGroup{},
		maxPosLock:       sync.Mutex{},
	}
	for _, tabletInfo := range tabletMap {
		if (masterCell != "" && tabletInfo.Alias.Cell != masterCell) ||
			topoproto.TabletAliasEqual(tabletInfo.Alias, avoidMasterTabletAlias) ||
			tabletInfo.Tablet.Type != topodatapb.TabletType_REPLICA {
			continue
		}
		maxPosSearch.waitGroup.Add(1)
		go maxPosSearch.processTablet(tabletInfo.Tablet)
	}
	maxPosSearch.waitGroup.Wait()

	if maxPosSearch.maxPosTablet == nil {
		return nil, nil
	}
	return maxPosSearch.maxPosTablet.Alias, nil
}

// EmergencyReparentShard will make the provided tablet the master for
// the shard, when the old master is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitSlaveTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("EmergencyReparentShard(%v)", topoproto.TabletAliasString(masterElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.emergencyReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, waitSlaveTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished EmergencyReparentShard")
	}
	return err
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
	masterElectTabletAliasStr := topoproto.TabletAliasString(masterElectTabletAlias)
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", masterElectTabletAliasStr)
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is already the master", topoproto.TabletAliasString(masterElectTabletAlias))
	}

	// Deal with the old master: try to remote-scrap it, if it's
	// truly dead we force-scrap it. Remove it from our map in any case.
	if shardInfo.HasMaster() {
		deleteOldMaster := true
		shardInfoMasterAliasStr := topoproto.TabletAliasString(shardInfo.MasterAlias)
		oldMasterTabletInfo, ok := tabletMap[shardInfoMasterAliasStr]
		if ok {
			delete(tabletMap, shardInfoMasterAliasStr)
		} else {
			oldMasterTabletInfo, err = wr.ts.GetTablet(ctx, shardInfo.MasterAlias)
			if err != nil {
				wr.logger.Warningf("cannot read old master tablet %v, won't touch it: %v", shardInfoMasterAliasStr, err)
				deleteOldMaster = false
			}
		}

		if deleteOldMaster {
			ev.OldMaster = *oldMasterTabletInfo.Tablet
			wr.logger.Infof("deleting old master %v", shardInfoMasterAliasStr)

			ctx, cancel := context.WithTimeout(ctx, waitSlaveTimeout)
			defer cancel()

			if err := topotools.DeleteTablet(ctx, wr.ts, oldMasterTabletInfo.Tablet); err != nil {
				wr.logger.Warningf("failed to delete old master tablet %v: %v", shardInfoMasterAliasStr, err)
			}
		}
	}

	// Stop replication on all slaves, get their current
	// replication position
	event.DispatchUpdate(ev, "stop replication on all slaves")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	statusMap := make(map[string]*replicationdatapb.Status)
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("getting replication position from %v", alias)
			ctx, cancel := context.WithTimeout(ctx, waitSlaveTimeout)
			defer cancel()
			rp, err := wr.tmc.StopReplicationAndGetStatus(ctx, tabletInfo.Tablet)
			if err != nil {
				wr.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", alias, err)
				return
			}
			mu.Lock()
			statusMap[alias] = rp
			mu.Unlock()
		}(alias, tabletInfo)
	}
	wg.Wait()

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Verify masterElect is alive and has the most advanced position
	masterElectStatus, ok := statusMap[masterElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("couldn't get master elect %v replication position", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	masterElectPos, err := mysql.DecodePosition(masterElectStatus.Position)
	if err != nil {
		return fmt.Errorf("cannot decode master elect position %v: %v", masterElectStatus.Position, err)
	}
	for alias, status := range statusMap {
		if alias == masterElectTabletAliasStr {
			continue
		}
		pos, err := mysql.DecodePosition(status.Position)
		if err != nil {
			return fmt.Errorf("cannot decode slave %v position %v: %v", alias, status.Position, err)
		}
		if !masterElectPos.AtLeast(pos) {
			return fmt.Errorf("tablet %v is more advanced than master elect tablet %v: %v > %v", alias, masterElectTabletAliasStr, status.Position, masterElectStatus)
		}
	}

	// Promote the masterElect
	wr.logger.Infof("promote slave %v", topoproto.TabletAliasString(masterElectTabletAlias))
	event.DispatchUpdate(ev, "promoting slave")
	rp, err := wr.tmc.PromoteSlave(ctx, masterElectTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("master-elect tablet %v failed to be upgraded to master: %v", topoproto.TabletAliasString(masterElectTabletAlias), err)
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithCancel(ctx)
	defer replCancel()

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
		if alias == masterElectTabletAliasStr {
			wgMaster.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				wr.logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, emergencyReparentShardOperation, masterElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSlaves.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSlaves.Done()
				wr.logger.Infof("setting new master on slave %v", alias)
				forceStartSlave := false
				if status, ok := statusMap[alias]; ok {
					forceStartSlave = status.SlaveIoRunning || status.SlaveSqlRunning
				}
				if err := wr.tmc.SetMaster(replCtx, tabletInfo.Tablet, masterElectTabletAlias, now, forceStartSlave); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v SetMaster failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one slave is done)
	wgMaster.Wait()
	if masterErr != nil {
		// The master failed, there is no way the
		// slaves will work.  So we cancel them all.
		wr.logger.Warningf("master failed to PopulateReparentJournal, canceling slaves")
		replCancel()
		wgSlaves.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	wr.logger.Infof("updating shard record with new master %v", topoproto.TabletAliasString(masterElectTabletAlias))
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = masterElectTabletAlias
		return nil
	}); err != nil {
		wgSlaves.Wait()
		return fmt.Errorf("failed to update shard master record: %v", err)
	}

	// Wait for the slaves to complete. If some of them fail, we
	// will rebuild the shard serving graph anyway
	wgSlaves.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some slaves failed to reparent")
		return err
	}

	return nil
}

// TabletExternallyReparented changes the type of new master for this shard to MASTER
// and updates it's tablet record in the topo. Updating the shard record is handled
// by the new master tablet
func (wr *Wrangler) TabletExternallyReparented(ctx context.Context, newMasterAlias *topodatapb.TabletAlias) error {

	tabletInfo, err := wr.ts.GetTablet(ctx, newMasterAlias)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read tablet record for %v: %v", newMasterAlias, err)
		return err
	}

	// Check the global shard record.
	tablet := tabletInfo.Tablet
	si, err := wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// We update the tablet only if it is not currently master
	if tablet.Type != topodatapb.TabletType_MASTER {
		log.Infof("TabletExternallyReparented: executing tablet type change to MASTER")

		// Create a reusable Reparent event with available info.
		ev := &events.Reparent{
			ShardInfo: *si,
			NewMaster: *tablet,
			OldMaster: topodatapb.Tablet{
				Alias: si.MasterAlias,
				Type:  topodatapb.TabletType_MASTER,
			},
		}
		defer func() {
			if err != nil {
				event.DispatchUpdate(ev, "failed: "+err.Error())
			}
		}()
		event.DispatchUpdate(ev, "starting external reparent")

		if err := wr.tmc.ChangeType(ctx, tablet, topodatapb.TabletType_MASTER); err != nil {
			log.Warningf("Error calling ChangeType on new master %v: %v", topoproto.TabletAliasString(newMasterAlias), err)
			return err
		}
		event.DispatchUpdate(ev, "finished")
	}
	return nil
}
