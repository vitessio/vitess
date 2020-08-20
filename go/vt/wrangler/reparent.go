/*
Copyright 2019 The Vitess Authors.

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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	initShardMasterOperation            = "InitShardMaster"
	plannedReparentShardOperation       = "PlannedReparentShard"
	emergencyReparentShardOperation     = "EmergencyReparentShard"
	tabletExternallyReparentedOperation = "TabletExternallyReparented" //nolint
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
		} else if ti.IsReplicaType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := wr.tmc.ReplicationStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("ReplicationStatus(%v) failed: %v", ti.AliasString(), err))
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
		return fmt.Errorf("master %v and potential replica not in same keyspace/shard", topoproto.TabletAliasString(shardInfo.MasterAlias))
	}

	// and do the remote command
	return wr.tmc.SetMaster(ctx, ti.Tablet, shardInfo.MasterAlias, 0, "", false)
}

// InitShardMaster will make the provided tablet the master for the shard.
func (wr *Wrangler) InitShardMaster(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, force bool, waitReplicasTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("InitShardMaster(%v)", topoproto.TabletAliasString(masterElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.initShardMasterLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, force, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMaster: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMaster")
	}
	return err
}

func (wr *Wrangler) initShardMasterLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, force bool, waitReplicasTimeout time.Duration) error {
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

	// Create a context for the following RPCs that respects waitReplicasTimeout
	resetCtx, resetCancel := context.WithTimeout(ctx, waitReplicasTimeout)
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
		// if any of the replicas failed
		return err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new master to break its replicas, return its replication
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
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Now tell the new master to insert the reparent_journal row,
	// and tell everybody else to become a replica of the new master,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the master to be able to commit its row in the
	// reparent_journal table, it needs connected replicas.
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgReplicas := sync.WaitGroup{}
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
			wgReplicas.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgReplicas.Done()
				wr.logger.Infof("initializing replica %v", alias)
				if err := wr.tmc.InitReplica(replCtx, tabletInfo.Tablet, masterElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitReplica failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one replica is done).
	wgMaster.Wait()
	if masterErr != nil {
		// The master failed, there is no way the
		// replicas will work.  So we cancel them all.
		wr.logger.Warningf("master failed to PopulateReparentJournal, canceling replicas")
		replCancel()
		wgReplicas.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterElectTabletAlias) {
		if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = masterElectTabletAlias
			return nil
		}); err != nil {
			wgReplicas.Wait()
			return fmt.Errorf("failed to update shard master record: %v", err)
		}
	}

	// Wait for the replicas to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgReplicas.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Create database if necessary on the master. replicas will get it too through
	// replication. Since the user called InitShardMaster, they've told us to
	// assume that whatever data is on all the replicas is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(masterElectTabletInfo.Tablet)))
	if _, err := wr.tmc.ExecuteFetchAsDba(ctx, masterElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}
	// Refresh the state to force the tabletserver to reconnect after db has been created.
	if err := wr.tmc.RefreshState(ctx, masterElectTabletInfo.Tablet); err != nil {
		log.Warningf("RefreshState failed: %v", err)
	}

	return nil
}

// PlannedReparentShard will make the provided tablet the master for the shard,
// when both the current and new master are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias, avoidMasterAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) (err error) {
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
	err = wr.plannedReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, avoidMasterAlias, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished PlannedReparentShard")
	}
	return err
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias, avoidMasterTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) error {
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

	// Check invariants we're going to depend on.
	if topoproto.TabletAliasEqual(masterElectTabletAlias, avoidMasterTabletAlias) {
		return fmt.Errorf("master-elect tablet %v is the same as the tablet to avoid", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	if masterElectTabletAlias == nil {
		if !topoproto.TabletAliasEqual(avoidMasterTabletAlias, shardInfo.MasterAlias) {
			event.DispatchUpdate(ev, "current master is different than -avoid_master, nothing to do")
			return nil
		}
		event.DispatchUpdate(ev, "searching for master candidate")
		masterElectTabletAlias, err = wr.chooseNewMaster(ctx, shardInfo, tabletMap, avoidMasterTabletAlias, waitReplicasTimeout)
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
	if topoproto.TabletAliasIsZero(shardInfo.MasterAlias) {
		return fmt.Errorf("the shard has no master, use EmergencyReparentShard")
	}

	// Find the current master (if any) based on the tablet states. We no longer
	// trust the shard record for this, because it is updated asynchronously.
	currentMaster := wr.findCurrentMaster(tabletMap)

	var reparentJournalPos string

	if currentMaster == nil {
		// We don't know who the current master is. Either there is no current
		// master at all (no tablet claims to be MASTER), or there is no clear
		// winner (multiple MASTER tablets with the same timestamp).
		// Check if it's safe to promote the selected master candidate.
		wr.logger.Infof("No clear winner found for current master term; checking if it's safe to recover by electing %v", masterElectTabletAliasStr)

		// As we contact each tablet, we'll send its replication position here.
		type tabletPos struct {
			tabletAliasStr string
			tablet         *topodatapb.Tablet
			pos            mysql.Position
		}
		positions := make(chan tabletPos, len(tabletMap))

		// First stop the world, to ensure no writes are happening anywhere.
		// Since we don't trust that we know which tablets might be acting as
		// masters, we simply demote everyone.
		//
		// Unlike the normal, single-master case, we don't try to undo this if
		// we bail out. If we're here, it means there is no clear master, so we
		// don't know that it's safe to roll back to the previous state.
		// Leaving everything read-only is probably safer than whatever weird
		// state we were in before.
		//
		// If any tablets are unreachable, we can't be sure it's safe, because
		// one of the unreachable ones might have a replication position farther
		// ahead than the candidate master.
		wgStopAll := sync.WaitGroup{}
		rec := concurrency.AllErrorRecorder{}

		stopAllCtx, stopAllCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer stopAllCancel()

		for tabletAliasStr, tablet := range tabletMap {
			wgStopAll.Add(1)
			go func(tabletAliasStr string, tablet *topodatapb.Tablet) {
				defer wgStopAll.Done()

				// Regardless of what type this tablet thinks it is, we always
				// call DemoteMaster to ensure the underlying MySQL is read-only
				// and to check its replication position. DemoteMaster is
				// idempotent so it's fine to call it on a replica that's
				// already read-only.
				wr.logger.Infof("demote tablet %v", tabletAliasStr)
				masterStatus, err := wr.tmc.DemoteMaster(stopAllCtx, tablet)
				if err != nil {
					rec.RecordError(vterrors.Wrapf(err, "DemoteMaster failed on contested master %v", tabletAliasStr))
					return
				}
				pos, err := mysql.DecodePosition(masterStatus.Position)
				if err != nil {
					rec.RecordError(vterrors.Wrapf(err, "can't decode replication position for tablet %v", tabletAliasStr))
					return
				}
				positions <- tabletPos{
					tabletAliasStr: tabletAliasStr,
					tablet:         tablet,
					pos:            pos,
				}
			}(tabletAliasStr, tablet.Tablet)
		}
		wgStopAll.Wait()
		close(positions)
		if rec.HasErrors() {
			return vterrors.Wrap(rec.Error(), "failed to demote all tablets")
		}

		// Make a map of tablet positions.
		tabletPosMap := make(map[string]tabletPos, len(tabletMap))
		for tp := range positions {
			tabletPosMap[tp.tabletAliasStr] = tp
		}

		// Make sure no tablet has a replication position farther ahead than the
		// candidate master. It's up to our caller to choose a suitable
		// candidate, and to choose another one if this check fails.
		//
		// Note that we still allow replication to run during this time, but we
		// assume that no new high water mark can appear because we demoted all
		// tablets to read-only.
		//
		// TODO: Consider temporarily replicating from another tablet to catch up.
		tp, ok := tabletPosMap[masterElectTabletAliasStr]
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "master-elect tablet %v not found in tablet map", masterElectTabletAliasStr)
		}
		masterElectPos := tp.pos
		for _, tp := range tabletPosMap {
			// The master elect pos has to be at least as far as every tablet.
			if !masterElectPos.AtLeast(tp.pos) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v position (%v) contains transactions not found in master-elect %v position (%v)",
					tp.tabletAliasStr, tp.pos, masterElectTabletAliasStr, masterElectPos)
			}
		}

		// Check we still have the topology lock.
		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, "lost topology lock; aborting")
		}

		// Promote the selected candidate to master.
		promoteCtx, promoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer promoteCancel()
		rp, err := wr.tmc.PromoteReplica(promoteCtx, masterElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "failed to promote %v to master", masterElectTabletAliasStr)
		}
		reparentJournalPos = rp
	} else if topoproto.TabletAliasEqual(currentMaster.Alias, masterElectTabletAlias) {
		// It is possible that a previous attempt to reparent failed to SetReadWrite
		// so call it here to make sure underlying mysql is ReadWrite
		rwCtx, rwCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer rwCancel()

		if err := wr.tmc.SetReadWrite(rwCtx, masterElectTabletInfo.Tablet); err != nil {
			return vterrors.Wrapf(err, "failed to SetReadWrite on current master %v", masterElectTabletAliasStr)
		}
		// The master is already the one we want according to its tablet record.
		refreshCtx, refreshCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer refreshCancel()

		// Get the position so we can try to fix replicas (below).
		rp, err := wr.tmc.MasterPosition(refreshCtx, masterElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "failed to get replication position of current master %v", masterElectTabletAliasStr)
		}
		reparentJournalPos = rp
	} else {
		// There is already a master and it's not the one we want.
		oldMasterTabletInfo := currentMaster
		ev.OldMaster = *oldMasterTabletInfo.Tablet

		// Before demoting the old master, first make sure replication is
		// working from the old master to the candidate master. If it's not
		// working, we can't do a planned reparent because the candidate won't
		// catch up.
		wr.logger.Infof("Checking replication on master-elect %v", masterElectTabletAliasStr)

		// First we find the position of the current master. Note that this is
		// just a snapshot of the position since we let it keep accepting new
		// writes until we're sure we're going to proceed.
		snapshotCtx, snapshotCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer snapshotCancel()

		snapshotPos, err := wr.tmc.MasterPosition(snapshotCtx, currentMaster.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "can't get replication position on current master %v; current master must be healthy to perform planned reparent", currentMaster.AliasString())
		}

		// Now wait for the master-elect to catch up to that snapshot point.
		// If it catches up to that point within the waitReplicasTimeout,
		// we can be fairly confident it will catch up on everything that's
		// happened in the meantime once we demote the master to stop writes.
		//
		// We do this as an idempotent SetMaster to make sure the replica knows
		// who the current master is.
		setMasterCtx, setMasterCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer setMasterCancel()

		err = wr.tmc.SetMaster(setMasterCtx, masterElectTabletInfo.Tablet, currentMaster.Alias, 0, snapshotPos, true)
		if err != nil {
			return vterrors.Wrapf(err, "replication on master-elect %v did not catch up in time; replication must be healthy to perform planned reparent", masterElectTabletAliasStr)
		}

		// Check we still have the topology lock.
		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, "lost topology lock; aborting")
		}

		// Demote the old master and get its replication position. It's fine if
		// the old master was already demoted, since DemoteMaster is idempotent.
		wr.logger.Infof("demote current master %v", oldMasterTabletInfo.Alias)
		event.DispatchUpdate(ev, "demoting old master")

		demoteCtx, demoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer demoteCancel()

		masterStatus, err := wr.tmc.DemoteMaster(demoteCtx, oldMasterTabletInfo.Tablet)
		if err != nil {
			return fmt.Errorf("old master tablet %v DemoteMaster failed: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
		}

		waitCtx, waitCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer waitCancel()

		waitErr := wr.tmc.WaitForPosition(waitCtx, masterElectTabletInfo.Tablet, masterStatus.Position)
		if waitErr != nil || ctx.Err() == context.DeadlineExceeded {
			// If the new master fails to catch up within the timeout,
			// we try to roll back to the original master before aborting.
			// It is possible that we have used up the original context, or that
			// not enough time is left on it before it times out.
			// But at this point we really need to be able to Undo so as not to
			// leave the cluster in a bad state.
			// So we create a fresh context based on context.Background().
			undoCtx, undoCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
			defer undoCancel()
			if undoErr := wr.tmc.UndoDemoteMaster(undoCtx, oldMasterTabletInfo.Tablet); undoErr != nil {
				log.Warningf("Encountered error while trying to undo DemoteMaster: %v", undoErr)
			}
			if waitErr != nil {
				return vterrors.Wrapf(err, "master-elect tablet %v failed to catch up with replication", masterElectTabletAliasStr)
			}
			return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "PlannedReparent timed out, please try again.")
		}

		promoteCtx, promoteCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer promoteCancel()
		rp, err := wr.tmc.PromoteReplica(promoteCtx, masterElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "master-elect tablet %v failed to be upgraded to master - please try again", masterElectTabletAliasStr)
		}

		if ctx.Err() == context.DeadlineExceeded {
			// PromoteReplica succeeded but the context has expired. PRS needs to be re-run to complete
			return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "PlannedReparent timed out after promoting new master. Please re-run to fixup replicas.")
		}
		reparentJournalPos = rp
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, "lost topology lock, aborting")
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")

	// We add a (hopefully) unique record to the reparent journal table on the
	// new master so we can check if replicas got it through replication.
	reparentJournalTimestamp := time.Now().UnixNano()

	// Point all replicas at the new master and check that they receive the
	// reparent journal entry, proving they are replicating from the new master.
	// We do this concurrently with adding the journal entry (below), because
	// if semi-sync is enabled, the update to the journal table can't succeed
	// until at least one replica is successfully attached to the new master.
	wgReplicas := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		if alias == masterElectTabletAliasStr {
			continue
		}
		wgReplicas.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wgReplicas.Done()
			wr.logger.Infof("setting new master on replica %v", alias)

			// We used to force replica start on the old master, but now that
			// we support "resuming" a PRS attempt that failed, we can no
			// longer assume that we know who the old master was.
			// Instead, we rely on the old master to remember that it needs
			// to start replication after being converted to a replica.
			forceStartReplication := false

			if err := wr.tmc.SetMaster(replCtx, tabletInfo.Tablet, masterElectTabletAlias, reparentJournalTimestamp, "", forceStartReplication); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v SetMaster failed: %v", alias, err))
				return
			}
		}(alias, tabletInfo)
	}

	// Add a reparent journal entry on the new master.
	wr.logger.Infof("populating reparent journal on new master %v", masterElectTabletAliasStr)
	err = wr.tmc.PopulateReparentJournal(replCtx, masterElectTabletInfo.Tablet, reparentJournalTimestamp, plannedReparentShardOperation, masterElectTabletAlias, reparentJournalPos)
	if err != nil {
		// The master failed. There's no way the replicas will work, so cancel them all.
		wr.logger.Warningf("master failed to PopulateReparentJournal, canceling replica reparent attempts")
		replCancel()
		wgReplicas.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", err)
	}

	// Wait for the replicas to complete.
	wgReplicas.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some replicas failed to reparent; retry PlannedReparentShard with the same new master alias to retry failed replicas")
		return err
	}

	return nil
}

// findCurrentMaster returns the current master of a shard, if any.
//
// The tabletMap must be a complete map (not a partial result) for the shard.
//
// The current master is whichever MASTER tablet (if any) has the highest
// MasterTermStartTime, which is the same rule that vtgate uses to route master
// traffic.
//
// The return value is nil if the current master can't be definitively
// determined. This can happen either if no tablet claims to be MASTER, or if
// multiple MASTER tablets claim to have the same timestamp (a tie).
func (wr *Wrangler) findCurrentMaster(tabletMap map[string]*topo.TabletInfo) *topo.TabletInfo {
	var currentMaster *topo.TabletInfo
	var currentMasterTime time.Time

	for _, tablet := range tabletMap {
		// Only look at masters.
		if tablet.Type != topodatapb.TabletType_MASTER {
			continue
		}
		// Fill in first master we find.
		if currentMaster == nil {
			currentMaster = tablet
			currentMasterTime = tablet.GetMasterTermStartTime()
			continue
		}
		// If we find any other masters, compare timestamps.
		newMasterTime := tablet.GetMasterTermStartTime()
		if newMasterTime.After(currentMasterTime) {
			currentMaster = tablet
			currentMasterTime = newMasterTime
			continue
		}
		if newMasterTime.Equal(currentMasterTime) {
			// A tie shouldn't happen unless the upgrade order was violated
			// (some vttablets have not yet been upgraded) or if we get really
			// unlucky. However, if it does happen, we need to be safe and not
			// assume we know who the true master is.
			wr.logger.Warningf("Multiple masters (%v and %v) are tied for MasterTermStartTime; can't determine the true master.",
				topoproto.TabletAliasString(currentMaster.Alias),
				topoproto.TabletAliasString(tablet.Alias))
			return nil
		}
	}

	return currentMaster
}

// maxReplPosSearch is a struct helping to search for a tablet with the largest replication
// position querying status from all tablets in parallel.
type maxReplPosSearch struct {
	wrangler            *Wrangler
	ctx                 context.Context
	waitReplicasTimeout time.Duration
	waitGroup           sync.WaitGroup
	maxPosLock          sync.Mutex
	maxPos              mysql.Position
	maxPosTablet        *topodatapb.Tablet
}

func (maxPosSearch *maxReplPosSearch) processTablet(tablet *topodatapb.Tablet) {
	defer maxPosSearch.waitGroup.Done()
	maxPosSearch.wrangler.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	replicaStatusCtx, cancelReplicaStatus := context.WithTimeout(maxPosSearch.ctx, maxPosSearch.waitReplicasTimeout)
	defer cancelReplicaStatus()

	status, err := maxPosSearch.wrangler.tmc.ReplicationStatus(replicaStatusCtx, tablet)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return
	}
	replPos, err := mysql.DecodePosition(status.Position)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("cannot decode replica %v position %v: %v", topoproto.TabletAliasString(tablet.Alias), status.Position, err)
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
	waitReplicasTimeout time.Duration) (*topodatapb.TabletAlias, error) {

	if avoidMasterTabletAlias == nil {
		return nil, fmt.Errorf("tablet to avoid for reparent is not provided, cannot choose new master")
	}
	var masterCell string
	if shardInfo.MasterAlias != nil {
		masterCell = shardInfo.MasterAlias.Cell
	}

	maxPosSearch := maxReplPosSearch{
		wrangler:            wr,
		ctx:                 ctx,
		waitReplicasTimeout: waitReplicasTimeout,
		waitGroup:           sync.WaitGroup{},
		maxPosLock:          sync.Mutex{},
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
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration, ignoredTablets sets.String) (err error) {
	// lock the shard
	actionMsg := emergencyReparentShardOperation
	if masterElectTabletAlias != nil {
		actionMsg += fmt.Sprintf("(%v)", topoproto.TabletAliasString(masterElectTabletAlias))
	}
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, actionMsg)
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.emergencyReparentShardLocked(ctx, ev, keyspace, shard, masterElectTabletAlias, waitReplicasTimeout, ignoredTablets)
	if err != nil {
		event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished EmergencyReparentShard")
	}
	return err
}

func (wr *Wrangler) emergencyReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration, ignoredTablets sets.String) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for shard %v in keyspace %v: %v", shard, keyspace, err)
	}

	statusMap, masterStatusMap, err := wr.stopReplicationAndBuildStatusMaps(ctx, ev, tabletMap, waitReplicasTimeout, ignoredTablets)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	validCandidates, err := wr.findValidReparentCandidates(statusMap, masterStatusMap)
	if err != nil {
		return err
	}
	if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	errChan := make(chan error)
	rec := &concurrency.AllErrorRecorder{}
	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()
	for candidate := range validCandidates {
		go func(alias string) {
			var err error
			defer func() { errChan <- err }()
			err = wr.WaitForRelayLogsToApply(groupCtx, tabletMap[alias], statusMap[alias])
		}(candidate)
	}

	resultCounter := 0
	for waitErr := range errChan {
		resultCounter++
		if waitErr != nil {
			rec.RecordError(waitErr)
			groupCancel()
		}
		if resultCounter == len(validCandidates) {
			break
		}
	}
	if len(rec.Errors) != 0 {
		return vterrors.Wrapf(rec.Error(), "could not apply all relay logs within the provided wait_replicas_timeout: %v", rec.Error())
	}

	var winningPosition mysql.Position
	var newMasterTabletAliasStr string
	for alias, position := range validCandidates {
		if winningPosition.IsZero() {
			winningPosition = position
			newMasterTabletAliasStr = alias
			continue
		}
		if position.AtLeast(winningPosition) {
			winningPosition = position
			newMasterTabletAliasStr = alias
		}
	}

	if masterElectTabletAlias != nil {
		newMasterTabletAliasStr = topoproto.TabletAliasString(masterElectTabletAlias)
		masterPos, ok := validCandidates[newMasterTabletAliasStr]
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "master elect %v has errant GTIDs", newMasterTabletAliasStr)
		}
		if !masterPos.AtLeast(winningPosition) {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "master elect: %v at position %v, is not fully caught up. Winning position: %v", newMasterTabletAliasStr, masterPos, winningPosition)
		}
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// Promote the masterElect
	wr.logger.Infof("promote tablet %v to master", newMasterTabletAliasStr)
	event.DispatchUpdate(ev, "promoting replica")
	rp, err := wr.tmc.PromoteReplica(ctx, tabletMap[newMasterTabletAliasStr].Tablet)
	if err != nil {
		return vterrors.Wrapf(err, "master-elect tablet %v failed to be upgraded to master: %v", newMasterTabletAliasStr, err)
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Reset replication on all replicas to point to the new master, and
	// insert test row in the new master.
	// Go through all the tablets:
	// - new master: populate the reparent journal
	// - everybody else: reparent to new master, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	errChan = make(chan error)

	handleMaster := func(alias string, tabletInfo *topo.TabletInfo) error {
		wr.logger.Infof("populating reparent journal on new master %v", alias)
		return wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, emergencyReparentShardOperation, tabletMap[newMasterTabletAliasStr].Alias, rp)
	}
	handleReplica := func(alias string, tabletInfo *topo.TabletInfo) {
		var err error
		defer func() { errChan <- err }()

		wr.logger.Infof("setting new master on replica %v", alias)
		forceStart := false
		if status, ok := statusMap[alias]; ok {
			forceStart = replicaWasRunning(status)
		}
		err = wr.tmc.SetMaster(replCtx, tabletInfo.Tablet, tabletMap[newMasterTabletAliasStr].Alias, now, "", forceStart)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetMaster failed: %v", alias, err)
		}
	}

	for alias, tabletInfo := range tabletMap {
		if alias == newMasterTabletAliasStr {
			continue
		} else if !ignoredTablets.Has(alias) {
			go handleReplica(alias, tabletInfo)
		}
	}

	masterErr := handleMaster(newMasterTabletAliasStr, tabletMap[newMasterTabletAliasStr])
	if masterErr != nil {
		wr.logger.Warningf("master failed to PopulateReparentJournal")
		replCancel()
		return vterrors.Wrapf(masterErr, "failed to PopulateReparentJournal on master: %v", masterErr)
	}

	return nil
}

// waitOnNMinusOneTablets will wait until N-1 tablets have responded via a supplied error channel. In that case that N-1 tablets have responded,
// the supplied cancel function will be called, and we will wait until N tablets return their errors, and then return an AllErrorRecorder to the caller.
func waitOnNMinusOneTablets(ctxCancel context.CancelFunc, tabletCount int, errorChannel chan error, acceptableErrCnt int) *concurrency.AllErrorRecorder {
	errCounter := 0
	successCounter := 0
	responseCounter := 0
	rec := &concurrency.AllErrorRecorder{}

	for err := range errorChannel {
		responseCounter++
		if err != nil {
			errCounter++
			rec.RecordError(err)
		} else {
			successCounter++
		}
		if responseCounter == tabletCount {
			// We must wait for any cancelled goroutines to return their error.
			break
		}
		if errCounter > acceptableErrCnt || successCounter == tabletCount-1 {
			ctxCancel()
		}
	}

	return rec
}

// findValidReparentCandidates will find valid candidates for emergency reparent, and if successful, returning them as a list of tablet aliases.
func (wr *Wrangler) findValidReparentCandidates(statusMap map[string]*replicationdatapb.StopReplicationStatus, masterStatusMap map[string]*replicationdatapb.MasterStatus) (map[string]mysql.Position, error) {
	// Build out replication status list from proto types.
	replicationStatusMap := make(map[string]*mysql.ReplicationStatus, len(statusMap))
	for alias, protoStatus := range statusMap {
		status := mysql.ProtoToReplicationStatus(protoStatus.After)
		replicationStatusMap[alias] = &status
	}

	// Determine if we need to find errant GTIDs.
	var gtidBased *bool
	for alias, status := range replicationStatusMap {
		if gtidBased == nil {
			_, ok := status.RelayLogPosition.GTIDSet.(mysql.Mysql56GTIDSet)
			gtidBased = pointer.BoolPtr(ok)
		} else if !*gtidBased {
			break
		} else if status.RelayLogPosition.IsZero() {
			// Bail. We have an odd one in the bunch.
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "encountered tablet %v with no relay log position, when at least one other tablet in the status map has GTID based relay log positions", alias)
		}
	}

	// Create relevant position list of errant GTID based positions for later comparison.
	positionMap := make(map[string]mysql.Position)
	for alias, status := range replicationStatusMap {
		// Find errantGTIDs and clean them from status map if relevant.
		if *gtidBased {
			// We need to remove this status from a copy of the list, otherwise the diff will be empty always.
			statusList := make([]*mysql.ReplicationStatus, 0, len(replicationStatusMap)-1)
			for a, s := range replicationStatusMap {
				if a != alias {
					statusList = append(statusList, s)
				}
			}
			relayLogGTIDSet, ok := status.RelayLogPosition.GTIDSet.(mysql.Mysql56GTIDSet)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "we got a filled in relay log position, but it's not of type Mysql56GTIDSet, even though we've determined we need to use GTID based assessment")
			}
			errantGTIDs, err := status.FindErrantGTIDs(statusList)
			if err != nil {
				// Could not find errant GTIDs when we must.
				return nil, err
			}
			if len(errantGTIDs) != 0 {
				// Skip inserting this tablet. It's not a valid candidate.
				continue
			}

			pos := mysql.Position{GTIDSet: relayLogGTIDSet}
			positionMap[alias] = pos
		} else {
			positionMap[alias] = status.Position
		}
	}

	for alias, masterStatus := range masterStatusMap {
		executedPosition, err := mysql.DecodePosition(masterStatus.Position)
		if err != nil {
			return nil, vterrors.Wrapf(err, "could not decode a master status executed position for tablet %v: %v", alias, err)
		}
		positionMap[alias] = executedPosition
	}

	return positionMap, nil
}

func (wr *Wrangler) stopReplicationAndBuildStatusMaps(ctx context.Context, ev *events.Reparent, tabletMap map[string]*topo.TabletInfo, waitReplicasTimeout time.Duration, ignoredTablets sets.String) (map[string]*replicationdatapb.StopReplicationStatus, map[string]*replicationdatapb.MasterStatus, error) {
	// Stop replication on all replicas, get their current
	// replication position
	event.DispatchUpdate(ev, "stop replication on all replicas")
	statusMap := make(map[string]*replicationdatapb.StopReplicationStatus)
	masterStatusMap := make(map[string]*replicationdatapb.MasterStatus)
	mu := sync.Mutex{}

	errChan := make(chan error)
	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()
	fillStatus := func(alias string, tabletInfo *topo.TabletInfo) {
		err := vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "fillStatus did not successfully complete")
		defer func() { errChan <- err }()

		wr.logger.Infof("getting replication position from %v", alias)
		var stopReplicationStatus *replicationdatapb.StopReplicationStatus
		_, stopReplicationStatus, err = wr.tmc.StopReplicationAndGetStatus(groupCtx, tabletInfo.Tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
		switch err {
		case mysql.ErrNotReplica:
			var masterStatus *replicationdatapb.MasterStatus
			masterStatus, err = wr.tmc.DemoteMaster(groupCtx, tabletInfo.Tablet)
			if err != nil {
				wr.logger.Warningf("replica %v thinks it's master but we failed to demote it", alias)
				err = vterrors.Wrapf(err, "replica %v thinks it's master but we failed to demote it: %v", alias, err)
				return
			}
			mu.Lock()
			masterStatusMap[alias] = masterStatus
			mu.Unlock()

		case nil:
			mu.Lock()
			statusMap[alias] = stopReplicationStatus
			mu.Unlock()

		default:
			wr.logger.Warningf("failed to get replication status from %v: %v", alias, err)
			err = vterrors.Wrapf(err, "error when getting replication status for alias %v: %v", alias, err)
		}
	}

	for alias, tabletInfo := range tabletMap {
		if !ignoredTablets.Has(alias) {
			go fillStatus(alias, tabletInfo)
		}
	}

	errRecorder := waitOnNMinusOneTablets(groupCancel, len(tabletMap)-ignoredTablets.Len(), errChan, 1)

	if len(errRecorder.Errors) > 1 {
		return nil, nil, vterrors.Wrapf(errRecorder.Error(), "encountered more than one error when trying to stop replication and get positions: %v", errRecorder.Error())
	}
	return statusMap, masterStatusMap, nil
}

// WaitForRelayLogsToApply will block execution waiting for the given tablets relay logs to apply, unless the supplied
// context is cancelled, or waitReplicasTimeout is exceeded.
func (wr *Wrangler) WaitForRelayLogsToApply(ctx context.Context, tabletInfo *topo.TabletInfo, status *replicationdatapb.StopReplicationStatus) error {
	var err error
	if status.After.RelayLogPosition != "" {
		err = wr.tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.RelayLogPosition)
	} else {
		err = wr.tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.FileRelayLogPosition)
	}
	return err
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

func replicaWasRunning(stopReplicationStatus *replicationdatapb.StopReplicationStatus) bool {
	return stopReplicationStatus.Before.IoThreadRunning || stopReplicationStatus.Before.SqlThreadRunning
}
