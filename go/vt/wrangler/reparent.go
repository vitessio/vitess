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

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	plannedReparentShardOperation       = "PlannedReparentShard"
	emergencyReparentShardOperation     = "EmergencyReparentShard"     //nolint
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
	err = grpcvtctldserver.NewVtctldServer(wr.ts).InitShardPrimaryLocked(ctx, ev, &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                keyspace,
		Shard:                   shard,
		PrimaryElectTabletAlias: masterElectTabletAlias,
		Force:                   force,
	}, waitReplicasTimeout, wr.tmc, wr.logger)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMaster: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMaster")
	}
	return err
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
		masterElectTabletAlias, err = reparentutil.ChooseNewPrimary(ctx, wr.tmc, shardInfo, tabletMap, avoidMasterTabletAlias, waitReplicasTimeout, wr.logger)
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
	currentMaster := reparentutil.FindCurrentPrimary(tabletMap, wr.logger)

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

// EmergencyReparentShard will make the provided tablet the master for
// the shard, when the old master is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, masterElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration, ignoredTablets sets.String) (err error) {
	_, err = reparentutil.NewEmergencyReparenter(wr.ts, wr.tmc, wr.logger).ReparentShard(
		ctx,
		keyspace,
		shard,
		reparentutil.EmergencyReparentOptions{
			NewPrimaryAlias:     masterElectTabletAlias,
			WaitReplicasTimeout: waitReplicasTimeout,
			IgnoreReplicas:      ignoredTablets,
		},
	)

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
