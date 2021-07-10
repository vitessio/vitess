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

package reparentutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// PlannedReparenter performs PlannedReparentShard operations.
type PlannedReparenter struct {
	ts     *topo.Server
	tmc    tmclient.TabletManagerClient
	logger logutil.Logger
}

// PlannedReparentOptions provides optional parameters to PlannedReparentShard
// operations. Options are passed by value, so it is safe for callers to mutate
// resue options structs for multiple calls.
type PlannedReparentOptions struct {
	NewPrimaryAlias     *topodatapb.TabletAlias
	AvoidPrimaryAlias   *topodatapb.TabletAlias
	WaitReplicasTimeout time.Duration

	// Private options managed internally. We use value-passing semantics to
	// set these options inside a PlannedReparent without leaking these details
	// back out to the caller.

	lockAction string
}

// NewPlannedReparenter returns a new PlannedReparenter object, ready to perform
// PlannedReparentShard operations using the given topo.Server,
// TabletManagerClient, and logger.
//
// Providing a nil logger instance is allowed.
func NewPlannedReparenter(ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger) *PlannedReparenter {
	pr := PlannedReparenter{
		ts:     ts,
		tmc:    tmc,
		logger: logger,
	}

	if pr.logger == nil {
		// Create a no-op logger so we can call functions on pr.logger without
		// needing to constantly check it for non-nil first.
		pr.logger = logutil.NewCallbackLogger(func(e *logutilpb.Event) {})
	}

	return &pr
}

// ReparentShard performs a PlannedReparentShard operation on the given keyspace
// and shard. It will make the provided tablet the primary for the shard, when
// both the current and desired primary are reachable and in a good state.
func (pr *PlannedReparenter) ReparentShard(ctx context.Context, keyspace string, shard string, opts PlannedReparentOptions) (*events.Reparent, error) {
	opts.lockAction = pr.getLockAction(opts)

	ctx, unlock, err := pr.ts.LockShard(ctx, keyspace, shard, opts.lockAction)
	if err != nil {
		return nil, err
	}

	defer unlock(&err)

	if opts.NewPrimaryAlias == nil && opts.AvoidPrimaryAlias == nil {
		shardInfo, err := pr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, err
		}

		opts.AvoidPrimaryAlias = shardInfo.MasterAlias
	}

	ev := &events.Reparent{}
	defer func() {
		switch err {
		case nil:
			event.DispatchUpdate(ev, "finished PlannedReparentShard")
		default:
			event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
		}
	}()

	err = pr.reparentShardLocked(ctx, ev, keyspace, shard, opts)

	return ev, err
}

func (pr *PlannedReparenter) getLockAction(opts PlannedReparentOptions) string {
	return fmt.Sprintf(
		"PlannedReparentShard(%v, AvoidPrimary = %v)",
		topoproto.TabletAliasString(opts.NewPrimaryAlias),
		topoproto.TabletAliasString(opts.AvoidPrimaryAlias),
	)
}

// preflightChecks checks some invariants that pr.reparentShardLocked() depends
// on. It returns a boolean to indicate if the reparent is a no-op (which
// happens iff the caller specified an AvoidPrimaryAlias and it's not the shard
// primary), as well as an error.
//
// It will also set the NewPrimaryAlias option if the caller did not specify
// one, provided it can choose a new primary candidate. See ChooseNewPrimary()
// for details on primary candidate selection.
func (pr *PlannedReparenter) preflightChecks(
	ctx context.Context,
	ev *events.Reparent,
	keyspace string,
	shard string,
	tabletMap map[string]*topo.TabletInfo,
	opts *PlannedReparentOptions, // we take a pointer here to set NewPrimaryAlias
) (isNoop bool, err error) {
	if topoproto.TabletAliasEqual(opts.NewPrimaryAlias, opts.AvoidPrimaryAlias) {
		return true, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary-elect tablet %v is the same as the tablet to avoid", topoproto.TabletAliasString(opts.NewPrimaryAlias))
	}

	if opts.NewPrimaryAlias == nil {
		if !topoproto.TabletAliasEqual(opts.AvoidPrimaryAlias, ev.ShardInfo.MasterAlias) {
			event.DispatchUpdate(ev, "current primary is different than AvoidPrimary, nothing to do")
			return true, nil
		}

		event.DispatchUpdate(ev, "searching for primary candidate")

		opts.NewPrimaryAlias, err = ChooseNewPrimary(ctx, pr.tmc, &ev.ShardInfo, tabletMap, opts.AvoidPrimaryAlias, opts.WaitReplicasTimeout, pr.logger)
		if err != nil {
			return true, err
		}

		if opts.NewPrimaryAlias == nil {
			return true, vterrors.Errorf(vtrpc.Code_INTERNAL, "cannot find a tablet to reparent to")
		}

		pr.logger.Infof("elected new primary candidate %v", topoproto.TabletAliasString(opts.NewPrimaryAlias))
		event.DispatchUpdate(ev, "elected new primary candidate")
	}

	primaryElectAliasStr := topoproto.TabletAliasString(opts.NewPrimaryAlias)

	newPrimaryTabletInfo, ok := tabletMap[primaryElectAliasStr]
	if !ok {
		return true, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary-elect tablet %v is not in the shard", primaryElectAliasStr)
	}

	ev.NewMaster = *newPrimaryTabletInfo.Tablet

	if topoproto.TabletAliasIsZero(ev.ShardInfo.MasterAlias) {
		return true, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "the shard has no current primary, use EmergencyReparentShard instead")
	}

	return false, nil
}

func (pr *PlannedReparenter) performGracefulPromotion(
	ctx context.Context,
	ev *events.Reparent,
	keyspace string,
	shard string,
	currentPrimary *topo.TabletInfo,
	primaryElect topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	opts PlannedReparentOptions,
) (string, error) {
	primaryElectAliasStr := topoproto.TabletAliasString(primaryElect.Alias)
	ev.OldMaster = *currentPrimary.Tablet

	// Before demoting the old primary, we're going to ensure that replication
	// is working from the old primary to the primary-elect. If replication is
	// not working, a PlannedReparent is not safe to do, because the candidate
	// won't catch up and we'll potentially miss transactions.
	pr.logger.Infof("checking replication on primary-elect %v", primaryElectAliasStr)

	// First, we find the position of the current primary. Note that this is
	// just a snapshot of the position, since we let it keep accepting writes
	// until we're sure we want to proceed with the promotion.
	snapshotCtx, snapshotCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer snapshotCancel()

	snapshotPos, err := pr.tmc.MasterPosition(snapshotCtx, currentPrimary.Tablet)
	if err != nil {
		return "", vterrors.Wrapf(err, "cannot get replication position on current primary %v; current primary must be healthy to perform PlannedReparent", currentPrimary.AliasString())
	}

	// Next, we wait for the primary-elect to catch up to that snapshot point.
	// If it can catch up within WaitReplicasTimeout, we can be fairly
	// confident that it will catch up on everything else that happens between
	// the snapshot point we grabbed above and when we demote the old primary
	// below.
	//
	// We do this as an idempotent SetMaster to make sure the replica knows who
	// the current primary is.
	setMasterCtx, setMasterCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer setMasterCancel()

	if err := pr.tmc.SetMaster(setMasterCtx, &primaryElect, currentPrimary.Alias, 0, snapshotPos, true); err != nil {
		return "", vterrors.Wrapf(err, "replication on primary-elect %v did not catch up in time; replication must be healthy to perform PlannedReparent", primaryElectAliasStr)
	}

	// Verify we still have the topology lock before doing the demotion.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return "", vterrors.Wrap(err, "lost topology lock; aborting")
	}

	// Next up, demote the current primary and get its replication position.
	// It's fine if the current primary was already demoted, since DemoteMaster
	// is idempotent.
	pr.logger.Infof("demoting current primary: %v", currentPrimary.AliasString())
	event.DispatchUpdate(ev, "demoting old primary")

	demoteCtx, demoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer demoteCancel()

	masterStatus, err := pr.tmc.DemoteMaster(demoteCtx, currentPrimary.Tablet)
	if err != nil {
		return "", vterrors.Wrapf(err, "failed to DemoteMaster on current primary %v: %v", currentPrimary.AliasString(), err)
	}

	// Wait for the primary-elect to catch up to the position we demoted the
	// current primary at. If it fails to catch up within WaitReplicasTimeout,
	// we will try to roll back to the original primary before aborting.
	waitCtx, waitCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer waitCancel()

	waitErr := pr.tmc.WaitForPosition(waitCtx, &primaryElect, masterStatus.Position)

	// Do some wrapping of errors to get the right codes and callstacks.
	var finalWaitErr error
	switch {
	case waitErr != nil:
		finalWaitErr = vterrors.Wrapf(waitErr, "primary-elect tablet %v failed to catch up with replication %v", primaryElectAliasStr, masterStatus.Position)
	case ctx.Err() == context.DeadlineExceeded:
		finalWaitErr = vterrors.New(vtrpc.Code_DEADLINE_EXCEEDED, "PlannedReparent timed out; please try again")
	}

	if finalWaitErr != nil {
		// It's possible that we've used up the calling context's timeout, or
		// that not enough time is left on the it to finish the rollback.
		// We create a new background context to avoid a partial rollback, which
		// could leave the cluster in a worse state than when we started.
		undoCtx, undoCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
		defer undoCancel()

		if undoErr := pr.tmc.UndoDemoteMaster(undoCtx, currentPrimary.Tablet); undoErr != nil {
			pr.logger.Warningf("encountered error while performing UndoDemoteMaster(%v): %v", currentPrimary.AliasString(), undoErr)
			finalWaitErr = vterrors.Wrapf(finalWaitErr, "encountered error while performing UndoDemoteMaster(%v): %v", currentPrimary.AliasString(), undoErr)
		}

		return "", finalWaitErr
	}

	// Primary-elect is caught up to the current primary. We can do the
	// promotion now.
	promoteCtx, promoteCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer promoteCancel()

	rp, err := pr.tmc.PromoteReplica(promoteCtx, &primaryElect)
	if err != nil {
		return "", vterrors.Wrapf(err, "primary-elect tablet %v failed to be promoted to primary; please try again", primaryElectAliasStr)
	}

	if ctx.Err() == context.DeadlineExceeded {
		// PromoteReplica succeeded, but we ran out of time. PRS needs to be
		// re-run to complete fully.
		return "", vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "PLannedReparent timed out after successfully promoting primary-elect %v; please re-run to fix up the replicas", primaryElectAliasStr)
	}

	return rp, nil
}

func (pr *PlannedReparenter) performPartialPromotionRecovery(ctx context.Context, primaryElect topodatapb.Tablet) (string, error) {
	// It's possible that a previous attempt to reparent failed to SetReadWrite,
	// so call it here to make sure the underlying MySQL is read-write on the
	// candidate primary.
	setReadWriteCtx, setReadWriteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer setReadWriteCancel()

	if err := pr.tmc.SetReadWrite(setReadWriteCtx, &primaryElect); err != nil {
		return "", vterrors.Wrapf(err, "failed to SetReadWrite on current primary %v", topoproto.TabletAliasString(primaryElect.Alias))
	}

	// The primary is already the one we want according to its tablet record.
	refreshCtx, refreshCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer refreshCancel()

	// Get the replication position so we can try to fix the replicas (back in
	// reparentShardLocked())
	reparentJournalPosition, err := pr.tmc.MasterPosition(refreshCtx, &primaryElect)
	if err != nil {
		return "", vterrors.Wrapf(err, "failed to get replication position of current primary %v", topoproto.TabletAliasString(primaryElect.Alias))
	}

	return reparentJournalPosition, nil
}

func (pr *PlannedReparenter) performPotentialPromotion(
	ctx context.Context,
	keyspace string,
	shard string,
	primaryElect topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
) (string, error) {
	primaryElectAliasStr := topoproto.TabletAliasString(primaryElect.Alias)

	pr.logger.Infof("no clear winner found for current master term; checking if it's safe to recover by electing %v", primaryElectAliasStr)

	type tabletPos struct {
		alias  string
		tablet *topodatapb.Tablet
		pos    mysql.Position
	}

	positions := make(chan tabletPos, len(tabletMap))

	// First, stop the world, to ensure no writes are happening anywhere. We
	// don't trust that we know which tablets might be acting as primaries, so
	// we simply demote everyone.
	//
	// Unlike the normal, single-primary case, we don't try to undo this if we
	// fail. If we've made it here, it means there is no clear primary, so we
	// don't know who it's safe to roll back to. Leaving everything read-only is
	// probably safer, or at least no worse, than whatever weird state we were
	// in before.
	//
	// If any tablets are unreachable, we can't be sure it's safe either,
	// because one of the unreachable tablets might have a replication position
	// further ahead than the candidate primary.

	var (
		stopAllWg sync.WaitGroup
		rec       concurrency.AllErrorRecorder
	)

	stopAllCtx, stopAllCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer stopAllCancel()

	for alias, tabletInfo := range tabletMap {
		stopAllWg.Add(1)

		go func(alias string, tablet *topodatapb.Tablet) {
			defer stopAllWg.Done()

			// Regardless of what type this tablet thinks it is, we will always
			// call DemoteMaster to ensure the underlying MySQL server is in
			// read-only, and to check its replication position. DemoteMaster is
			// idempotent, so it's fine to call it on a replica (or other
			// tablet type), that's already in read-only.
			pr.logger.Infof("demoting tablet %v", alias)

			masterStatus, err := pr.tmc.DemoteMaster(stopAllCtx, tablet)
			if err != nil {
				rec.RecordError(vterrors.Wrapf(err, "DemoteMaster(%v) failed on contested primary", alias))

				return
			}

			pos, err := mysql.DecodePosition(masterStatus.Position)
			if err != nil {
				rec.RecordError(vterrors.Wrapf(err, "cannot decode replication position (%v) for demoted tablet %v", masterStatus.Position, alias))

				return
			}

			positions <- tabletPos{
				alias:  alias,
				tablet: tablet,
				pos:    pos,
			}
		}(alias, tabletInfo.Tablet)
	}

	stopAllWg.Wait()
	close(positions)

	if rec.HasErrors() {
		return "", vterrors.Wrap(rec.Error(), "failed to demote all tablets")
	}

	// Construct a mapping of alias to tablet position.
	tabletPosMap := make(map[string]tabletPos, len(tabletMap))
	for tp := range positions {
		tabletPosMap[tp.alias] = tp
	}

	// Make sure no tablet has a more advanced position than the candidate
	// primary. It's up to the caller to choose a suitable candidate, and to
	// choose another if this check fails.
	//
	// Note that we still allow replication to run during this time, but we
	// assume that no new high water mark can appear because we just demoted all
	// tablets to read-only, so there should be no new transactions.
	//
	// TODO: consider temporarily replicating from another tablet to catch up,
	// if the candidate primary is behind that tablet.
	tp, ok := tabletPosMap[primaryElectAliasStr]
	if !ok {
		return "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary-elect tablet %v not found in tablet map", primaryElectAliasStr)
	}

	primaryElectPos := tp.pos

	for _, tp := range tabletPosMap {
		// The primary-elect pos has to be at least as advanced as every tablet
		// in the shard.
		if !primaryElectPos.AtLeast(tp.pos) {
			return "", vterrors.Errorf(
				vtrpc.Code_FAILED_PRECONDITION,
				"tablet %v (position: %v) contains transactions not found in primary-elect %v (position: %v)",
				tp.alias, tp.pos, primaryElectAliasStr, primaryElectPos,
			)
		}
	}

	// Check that we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return "", vterrors.Wrap(err, "lost topology lock; aborting")
	}

	// Promote the candidate primary to type:MASTER.
	promoteCtx, promoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer promoteCancel()

	rp, err := pr.tmc.PromoteReplica(promoteCtx, &primaryElect)
	if err != nil {
		return "", vterrors.Wrapf(err, "failed to promote %v to primary", primaryElectAliasStr)
	}

	return rp, nil
}

func (pr *PlannedReparenter) reparentShardLocked(
	ctx context.Context,
	ev *events.Reparent,
	keyspace string,
	shard string,
	opts PlannedReparentOptions,
) error {
	shardInfo, err := pr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")

	tabletMap, err := pr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check invariants that PlannedReparentShard depends on.
	if isNoop, err := pr.preflightChecks(ctx, ev, keyspace, shard, tabletMap, &opts); err != nil {
		return err
	} else if isNoop {
		return nil
	}

	currentPrimary := FindCurrentPrimary(tabletMap, pr.logger)
	reparentJournalPos := ""

	// Depending on whether we can find a current primary, and what the caller
	// specified as the candidate primary, we will do one of three kinds of
	// promotions:
	//
	// 1) There is no clear current primary. In this case we will try to
	// determine if it's safe to promote the candidate specified by the caller.
	// If it's not -- including if any tablet in the shard is unreachable -- we
	// bail. We also don't attempt to rollback a failed demotion in this case.
	//
	// 2) The current primary is the same as the candidate primary specified by
	// the caller. In this case, we assume there was a previous PRS for this
	// primary, and the caller is re-issuing the call to fix-up any replicas. We
	// also idempotently set the desired primary as read-write, just in case.
	//
	// 3) The current primary and the desired primary differ. In this case, we
	// perform a graceful promotion, in which we validate the desired primary is
	// sufficiently up-to-date, demote the current primary, wait for the desired
	// primary to catch up to that position, and set the desired primary
	// read-write. We will attempt to rollback a failed demotion in this case,
	// unlike in case (1), because we have a known good state to rollback to.
	//
	// In all cases, we will retrieve the reparent journal position that was
	// inserted in the new primary's journal, so we can use it below to check
	// that all the replicas have attached to new primary successfully.
	switch {
	case currentPrimary == nil:
		// Case (1): no clear current primary. Try to find a safe promotion
		// candidate, and promote to it.
		reparentJournalPos, err = pr.performPotentialPromotion(ctx, keyspace, shard, ev.NewMaster, tabletMap)
	case topoproto.TabletAliasEqual(currentPrimary.Alias, opts.NewPrimaryAlias):
		// Case (2): desired new primary is the current primary. Attempt to fix
		// up replicas to recover from a previous partial promotion.
		reparentJournalPos, err = pr.performPartialPromotionRecovery(ctx, ev.NewMaster)
	default:
		// Case (3): desired primary and current primary differ. Do a graceful
		// demotion-then-promotion.
		reparentJournalPos, err = pr.performGracefulPromotion(ctx, ev, keyspace, shard, currentPrimary, ev.NewMaster, tabletMap, opts)
	}

	if err != nil {
		return err
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, "lost topology lock, aborting")
	}

	if err := pr.reparentTablets(ctx, ev, reparentJournalPos, tabletMap, opts); err != nil {
		return err
	}

	return nil
}

func (pr *PlannedReparenter) reparentTablets(
	ctx context.Context,
	ev *events.Reparent,
	reparentJournalPosition string,
	tabletMap map[string]*topo.TabletInfo,
	opts PlannedReparentOptions,
) error {
	// Create a cancellable context for the entire set of reparent operations.
	// If any error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer replCancel()

	// Go thorugh all the tablets.
	// - New primary: populate the reparent journal.
	// - Everybody else: reparent to the new primary; wait for the reparent
	//	 journal row.
	event.DispatchUpdate(ev, "reparenting all tablets")

	// We add a (hopefully) unique record to the reparent journal table on the
	// new primary, so we can check if replicas got it through replication.
	reparentJournalTimestamp := time.Now().UnixNano()
	primaryElectAliasStr := topoproto.TabletAliasString(ev.NewMaster.Alias)
	replicasWg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}

	// Point all replicas at the new primary and check that they receive the
	// reparent journal entry, proving that they are replicating from the new
	// primary. We do this concurrently with  adding the journal entry (after
	// this loop), because if semi-sync is enabled, the update to the journal
	// table will block until at least one replica is successfully attached to
	// the new primary.
	for alias, tabletInfo := range tabletMap {
		if alias == primaryElectAliasStr {
			continue
		}

		replicasWg.Add(1)

		go func(alias string, tablet *topodatapb.Tablet) {
			defer replicasWg.Done()
			pr.logger.Infof("setting new primary on replica %v", alias)

			// Note: we used to force replication to start on the old primary,
			// but now that we support "resuming" a previously-failed PRS
			// attempt, we can no longer assume that we know who the former
			// primary was. Instead, we rely on the former primary to remember
			// that it needs to start replication after transitioning from
			// MASTER => REPLICA.
			forceStartReplication := false
			if err := pr.tmc.SetMaster(replCtx, tablet, ev.NewMaster.Alias, reparentJournalTimestamp, "", forceStartReplication); err != nil {
				rec.RecordError(vterrors.Wrapf(err, "tablet %v failed to SetMaster(%v): %v", alias, primaryElectAliasStr, err))
			}
		}(alias, tabletInfo.Tablet)
	}

	// Add a reparent journal entry on the new primary. If semi-sync is enabled,
	// this blocks until at least one replica is reparented (above) and
	// successfully replicating from the new primary.
	//
	// If we fail to populate the reparent journal, there's no way the replicas
	// will work, so we cancel the ongoing reparent RPCs and bail out.
	pr.logger.Infof("populating reparent journal on new primary %v", primaryElectAliasStr)
	if err := pr.tmc.PopulateReparentJournal(replCtx, &ev.NewMaster, reparentJournalTimestamp, "PlannedReparentShard", ev.NewMaster.Alias, reparentJournalPosition); err != nil {
		pr.logger.Warningf("primary failed to PopulateReparentJournal (position: %v); cancelling replica reparent attempts", reparentJournalPosition)
		replCancel()
		replicasWg.Wait()

		return vterrors.Wrapf(err, "failed PopulateReparentJournal(primary=%v, ts=%v, pos=%v): %v", primaryElectAliasStr, reparentJournalTimestamp, reparentJournalPosition, err)
	}

	// Reparent journal has been populated on the new primary. We just need to
	// wait for all the replicas to receive it.
	replicasWg.Wait()

	if err := rec.Error(); err != nil {
		msg := "some replicas failed to reparent; retry PlannedReparentShard with the same new primary alias (%v) to retry failed replicas"
		pr.logger.Errorf2(err, msg, primaryElectAliasStr)
		return vterrors.Wrapf(err, msg, primaryElectAliasStr)
	}

	return nil
}
