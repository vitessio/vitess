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
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// EmergencyReparenter performs EmergencyReparentShard operations.
type EmergencyReparenter struct {
	ts     *topo.Server
	tmc    tmclient.TabletManagerClient
	logger logutil.Logger
}

// EmergencyReparentOptions provides optional parameters to
// EmergencyReparentShard operations. Options are passed by value, so it is safe
// for callers to mutate and reuse options structs for multiple calls.
type EmergencyReparentOptions struct {
	NewPrimaryAlias *topodatapb.TabletAlias
	IgnoreReplicas  sets.Set[string]
	// WaitAllTablets is used to specify whether ERS should wait for all the tablets to return and not proceed
	// further after n-1 tablets have returned.
	WaitAllTablets            bool
	WaitReplicasTimeout       time.Duration
	PreventCrossCellPromotion bool
	ExpectedPrimaryAlias      *topodatapb.TabletAlias
	// AllowSplitBrainPromotion lets ERS proceed when two leading candidates have
	// incomparable Combined GTID positions (suspected split-brain). Off by default —
	// operator escape hatch. When set, the upfront uniformCombined check and the
	// secondary AtLeast check in findMostAdvanced log a warning and continue instead
	// of aborting with FAILED_PRECONDITION. The losing side's unique GTIDs will
	// become errant on those tablets after promotion.
	AllowSplitBrainPromotion bool

	// Private options managed internally. We use value passing to avoid leaking
	// these details back out.
	lockAction string
	durability policy.Durabler
}

// counters for Emergency Reparent Shard
var (
	ersCounter = stats.NewCountersWithMultiLabels(
		"EmergencyReparentCounts", "Number of times Emergency Reparent Shard has been run",
		[]string{"Keyspace", "Shard", "Result"},
	)
	ersFilteredCandidates = stats.NewCountersWithMultiLabels(
		"EmergencyReparentFilteredCandidates", "Number of candidates filtered out during EmergencyReparentShard because their Combined position was not the most advanced. A single ERS run may increment this twice if errant-GTID detection forces a second wait pass over the surviving candidates.",
		[]string{"Keyspace", "Shard"},
	)
	ersRelayLogApplyFailedCandidates = stats.NewCountersWithMultiLabels(
		"EmergencyReparentRelayLogFailedCandidates", "Number of candidates that failed to apply relay logs during EmergencyReparentShard",
		[]string{"Keyspace", "Shard"},
	)
	ersSplitBrainOverrides = stats.NewCountersWithMultiLabels(
		"EmergencyReparentSplitBrainOverrides", "Number of split-brain detections bypassed by AllowSplitBrainPromotion=true during EmergencyReparentShard. Counted per detection, not per ERS run — a single run may increment this twice if the errant-GTID re-wait pass also encounters incomparable Combined positions in the survivor set. The non-promoted side's unique GTIDs will become errant on those tablets after promotion.",
		[]string{"Keyspace", "Shard"},
	)
)

// NewEmergencyReparenter returns a new EmergencyReparenter object, ready to
// perform EmergencyReparentShard operations using the given topo.Server,
// TabletManagerClient, and logger.
//
// Providing a nil logger instance is allowed.
func NewEmergencyReparenter(ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger) *EmergencyReparenter {
	erp := EmergencyReparenter{
		ts:     ts,
		tmc:    tmc,
		logger: logger,
	}

	if erp.logger == nil {
		// Create a no-op logger so we can call functions on er.logger without
		// needed to constantly check for non-nil.
		erp.logger = logutil.NewCallbackLogger(func(*logutilpb.Event) {})
	}

	return &erp
}

// ReparentShard performs the EmergencyReparentShard operation on the given
// keyspace and shard.
func (erp *EmergencyReparenter) ReparentShard(ctx context.Context, keyspace string, shard string, opts EmergencyReparentOptions) (*events.Reparent, error) {
	var err error
	statsLabels := []string{keyspace, shard}

	opts.lockAction = erp.getLockAction(opts.NewPrimaryAlias)
	// First step is to lock the shard for the given operation, if not already locked
	if err = topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		var unlock func(*error)
		ctx, unlock, err = erp.ts.LockShard(ctx, keyspace, shard, opts.lockAction)
		if err != nil {
			ersCounter.Add(append(statsLabels, failureResult), 1)
			return nil, err
		}
		defer unlock(&err)
	}

	// dispatch success or failure of ERS
	startTime := time.Now()
	ev := &events.Reparent{}
	defer func() {
		reparentShardOpTimings.Add("EmergencyReparentShard", time.Since(startTime))
		switch err {
		case nil:
			ersCounter.Add(append(statsLabels, successResult), 1)
			event.DispatchUpdate(ev, "finished EmergencyReparentShard")
		default:
			ersCounter.Add(append(statsLabels, failureResult), 1)
			event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
		}
	}()

	err = erp.reparentShardLocked(ctx, ev, keyspace, shard, opts)

	return ev, err
}

func (erp *EmergencyReparenter) getLockAction(newPrimaryAlias *topodatapb.TabletAlias) string {
	action := "EmergencyReparentShard"

	if newPrimaryAlias != nil {
		action += fmt.Sprintf("(%v)", topoproto.TabletAliasString(newPrimaryAlias))
	}

	return action
}

// reparentShardLocked performs Emergency Reparent Shard operation assuming that the shard is already locked
func (erp *EmergencyReparenter) reparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, opts EmergencyReparentOptions) (err error) {
	// log the starting of the operation and increment the counter
	erp.logger.Infof("will initiate emergency reparent shard in keyspace - %s, shard - %s", keyspace, shard)

	var (
		stoppedReplicationSnapshot *replicationSnapshot

		// replicasToRestart is the list of replicas that need replication to be restarted
		// in the case of an error after their IO threads have been stopped, but before
		// the ERS restarts them as part of a successful reparent.
		replicasToRestart []*topodatapb.Tablet

		shardInfo                  *topo.ShardInfo
		prevPrimary                *topodatapb.Tablet
		tabletMap                  map[string]*topo.TabletInfo
		validCandidates            map[string]*RelayLogPositions
		intermediateSource         *topodatapb.Tablet
		validCandidateTablets      []*topodatapb.Tablet
		validReplacementCandidates []*topodatapb.Tablet
		betterCandidate            *topodatapb.Tablet
		isIdeal                    bool
		isGTIDBased                bool
	)

	defer func() {
		// If we succeeded, or there are no replicas that need replication restarted,
		// we can return early.
		if err == nil || len(replicasToRestart) == 0 {
			return
		}

		// We create a new context with a fresh timeout so that the parent context does not cancel early while
		// we attempt to restart replication on the stopped replicas.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), topo.RemoteOperationTimeout)
		defer cancel()

		// Make sure we still have the shard lock.
		if lockErr := topo.CheckShardLocked(ctx, keyspace, shard); lockErr != nil {
			erp.logger.Warningf("skipping replication restart cleanup because the shard lock was lost for %s/%s: %v", keyspace, shard, lockErr)
			return
		}

		cleanupErr := erp.restartReplicationOnStoppedReplicas(ctx, prevPrimary, replicasToRestart, opts.durability)
		if cleanupErr == nil {
			return
		}

		err = vterrors.Wrapf(err, "restart replication cleanup failed: %v", cleanupErr)
	}()

	shardInfo, err = erp.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	if opts.ExpectedPrimaryAlias != nil && !topoproto.TabletAliasEqual(opts.ExpectedPrimaryAlias, shardInfo.PrimaryAlias) {
		return vterrors.Errorf(
			vtrpc.Code_FAILED_PRECONDITION, "primary %s is not equal to expected alias %s",
			topoproto.TabletAliasString(shardInfo.PrimaryAlias),
			topoproto.TabletAliasString(opts.ExpectedPrimaryAlias),
		)
	}

	keyspaceDurability, err := erp.ts.GetKeyspaceDurability(ctx, keyspace)
	if err != nil {
		return err
	}

	erp.logger.Infof("Getting a new durability policy for %v", keyspaceDurability)
	opts.durability, err = policy.GetDurabilityPolicy(keyspaceDurability)
	if err != nil {
		return err
	}

	// get the previous primary according to the topology server,
	// we use this information to choose the best candidate in the same cell
	// and to undo promotion in case of failure
	if shardInfo.PrimaryAlias != nil {
		prevPrimaryInfo, err := erp.ts.GetTablet(ctx, shardInfo.PrimaryAlias)
		if err != nil {
			return err
		}
		prevPrimary = prevPrimaryInfo.Tablet
	}

	// read all the tablets and their information
	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err = erp.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v", keyspace, shard)
	}

	// Stop replication on all the tablets and build their status map
	stoppedReplicationSnapshot, err = stopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, shardInfo.PrimaryAlias, topo.RemoteOperationTimeout, opts.IgnoreReplicas, opts.NewPrimaryAlias, opts.durability, opts.WaitAllTablets, erp.logger)

	// If stoppedReplicationSnapshot is not nil, it means we have stopped replication on at
	// least one replica. We'll keep track of the replicas that had their IO threads stopped
	// so we can restart them later in case of an error that causes us to return early and
	// leaves replication stopped. We do this before checking the error so that we ensure we
	// handle partial failures (where we've stopped some replicas but failed on others) correctly.
	if stoppedReplicationSnapshot != nil {
		var skippedSQLStopped []*topodatapb.Tablet
		replicasToRestart, skippedSQLStopped = stoppedReplicationSnapshot.replicasWithStoppedIO(tabletMap)
		// Surface replicas we deliberately did not restart so the operator can
		// decide whether to restart their IO threads manually — StartReplication
		// would have started SQL_THREAD too, which they had explicitly stopped.
		for _, replica := range skippedSQLStopped {
			erp.logger.Warningf("not restarting replication on %s after potential failed ERS: IO_THREAD was running pre-ERS but SQL_THREAD was stopped, and StartReplication would silently start both", topoproto.TabletAliasString(replica.Alias))
		}
	}

	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps")
	}

	// check that we still have the shard lock. If we don't then we can terminate at this point
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	// find the positions of all the valid candidates.
	validCandidates, isGTIDBased, err = FindPositionsOfAllCandidates(stoppedReplicationSnapshot.statusMap, stoppedReplicationSnapshot.primaryStatusMap)
	if err != nil {
		return err
	}
	// Restrict the valid candidates list. We remove any tablet which is of the type DRAINED, RESTORE or BACKUP.
	validCandidates, err = restrictValidCandidates(validCandidates, tabletMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// For GTID-based flavors, filter to tablets at the leading Combined position and wait
	// only for that group — one success is sufficient. Abort upfront on incomparable maxima
	// (suspected split-brain — see AGENTS.md / CLAUDE.md ERS section). For non-GTID flavors
	// (FilePos, MariaDB) Combined is the executed position, so the filter is unsafe — keep
	// pre-PR behavior of waiting for every candidate and failing on any error.
	relayLogWaitCandidates := validCandidates
	requireAll := true
	if isGTIDBased {
		relayLogWaitCandidates, err = erp.filterAndCheckUniform(validCandidates, keyspace, shard, "candidates", opts.AllowSplitBrainPromotion)
		if err != nil {
			return err
		}
		requireAll = false
	}

	relayLogSuccessMap, err := erp.applyRelayLogsAndReconcile(ctx, relayLogWaitCandidates, validCandidates, tabletMap, stoppedReplicationSnapshot.statusMap, opts.WaitReplicasTimeout, requireAll, keyspace, shard)
	if err != nil {
		return err
	}

	// For GTID based replication, we will run errant GTID detection.
	if isGTIDBased {
		// Snapshot validCandidates before errant-GTID detection so we can restore the set
		// if AllowSplitBrainPromotion is set and detection wipes every candidate. The
		// canonical case is a mutually-errant two-tablet split-brain where each side has
		// unique writes under its own server UUID — findErrantGTIDs flags both sides as
		// errant relative to each other and removes both. maps.Clone defends against a
		// future refactor that mutates the input map.
		preErrantCandidates := maps.Clone(validCandidates)
		validCandidates, err = erp.findErrantGTIDs(ctx, validCandidates, stoppedReplicationSnapshot.statusMap, tabletMap, opts.WaitReplicasTimeout)
		if err != nil {
			return err
		}
		// Detect whether any of the candidates we originally waited on (the leading group)
		// survived errant-GTID detection. The split-brain failure mode we need to guard
		// against is mutually-errant leaders being pruned while a lagging non-errant
		// candidate remains — promoting the lagger would lose unique writes from both
		// split-brain sides, which is strictly worse than the documented "pick one side;
		// losing side becomes errant" semantics. Treating "no leading survivor" the same
		// as "no survivor at all" prevents a lagger from being elected on the second-pass
		// re-wait below.
		leadingSurvived := false
		for alias := range validCandidates {
			if _, isLeading := relayLogWaitCandidates[alias]; isLeading {
				leadingSurvived = true
				break
			}
		}
		if !leadingSurvived {
			if !opts.AllowSplitBrainPromotion {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent: all leading candidates have errant GTIDs")
			}
			// AllowSplitBrainPromotion already incremented the override metric upfront in
			// filterAndCheckUniform — don't double-count here. Restore the pre-detection
			// LEADING set (intersected with relayLogWaitCandidates so laggers can't be
			// elected by findMostAdvanced) and let the sort pick a side. The non-promoted
			// side's unique GTIDs will become errant after promotion.
			erp.logger.Warningf("AllowSplitBrainPromotion=true: no leading candidates survived errant-GTID detection — restoring the pre-detection leading set so findMostAdvanced can pick a side without promoting a lagger")
			restored := make(map[string]*RelayLogPositions, len(relayLogWaitCandidates))
			for alias := range relayLogWaitCandidates {
				if pos, ok := preErrantCandidates[alias]; ok {
					restored[alias] = pos
				}
			}
			validCandidates = restored
		}

		// If errant detection removed every originally-applied tablet, promotion candidates
		// only include "unwaited survivors" (filter-excluded laggers, or peers cancelled mid-apply
		// by our short-circuit) — promoting one risks received-but-unapplied transactions. Run a
		// second filter/uniform/wait pass over the survivors. Uniformity is re-checked because
		// the survivor set is a different shape than the original leading group.
		//
		// If at least one applied tablet survived no re-wait is needed: findMostAdvanced prefers
		// it via the Executed tie-break, and pos.AtLeast() gates NewPrimaryAlias against the
		// winner so an unwaited peer with lower Executed cannot be promoted.
		appliedSurvived := false
		for alias := range validCandidates {
			if applied, ok := relayLogSuccessMap[alias]; ok && applied {
				appliedSurvived = true
				break
			}
		}
		if !appliedSurvived {
			erp.logger.Warningf("all originally-applied candidates were removed by errant-GTID detection; running second relay-log-apply wait on surviving candidates before promotion")
			rewaitCandidates, err := erp.filterAndCheckUniform(validCandidates, keyspace, shard, "surviving unwaited candidates", opts.AllowSplitBrainPromotion)
			if err != nil {
				return err
			}
			if _, err := erp.applyRelayLogsAndReconcile(ctx, rewaitCandidates, validCandidates, tabletMap, stoppedReplicationSnapshot.statusMap, opts.WaitReplicasTimeout, false /* requireAll */, keyspace, shard); err != nil {
				return err
			}
			if len(validCandidates) == 0 {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent: all surviving candidates failed the second relay-log-apply wait")
			}
		}
	}

	// Find the intermediate source for replication that we want other tablets to replicate from.
	// This step chooses the most advanced tablet. Further ties are broken by using the promotion rule.
	// In case the user has specified a tablet specifically, then it is selected, as long as it is the most advanced.
	// Here we also check for split brain scenarios and check that the selected replica must be more advanced than all the other valid candidates.
	// We fail in case there is a split brain detected.
	// The validCandidateTablets list is sorted by the replication positions with ties broken by promotion rules.
	intermediateSource, validCandidateTablets, err = erp.findMostAdvanced(validCandidates, tabletMap, opts, keyspace, shard)
	if err != nil {
		return err
	}
	erp.logger.Infof("intermediate source selected - %v", intermediateSource.Alias)

	// After finding the intermediate source, we want to filter the valid candidate list by the following criteria -
	// 1. Only keep the tablets which can make progress after being promoted (have sufficient reachable semi-sync ackers)
	// 2. Remove the tablets with the Must_not promote rule
	// 3. Remove cross-cell tablets if PreventCrossCellPromotion is specified
	// Our final primary candidate MUST belong to this list of valid candidates
	validCandidateTablets, err = erp.filterValidCandidates(validCandidateTablets, stoppedReplicationSnapshot.reachableTablets, stoppedReplicationSnapshot.tabletsBackupState, prevPrimary, opts)
	if err != nil {
		return err
	}

	// Check whether the intermediate source candidate selected is ideal or if it can be improved later.
	// If the intermediateSource is ideal, then we can be certain that it is part of the valid candidates list.
	isIdeal, err = erp.isIntermediateSourceIdeal(intermediateSource, validCandidateTablets, tabletMap, opts)
	if err != nil {
		return err
	}
	erp.logger.Infof("intermediate source is ideal candidate- %v", isIdeal)

	// Check (again) we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	// Relay logs have been successfully applied and we're ready to start repointing replicas,
	// so we no longer need to restart replication manually in the event of an error.
	replicasToRestart = nil

	// initialize the newPrimary with the intermediate source, override this value if it is not the ideal candidate
	newPrimary := intermediateSource
	if !isIdeal {
		// we now reparent all the tablets to start replicating from the intermediate source
		// we do not promote the tablet or change the shard record. We only change the replication for all the other tablets
		// it also returns the list of the tablets that started replication successfully including itself part of the validCandidateTablets list.
		// These are the candidates that we can use to find a replacement.
		validReplacementCandidates, err = erp.promoteIntermediateSource(ctx, ev, intermediateSource, tabletMap, stoppedReplicationSnapshot.statusMap, validCandidateTablets, opts)
		if err != nil {
			return err
		}

		// try to find a better candidate using the list we got back
		// We prefer to choose a candidate which is in the same cell as our previous primary and of the best possible durability rule.
		// However, if there is an explicit request from the user to promote a specific tablet, then we choose that tablet.
		betterCandidate, err = erp.identifyPrimaryCandidate(intermediateSource, validReplacementCandidates, tabletMap, opts)
		if err != nil {
			return err
		}

		// if our better candidate is different from our intermediate source, then we wait for it to catch up to the intermediate source
		if !topoproto.TabletAliasEqual(betterCandidate.Alias, intermediateSource.Alias) {
			err = waitForCatchUp(ctx, erp.tmc, erp.logger, betterCandidate, intermediateSource, opts.WaitReplicasTimeout)
			if err != nil {
				return err
			}
			newPrimary = betterCandidate
		}

		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, lostTopologyLockMsg)
		}
	}

	// The new primary which will be promoted will always belong to the validCandidateTablets list because -
	// 	1. 	if the intermediate source is ideal - then we know the intermediate source was in the validCandidateTablets list
	// 		since we used that list
	//	2. 	if the intermediate source isn't ideal - we take the intersection of the validCandidateTablets list and the one we
	//		were able to reach during the promotion of intermediate source, as possible candidates. So the final candidate (even if
	//		it is the intermediate source itself) will belong to the list
	// Since the new primary tablet belongs to the validCandidateTablets list, we no longer need any additional constraint checks

	// Final step is to promote our primary candidate
	_, err = erp.reparentReplicas(ctx, ev, newPrimary, tabletMap, stoppedReplicationSnapshot.statusMap, opts, false /* intermediateReparent */)
	if err != nil {
		return err
	}
	ev.NewPrimary = newPrimary.CloneVT()
	return err
}

// restartReplicationOnStoppedReplicas restarts replication on replicas whose IO threads were
// stopped by ERS before the operation aborted.
func (erp *EmergencyReparenter) restartReplicationOnStoppedReplicas(
	ctx context.Context,
	prevPrimary *topodatapb.Tablet,
	replicas []*topodatapb.Tablet,
	durability policy.Durabler,
) error {
	erp.logger.Infof("restarting replication on %d replicas whose IO threads were stopped by ERS", len(replicas))

	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// Start replication on each stopped replica concurrently.
	for _, replica := range replicas {
		alias := topoproto.TabletAliasString(replica.Alias)

		semiSync := false
		if prevPrimary != nil {
			semiSync = policy.IsReplicaSemiSync(durability, prevPrimary, replica)
		}

		wg.Go(func() {
			erp.logger.Infof("restarting replication on %q after failed ERS", alias)
			if err := erp.tmc.StartReplication(ctx, replica, semiSync); err != nil {
				err := vterrors.Wrapf(err, "failed to restart replication on %q after failed ERS", alias)
				rec.RecordError(err)
			}
		})
	}

	wg.Wait()

	if rec.HasErrors() {
		return rec.Error()
	}

	return nil
}

// relayLogResult is the result of waiting for a single tablet to apply relay logs.
type relayLogResult struct {
	alias string
	err   error
}

// filterAndCheckUniform applies filterToMostAdvancedCombined to validCandidates, increments
// the filtered-count metric, and returns a FAILED_PRECONDITION error if the resulting set
// has incomparable Combined positions (suspected split-brain). The descriptor parameter is
// interpolated into the error message to identify which ERS pipeline stage detected the
// split-brain (first wait vs. errant-GTID re-wait).
//
// If allowSplitBrain is true, an incomparable result logs a warning and returns the filtered
// set without erroring — the operator-escape-hatch path that accepts errant GTIDs on the
// losing side in exchange for forcing ERS through.
func (erp *EmergencyReparenter) filterAndCheckUniform(
	validCandidates map[string]*RelayLogPositions,
	keyspace, shard, descriptor string,
	allowSplitBrain bool,
) (map[string]*RelayLogPositions, error) {
	filtered := filterToMostAdvancedCombined(validCandidates, erp.logger)
	if !uniformCombined(filtered) {
		if !allowSplitBrain {
			return nil, vterrors.Errorf(
				vtrpc.Code_FAILED_PRECONDITION,
				"emergency reparent aborted: %s have incomparable Combined GTID positions (suspected split-brain): %s",
				descriptor,
				describeCombinedPositions(filtered),
			)
		}
		erp.logger.Warningf("AllowSplitBrainPromotion=true: %s have incomparable Combined GTID positions (suspected split-brain): %s — proceeding under operator override; losing side's unique GTIDs will become errant", descriptor, describeCombinedPositions(filtered))
		ersSplitBrainOverrides.Add([]string{keyspace, shard}, 1)
	}
	if excluded := int64(len(validCandidates) - len(filtered)); excluded > 0 {
		ersFilteredCandidates.Add([]string{keyspace, shard}, excluded)
	}
	return filtered, nil
}

// applyRelayLogsAndReconcile waits on waitCandidates, then mutates validCandidates:
// applied tablets get Executed bumped to Combined (so the sorter prefers them via the
// existing Combined→Executed tie-break), failed tablets are removed (so they cannot
// be picked for promotion or catch-up). The returned successMap lets callers track
// which aliases were waited on across multiple passes.
func (erp *EmergencyReparenter) applyRelayLogsAndReconcile(
	ctx context.Context,
	waitCandidates map[string]*RelayLogPositions,
	validCandidates map[string]*RelayLogPositions,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	waitReplicasTimeout time.Duration,
	requireAll bool,
	keyspace, shard string,
) (successMap map[string]bool, err error) {
	successMap, waitErr := erp.waitForAllRelayLogsToApply(ctx, waitCandidates, tabletMap, statusMap, waitReplicasTimeout, requireAll)
	// Increment the failure metric before any error return so operators still see failure
	// counts when ERS aborts.
	failedCount := 0
	for _, applied := range successMap {
		if !applied {
			failedCount++
		}
	}
	if failedCount > 0 {
		ersRelayLogApplyFailedCandidates.Add([]string{keyspace, shard}, int64(failedCount))
	}
	if waitErr != nil {
		return successMap, waitErr
	}
	// Reconcile validCandidates with the wait results. Removing failed tablets is safe
	// because the upfront uniformCombined check guarantees every wait candidate shared
	// the same Combined position — failed tablets carry no unique GTIDs vs the applied
	// tablet, so no split-brain signal is lost. Cancelled tablets (absent from successMap)
	// are left untouched; the sort ranks them below applied peers at the same Combined.
	for alias, applied := range successMap {
		if applied {
			if pos, ok := validCandidates[alias]; ok {
				pos.Executed = pos.Combined
			}
		} else {
			delete(validCandidates, alias)
		}
	}
	return successMap, nil
}

// waitForAllRelayLogsToApply waits for the given candidates to apply their relay logs
// and returns a successMap with three possible per-alias outcomes:
//
//   - successMap[alias] == true:  fully applied, OR no statusMap entry (no relay log
//     to apply — already at its position; the caller treats this the same as applied).
//   - successMap[alias] == false: genuinely failed (RPC error, MySQL error, or timeout).
//   - alias absent:               cancelled by our own short-circuit after a peer
//     succeeded, or by the parent context.
//
// When requireAll is true the function aborts on the first failure (pre-PR semantics,
// used for non-GTID flavors). When false it short-circuits on the first success and
// treats subsequent cancellation errors as expected. Does not mutate validCandidates.
func (erp *EmergencyReparenter) waitForAllRelayLogsToApply(
	ctx context.Context,
	validCandidates map[string]*RelayLogPositions,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	waitReplicasTimeout time.Duration,
	requireAll bool,
) (successMap map[string]bool, err error) {
	resultCh := make(chan relayLogResult, len(validCandidates))

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	successMap = make(map[string]bool, len(validCandidates))
	waiterCount := 0
	preSatisfied := 0

	for candidate := range validCandidates {
		// When we called stopReplicationAndBuildStatusMaps, we got back two
		// maps: (1) the StopReplicationStatus of any replicas that actually
		// stopped replication; and (2) the PrimaryStatus of anything that
		// returned ErrNotReplica, which is a tablet that is either the current
		// primary or is stuck thinking it is a PRIMARY but is not in actuality.
		//
		// If we have a tablet in the validCandidates map that does not appear
		// in the statusMap, then we have either (a) the current primary, which
		// is not replicating, so it is not applying relay logs; or (b) a tablet
		// that is stuck thinking it is PRIMARY but is not in actuality. Such a
		// tablet has no relay logs to apply, so it is already at its position —
		// mark it applied in successMap so applyRelayLogsAndReconcile bumps
		// Executed=Combined (Combined is its executed position by construction
		// from FindPositionsOfAllCandidates). Without that bump, peers cancelled
		// at the same Combined could keep their pre-wait Executed and sort ahead
		// in findMostAdvanced, risking promotion of a tablet with received-but-
		// unapplied transactions. In case (b) the stuck PRIMARY will most likely
		// fail downstream split-brain / errant-GTID checks anyway.
		status, ok := statusMap[candidate]
		if !ok {
			erp.logger.Infof("EmergencyReparent candidate %v not in replica status map; this means it was not running replication (because it was formerly PRIMARY), so skipping WaitForRelayLogsToApply step for this candidate", candidate)
			successMap[candidate] = true
			preSatisfied++
			continue
		}

		go func(alias string, status *replicationdatapb.StopReplicationStatus) {
			resultCh <- relayLogResult{
				alias: alias,
				err:   WaitForRelayLogsToApply(groupCtx, erp.tmc, tabletMap[alias], status),
			}
		}(candidate, status)

		waiterCount++
	}

	// If no candidates needed to apply relay logs (e.g., initialization with no replication
	// or every candidate was pre-satisfied), there's nothing to wait for. Still honour an
	// outer context cancellation — otherwise an operator-aborted ERS could continue into
	// later steps that intentionally use context.WithoutCancel (e.g., the deferred replica
	// restart and the final reparentReplicas call).
	if waiterCount == 0 {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return successMap, vterrors.Wrapf(ctxErr, "emergency reparent aborted while waiting for relay logs to apply")
		}
		return successMap, nil
	}

	// In optimization mode, a pre-satisfied candidate is already a winner — short-circuit
	// the wait. The remaining waiters will return context.Canceled, which the cancellation
	// classification below treats as expected noise (successes > 0).
	if !requireAll && preSatisfied > 0 {
		groupCancel()
	}

	successes := preSatisfied
	var firstFailure error
	for range waiterCount {
		result := <-resultCh
		if result.err != nil {
			// Cancellation errors arriving after our own groupCancel(), or after the parent
			// ctx was cancelled, are expected noise — omit them entirely. Must also check
			// vterrors.Code (gRPC-wrapped CANCELED), which does NOT satisfy errors.Is(err,
			// context.Canceled). Non-cancellation errors after a trigger are conservatively
			// counted as real failures — over-reporting is preferable to dropping a genuine
			// failure signal.
			isCancellation := errors.Is(result.err, context.Canceled) || vterrors.Code(result.err) == vtrpc.Code_CANCELED
			weTriggeredCancellation := (!requireAll && successes > 0) || (requireAll && firstFailure != nil) || ctx.Err() != nil
			if isCancellation && weTriggeredCancellation {
				continue
			}
			successMap[result.alias] = false
			if firstFailure == nil {
				firstFailure = result.err
				erp.logger.Warningf("EmergencyReparent candidate %s failed to apply relay logs: %v", result.alias, result.err)
				if requireAll {
					groupCancel()
				}
			}
			continue
		}
		if !requireAll && successes == 0 {
			groupCancel()
		}
		successMap[result.alias] = true
		successes++
	}

	// If the parent context was cancelled, surface that regardless of mode or how many
	// candidates were pre-satisfied. Pre-PR errgroup.Wait surfaced any cancellation; this
	// keeps behavior symmetric across preSatisfied=0 and preSatisfied>0 paths and avoids
	// misreporting a "all candidates failed" error when the real cause was operator abort.
	// A real tablet failure (firstFailure set) takes precedence — it happened first and is
	// more actionable.
	if ctxErr := ctx.Err(); ctxErr != nil && firstFailure == nil {
		return successMap, vterrors.Wrapf(ctxErr, "emergency reparent aborted while waiting for relay logs to apply")
	}
	if requireAll && firstFailure != nil {
		return successMap, vterrors.Wrapf(firstFailure, "could not apply all relay logs within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}
	if successes == 0 {
		if firstFailure != nil {
			return successMap, vterrors.Wrapf(firstFailure, "all candidates failed to apply relay logs within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
		}
		return successMap, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "all candidates failed to apply relay logs within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}

	return successMap, nil
}

// findMostAdvanced finds the intermediate source for ERS. We always choose the most advanced one from our valid candidates list. Further ties are broken by looking at the promotion rules.
func (erp *EmergencyReparenter) findMostAdvanced(
	validCandidates map[string]*RelayLogPositions,
	tabletMap map[string]*topo.TabletInfo,
	opts EmergencyReparentOptions,
	keyspace, shard string,
) (*topodatapb.Tablet, []*topodatapb.Tablet, error) {
	erp.logger.Infof("started finding the intermediate source")
	if len(validCandidates) == 0 {
		return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}
	// convert the valid candidates into a list so that we can use it for sorting
	validTablets, tabletPositions, err := getValidCandidatesAndPositionsAsList(validCandidates, tabletMap)
	if err != nil {
		return nil, nil, err
	}

	// sort the tablets for finding the best intermediate source in ERS
	err = sortTabletsForReparent(validTablets, tabletPositions, nil, opts.durability)
	if err != nil {
		return nil, nil, err
	}
	for _, tablet := range validTablets {
		erp.logger.Infof("finding intermediate source - sorted replica: %v", tablet.Alias)
	}

	// The first tablet in the sorted list will be the most eligible candidate unless explicitly asked for some other tablet
	winningPrimaryTablet := validTablets[0]
	winningPosition := tabletPositions[0]
	winningIdx := 0

	// Safety nets for AllowSplitBrainPromotion. The sort comparator is not guaranteed
	// to be a strict weak order when dominance and alias tiebreakers disagree (e.g.,
	// triple {A, B, C} where A and B are incomparable, B strictly dominates C, and
	// aliases produce A < B < C < A as a cycle). Go's sort can then put a strictly-
	// dominated tablet at index 0. Both safety nets are gated on the flag because
	// pre-PR behaviour was to abort the whole ERS via the secondary AtLeast check below
	// for any non-uniform leading set, and we only want to silently correct under the
	// explicit operator override.
	if opts.AllowSplitBrainPromotion {
		// Safety net #1: swap to a strict dominator if one exists. Dominance is a
		// partial order so the loop terminates in at most len(validTablets)-1 swaps.
		for {
			dominatorIdx := -1
			for i := range validTablets {
				if i == winningIdx {
					continue
				}
				if tabletPositions[i].AtLeast(winningPosition) && !winningPosition.AtLeast(tabletPositions[i]) {
					dominatorIdx = i
					break
				}
			}
			if dominatorIdx == -1 {
				break
			}
			erp.logger.Warningf("findMostAdvanced: sort winner %v is strictly dominated by %v — swapping (sort comparator is non-transitive under partial-order positions)",
				topoproto.TabletAliasString(winningPrimaryTablet.Alias),
				topoproto.TabletAliasString(validTablets[dominatorIdx].Alias))
			winningIdx = dominatorIdx
			winningPrimaryTablet = validTablets[dominatorIdx]
			winningPosition = tabletPositions[dominatorIdx]
		}

		// Safety net #2: prefer an applied (Executed.Equal(Combined)) candidate over a
		// cancelled-mid-apply winner when one exists at an incomparable Combined.
		// applyRelayLogsAndReconcile bumps Executed=Combined exactly for applied +
		// pre-satisfied PRIMARY-likes, leaving cancelled and lagging tablets with their
		// pre-wait Executed — Executed.Equal(Combined) is the proxy for "safe to promote
		// without losing received-but-unapplied transactions". The strictly-behind skip
		// prevents preferring a lagger that would lose the winner's leading data.
		if !winningPosition.Executed.Equal(winningPosition.Combined) {
			for i := range validTablets {
				if i == winningIdx {
					continue
				}
				otherPos := tabletPositions[i]
				if !otherPos.Executed.Equal(otherPos.Combined) {
					continue
				}
				// Skip strictly-behind candidates: winner.Combined dominates other.Combined.
				if winningPosition.Combined.AtLeast(otherPos.Combined) && !otherPos.Combined.AtLeast(winningPosition.Combined) {
					continue
				}
				erp.logger.Warningf("findMostAdvanced: sort winner %v is unapplied (Executed != Combined) under split-brain non-total ordering — preferring applied candidate %v",
					topoproto.TabletAliasString(winningPrimaryTablet.Alias),
					topoproto.TabletAliasString(validTablets[i].Alias))
				winningPrimaryTablet = validTablets[i]
				winningPosition = otherPos
				break
			}
		}
	}

	// We have already removed the tablets with errant GTIDs before calling this function. At this point our winning position must be a
	// superset of all the other valid positions. If that is not the case, then we have a split brain scenario, and we should cancel the ERS —
	// unless AllowSplitBrainPromotion is set, in which case the operator has accepted that the losing side's unique GTIDs will become errant.
	// The override counter is incremented here as well as in filterAndCheckUniform because the upfront check can miss the Combined-equal-but-
	// Executed-incomparable shape that AtLeast catches here (Combined.Equal → falls through to Executed.AtLeast, which can be false in both
	// directions if either tablet has executed unique errant GTIDs).
	splitBrainBypassed := false
	for i, position := range tabletPositions {
		if !winningPosition.AtLeast(position) {
			if !opts.AllowSplitBrainPromotion {
				return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "split brain detected between servers - %v and %v", topoproto.TabletAliasString(winningPrimaryTablet.Alias), topoproto.TabletAliasString(validTablets[i].Alias))
			}
			erp.logger.Warningf("AllowSplitBrainPromotion=true: split brain detected between %v and %v — proceeding under operator override; losing side's unique GTIDs will become errant", topoproto.TabletAliasString(winningPrimaryTablet.Alias), topoproto.TabletAliasString(validTablets[i].Alias))
			splitBrainBypassed = true
		}
	}
	if splitBrainBypassed {
		ersSplitBrainOverrides.Add([]string{keyspace, shard}, 1)
	}

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs)
	if opts.NewPrimaryAlias != nil {
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.NewPrimaryAlias)
		pos, ok := validCandidates[requestedPrimaryAlias]
		if !ok {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "requested primary elect %v has errant GTIDs", requestedPrimaryAlias)
		}
		// If the requested tablet is as advanced as the most advanced tablet, use it for promotion.
		// Otherwise let it catch up to the most advanced tablet and don't change the intermediate source —
		// unless the position is INCOMPARABLE with the winner (true split-brain shape, neither side
		// dominates) AND AllowSplitBrainPromotion is set, in which case the operator's explicit choice
		// overrides the AtLeast check. We deliberately do not override for strictly-lagging requested
		// primaries: those still need the existing catch-up path to avoid losing transactions the
		// winner has.
		incomparable := !pos.AtLeast(winningPosition) && !winningPosition.AtLeast(pos)
		if pos.AtLeast(winningPosition) || (opts.AllowSplitBrainPromotion && incomparable) {
			requestedPrimaryInfo, isFound := tabletMap[requestedPrimaryAlias]
			if !isFound {
				return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", requestedPrimaryAlias)
			}
			if incomparable {
				// The override forces past the AtLeast sort check, not past the relay-log-apply
				// requirement. applyRelayLogsAndReconcile bumps Executed=Combined for applied
				// tablets and pre-satisfied PRIMARY-likes, but leaves cancelled-mid-apply peers
				// with their pre-wait Executed. Refusing the requested tablet here when those are
				// not equal prevents promoting a tablet with received-but-unapplied transactions.
				if !pos.Executed.Equal(pos.Combined) {
					return nil, nil, vterrors.Errorf(
						vtrpc.Code_FAILED_PRECONDITION,
						"AllowSplitBrainPromotion=true: requested primary %v did not complete the relay-log apply wait (Executed=%v != Combined=%v) — re-run without --new-primary to let ERS pick an applied candidate, or specify one that finished applying",
						requestedPrimaryAlias, pos.Executed, pos.Combined,
					)
				}
				// "Incomparable with the sort winner" is not sufficient under partial-order
				// GTID sets — the requested tablet can be strictly dominated by another valid
				// candidate that happens to be incomparable with the (non-deterministic) sort
				// winner. Reject if any other validCandidate strictly dominates the requested
				// tablet, so we don't lose transactions present on that dominator.
				for otherAlias, otherPos := range validCandidates {
					if otherAlias == requestedPrimaryAlias {
						continue
					}
					if otherPos.AtLeast(pos) && !pos.AtLeast(otherPos) {
						return nil, nil, vterrors.Errorf(
							vtrpc.Code_FAILED_PRECONDITION,
							"AllowSplitBrainPromotion=true: requested primary %v is strictly dominated by candidate %v — re-run without --new-primary to let ERS pick the dominant candidate, or specify a non-dominated one",
							requestedPrimaryAlias, otherAlias,
						)
					}
				}
				erp.logger.Warningf("AllowSplitBrainPromotion=true: requested primary %v has a position incomparable with the sort winner %v — honouring operator's explicit choice; non-promoted side's unique GTIDs will become errant", requestedPrimaryAlias, topoproto.TabletAliasString(winningPrimaryTablet.Alias))
			}
			winningPrimaryTablet = requestedPrimaryInfo.Tablet
		}
	}

	return winningPrimaryTablet, validTablets, nil
}

// promoteIntermediateSource reparents all the other tablets to start replicating from the intermediate source.
// It does not promote this tablet to a primary instance, we only let other replicas start replicating from this tablet
func (erp *EmergencyReparenter) promoteIntermediateSource(
	ctx context.Context,
	ev *events.Reparent,
	source *topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	validCandidateTablets []*topodatapb.Tablet,
	opts EmergencyReparentOptions,
) ([]*topodatapb.Tablet, error) {
	// Create a tablet map from all the valid replicas
	validTabletMap := map[string]*topo.TabletInfo{}
	for _, candidate := range validCandidateTablets {
		alias := topoproto.TabletAliasString(candidate.Alias)
		validTabletMap[alias] = tabletMap[alias]
	}

	// we reparent all the other valid tablets to start replication from our new source
	// we wait for all the replicas so that we can choose a better candidate from the ones that started replication later
	reachableTablets, err := erp.reparentReplicas(ctx, ev, source, validTabletMap, statusMap, opts, true /* intermediateReparent */)
	if err != nil {
		return nil, err
	}

	// also include the current tablet for being considered as part of valid candidates for ERS promotion
	reachableTablets = append(reachableTablets, source)

	// The only valid candidates for improvement are the ones which are reachable and part of the valid candidate list.
	// Here we need to be careful not to mess up the ordering of tablets in validCandidateTablets, since the list is sorted by the
	// replication positions.
	var validCandidatesForImprovement []*topodatapb.Tablet
	for _, tablet := range validCandidateTablets {
		if topoproto.IsTabletInList(tablet, reachableTablets) {
			validCandidatesForImprovement = append(validCandidatesForImprovement, tablet)
		}
	}
	return validCandidatesForImprovement, nil
}

// reparentReplicas reparents all the replicas provided and populates the reparent journal on the primary if asked.
// Also, it returns the replicas which started replicating only in the case where we wait for all the replicas
func (erp *EmergencyReparenter) reparentReplicas(
	ctx context.Context,
	ev *events.Reparent,
	newPrimaryTablet *topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	opts EmergencyReparentOptions,
	intermediateReparent bool, // intermediateReparent represents whether the reparenting of the replicas is the final reparent or not.
	// Since ERS can sometimes promote a tablet, which isn't a candidate for promotion, if it is the most advanced, we don't want to
	// call PromoteReplica on it. We just want to get all replicas to replicate from it to get caught up, after which we'll promote the primary
	// candidate separately. During the final promotion, we call `PromoteReplica` and `PopulateReparentJournal`.
) ([]*topodatapb.Tablet, error) {
	var (
		replicasStartedReplication []*topodatapb.Tablet
		replicaMutex               sync.Mutex
	)

	// WithoutCancel preserves ctx values (tracing, caller ID) but lets replicas
	// finish SetReplicationSource RPCs after the parent context is cancelled.
	replCtx, replCancel := context.WithTimeout(context.WithoutCancel(ctx), opts.WaitReplicasTimeout)
	primaryCtx, primaryCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer primaryCancel()

	event.DispatchUpdate(ev, "reparenting all tablets")

	// Create a context and cancel function to watch for the first successful
	// SetReplicationSource call on a replica. We use a background context so that this
	// context is only ever Done when its cancel is called by the background
	// goroutine we're about to spin up.
	//
	// Similarly, create a context and cancel for the replica waiter goroutine
	// to signal when all replica goroutines have finished. In the case where at
	// least one replica succeeds, replSuccessCtx will be canceled first, while
	// allReplicasDoneCtx is guaranteed to be canceled within
	// opts.WaitReplicasTimeout plus some jitter.
	replSuccessCtx, replSuccessCancel := context.WithCancel(context.Background())
	allReplicasDoneCtx, allReplicasDoneCancel := context.WithCancel(context.Background())

	now := time.Now().UnixNano()
	replWg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}

	handlePrimary := func(alias string, tablet *topodatapb.Tablet) error {
		if !intermediateReparent {
			var position string
			var err error
			if ev.ShardInfo.PrimaryAlias == nil {
				erp.logger.Infof("setting up %v as new primary for an uninitialized cluster", alias)
				// we call InitPrimary when the PrimaryAlias in the ShardInfo is empty. This happens when we have an uninitialized cluster.
				position, err = erp.tmc.InitPrimary(primaryCtx, tablet, policy.SemiSyncAckers(opts.durability, tablet) > 0)
			} else {
				erp.logger.Infof("starting promotion for the new primary - %v", alias)
				// we call PromoteReplica which changes the tablet type, fixes the semi-sync, set the primary to read-write and flushes the binlogs
				position, err = erp.tmc.PromoteReplica(primaryCtx, tablet, policy.SemiSyncAckers(opts.durability, tablet) > 0)
			}
			if err != nil {
				return vterrors.Wrapf(err, "primary-elect tablet %v failed to be upgraded to primary", alias)
			}
			erp.logger.Infof("populating reparent journal on new primary %v", alias)
			err = erp.tmc.PopulateReparentJournal(primaryCtx, tablet, now, opts.lockAction, tablet.Alias, position)
			if err != nil {
				return vterrors.Wrapf(err, "failed to PopulateReparentJournal on primary")
			}
		}
		return nil
	}

	handleReplica := func(alias string, ti *topo.TabletInfo) {
		defer replWg.Done()
		erp.logger.Infof("setting new primary on replica %v", alias)

		forceStart := false
		if status, ok := statusMap[alias]; ok {
			fs, err := ReplicaWasRunning(status)
			if err != nil {
				err = vterrors.Wrapf(err, "tablet %v could not determine StopReplicationStatus", alias)
				rec.RecordError(err)

				return
			}

			forceStart = fs
		}

		err := erp.tmc.SetReplicationSource(replCtx, ti.Tablet, newPrimaryTablet.Alias, 0, "", forceStart, policy.IsReplicaSemiSync(opts.durability, newPrimaryTablet, ti.Tablet), 0)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetReplicationSource failed", alias)
			rec.RecordError(err)

			return
		}

		replicaMutex.Lock()
		replicasStartedReplication = append(replicasStartedReplication, ti.Tablet)
		replicaMutex.Unlock()

		// Signal that at least one goroutine succeeded to SetReplicationSource.
		// We do this only when we do not want to wait for all the replicas.
		if !intermediateReparent {
			replSuccessCancel()
		}
	}

	numReplicas := 0

	for alias, ti := range tabletMap {
		switch {
		case alias == topoproto.TabletAliasString(newPrimaryTablet.Alias):
			continue
		case !opts.IgnoreReplicas.Has(alias):
			replWg.Add(1)
			numReplicas++
			go handleReplica(alias, ti)
		}
	}

	// Spin up a background goroutine to wait until all replica goroutines
	// finished. Polling this way allows us to have reparentReplicas return
	// success as soon as (a) the primary successfully populates its reparent
	// journal and (b) at least one replica successfully begins replicating.
	//
	// If we were to follow the more common pattern of blocking on replWg.Wait()
	// in the main body of promoteNewPrimary, we would be bound to the
	// time of slowest replica, instead of the time of the fastest successful
	// replica, and we want ERS to be fast.
	//
	// This goroutine also cancels replCtx after all replicas finish, so that
	// replicas that are still in-flight can complete their SetReplicationSource
	// calls even when this function returns early. For non-intermediate
	// reparents, this function returns after the first successful replica;
	// for intermediate reparents, it waits for all replicas to finish.
	// On primary failure, replCancel() is called immediately below,
	// which is safe because cancel functions are idempotent.
	go func() {
		replWg.Wait()
		allReplicasDoneCancel()
		replCancel()
	}()

	primaryErr := handlePrimary(topoproto.TabletAliasString(newPrimaryTablet.Alias), newPrimaryTablet)
	if primaryErr != nil {
		erp.logger.Errorf("failed to promote %s to primary", topoproto.TabletAliasString(newPrimaryTablet.Alias))
		replCancel()

		return nil, vterrors.Wrapf(primaryErr, "failed to promote %v to primary", topoproto.TabletAliasString(newPrimaryTablet.Alias))
	}

	select {
	case <-replSuccessCtx.Done():
		// At least one replica was able to SetReplicationSource successfully
		// Here we do not need to return the replicas which started replicating
		return nil, nil
	case <-allReplicasDoneCtx.Done():
		// There are certain timing issues between replSuccessCtx.Done firing
		// and allReplicasDoneCtx.Done firing, so we check again if truly all
		// replicas failed (where `numReplicas` goroutines recorded an error) or
		// one or more actually managed to succeed.
		errCount := len(rec.Errors)

		switch {
		case errCount > numReplicas:
			// Technically, rec.Errors should never be greater than numReplicas,
			// but it's better to err on the side of caution here, but also
			// we're going to be explicit that this is doubly unexpected.
			return nil, vterrors.Wrapf(rec.Error(), "received more errors (= %d) than replicas (= %d), which should be impossible", errCount, numReplicas)
		case errCount == numReplicas:
			if len(tabletMap) <= 2 {
				// If there are at most 2 tablets in the tablet map, we shouldn't be failing the promotion if the replica fails to SetReplicationSource.
				// The failing replica is probably the old primary that is down, so it is okay if it fails. We still log a warning message in the logs.
				erp.logger.Warningf("Failed to set the MySQL replication source during ERS but because there is only one other tablet we assume it is the one that had failed and will progress with the reparent. Error: %v", rec.Error())
				return nil, nil
			}
			return nil, vterrors.Wrapf(rec.Error(), "%d replica(s) failed", numReplicas)
		default:
			return replicasStartedReplication, nil
		}
	}
}

// isIntermediateSourceIdeal is used to find whether the intermediate source that ERS chose is also the ideal one or not
func (erp *EmergencyReparenter) isIntermediateSourceIdeal(
	intermediateSource *topodatapb.Tablet,
	validCandidates []*topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	opts EmergencyReparentOptions,
) (bool, error) {
	// we try to find a better candidate with the current list of valid candidates, and if it matches our current primary candidate, then we return true
	candidate, err := erp.identifyPrimaryCandidate(intermediateSource, validCandidates, tabletMap, opts)
	if err != nil {
		return false, err
	}
	return candidate == intermediateSource, nil
}

// identifyPrimaryCandidate is used to find the final candidate for ERS promotion
func (erp *EmergencyReparenter) identifyPrimaryCandidate(
	intermediateSource *topodatapb.Tablet,
	validCandidates []*topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	opts EmergencyReparentOptions,
) (candidate *topodatapb.Tablet, err error) {
	defer func() {
		if candidate != nil {
			erp.logger.Infof("found better candidate - %v", candidate.Alias)
		}
	}()

	if len(validCandidates) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	if opts.NewPrimaryAlias != nil {
		// explicit request to promote a specific tablet
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.NewPrimaryAlias)
		requestedPrimaryInfo, isFound := tabletMap[requestedPrimaryAlias]
		if !isFound {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", requestedPrimaryAlias)
		}
		if topoproto.IsTabletInList(requestedPrimaryInfo.Tablet, validCandidates) {
			return requestedPrimaryInfo.Tablet, nil
		}
		return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "requested candidate %v is not in valid candidates list", requestedPrimaryAlias)
	}

	// We have already selected an intermediate source which was selected based on the replication position
	// (ties broken by promotion rules), but that tablet might not even be a valid candidate i.e. it could
	// be in a different cell when we have PreventCrossCellPromotion specified, or it could have a promotion rule of
	// MustNot. Even if it is valid, there could be a tablet with a better promotion rule. This is what we try to
	// find here.
	// We go over all the promotion rules in descending order of priority and try and find a valid candidate with
	// that promotion rule.
	// If the intermediate source has the same promotion rules as some other tablets, then we prioritize using
	// the intermediate source since we won't have to wait for the new candidate to catch up!
	for _, promotionRule := range promotionrule.AllPromotionRules() {
		candidates := getTabletsWithPromotionRules(opts.durability, validCandidates, promotionRule)
		candidate = findCandidate(intermediateSource, candidates)
		if candidate != nil {
			return candidate, nil
		}
	}
	// Unreachable code.
	// We should have found at least 1 tablet in the valid list.
	// If the list is empty, then we should have errored out much sooner.
	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unreachable - did not find a valid primary candidate even though the valid candidate list was non-empty")
}

// filterValidCandidates filters valid tablets, keeping only the ones which can successfully be promoted without any
// constraint failures and can make forward progress on being promoted. It will filter out candidates taking backups
// if possible.
func (erp *EmergencyReparenter) filterValidCandidates(validTablets []*topodatapb.Tablet, tabletsReachable []*topodatapb.Tablet, tabletsBackupState map[string]bool, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) ([]*topodatapb.Tablet, error) {
	var restrictedValidTablets []*topodatapb.Tablet
	var notPreferredValidTablets []*topodatapb.Tablet
	for _, tablet := range validTablets {
		tabletAliasStr := topoproto.TabletAliasString(tablet.Alias)
		// Remove tablets which have MustNot promote rule since they must never be promoted
		if policy.PromotionRule(opts.durability, tablet) == promotionrule.MustNot {
			erp.logger.Infof("Removing %s from list of valid candidates for promotion because it has the Must Not promote rule", tabletAliasStr)
			if opts.NewPrimaryAlias != nil && topoproto.TabletAliasEqual(opts.NewPrimaryAlias, tablet.Alias) {
				return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "proposed primary %s has a must not promotion rule", topoproto.TabletAliasString(opts.NewPrimaryAlias))
			}
			continue
		}
		// If ERS is configured to prevent cross cell promotions, remove any tablet not from the same cell as the previous primary
		if opts.PreventCrossCellPromotion && prevPrimary != nil && tablet.Alias.Cell != prevPrimary.Alias.Cell {
			erp.logger.Infof("Removing %s from list of valid candidates for promotion because it isn't in the same cell as the previous primary", tabletAliasStr)
			if opts.NewPrimaryAlias != nil && topoproto.TabletAliasEqual(opts.NewPrimaryAlias, tablet.Alias) {
				return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "proposed primary %s is is a different cell as the previous primary", topoproto.TabletAliasString(opts.NewPrimaryAlias))
			}
			continue
		}
		// Remove any tablet which cannot make forward progress using the list of tablets we have reached
		if !canEstablishForTablet(opts.durability, tablet, tabletsReachable) {
			erp.logger.Infof("Removing %s from list of valid candidates for promotion because it will not be able to make forward progress on promotion with the tablets currently reachable", tabletAliasStr)
			if opts.NewPrimaryAlias != nil && topoproto.TabletAliasEqual(opts.NewPrimaryAlias, tablet.Alias) {
				return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "proposed primary %s will not be able to make forward progress on being promoted", topoproto.TabletAliasString(opts.NewPrimaryAlias))
			}
			continue
		}
		// Put candidates that are running a backup in a separate list
		backingUp, ok := tabletsBackupState[tabletAliasStr]
		if ok && backingUp {
			erp.logger.Infof("Setting %s in list of valid candidates taking a backup", tabletAliasStr)
			notPreferredValidTablets = append(notPreferredValidTablets, tablet)
		} else {
			restrictedValidTablets = append(restrictedValidTablets, tablet)
		}
	}
	if len(restrictedValidTablets) > 0 {
		return restrictedValidTablets, nil
	}

	return notPreferredValidTablets, nil
}

// findErrantGTIDs tries to find errant GTIDs for the valid candidates and returns the updated list of valid candidates.
// This function does not actually return the identities of errant GTID tablets, if any. It only returns the identities of non-errant GTID tablets, which are eligible for promotion.
// The caller of this function (ERS) will then choose from among the list of candidate tablets, based on higher-level criteria.
func (erp *EmergencyReparenter) findErrantGTIDs(
	ctx context.Context,
	validCandidates map[string]*RelayLogPositions,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	tabletMap map[string]*topo.TabletInfo,
	waitReplicasTimeout time.Duration,
) (map[string]*RelayLogPositions, error) {
	// First we need to collect the reparent journal length for all the candidates.
	// This will tell us, which of the tablets are severly lagged, and haven't even seen all the primary promotions.
	// Such severely lagging tablets cannot be used to find errant GTIDs in other tablets, seeing that they themselves don't have enough information.
	reparentJournalLen, err := erp.gatherReparenJournalInfo(ctx, validCandidates, tabletMap, waitReplicasTimeout)
	if err != nil {
		return nil, err
	}

	// Find the maximum length of the reparent journal among all the candidates.
	var maxLen int32
	for _, length := range reparentJournalLen {
		maxLen = max(maxLen, length)
	}

	// Find the candidates with the maximum length of the reparent journal.
	var maxLenCandidates []string
	for alias, length := range reparentJournalLen {
		if length == maxLen {
			maxLenCandidates = append(maxLenCandidates, alias)
		}
	}

	// We use all the candidates with the maximum length of the reparent journal to find the errant GTIDs amongst them.
	var maxLenPositions []replication.Position
	updatedValidCandidates := make(map[string]*RelayLogPositions)
	for _, candidate := range maxLenCandidates {
		candidatePositions := validCandidates[candidate]
		if candidatePositions == nil || candidatePositions.IsZero() {
			erp.logger.Warningf("skipping candidate %s during errant GTID detection: nil or zero positions", candidate)
			continue
		}

		status, ok := statusMap[candidate]
		if !ok {
			// If the tablet is not in the status map, and has the maximum length of the reparent journal,
			// then it should be the latest primary and we don't need to run any errant GTID detection on it!
			// There is a very unlikely niche case that can happen where we see two tablets report themselves as having
			// the maximum reparent journal length and also be primaries. Here is the outline of it -
			// 1. Tablet A is the primary and reparent journal length is 3.
			// 2. It gets network partitioned, and we promote tablet B as the new primary.
			// 3. tablet B gets network partitioned before it has written to the reparent journal, and a new ERS call ensues.
			// 4. During this ERS call, both A and B are seen online. They would both report being primary tablets with the same reparent journal length.
			// Even in this case, the best we can do is not run errant GTID detection on either, and let the split brain detection code
			// deal with it, if A in fact has errant GTIDs.
			maxLenPositions = append(maxLenPositions, candidatePositions.Combined)
			updatedValidCandidates[candidate] = validCandidates[candidate]
			continue
		}
		// Store all the other candidate's positions so that we can run errant GTID detection using them.
		otherPositions := make([]replication.Position, 0, len(maxLenCandidates)-1)
		for _, otherCandidate := range maxLenCandidates {
			if otherCandidate == candidate {
				continue
			}
			otherPosition := validCandidates[otherCandidate]
			if otherPosition != nil && !otherPosition.IsZero() {
				otherPositions = append(otherPositions, otherPosition.Combined)
			}
		}
		// Run errant GTID detection and throw away any tablet that has errant GTIDs.
		afterStatus := replication.ProtoToReplicationStatus(status.After)
		errantGTIDs, err := replication.FindErrantGTIDs(afterStatus.RelayLogPosition, afterStatus.SourceUUID, otherPositions)
		if err != nil {
			return nil, err
		}
		if errantGTIDs != nil {
			log.Error(fmt.Sprintf("skipping %v with GTIDSet:%v because we detected errant GTIDs - %v", candidate, afterStatus.RelayLogPosition.GTIDSet, errantGTIDs))
			continue
		}
		maxLenPositions = append(maxLenPositions, candidatePositions.Combined)
		updatedValidCandidates[candidate] = validCandidates[candidate]
	}

	// For all the other tablets, that are lagged enough that they haven't seen all the reparent journal entries,
	// we run errant GTID detection by using the tablets with the maximum length of the reparent journal.
	// We throw away any tablet that has errant GTIDs.
	for alias, length := range reparentJournalLen {
		if length == maxLen {
			continue
		}
		// Here we don't want to send the source UUID. The reason is that all of these tablets are lagged,
		// so we don't need to use the source UUID to discount any GTIDs.
		// To explain this point further, let me use an example. Consider the following situation -
		// 1. Tablet A is the primary and B is a rdonly replica.
		// 2. They both get network partitioned, and then a new ERS call ensues, and we promote tablet C.
		// 3. Tablet C also fails, and we run a new ERS call.
		// 4. During this ERS, B comes back online and is visible. Since it hasn't seen the last reparent journal entry
		//    it will be considered lagged.
		// 5. If it has an errant GTID that was written by A, then we want to find that errant GTID. Since B hasn't reparented to a
		//    different tablet, it would still be replicating from A. This means its server UUID would be A.
		// 6. Because we don't want to discount the writes from tablet A, when we're doing the errant GTID detection on B, we
		//    choose not to pass in the server UUID.
		// This exact scenario outlined above, can be found in the test for this function, subtest `Case 5a`.
		// The idea is that if the tablet is lagged, then even the server UUID that it is replicating from
		// should not be considered a valid source of writes that no other tablet has.
		candidatePositions := validCandidates[alias]
		if candidatePositions == nil || candidatePositions.IsZero() {
			continue
		}
		errantGTIDs, err := replication.FindErrantGTIDs(candidatePositions.Combined, replication.SID{}, maxLenPositions)
		if err != nil {
			return nil, err
		}
		if errantGTIDs != nil {
			log.Error(fmt.Sprintf("skipping %v with GTIDSet:%v because we detected errant GTIDs - %v", alias, candidatePositions, errantGTIDs))
			continue
		}
		updatedValidCandidates[alias] = candidatePositions
	}

	return updatedValidCandidates, nil
}

// gatherReparenJournalInfo reads the reparent journal information from all the tablets in the valid candidates list.
func (erp *EmergencyReparenter) gatherReparenJournalInfo(
	ctx context.Context,
	validCandidates map[string]*RelayLogPositions,
	tabletMap map[string]*topo.TabletInfo,
	waitReplicasTimeout time.Duration,
) (map[string]int32, error) {
	reparentJournalLen := make(map[string]int32)
	var mu sync.Mutex
	errCh := make(chan concurrency.Error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

	for candidate := range validCandidates {
		go func(alias string) {
			var err error
			var length int32
			defer func() {
				errCh <- concurrency.Error{
					Err: err,
				}
			}()
			length, err = erp.tmc.ReadReparentJournalInfo(groupCtx, tabletMap[alias].Tablet)
			mu.Lock()
			defer mu.Unlock()
			reparentJournalLen[alias] = length
		}(candidate)

		waiterCount++
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        waiterCount,
		NumRequiredSuccesses: waiterCount,
		NumAllowedErrors:     0,
	}
	rec := errgroup.Wait(groupCancel, errCh)

	if len(rec.Errors) != 0 {
		return nil, vterrors.Wrapf(rec.Error(), "could not read reparent journal information within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}

	return reparentJournalLen, nil
}
