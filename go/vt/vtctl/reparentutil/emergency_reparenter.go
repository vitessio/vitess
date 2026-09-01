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
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
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

// counters for Emergency Reparent Shard
var (
	ersCounter = stats.NewCountersWithMultiLabels(
		"EmergencyReparentCounts", "Number of times Emergency Reparent Shard has been run",
		[]string{"Keyspace", "Shard", "Result"},
	)
	ersSplitBrainOverrides = stats.NewCountersWithMultiLabels(
		"EmergencyReparentSplitBrainOverrides", "Number of Emergency Reparent Shard split-brain promotions completed for an explicitly selected primary",
		[]string{"Keyspace", "Shard"},
	)
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
	AllowSplitBrainPromotion  bool
	PreventCrossCellPromotion bool
	ExpectedPrimaryAlias      *topodatapb.TabletAlias

	// Private options managed internally. We use value passing to avoid leaking
	// these details back out.
	lockAction string
	durability policy.Durabler
}

// ersCandidate holds the state ERS tracks for one promotion candidate. It is
// built once, then reused as the candidate pool is filtered, so its pointer is a
// stable identity for the whole run.
type ersCandidate struct {
	info      *topo.TabletInfo
	positions *RelayLogPositions

	mysqlVersion    mysqlctl.ServerVersion
	mysqlFlavor     mysqlctl.MySQLFlavor
	hasMySQLVersion bool

	// stopStatus is nil for a tablet that reported itself as primary rather than
	// stopping replication, which is how the pipeline tells the two apart.
	stopStatus   *replicationdatapb.StopReplicationStatus
	takingBackup bool

	// reparentJournalLen is filled in during errant-GTID detection.
	reparentJournalLen int32
}

func (c *ersCandidate) alias() string {
	return topoproto.TabletAliasString(c.info.Alias)
}

func (c *ersCandidate) tablet() *topodatapb.Tablet {
	return c.info.Tablet
}

// isSameTablet compares aliases; a candidate without one never matches.
func (c *ersCandidate) isSameTablet(other *ersCandidate) bool {
	if c == nil || other == nil || c.info == nil || other.info == nil {
		return false
	}
	if topoproto.TabletAliasIsZero(c.info.Alias) || topoproto.TabletAliasIsZero(other.info.Alias) {
		return false
	}

	return topoproto.TabletAliasEqual(c.info.Alias, other.info.Alias)
}

// relayLogResult is a single tablet's result from waiting on its relay logs to apply.
type relayLogResult struct {
	candidate *ersCandidate
	err       error
}

// relayLogWaitResult is the per-tablet outcome of waitForRelayLogsToApply.
type relayLogWaitResult struct {
	// applied are the tablets that finished applying their relay logs.
	applied []*ersCandidate

	// failed are the tablets that couldn't apply their relay logs (RPC error, MySQL
	// error or timeout).
	failed []*ersCandidate

	// cancelled are the tablets we stopped waiting on, because a peer finished or failed
	// first, or the reparent was aborted. We know nothing about their apply progress.
	cancelled []*ersCandidate
}

// findERSCandidateByAlias returns a single *ersCandidate by tablet alias.
func findERSCandidateByAlias(candidates []*ersCandidate, alias *topodatapb.TabletAlias) *ersCandidate {
	for _, candidate := range candidates {
		if topoproto.TabletAliasEqual(candidate.info.Alias, alias) {
			return candidate
		}
	}

	return nil
}

// ersCandidateAliases returns a slice of tablet alias strings of candidates.
func ersCandidateAliases(candidates []*ersCandidate) []string {
	aliases := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		aliases = append(aliases, candidate.alias())
	}

	return aliases
}

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

	if err = validateEmergencyReparentOptions(opts); err != nil {
		return nil, err
	}
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

func validateEmergencyReparentOptions(opts EmergencyReparentOptions) error {
	if !opts.AllowSplitBrainPromotion {
		return nil
	}
	if opts.NewPrimaryAlias == nil || topoproto.TabletAliasIsZero(opts.NewPrimaryAlias) {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "split-brain promotion requires an explicitly requested primary (--new-primary)")
	}
	return nil
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
		candidates                 []*ersCandidate
		intermediateSource         *ersCandidate
		validReplacementCandidates []*ersCandidate
		betterCandidate            *ersCandidate
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
		replicasToRestart = stoppedReplicationSnapshot.replicasWithStoppedIO(tabletMap)
	}

	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps")
	}

	// check that we still have the shard lock. If we don't then we can terminate at this point
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	// Find the positions of all the valid candidates.
	candidates, isGTIDBased, err = buildERSCandidates(stoppedReplicationSnapshot, tabletMap)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// A candidate that is behind the most-advanced received (Combined) position was never
	// going to win the election, so for GTID-based shards we only wait on the leading
	// candidates. They all received the same changes, so the first one to finish applying
	// is enough; the others are still repointed to the new primary afterwards, they just
	// can't hold up or fail the reparent. For non-GTID-based shards (FilePos, MariaDB) the
	// Combined position only reflects what is executed, so we keep the previous behaviour
	// of waiting for every candidate and failing on any error.
	waitCandidates := candidates
	requireAll := true
	splitBrainOverrideActive := false
	var suspectedSplitBrainCandidates []*ersCandidate
	if isGTIDBased {
		waitCandidates = filterToMostAdvancedCombined(candidates, erp.logger)
		requireAll = !hasUniformCombinedPosition(waitCandidates)
		if requireAll {
			leadingPositions := describeCombinedPositions(waitCandidates)
			if !opts.AllowSplitBrainPromotion {
				suspectedSplitBrainCandidates = slices.Clone(waitCandidates)
			} else {
				requestedPrimary := topoproto.TabletAliasString(opts.NewPrimaryAlias)
				requestedCandidate := findERSCandidateByAlias(waitCandidates, opts.NewPrimaryAlias)
				if requestedCandidate == nil {
					return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "requested primary %s is not a leading candidate in the suspected split-brain: %s", requestedPrimary, leadingPositions)
				}

				splitBrainOverrideActive = true
				erp.logger.Warningf("EmergencyReparentShard attempting split-brain promotion in keyspace %s shard %s for new primary %s; the divergent branches of the other leading candidates (%s) will be discarded and those tablets may require rebuilding from the new primary", keyspace, shard, requestedPrimary, leadingPositions)

				// Promote exactly the requested primary and discard the divergent branches.
				// Reducing the candidate set here means the relay-log wait, the errant-GTID
				// skip, and the election all operate on that one tablet: only it needs to apply
				// its relay logs, so a wedged losing branch cannot block the recovery this
				// override exists for.
				candidates = []*ersCandidate{requestedCandidate}
				waitCandidates = candidates
			}
		}
	}

	// The wait budget also covers the possible second wait after errant GTID detection
	// below, so ERS spends at most WaitReplicasTimeout in total waiting for relay logs
	// to apply. Time spent in errant GTID detection doesn't count against it
	waitStart := time.Now()
	var waitResult *relayLogWaitResult
	candidates, waitResult, err = erp.applyRelayLogsAndReconcile(ctx, waitCandidates, candidates, opts.WaitReplicasTimeout, requireAll, isGTIDBased)
	if err != nil {
		return err
	}
	relayLogBudgetLeft := max(opts.WaitReplicasTimeout-time.Since(waitStart), 0)

	// A tablet that failed its relay log wait returned a real error, so don't let it
	// count as a semi-sync acker in the forward-progress checks below; promoting a
	// primary whose only acker is broken would wedge it waiting for an ACK. Cancelled
	// waits say nothing about the tablet and stay counted
	nonAckers := ersCandidateAliases(waitResult.failed)

	// Tablets whose replication was fully stopped before the reparent are repointed
	// without being started, so they can't send semi-sync ACKs either until an operator
	// starts them
	for alias, status := range stoppedReplicationSnapshot.statusMap {
		if wasRunning, wasRunningErr := ReplicaWasRunning(status); wasRunningErr != nil || !wasRunning {
			nonAckers = append(nonAckers, alias)
		}
	}

	// For GTID based replication, we will run errant GTID detection.
	if isGTIDBased && !splitBrainOverrideActive {
		// Errant GTID detection may only treat all-empty candidates as a brand-new
		// shard when the topology agrees it was never initialized: a shard that has
		// recorded a primary has history to protect, even if every reachable tablet
		// lost it
		shardNeverInitialized := !ev.ShardInfo.HasPrimary() && ev.ShardInfo.PrimaryTermStartTime == nil
		// Failed waiters are only ever removed from a uniform leading group (a
		// requireAll wait aborts on failure instead of removing anyone), so a failed
		// tablet received exactly what the surviving leaders received, including every
		// reparent journal entry: its evidence is max-journal-grade by construction.
		// A failed waiter was dropped from the candidates but is still in this list, so
		// its position survives the reconcile that removed it
		var failedEvidence []replication.Position
		for _, failedCandidate := range waitResult.failed {
			if failedCandidate.positions != nil && !failedCandidate.positions.IsZero() {
				failedEvidence = append(failedEvidence, failedCandidate.positions.Combined)
			}
		}
		var starved []*ersCandidate
		candidates, starved, err = erp.findErrantGTIDs(ctx, candidates, opts.WaitReplicasTimeout, failedEvidence, shardNeverInitialized)
		if err != nil {
			return err
		}
		starvedAliases := ersCandidateAliases(starved)

		// A candidate accepted without any evidence may be a blind spot of our own
		// making: reparent journal counts only advance when relay logs are applied, so
		// a candidate we skipped in the relay log wait that received the latest journal
		// entry without applying it yet reads as lagged and can't corroborate the
		// leader. Wait on the skipped candidates so their counts become truthful, then
		// re-run the detection. In steady state every candidate shares the same journal
		// count and detection has evidence, so this only triggers when ERS runs shortly
		// after a previous reparent
		if len(starved) > 0 {
			rescueCandidates := make([]*ersCandidate, 0, len(candidates))
			for _, candidate := range candidates {
				if slices.Contains(waitCandidates, candidate) {
					continue
				}
				if candidate.stopStatus == nil {
					continue
				}
				rescueCandidates = append(rescueCandidates, candidate)
			}
			if len(rescueCandidates) > 0 && relayLogBudgetLeft <= 0 {
				// The blind spot only exists because we skipped these peers in the relay
				// log wait, and no budget remains to make their journal counts truthful.
				// Accepting the starved leader now would promote it without ever proving
				// the missing evidence wasn't of our own making, which could mean electing
				// a leader with an errant GTID that a peer would have exposed. Fail closed
				// rather than run a doomed zero-budget wait or accept it unconfirmed.
				return vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "errant GTID detection could not corroborate %v and the relay log wait budget (%s) was exhausted before the skipped candidates could apply their relay logs", starvedAliases, opts.WaitReplicasTimeout)
			} else if len(rescueCandidates) > 0 {
				erp.logger.Warningf("errant GTID detection had no evidence to corroborate %v; waiting for the skipped candidates to apply their relay logs and re-running the detection", starvedAliases)
				// A dominated rescue candidate can't hold a journal entry its dominator
				// lacks, so only the most-advanced skipped candidates are waited on: a
				// stuck straggler must not abort the reparent from the rescue path.
				// Racing the remainder is only safe when they all received the same
				// changes; incomparable maxima are each waited on so the race can't
				// cancel the peer that actually holds the latest journal entry
				rescueCandidates = filterToMostAdvancedCombined(rescueCandidates, erp.logger)
				requireAll = !hasUniformCombinedPosition(rescueCandidates)
				rescueStart := time.Now()
				var rescueResult *relayLogWaitResult
				candidates, rescueResult, err = erp.applyRelayLogsAndReconcile(ctx, rescueCandidates, candidates, relayLogBudgetLeft, requireAll, true /* isGTIDBased */)
				if err != nil {
					return err
				}
				relayLogBudgetLeft = max(relayLogBudgetLeft-time.Since(rescueStart), 0)
				nonAckers = append(nonAckers, ersCandidateAliases(rescueResult.failed)...)
				waitResult.applied = append(waitResult.applied, rescueResult.applied...)

				// If a candidate still has no evidence now that the counts are
				// truthful, the other candidates genuinely lack journal entries and
				// there is nothing more to compare against, same as before this
				// optimization: accept it
				candidates, _, err = erp.findErrantGTIDs(ctx, candidates, opts.WaitReplicasTimeout, failedEvidence, shardNeverInitialized)
				if err != nil {
					return err
				}
			}
		}

		if len(suspectedSplitBrainCandidates) > 0 {
			survivingLeaders := 0
			for _, suspectedCandidate := range suspectedSplitBrainCandidates {
				if slices.Contains(candidates, suspectedCandidate) {
					survivingLeaders++
				}
			}
			if survivingLeaders != 1 {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "suspected split-brain: leading candidates have incomparable Combined GTID positions: %s; to continue, choose the history to preserve with --new-primary and --allow-split-brain-promotion, discarding transactions that exist only on the other leaders", describeCombinedPositions(suspectedSplitBrainCandidates))
			}
		}

		if len(candidates) == 0 {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent: all candidates have errant GTIDs")
		}

		// If errant GTID detection removed every tablet that applied its relay logs, the
		// surviving candidates may still have relay logs to apply. Wait on the leading
		// survivors before electing one; we never promote a tablet that hasn't applied
		// everything it received
		var appliedSurvived bool
		for _, appliedCandidate := range waitResult.applied {
			if slices.Contains(candidates, appliedCandidate) {
				appliedSurvived = true
				break
			}
		}
		if !appliedSurvived {
			rewaitCandidates := filterToMostAdvancedCombined(candidates, erp.logger)
			// A leading survivor that is in the status map still has relay logs to apply;
			// promoting it without waiting would violate the received-but-unapplied rule.
			// A demoted former primary is absent from the status map and is exempt (its
			// received and executed positions are equal), so it needs no wait.
			needsWait := false
			for _, candidate := range rewaitCandidates {
				if candidate.stopStatus != nil {
					needsWait = true
					break
				}
			}
			if needsWait && relayLogBudgetLeft <= 0 {
				// A candidate needs to apply relay logs but no budget remains to wait; the
				// wait would fail on an already-expired context, so fail cleanly with the
				// real reason instead of a synthetic deadline error.
				return vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "no candidate that applied its relay logs survived errant GTID detection and the relay log wait budget (%s) was exhausted before the remaining candidates could apply their relay logs", opts.WaitReplicasTimeout)
			}
			if needsWait {
				erp.logger.Warningf("no candidate that applied its relay logs survived errant GTID detection; waiting for the remaining candidates to apply their relay logs")
				requireAll = !hasUniformCombinedPosition(rewaitCandidates)
				var rewaitResult *relayLogWaitResult
				candidates, rewaitResult, err = erp.applyRelayLogsAndReconcile(ctx, rewaitCandidates, candidates, relayLogBudgetLeft, requireAll, true /* isGTIDBased */)
				if err != nil {
					return err
				}
				nonAckers = append(nonAckers, ersCandidateAliases(rewaitResult.failed)...)
			}
		}
	}

	// Find the intermediate source for replication that we want other tablets to replicate from.
	// This step chooses the most advanced tablet. Further ties are broken by using the promotion rule.
	// In case the user has specified a tablet specifically, then it is selected, as long as it is the most advanced.
	// Here we also check for split brain scenarios and check that the selected replica must be more advanced than all the other valid candidates.
	// We fail in case there is a split brain detected.
	// The candidates are sorted by replication position with ties broken by promotion rules.
	intermediateSource, candidates, err = erp.findMostAdvanced(candidates, opts)
	if err != nil {
		return err
	}
	erp.logger.Infof("intermediate source selected - %s", intermediateSource.alias())

	// After finding the intermediate source, we want to filter the valid candidate list by the following criteria -
	// 1. Only keep the tablets which can make progress after being promoted (have sufficient reachable semi-sync ackers)
	// 2. Remove the tablets with the Must_not promote rule
	// 3. Remove cross-cell tablets if PreventCrossCellPromotion is specified
	// Our final primary candidate MUST belong to this list of valid candidates
	candidates, err = erp.filterValidCandidates(candidates, stoppedReplicationSnapshot.reachableTablets, nonAckers, prevPrimary, opts)
	if err != nil {
		return err
	}

	// Check whether the intermediate source candidate selected is ideal or if it can be improved later.
	// If the intermediateSource is ideal, then we can be certain that it is part of the valid candidates list.
	isIdeal, err = erp.isIntermediateSourceIdeal(intermediateSource, candidates, opts)
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
		// It also returns the candidates that started replication successfully, including the intermediate source.
		// These are the candidates that we can use to find a replacement.
		validReplacementCandidates, err = erp.promoteIntermediateSource(ctx, ev, intermediateSource, candidates, stoppedReplicationSnapshot.statusMap, opts)
		if err != nil {
			return err
		}

		// try to find a better candidate using the list we got back
		// We prefer to choose a candidate which is in the same cell as our previous primary and of the best possible durability rule.
		// However, if there is an explicit request from the user to promote a specific tablet, then we choose that tablet.
		betterCandidate, err = erp.identifyPrimaryCandidate(intermediateSource, validReplacementCandidates, opts)
		if err != nil {
			return err
		}

		// if our better candidate is different from our intermediate source, then we wait for it to catch up to the intermediate source
		if !topoproto.TabletAliasEqual(betterCandidate.tablet().Alias, intermediateSource.tablet().Alias) {
			err = waitForCatchUp(ctx, erp.tmc, erp.logger, betterCandidate.tablet(), intermediateSource.tablet(), opts.WaitReplicasTimeout)
			if err != nil {
				return err
			}
			newPrimary = betterCandidate
		}

		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, lostTopologyLockMsg)
		}
	}

	// The new primary which will be promoted will always belong to the valid candidates because -
	// 	1. 	if the intermediate source is ideal - then we know the intermediate source was in the valid candidates
	// 		since we used that list
	//	2. 	if the intermediate source isn't ideal - we take the intersection of the valid candidates and the ones we
	//		were able to reach during the promotion of intermediate source, as possible candidates. So the final candidate (even if
	//		it is the intermediate source itself) will belong to the list
	// Since the new primary belongs to the valid candidates, we no longer need any additional constraint checks

	// Final step is to promote our primary candidate
	_, err = erp.reparentReplicas(ctx, ev, newPrimary.tablet(), tabletMap, stoppedReplicationSnapshot.statusMap, opts, nonAckers, splitBrainOverrideActive, false /* intermediateReparent */)
	if err != nil {
		return err
	}

	ev.NewPrimary = newPrimary.tablet().CloneVT()
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

// waitForRelayLogsToApply waits for the given candidates to apply their relay logs and
// reports the per-tablet outcome. With requireAll any failure fails the whole wait;
// without it the candidates all received the same changes, so the first one to finish
// applying wins and the remaining waits are cancelled.
func (erp *EmergencyReparenter) waitForRelayLogsToApply(
	ctx context.Context,
	candidates []*ersCandidate,
	waitReplicasTimeout time.Duration,
	requireAll bool,
) (*relayLogWaitResult, error) {
	resultCh := make(chan relayLogResult, len(candidates))

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

	for _, candidate := range candidates {
		// stopStatus is set for replicas that stopped replication. It is nil for
		// anything that returned ErrNotReplica, which is either the current primary
		// or a tablet that is stuck thinking it is PRIMARY but is not in actuality.
		//
		// If stopStatus is nil, we have either (a) the current primary, which is
		// not replicating, so it is not applying relay logs; or (b) a tablet that
		// is stuck thinking it is PRIMARY but is not in actuality. In that
		// second case - (b) - we will most likely find that the stuck PRIMARY
		// does not have a winning position, and fail the ERS. If, on the other
		// hand, it does have a winning position, we are trusting the operator
		// to know what they are doing by emergency-reparenting onto that
		// tablet. In either case, it does not make sense to wait for relay logs
		// to apply on a tablet that was never applying relay logs in the first
		// place, so we skip it, and log that we did.
		if candidate.stopStatus == nil {
			erp.logger.Infof("EmergencyReparent candidate %s not in replica status map; this means it was not running replication (because it was formerly PRIMARY), so skipping WaitForRelayLogsToApply step for this candidate", candidate.alias())
			continue
		}

		go func(candidate *ersCandidate) {
			resultCh <- relayLogResult{
				candidate: candidate,
				err:       WaitForRelayLogsToApply(groupCtx, erp.tmc, candidate.info, candidate.stopStatus),
			}
		}(candidate)

		waiterCount++
	}

	result := &relayLogWaitResult{}

	// nothing to wait for. still fail if the reparent was aborted
	if waiterCount == 0 {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, vterrors.Wrapf(ctxErr, "emergency reparent aborted while waiting for relay logs to apply")
		}
		return result, nil
	}

	var firstFailure error
	var weCancelled bool
	for range waiterCount {
		res := <-resultCh
		switch {
		case res.err == nil:
			result.applied = append(result.applied, res.candidate)
			if !requireAll && !weCancelled {
				// one of the candidates finished applying. the others received the same
				// changes and can't do better, so stop waiting on them
				weCancelled = true
				groupCancel()
			}
		case (weCancelled || errors.Is(ctx.Err(), context.Canceled)) && isCancellationError(res.err):
			// we stopped waiting on this tablet on purpose (or the reparent was
			// explicitly aborted), it didn't fail. a parent deadline expiry is not
			// intentional: the tablet didn't finish in budget and counts as failed below
			result.cancelled = append(result.cancelled, res.candidate)
		default:
			result.failed = append(result.failed, res.candidate)
			erp.logger.Warningf("EmergencyReparent candidate %s failed to apply relay logs: %v", res.candidate.alias(), res.err)
			if firstFailure == nil {
				firstFailure = res.err
				if requireAll {
					// a single failure already fails the wait, no point waiting for the
					// others
					weCancelled = true
					groupCancel()
				}
			}
		}
	}

	// an aborted reparent beats any per-tablet outcome. only an explicit cancellation
	// counts as an abort here; a parent deadline expiry is indistinguishable from the
	// relay log budget expiring (they usually share a deadline), so it keeps the timeout
	// wording below instead
	if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
		return result, vterrors.Wrapf(ctxErr, "emergency reparent aborted while waiting for relay logs to apply")
	}
	if requireAll && firstFailure != nil {
		return result, vterrors.Wrapf(firstFailure, "could not apply all relay logs within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}
	if len(result.applied) == 0 {
		// defensive fallbacks: this must never return success without a single applied
		// candidate, even if a future classification change leaves firstFailure nil
		err := firstFailure
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			err = vterrors.Errorf(vtrpc.Code_INTERNAL, "no relay log wait succeeded or failed")
		}
		return result, vterrors.Wrapf(err, "all candidates failed to apply relay logs within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}

	return result, nil
}

// applyRelayLogsAndReconcile waits for the waitCandidates to apply their relay logs and
// reconciles validCandidates with the outcome: failed tablets are removed as they can't be
// promoted, and applied tablets get their Executed position bumped to Combined so the
// sorter prefers them over peers whose wait we cancelled midway. The bump only applies to
// GTID-based candidates; non-GTID candidates store their executed position in Combined and
// intentionally leave Executed zero, so rewriting it would change how they sort against an
// unwaited former primary.
func (erp *EmergencyReparenter) applyRelayLogsAndReconcile(
	ctx context.Context,
	waitCandidates []*ersCandidate,
	validCandidates []*ersCandidate,
	waitReplicasTimeout time.Duration,
	requireAll bool,
	isGTIDBased bool,
) ([]*ersCandidate, *relayLogWaitResult, error) {
	waitResult, err := erp.waitForRelayLogsToApply(ctx, waitCandidates, waitReplicasTimeout, requireAll)
	if err != nil {
		return validCandidates, waitResult, err
	}

	reconciled := make([]*ersCandidate, 0, len(validCandidates))
	for _, candidate := range validCandidates {
		if slices.Contains(waitResult.failed, candidate) {
			erp.logger.Warningf("EmergencyReparent candidate %s failed to apply its relay logs and cannot be promoted; removing it from the valid candidates", candidate.alias())
			continue
		}
		reconciled = append(reconciled, candidate)
	}
	for _, candidate := range waitResult.applied {
		if !slices.Contains(reconciled, candidate) {
			continue
		}
		erp.logger.Infof("EmergencyReparent candidate %s applied all of its received relay logs", candidate.alias())
		if isGTIDBased {
			candidate.positions.Executed = candidate.positions.Combined
		}
	}
	for _, candidate := range waitResult.cancelled {
		erp.logger.Infof("EmergencyReparent candidate %s had its relay log wait cancelled after a peer finished applying; keeping its received position", candidate.alias())
	}

	return reconciled, waitResult, nil
}

// findMostAdvanced finds the intermediate source for ERS. We always choose the most advanced one from our valid candidates list. Further ties are broken by looking at the promotion rules.
// It sorts validCandidates in place and returns that same slice, so the caller's ordering
// is replaced by the promotion order rather than preserved.
func (erp *EmergencyReparenter) findMostAdvanced(
	validCandidates []*ersCandidate,
	opts EmergencyReparentOptions,
) (*ersCandidate, []*ersCandidate, error) {
	erp.logger.Infof("started finding the intermediate source")
	if len(validCandidates) == 0 {
		return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Version ordering is disabled when the candidates span flavor families. The guard is
	// scoped to the candidates being sorted, so a tablet elsewhere in the shard that is
	// not a candidate cannot disable version comparison for the ones being elected among.
	mysqlVersions, mixedFlavorFamilies := usableERSCandidateMySQLVersions(validCandidates)
	if mixedFlavorFamilies {
		erp.logger.Warningf("reparent candidates span multiple MySQL flavor families; skipping version-aware election")
	}
	if err := sortERSCandidates(validCandidates, mysqlVersions, opts.durability); err != nil {
		return nil, nil, err
	}
	for _, candidate := range validCandidates {
		erp.logger.Infof("finding intermediate source - sorted replica: %s", candidate.alias())
	}

	// The first tablet in the sorted list will be the most eligible candidate unless explicitly asked for some other tablet
	winningCandidate := validCandidates[0]
	winningPosition := winningCandidate.positions

	// We have already removed the tablets with errant GTIDs before calling this function. At this point our winning position must be a
	// superset of all the other valid positions. If any position is incomparable with it, then we have a split brain scenario, and we
	// should cancel the ERS. Split brain is about divergent received history, so we only compare the Combined positions; the Executed
	// positions can be transiently incomparable at an equal Combined position (multi-threaded apply gaps) without any divergence.
	// Reciprocally contained but unequal positions are divergent too, containment just can't order them (MariaDB GTID containment
	// ignores the origin server), so they must also fail closed. The divergent pair can sit behind a candidate that dominates both
	// of them, so reciprocal containment is checked between every pair of candidates, not just against the winning position
	for i, candidate := range validCandidates {
		position := candidate.positions
		if haveIncomparablePositions(winningPosition.Combined, position.Combined) {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "split brain detected between servers - %s and %s", winningCandidate.alias(), candidate.alias())
		}
		// Keep the sort's maximum-at-index-zero guarantee as a defense-in-depth invariant.
		if hasDominantReparentPosition(position, winningPosition) {
			return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate sorting error: %s has a more advanced position than the chosen candidate %s", candidate.alias(), winningCandidate.alias())
		}
		for j := i + 1; j < len(validCandidates); j++ {
			if haveReciprocallyContainedPositions(position.Combined, validCandidates[j].positions.Combined) {
				return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "split brain detected between servers - %s and %s", candidate.alias(), validCandidates[j].alias())
			}
		}
	}

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs)
	if opts.NewPrimaryAlias != nil {
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.NewPrimaryAlias)
		requestedCandidate := findERSCandidateByAlias(validCandidates, opts.NewPrimaryAlias)
		if requestedCandidate == nil {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "requested primary elect %v has errant GTIDs", requestedPrimaryAlias)
		}
		// if the requested tablet is as advanced as the most advanced tablet, then we can just use it for promotion.
		// otherwise, we should let it catchup to the most advanced tablet and not change the intermediate source
		if requestedCandidate.positions.AtLeast(winningPosition) {
			winningCandidate = requestedCandidate
		}
	}

	return winningCandidate, validCandidates, nil
}

// promoteIntermediateSource reparents all the other tablets to start replicating from the intermediate source.
// It does not promote this tablet to a primary instance, we only let other replicas start replicating from this tablet
func (erp *EmergencyReparenter) promoteIntermediateSource(
	ctx context.Context,
	ev *events.Reparent,
	source *ersCandidate,
	validCandidates []*ersCandidate,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	opts EmergencyReparentOptions,
) ([]*ersCandidate, error) {
	// Create a tablet map from all the valid replicas
	validTabletMap := map[string]*topo.TabletInfo{}
	for _, candidate := range validCandidates {
		validTabletMap[candidate.alias()] = candidate.info
	}

	// we reparent all the other valid tablets to start replication from our new source
	// we wait for all the replicas so that we can choose a better candidate from the ones that started replication later
	// The intermediate reparent doesn't run the acker quorum gate, so it has no
	// non-ackers to exclude.
	reachableTablets, err := erp.reparentReplicas(ctx, ev, source.tablet(), validTabletMap, statusMap, opts, nil /* nonAckers */, false /* splitBrainOverrideActive */, true /* intermediateReparent */)
	if err != nil {
		return nil, err
	}

	// also include the current tablet for being considered as part of valid candidates for ERS promotion
	reachableTablets = append(reachableTablets, source.tablet())
	reachableAliases := sets.New[string]()
	for _, tablet := range reachableTablets {
		reachableAliases.Insert(topoproto.TabletAliasString(tablet.Alias))
	}

	// The only valid candidates for improvement are the ones which are reachable and part of the valid candidate list.
	// Preserve candidate order here because the slice is already sorted by replication position.
	validCandidatesForImprovement := make([]*ersCandidate, 0, len(validCandidates))
	for _, candidate := range validCandidates {
		if reachableAliases.Has(candidate.alias()) {
			validCandidatesForImprovement = append(validCandidatesForImprovement, candidate)
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
	nonAckers []string, // nonAckers are tablets that can't be relied on to send semi-sync ACKs, so they don't count towards the acker quorum gate below even when their repoint succeeds. This must match the set filterValidCandidates excluded when it proved forward progress.
	splitBrainOverrideActive bool, // splitBrainOverrideActive records the promotion in the split-brain override counter once it commits. An override that aborts before the promotion discarded nothing, and a replica repoint failure afterwards doesn't undo it, so the counter is bumped right after the reparent journal write.
	intermediateReparent bool, // intermediateReparent represents whether the reparenting of the replicas is the final reparent or not.
	// Since ERS can sometimes promote a tablet, which isn't a candidate for promotion, if it is the most advanced, we don't want to
	// call PromoteReplica on it. We just want to get all replicas to replicate from it to get caught up, after which we'll promote the primary
	// candidate separately. During the final promotion, we call `PromoteReplica` and `PopulateReparentJournal`.
) ([]*topodatapb.Tablet, error) {
	var (
		replicasStartedReplication []*topodatapb.Tablet
		replicaMutex               sync.Mutex
		ackersRepointed            int
	)

	nonAckerSet := sets.New(nonAckers...)

	// WithoutCancel preserves ctx values (tracing, caller ID) but lets replicas
	// finish SetReplicationSource RPCs after the parent context is cancelled.
	replCtx, replCancel := context.WithTimeout(context.WithoutCancel(ctx), opts.WaitReplicasTimeout)

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

	// The reparent journal write on the new primary blocks until enough semi-sync
	// ackers are connected to it, which only happens once their repoints below
	// complete. A repoint can stall in STOP REPLICA behind a busy applier, so the
	// promotion waits for this quorum before making the new primary read-write: it is
	// the same wait the journal write would do implicitly, moved to where aborting is
	// still clean. Repointed-but-not-started tablets don't count, they cannot ACK
	ackerQuorumCtx, ackerQuorumCancel := context.WithCancel(context.Background())
	defer ackerQuorumCancel()
	ackersNeeded := policy.SemiSyncAckers(opts.durability, newPrimaryTablet)
	if ackersNeeded == 0 {
		ackerQuorumCancel()
	}

	now := time.Now().UnixNano()
	replWg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}

	handlePrimary := func(primaryCtx context.Context, alias string, tablet *topodatapb.Tablet) error {
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
		defer func() {
			if r := recover(); r != nil {
				err := vterrors.Errorf(vtrpc.Code_INTERNAL, "panic in replica handler for %v: %v", alias, r)
				erp.logger.Errorf("%v", err)
				rec.RecordError(err)
			}
		}()
		erp.logger.Infof("setting new primary on replica %v", alias)

		forceStart := false
		status, inStatusMap := statusMap[alias]
		if inStatusMap {
			fs, err := ReplicaWasRunning(status)
			if err != nil {
				err = vterrors.Wrapf(err, "tablet %v could not determine StopReplicationStatus", alias)
				rec.RecordError(err)

				return
			}

			forceStart = fs
		}

		semiSync := policy.IsReplicaSemiSync(opts.durability, newPrimaryTablet, ti.Tablet)
		err := erp.tmc.SetReplicationSource(replCtx, ti.Tablet, newPrimaryTablet.Alias, 0, "", forceStart, semiSync, 0)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetReplicationSource failed", alias)
			rec.RecordError(err)

			return
		}

		replicaMutex.Lock()
		replicasStartedReplication = append(replicasStartedReplication, ti.Tablet)
		// The repoint leaves the tablet replicating, and therefore ACKing, when we asked
		// for a start or when it had no replication configured at all (a demoted former
		// primary or a fresh tablet: SetReplicationSource always starts those). A tablet
		// flagged as a non-acker can't be relied on to ACK, so it doesn't count towards
		// the quorum even when its repoint succeeds; this keeps the gate consistent with
		// the acker accounting filterValidCandidates used to prove forward progress
		if semiSync && (forceStart || !inStatusMap) && !nonAckerSet.Has(alias) {
			ackersRepointed++
			if ackersRepointed >= ackersNeeded {
				ackerQuorumCancel()
			}
		}
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
		defer allReplicasDoneCancel()
		defer replCancel()
		defer func() {
			if r := recover(); r != nil {
				erp.logger.Errorf("panic while waiting for replicas to finish: %v", r)
			}
		}()
		replWg.Wait()
	}()

	// Hold the promotion until the acker quorum is repointed; if every replica finished
	// without reaching it, the journal write below could never complete and the shard is
	// still clean to abort. A completed repoint doesn't prove the acker's IO thread has
	// connected yet, but the ackers this wait exists for were receiving from the old
	// primary moments ago; a tablet that can't connect at all is the same exposure the
	// journal write always had
	if !intermediateReparent {
		select {
		case <-ackerQuorumCtx.Done():
		case <-allReplicasDoneCtx.Done():
			replicaMutex.Lock()
			repointed := ackersRepointed
			replicaMutex.Unlock()
			if repointed < ackersNeeded {
				replCancel()
				err := vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "%d of %d needed semi-sync ackers were repointed", repointed, ackersNeeded)
				if recErr := rec.Error(); recErr != nil {
					err = recErr
				}
				return nil, vterrors.Wrapf(err, "not enough semi-sync ackers were reachable to guarantee the promotion of %v can make progress", topoproto.TabletAliasString(newPrimaryTablet.Alias))
			}
		case <-ctx.Done():
			replCancel()
			return nil, vterrors.Wrapf(ctx.Err(), "emergency reparent aborted before promoting %v", topoproto.TabletAliasString(newPrimaryTablet.Alias))
		}

		// The quorum wait can outlast the caller's last lock check, and promoting after
		// losing the shard lock could create a second read-write primary under whoever
		// holds it now
		if err := topo.CheckShardLocked(ctx, ev.ShardInfo.Keyspace(), ev.ShardInfo.ShardName()); err != nil {
			replCancel()
			return nil, vterrors.Wrap(err, lostTopologyLockMsg)
		}
	}

	// The promotion budget starts here, after the quorum wait, so a slow-but-successful
	// repoint can't leave the journal write with an already-drained context
	primaryCtx, primaryCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer primaryCancel()
	primaryErr := handlePrimary(primaryCtx, topoproto.TabletAliasString(newPrimaryTablet.Alias), newPrimaryTablet)
	if primaryErr != nil {
		erp.logger.Errorf("failed to promote %s to primary", topoproto.TabletAliasString(newPrimaryTablet.Alias))
		replCancel()

		return nil, vterrors.Wrapf(primaryErr, "failed to promote %v to primary", topoproto.TabletAliasString(newPrimaryTablet.Alias))
	}

	// The journal write above committed the override's lossy history, so it is counted
	// here even if repointing the replicas below fails
	if splitBrainOverrideActive && !intermediateReparent {
		ersSplitBrainOverrides.Add([]string{ev.ShardInfo.Keyspace(), ev.ShardInfo.ShardName()}, 1)
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
	intermediateSource *ersCandidate,
	validCandidates []*ersCandidate,
	opts EmergencyReparentOptions,
) (bool, error) {
	// we try to find a better candidate with the current list of valid candidates, and if it matches our current primary candidate, then we return true
	candidate, err := erp.identifyPrimaryCandidate(intermediateSource, validCandidates, opts)
	if err != nil {
		return false, err
	}
	return candidate.isSameTablet(intermediateSource), nil
}

// identifyPrimaryCandidate is used to find the final candidate for ERS promotion.
//
// Version and flavor state travels with each candidate, and the flavor-family
// guard is scoped to the candidates considered in each promotion tier.
func (erp *EmergencyReparenter) identifyPrimaryCandidate(
	intermediateSource *ersCandidate,
	validCandidates []*ersCandidate,
	opts EmergencyReparentOptions,
) (candidate *ersCandidate, err error) {
	defer func() {
		if candidate != nil {
			erp.logger.Infof("found better candidate - %s", candidate.alias())
		}
	}()

	if len(validCandidates) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	if opts.NewPrimaryAlias != nil {
		// explicit request to promote a specific tablet
		requestedCandidate := findERSCandidateByAlias(validCandidates, opts.NewPrimaryAlias)
		if requestedCandidate != nil {
			return requestedCandidate, nil
		}
		return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "requested candidate %v is not in valid candidates list", topoproto.TabletAliasString(opts.NewPrimaryAlias))
	}

	// We have already selected an intermediate source which was selected based on the replication position
	// (ties broken by promotion rules), but that tablet might not even be a valid candidate i.e. it could
	// be in a different cell when we have PreventCrossCellPromotion specified, or it could have a promotion rule of
	// MustNot. Even if it is valid, there could be a tablet with a better promotion rule. This is what we try to
	// find here.
	// We go over all the promotion rules in descending order of priority and try and find a valid candidate with
	// that promotion rule.
	// If the intermediate source has the same promotion rules as some other tablets, we prefer a
	// lower-version candidate to maintain replication compatibility, accepting the catch-up cost.
	// If versions are equal, we still prefer the intermediate source to avoid catch-up.
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

// filterValidCandidates filters valid candidates, keeping only the ones which can successfully be promoted without any
// constraint failures and can make forward progress on being promoted. It will filter out candidates taking backups
// if possible. The nonAckers are reachable tablets that won't be able to send semi-sync ACKs after the promotion,
// so they don't count towards a candidate's forward progress; they were still reached, so a non-acker remains
// individually promotable.
func (erp *EmergencyReparenter) filterValidCandidates(validCandidates []*ersCandidate, tabletsReachable []*topodatapb.Tablet, nonAckers []string, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) ([]*ersCandidate, error) {
	ackersReachable := removeTabletsByAlias(tabletsReachable, nonAckers)
	restrictedValidCandidates := make([]*ersCandidate, 0, len(validCandidates))
	notPreferredValidCandidates := make([]*ersCandidate, 0, len(validCandidates))
	for _, candidate := range validCandidates {
		tablet := candidate.tablet()
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
		// Remove any tablet which cannot make forward progress using the list of tablets we have reached.
		// A candidate never counts as its own acker, so a non-acking candidate is kept in the list
		// purely to preserve its reached status
		reachedForTablet := ackersReachable
		if slices.Contains(nonAckers, tabletAliasStr) {
			reachedForTablet = append(slices.Clip(ackersReachable), tablet)
		}
		if !canEstablishForTablet(opts.durability, tablet, reachedForTablet) {
			erp.logger.Infof("Removing %s from list of valid candidates for promotion because it will not be able to make forward progress on promotion with the tablets currently reachable", tabletAliasStr)
			if opts.NewPrimaryAlias != nil && topoproto.TabletAliasEqual(opts.NewPrimaryAlias, tablet.Alias) {
				return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "proposed primary %s will not be able to make forward progress on being promoted", topoproto.TabletAliasString(opts.NewPrimaryAlias))
			}
			continue
		}
		// Put candidates that are running a backup in a separate list
		if candidate.takingBackup {
			erp.logger.Infof("Setting %s in list of valid candidates taking a backup", tabletAliasStr)
			notPreferredValidCandidates = append(notPreferredValidCandidates, candidate)
		} else {
			restrictedValidCandidates = append(restrictedValidCandidates, candidate)
		}
	}
	if len(restrictedValidCandidates) > 0 {
		// A backup only costs a candidate its place when a preferred one exists, so this
		// is the first point where we know the requested primary is actually being
		// dropped. Say why here: otherwise it resurfaces as a bare "not in valid
		// candidates list" from identifyPrimaryCandidate, which names the consequence
		// and not the cause
		if opts.NewPrimaryAlias != nil && findERSCandidateByAlias(notPreferredValidCandidates, opts.NewPrimaryAlias) != nil {
			return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "proposed primary %s is taking a backup and other candidates are available", topoproto.TabletAliasString(opts.NewPrimaryAlias))
		}
		return restrictedValidCandidates, nil
	}

	return notPreferredValidCandidates, nil
}

// findErrantGTIDs tries to find errant GTIDs for the valid candidates and returns the updated list of valid candidates.
// This function does not actually return the identities of errant GTID tablets, if any. It only returns the identities of non-errant GTID tablets, which are eligible for promotion.
// The caller of this function (ERS) will then choose from among the list of candidate tablets, based on higher-level criteria.
// The extraEvidence positions belong to tablets that are no longer promotion candidates (they failed their relay log
// wait) but whose received positions still corroborate GTIDs on the candidates; extra evidence can only reduce false
// positives, a truly errant GTID is one that no other tablet has.
// The second return value reports the candidates that were accepted with no evidence at all to compare against: the
// caller decides whether that blind spot is acceptable or of its own making (see the rescue wait in
// reparentShardLocked).
func (erp *EmergencyReparenter) findErrantGTIDs(
	ctx context.Context,
	validCandidates []*ersCandidate,
	waitReplicasTimeout time.Duration,
	extraEvidence []replication.Position,
	shardNeverInitialized bool,
) ([]*ersCandidate, []*ersCandidate, error) {
	allPositionsZero := len(validCandidates) > 0
	for _, candidate := range validCandidates {
		if candidate.positions == nil || !candidate.positions.IsZero() {
			allPositionsZero = false
			break
		}
	}

	// First we need to collect the reparent journal length for all the candidates.
	// This will tell us, which of the tablets are severly lagged, and haven't even seen all the primary promotions.
	// Such severely lagging tablets cannot be used to find errant GTIDs in other tablets, seeing that they themselves don't have enough information.
	// Zero-position candidates are included: their journal rows survive a GTID wipe and
	// prove the shard has promotion history even when no GTID state is left to compare.
	// A missing journal table is only tolerated when the topology says never initialized
	// and no candidate has any GTIDs; a nonzero position anywhere proves history, so an
	// unreadable journal depth must fail the gather
	if err := erp.gatherReparentJournalInfo(ctx, validCandidates, waitReplicasTimeout, shardNeverInitialized && allPositionsZero); err != nil {
		return nil, nil, err
	}

	// Find the maximum length of the reparent journal among all the candidates.
	var maxLen int32
	for _, candidate := range validCandidates {
		maxLen = max(maxLen, candidate.reparentJournalLen)
	}

	// A shard where every candidate has an empty GTID position and an empty reparent
	// journal has never seen a promotion: it is being initialized, and every candidate
	// is an equally valid first primary. The topology must agree the shard was never
	// initialized, though: a shard that has recorded a primary has history to protect
	// even when every reachable tablet lost both its GTIDs and its sidecar tables.
	// Empty positions alongside journal history mean the GTID state was wiped instead,
	// which fails closed below.
	if allPositionsZero && maxLen == 0 {
		if !shardNeverInitialized {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "every candidate reports an empty GTID position and an empty reparent journal, but the shard topology records a previous primary: refusing to re-initialize a shard that has history to protect; restore a tablet with the shard's data before retrying")
		}
		return slices.Clone(validCandidates), nil, nil
	}

	// A tablet with nil or zero positions has no GTIDs to corroborate anyone and can't be
	// promoted over tablets with real history, so it is dropped from candidacy up front.
	nonZeroCandidates := make([]*ersCandidate, 0, len(validCandidates))
	for _, candidate := range validCandidates {
		if candidate.positions == nil || candidate.positions.IsZero() {
			erp.logger.Warningf("skipping candidate %s during errant GTID detection: nil or zero positions", candidate.alias())
			continue
		}
		nonZeroCandidates = append(nonZeroCandidates, candidate)
	}

	// Find the candidates with the maximum length of the reparent journal. A dropped
	// zero-position tablet can't be part of the evidence tier: it has no GTIDs to
	// compare anyone against.
	maxLenCandidates := make([]*ersCandidate, 0, len(nonZeroCandidates))
	for _, candidate := range nonZeroCandidates {
		if candidate.reparentJournalLen != maxLen {
			continue
		}
		maxLenCandidates = append(maxLenCandidates, candidate)
	}

	// If every tablet holding the latest reparent journal history had its GTID state
	// wiped, the surviving candidates provably missed a promotion and no evidence is
	// left to prove what it contained. Promoting one of them could silently discard
	// the missed history, so fail closed and leave the decision to an operator.
	if len(maxLenCandidates) == 0 && len(validCandidates) > 0 {
		var wipedLeaders []string
		for _, candidate := range validCandidates {
			if candidate.reparentJournalLen == maxLen {
				wipedLeaders = append(wipedLeaders, candidate.alias())
			}
		}
		slices.Sort(wipedLeaders)
		return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "errant GTID detection has no usable evidence: the candidates with the latest reparent journal history (%s, %d entries) have empty GTID positions, so the remaining candidates cannot be proven to have seen the latest promotion; restore the GTID state or data of a wiped tablet before retrying; removing the wiped tablets from the shard instead would discard the missed promotion's transactions", strings.Join(wipedLeaders, ", "), maxLen)
	}

	// We use all the candidates with the maximum length of the reparent journal to find the errant GTIDs amongst them.
	var maxLenPositions []replication.Position
	var starvedCandidates []*ersCandidate
	// Survivor order carries no contract: findMostAdvanced sorts these again, with a
	// tablet-alias tiebreak, before anything is elected
	var updatedValidCandidates []*ersCandidate
	for _, candidate := range maxLenCandidates {
		candidatePositions := candidate.positions
		if candidate.stopStatus == nil {
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
			updatedValidCandidates = append(updatedValidCandidates, candidate)
			continue
		}
		// Store all the other candidate's positions so that we can run errant GTID detection using them.
		otherPositions := make([]replication.Position, 0, len(maxLenCandidates)-1+len(extraEvidence))
		for _, otherCandidate := range maxLenCandidates {
			if otherCandidate == candidate {
				continue
			}
			otherPositions = append(otherPositions, otherCandidate.positions.Combined)
		}
		otherPositions = append(otherPositions, extraEvidence...)
		// FindErrantGTIDs accepts a candidate's GTID set as-is when there is nothing to
		// compare it against; report those candidates so the caller can decide whether
		// the missing evidence is acceptable.
		if len(otherPositions) == 0 {
			starvedCandidates = append(starvedCandidates, candidate)
		}
		// Run errant GTID detection and throw away any tablet that has errant GTIDs.
		afterStatus := replication.ProtoToReplicationStatus(candidate.stopStatus.After)
		errantGTIDs, err := replication.FindErrantGTIDs(afterStatus.RelayLogPosition, afterStatus.SourceUUID, otherPositions)
		if err != nil {
			return nil, nil, err
		}
		if errantGTIDs != nil {
			log.Error(fmt.Sprintf("skipping %s with GTIDSet:%v because we detected errant GTIDs - %v", candidate.alias(), afterStatus.RelayLogPosition.GTIDSet, errantGTIDs))
			continue
		}
		maxLenPositions = append(maxLenPositions, candidatePositions.Combined)
		updatedValidCandidates = append(updatedValidCandidates, candidate)
	}

	// The extra evidence positions also corroborate the lagged tablets below.
	maxLenPositions = append(maxLenPositions, extraEvidence...)

	// For all the other tablets, that are lagged enough that they haven't seen all the reparent journal entries,
	// we run errant GTID detection by using the tablets with the maximum length of the reparent journal.
	// We throw away any tablet that has errant GTIDs.
	for _, candidate := range nonZeroCandidates {
		if candidate.reparentJournalLen == maxLen {
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
		candidatePositions := candidate.positions
		errantGTIDs, err := replication.FindErrantGTIDs(candidatePositions.Combined, replication.SID{}, maxLenPositions)
		if err != nil {
			return nil, nil, err
		}
		if errantGTIDs != nil {
			log.Error(fmt.Sprintf("skipping %s with GTIDSet:%v because we detected errant GTIDs - %v", candidate.alias(), candidatePositions, errantGTIDs))
			continue
		}
		updatedValidCandidates = append(updatedValidCandidates, candidate)
	}

	return updatedValidCandidates, starvedCandidates, nil
}

// gatherReparentJournalInfo reads the reparent journal information from all the tablets in the valid candidates list.
func (erp *EmergencyReparenter) gatherReparentJournalInfo(
	ctx context.Context,
	validCandidates []*ersCandidate,
	waitReplicasTimeout time.Duration,
	tolerateMissingJournal bool,
) error {
	errCh := make(chan concurrency.Error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

	for _, candidate := range validCandidates {
		go func(candidate *ersCandidate) {
			var err error
			var length int32
			defer func() {
				errCh <- concurrency.Error{
					Err: err,
				}
			}()
			length, err = erp.tmc.ReadReparentJournalInfo(groupCtx, candidate.tablet())
			if err != nil && tolerateMissingJournal {
				// A brand-new shard has no sidecar tables yet: treat a missing journal
				// table as zero entries so ERS can still initialize it
				if sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError); ok &&
					(sqlErr.Number() == sqlerror.ERNoSuchTable || sqlErr.Number() == sqlerror.ERBadDb) {
					erp.logger.Warningf("treating missing reparent journal table on %s as zero entries during errant GTID detection: %v", candidate.alias(), err)
					length, err = 0, nil
				}
			}
			candidate.reparentJournalLen = length
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
		return vterrors.Wrapf(rec.Error(), "could not read reparent journal information within the provided waitReplicasTimeout (%s)", waitReplicasTimeout)
	}

	return nil
}
