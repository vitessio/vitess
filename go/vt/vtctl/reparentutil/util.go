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

	"vitess.io/vitess/go/vt/concurrency"

	"vitess.io/vitess/go/mysql"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"

	"vitess.io/vitess/go/vt/topotools/events"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

func waitForAllRelayLogsToApply(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, waitReplicasTimeout time.Duration) error {
	errCh := make(chan error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

	for candidate := range validCandidates {
		// When we called StopReplicationAndBuildStatusMaps, we got back two
		// maps: (1) the StopReplicationStatus of any replicas that actually
		// stopped replication; and (2) the MasterStatus of anything that
		// returned ErrNotReplica, which is a tablet that is either the current
		// primary or is stuck thinking it is a MASTER but is not in actuality.
		//
		// If we have a tablet in the validCandidates map that does not appear
		// in the statusMap, then we have either (a) the current primary, which
		// is not replicating, so it is not applying relay logs; or (b) a tablet
		// that is stuck thinking it is MASTER but is not in actuality. In that
		// second case - (b) - we will most likely find that the stuck MASTER
		// does not have a winning position, and fail the ERS. If, on the other
		// hand, it does have a winning position, we are trusting the operator
		// to know what they are doing by emergency-reparenting onto that
		// tablet. In either case, it does not make sense to wait for relay logs
		// to apply on a tablet that was never applying relay logs in the first
		// place, so we skip it, and log that we did.
		status, ok := statusMap[candidate]
		if !ok {
			logger.Infof("EmergencyReparent candidate %v not in replica status map; this means it was not running replication (because it was formerly PRIMARY), so skipping WaitForRelayLogsToApply step for this candidate", candidate)
			continue
		}

		go func(alias string, status *replicationdatapb.StopReplicationStatus) {
			var err error
			defer func() { errCh <- err }()
			err = WaitForRelayLogsToApply(groupCtx, tmc, tabletMap[alias], status)
		}(candidate, status)

		waiterCount++
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        waiterCount,
		NumRequiredSuccesses: waiterCount,
		NumAllowedErrors:     0,
	}
	rec := errgroup.Wait(groupCancel, errCh)

	if len(rec.Errors) != 0 {
		return vterrors.Wrapf(rec.Error(), "could not apply all relay logs within the provided waitReplicasTimeout (%s): %v", waitReplicasTimeout, rec.Error())
	}

	return nil
}

// reparentReplicas reparents all the replicas provided and populates the reparent journal on the primary.
// Also, it returns the replicas which started replicating only in the case where we wait for all the replicas
func reparentReplicas(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient, newPrimaryTablet *topodatapb.Tablet, lockAction string, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, opts EmergencyReparentOptions, waitForAllReplicas bool, populateReparentJournal bool) ([]*topodatapb.Tablet, error) {

	var replicasStartedReplication []*topodatapb.Tablet
	var replicaMutex sync.Mutex

	replCtx, replCancel := context.WithTimeout(ctx, opts.waitReplicasTimeout)
	defer replCancel()

	event.DispatchUpdate(ev, "reparenting all tablets")

	// Create a context and cancel function to watch for the first successful
	// SetMaster call on a replica. We use a background context so that this
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
		position, err := tmc.MasterPosition(replCtx, tablet)
		if err != nil {
			return err
		}
		if populateReparentJournal {
			logger.Infof("populating reparent journal on new primary %v", alias)
			return tmc.PopulateReparentJournal(replCtx, tablet, now, lockAction, newPrimaryTablet.Alias, position)
		}
		return nil
	}

	handleReplica := func(alias string, ti *topo.TabletInfo) {
		defer replWg.Done()
		logger.Infof("setting new primary on replica %v", alias)

		forceStart := false
		if status, ok := statusMap[alias]; ok {
			fs, err := ReplicaWasRunning(status)
			if err != nil {
				err = vterrors.Wrapf(err, "tablet %v could not determine StopReplicationStatus: %v", alias, err)
				rec.RecordError(err)

				return
			}

			forceStart = fs
		}

		err := tmc.SetMaster(replCtx, ti.Tablet, newPrimaryTablet.Alias, 0, "", forceStart)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetReplicationSource failed: %v", alias, err)
			rec.RecordError(err)

			return
		}

		replicaMutex.Lock()
		replicasStartedReplication = append(replicasStartedReplication, ti.Tablet)
		replicaMutex.Unlock()

		// Signal that at least one goroutine succeeded to SetReplicationSource.
		// We do this only when we do not want to wait for all the replicas
		if !waitForAllReplicas {
			replSuccessCancel()
		}
	}

	numReplicas := 0

	for alias, ti := range tabletMap {
		switch {
		case alias == topoproto.TabletAliasString(newPrimaryTablet.Alias):
			continue
		case !opts.ignoreReplicas.Has(alias):
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
	go func() {
		replWg.Wait()
		allReplicasDoneCancel()
	}()

	primaryErr := handlePrimary(topoproto.TabletAliasString(newPrimaryTablet.Alias), newPrimaryTablet)
	if primaryErr != nil {
		logger.Warningf("primary failed to PopulateReparentJournal")
		replCancel()

		return nil, vterrors.Wrapf(primaryErr, "failed to PopulateReparentJournal on primary: %v", primaryErr)
	}

	select {
	case <-replSuccessCtx.Done():
		// At least one replica was able to SetMaster successfully
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
			return nil, vterrors.Wrapf(rec.Error(), "received more errors (= %d) than replicas (= %d), which should be impossible: %v", errCount, numReplicas, rec.Error())
		case errCount == numReplicas:
			return nil, vterrors.Wrapf(rec.Error(), "%d replica(s) failed: %v", numReplicas, rec.Error())
		default:
			return replicasStartedReplication, nil
		}
	}
}

// ChooseNewPrimary finds a tablet that should become a primary after reparent.
// The criteria for the new primary-elect are (preferably) to be in the same
// cell as the current primary, and to be different from avoidPrimaryAlias. The
// tablet with the most advanced replication position is chosen to minimize the
// amount of time spent catching up with the current primary.
//
// Note that the search for the most advanced replication position will race
// with transactions being executed on the current primary, so when all tablets
// are at roughly the same position, then the choice of new primary-elect will
// be somewhat unpredictable.
func ChooseNewPrimary(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidPrimaryAlias *topodatapb.TabletAlias,
	waitReplicasTimeout time.Duration,
	// (TODO:@ajm188) it's a little gross we need to pass this, maybe embed in the context?
	logger logutil.Logger,
) (*topodatapb.TabletAlias, error) {
	if avoidPrimaryAlias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet to avoid for reparent is not provided, cannot choose new primary")
	}

	var primaryCell string
	if shardInfo.PrimaryAlias != nil {
		primaryCell = shardInfo.PrimaryAlias.Cell
	}

	var (
		searcher = topotools.NewMaxReplicationPositionSearcher(tmc, logger, waitReplicasTimeout)
		wg       sync.WaitGroup
	)

	for _, tablet := range tabletMap {
		switch {
		case primaryCell != "" && tablet.Alias.Cell != primaryCell:
			continue
		case topoproto.TabletAliasEqual(tablet.Alias, avoidPrimaryAlias):
			continue
		case tablet.Tablet.Type != topodatapb.TabletType_REPLICA:
			continue
		}

		wg.Add(1)

		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			searcher.ProcessTablet(ctx, tablet)
		}(tablet.Tablet)
	}

	wg.Wait()

	if maxPosTablet := searcher.MaxPositionTablet(); maxPosTablet != nil {
		return maxPosTablet.Alias, nil
	}

	return nil, nil
}

// FindCurrentPrimary returns the current primary tablet of a shard, if any. The
// current primary is whichever tablet of type PRIMARY (if any) has the most
// recent PrimaryTermStartTime, which is the same rule that vtgate uses to route
// primary traffic.
//
// The return value is nil if the current primary cannot be definitively
// determined. This can happen either if no tablet claims to be type PRIMARY, or
// if multiple tablets claim to be type PRIMARY and happen to have the same
// PrimaryTermStartTime timestamp (a tie).
//
// The tabletMap must be a complete map (not a partial result) for the shard.
func FindCurrentPrimary(tabletMap map[string]*topo.TabletInfo, logger logutil.Logger) *topo.TabletInfo {
	var (
		currentPrimary       *topo.TabletInfo
		currentTermStartTime time.Time
	)

	for _, tablet := range tabletMap {
		if tablet.Type != topodatapb.TabletType_PRIMARY {
			continue
		}

		if currentPrimary == nil {
			currentPrimary = tablet
			currentTermStartTime = tablet.GetPrimaryTermStartTime()
			continue
		}

		otherPrimaryTermStartTime := tablet.GetPrimaryTermStartTime()
		if otherPrimaryTermStartTime.After(currentTermStartTime) {
			currentPrimary = tablet
			currentTermStartTime = otherPrimaryTermStartTime
		} else if otherPrimaryTermStartTime.Equal(currentTermStartTime) {
			// A tie should not happen unless the upgrade order was violated
			// (e.g. some vttablets have not been upgraded) or if we get really
			// unlucky.
			//
			// Either way, we need to be safe and not assume we know who the
			// true primary is.
			logger.Warningf(
				"Multiple primaries (%v and %v) are tied for PrimaryTermStartTime; can't determine the true primary.",
				topoproto.TabletAliasString(currentPrimary.Alias),
				topoproto.TabletAliasString(tablet.Alias),
			)

			return nil
		}
	}

	return currentPrimary
}

// waitForCatchingUp promotes the newer candidate over the primary candidate that we have, but it does not set to start accepting writes
func waitForCatchingUp(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, ev *events.Reparent, logger logutil.Logger, prevPrimary, newPrimary *topodatapb.Tablet,
	lockAction string, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, opts EmergencyReparentOptions) error {
	// Find the primary position of the previous primary
	pos, err := tmc.MasterPosition(ctx, prevPrimary)
	if err != nil {
		return err
	}

	// Wait until the new primary has caught upto that position
	// TODO - discuss, what happens in case of timeout
	err = tmc.WaitForPosition(ctx, newPrimary, pos)
	if err != nil {
		return err
	}
	return nil
}

func getLockAction(newPrimaryAlias *topodatapb.TabletAlias) string {
	action := "EmergencyReparentShard"

	if newPrimaryAlias != nil {
		action += fmt.Sprintf("(%v)", topoproto.TabletAliasString(newPrimaryAlias))
	}

	return action
}

// restrictValidCandidates is used to restrict some candidates from being considered eligible for becoming the intermediate primary
func restrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)
	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		// We do not allow Experimental type of tablets to be considered for replication
		if candidateInfo.Type == topodatapb.TabletType_EXPERIMENTAL {
			continue
		}
		restrictedValidCandidates[candidate] = position
	}
	return restrictedValidCandidates, nil
}

// findIntermediatePrimaryCandidate finds the intermediate primary candidate for ERS. We always choose the most advanced one from our valid candidates list
func findIntermediatePrimaryCandidate(logger logutil.Logger, prevPrimary *topodatapb.Tablet, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo, opts EmergencyReparentOptions) (*topodatapb.Tablet, []*topodatapb.Tablet, error) {
	// convert the valid candidates into a list so that we can use it for sorting
	validTablets, tabletPositions, err := getValidCandidatesAndPositionsAsList(validCandidates, tabletMap)
	if err != nil {
		return nil, nil, err
	}

	idealCell := ""
	if prevPrimary != nil {
		idealCell = prevPrimary.Alias.Cell
	}

	// sort the tablets for finding the best intermediate primary in ERS
	err = sortTabletsForERS(validTablets, tabletPositions, idealCell)
	if err != nil {
		return nil, nil, err
	}

	// The first tablet in the sorted list will be the most eligible candidate unless explicitly asked for some other tablet
	winningPrimaryTablet := validTablets[0]
	winningPosition := tabletPositions[0]

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs)
	// Also, if the candidate is
	if opts.newPrimaryAlias != nil {
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.newPrimaryAlias)
		pos, ok := validCandidates[requestedPrimaryAlias]
		if !ok {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "requested primary elect %v has errant GTIDs", requestedPrimaryAlias)
		}
		if pos.AtLeast(winningPosition) {
			requestedPrimaryInfo, isFound := tabletMap[requestedPrimaryAlias]
			if !isFound {
				return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", requestedPrimaryAlias)
			}
			winningPrimaryTablet = requestedPrimaryInfo.Tablet
		}
	}

	return winningPrimaryTablet, validTablets, nil
}

// getValidCandidatesAndPositionsAsList converts the valid candidates from a map to a list of tablets, making it easier to sort
func getValidCandidatesAndPositionsAsList(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) ([]*topodatapb.Tablet, []mysql.Position, error) {
	var validTablets []*topodatapb.Tablet
	var tabletPositions []mysql.Position
	for tabletAlias, position := range validCandidates {
		tablet, isFound := tabletMap[tabletAlias]
		if !isFound {
			return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", tabletAlias)
		}
		validTablets = append(validTablets, tablet.Tablet)
		tabletPositions = append(tabletPositions, position)
	}
	return validTablets, tabletPositions, nil
}

// intermediateCandidateIsIdeal is used to find whether the intermediate candidate that ERS chose is also the ideal one or not
func intermediateCandidateIsIdeal(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, opts EmergencyReparentOptions) bool {
	// we try to find a better candidate with the current list of valid candidates, and if it matches our current primary candidate, then we return true
	return getBetterCandidate(newPrimary, prevPrimary, validCandidates, tabletMap, opts) == newPrimary
}

// getBetterCandidate is used to find a better candidate for ERS promotion
func getBetterCandidate(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, opts EmergencyReparentOptions) *topodatapb.Tablet {
	if opts.newPrimaryAlias != nil {
		// explicit request to promote a specific tablet
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.newPrimaryAlias)
		requestedPrimaryInfo, isFound := tabletMap[requestedPrimaryAlias]
		if isFound {
			return requestedPrimaryInfo.Tablet
		}
	}
	var preferredCandidates []*topodatapb.Tablet
	var neutralReplicas []*topodatapb.Tablet
	for _, candidate := range validCandidates {
		promotionRule := PromotionRule(candidate)
		if promotionRule == MustPromoteRule || promotionRule == PreferPromoteRule {
			preferredCandidates = append(preferredCandidates, candidate)
		}
		if promotionRule == NeutralPromoteRule {
			neutralReplicas = append(neutralReplicas, candidate)
		}
	}

	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the primary
	// There's many options. We may wish to replace the server we promoted with a better one.
	candidate := findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, preferredCandidates, true, true)
	if candidate != nil {
		return candidate
	}
	candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, preferredCandidates, false, true)
	if candidate != nil {
		return candidate
	}
	// do not have a preferred candidate in the same cell

	if !opts.preventCrossCellPromotion {
		candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, preferredCandidates, true, false)
		if candidate != nil {
			return candidate
		}
		candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, preferredCandidates, false, false)
		if candidate != nil {
			return candidate
		}
	}

	candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, neutralReplicas, true, true)
	if candidate != nil {
		return candidate
	}
	candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, neutralReplicas, false, true)
	if candidate != nil {
		return candidate
	}

	if !opts.preventCrossCellPromotion {
		candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, neutralReplicas, true, false)
		if candidate != nil {
			return candidate
		}
		candidate = findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary, neutralReplicas, false, false)
		if candidate != nil {
			return candidate
		}
	}

	return newPrimary
}

func findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary *topodatapb.Tablet, possibleCandidates []*topodatapb.Tablet, checkEqualPrimary bool, checkSameCell bool) *topodatapb.Tablet {
	for _, candidate := range possibleCandidates {
		if checkEqualPrimary && !(topoproto.TabletAliasEqual(newPrimary.Alias, candidate.Alias)) {
			continue
		}
		if checkSameCell && prevPrimary != nil && !(prevPrimary.Alias.Cell == candidate.Alias.Cell) {
			continue
		}
		return candidate
	}
	return nil
}

// promoteIntermediatePrimary promotes the primary candidate that we have, but it does not yet set to start accepting writes
func promoteIntermediatePrimary(ctx context.Context, tmc tmclient.TabletManagerClient, ev *events.Reparent, logger logutil.Logger, newPrimary *topodatapb.Tablet,
	lockAction string, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, opts EmergencyReparentOptions) ([]*topodatapb.Tablet, error) {
	// we reparent all the other tablets to start replication from our new primary
	// we wait for all the replicas so that we can choose a better candidate from the ones that started replication later
	validCandidatesForImprovement, err := reparentReplicas(ctx, ev, logger, tmc, newPrimary, lockAction, tabletMap, statusMap, opts, true, false)
	if err != nil {
		return nil, err
	}

	// also include the current tablet for being considered as part of valid candidates for ERS promotion
	validCandidatesForImprovement = append(validCandidatesForImprovement, newPrimary)
	return validCandidatesForImprovement, nil
}

// checkIfConstraintsSatisfied is used to check whether the constraints for ERS are satisfied or not.
func checkIfConstraintsSatisfied(newPrimary, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) error {
	if opts.preventCrossCellPromotion && prevPrimary != nil && newPrimary.Alias.Cell != prevPrimary.Alias.Cell {
		return vterrors.Errorf(vtrpc.Code_ABORTED, "elected primary does not satisfy geographic constraint - %s", topoproto.TabletAliasString(newPrimary.Alias))
	}
	if PromotionRule(newPrimary) == MustNotPromoteRule {
		return vterrors.Errorf(vtrpc.Code_ABORTED, "elected primary does not satisfy promotion rule constraint - %s", topoproto.TabletAliasString(newPrimary.Alias))
	}
	return nil
}
