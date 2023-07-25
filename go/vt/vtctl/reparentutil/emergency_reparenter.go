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

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
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
	NewPrimaryAlias           *topodatapb.TabletAlias
	IgnoreReplicas            sets.Set[string]
	WaitReplicasTimeout       time.Duration
	PreventCrossCellPromotion bool

	// Private options managed internally. We use value passing to avoid leaking
	// these details back out.
	lockAction string
	durability Durabler
}

// counters for Emergency Reparent Shard
var (
	ersCounter        = stats.NewGauge("ers_counter", "Number of times Emergency Reparent Shard has been run")
	ersSuccessCounter = stats.NewGauge("ers_success_counter", "Number of times Emergency Reparent Shard has succeeded")
	ersFailureCounter = stats.NewGauge("ers_failure_counter", "Number of times Emergency Reparent Shard has failed")
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
	opts.lockAction = erp.getLockAction(opts.NewPrimaryAlias)
	// First step is to lock the shard for the given operation, if not already locked
	if err = topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		var unlock func(*error)
		ctx, unlock, err = erp.ts.LockShard(ctx, keyspace, shard, opts.lockAction)
		if err != nil {
			return nil, err
		}
		defer unlock(&err)
	}

	// dispatch success or failure of ERS
	ev := &events.Reparent{}
	defer func() {
		switch err {
		case nil:
			ersSuccessCounter.Add(1)
			event.DispatchUpdate(ev, "finished EmergencyReparentShard")
		default:
			ersFailureCounter.Add(1)
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
	ersCounter.Add(1)

	var (
		stoppedReplicationSnapshot *replicationSnapshot
		shardInfo                  *topo.ShardInfo
		prevPrimary                *topodatapb.Tablet
		tabletMap                  map[string]*topo.TabletInfo
		validCandidates            map[string]mysql.Position
		intermediateSource         *topodatapb.Tablet
		validCandidateTablets      []*topodatapb.Tablet
		validReplacementCandidates []*topodatapb.Tablet
		betterCandidate            *topodatapb.Tablet
		isIdeal                    bool
	)

	shardInfo, err = erp.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	keyspaceDurability, err := erp.ts.GetKeyspaceDurability(ctx, keyspace)
	if err != nil {
		return err
	}

	erp.logger.Infof("Getting a new durability policy for %v", keyspaceDurability)
	opts.durability, err = GetDurabilityPolicy(keyspaceDurability)
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
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", keyspace, shard, err)
	}

	// Stop replication on all the tablets and build their status map
	stoppedReplicationSnapshot, err = stopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, topo.RemoteOperationTimeout, opts.IgnoreReplicas, opts.NewPrimaryAlias, opts.durability, erp.logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	// check that we still have the shard lock. If we don't then we can terminate at this point
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// find the valid candidates for becoming the primary
	// this is where we check for errant GTIDs and remove the tablets that have them from consideration
	validCandidates, err = FindValidEmergencyReparentCandidates(stoppedReplicationSnapshot.statusMap, stoppedReplicationSnapshot.primaryStatusMap)
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

	// Wait for all candidates to apply relay logs
	if err = erp.waitForAllRelayLogsToApply(ctx, validCandidates, tabletMap, stoppedReplicationSnapshot.statusMap, opts.WaitReplicasTimeout); err != nil {
		return err
	}

	// Find the intermediate source for replication that we want other tablets to replicate from.
	// This step chooses the most advanced tablet. Further ties are broken by using the promotion rule.
	// In case the user has specified a tablet specifically, then it is selected, as long as it is the most advanced.
	// Here we also check for split brain scenarios and check that the selected replica must be more advanced than all the other valid candidates.
	// We fail in case there is a split brain detected.
	// The validCandidateTablets list is sorted by the replication positions with ties broken by promotion rules.
	intermediateSource, validCandidateTablets, err = erp.findMostAdvanced(validCandidates, tabletMap, opts)
	if err != nil {
		return err
	}
	erp.logger.Infof("intermediate source selected - %v", intermediateSource.Alias)

	// After finding the intermediate source, we want to filter the valid candidate list by the following criteria -
	// 1. Only keep the tablets which can make progress after being promoted (have sufficient reachable semi-sync ackers)
	// 2. Remove the tablets with the Must_not promote rule
	// 3. Remove cross-cell tablets if PreventCrossCellPromotion is specified
	// Our final primary candidate MUST belong to this list of valid candidates
	validCandidateTablets, err = erp.filterValidCandidates(validCandidateTablets, stoppedReplicationSnapshot.reachableTablets, prevPrimary, opts)
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
	if err = topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

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
	}

	// The new primary which will be promoted will always belong to the validCandidateTablets list because -
	// 	1. 	if the intermediate source is ideal - then we know the intermediate source was in the validCandidateTablets list
	// 		since we used that list
	//	2. 	if the intermediate source isn't ideal - we take the intersection of the validCandidateTablets list and the one we
	//		were able to reach during the promotion of intermediate source, as possible candidates. So the final candidate (even if
	//		it is the intermediate source itself) will belong to the list
	// Since the new primary tablet belongs to the validCandidateTablets list, we no longer need any additional constraint checks

	// Final step is to promote our primary candidate
	err = erp.promoteNewPrimary(ctx, ev, newPrimary, opts, tabletMap, stoppedReplicationSnapshot.statusMap)
	if err != nil {
		return err
	}

	ev.NewPrimary = proto.Clone(newPrimary).(*topodatapb.Tablet)
	return err
}

func (erp *EmergencyReparenter) waitForAllRelayLogsToApply(
	ctx context.Context,
	validCandidates map[string]mysql.Position,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	waitReplicasTimeout time.Duration,
) error {
	errCh := make(chan concurrency.Error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	waiterCount := 0

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
		// that is stuck thinking it is PRIMARY but is not in actuality. In that
		// second case - (b) - we will most likely find that the stuck PRIMARY
		// does not have a winning position, and fail the ERS. If, on the other
		// hand, it does have a winning position, we are trusting the operator
		// to know what they are doing by emergency-reparenting onto that
		// tablet. In either case, it does not make sense to wait for relay logs
		// to apply on a tablet that was never applying relay logs in the first
		// place, so we skip it, and log that we did.
		status, ok := statusMap[candidate]
		if !ok {
			erp.logger.Infof("EmergencyReparent candidate %v not in replica status map; this means it was not running replication (because it was formerly PRIMARY), so skipping WaitForRelayLogsToApply step for this candidate", candidate)
			continue
		}

		go func(alias string, status *replicationdatapb.StopReplicationStatus) {
			var err error
			defer func() {
				errCh <- concurrency.Error{
					Err: err,
				}
			}()
			err = WaitForRelayLogsToApply(groupCtx, erp.tmc, tabletMap[alias], status)
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

// findMostAdvanced finds the intermediate source for ERS. We always choose the most advanced one from our valid candidates list. Further ties are broken by looking at the promotion rules.
func (erp *EmergencyReparenter) findMostAdvanced(
	validCandidates map[string]mysql.Position,
	tabletMap map[string]*topo.TabletInfo,
	opts EmergencyReparentOptions,
) (*topodatapb.Tablet, []*topodatapb.Tablet, error) {
	erp.logger.Infof("started finding the intermediate source")
	// convert the valid candidates into a list so that we can use it for sorting
	validTablets, tabletPositions, err := getValidCandidatesAndPositionsAsList(validCandidates, tabletMap)
	if err != nil {
		return nil, nil, err
	}

	// sort the tablets for finding the best intermediate source in ERS
	err = sortTabletsForReparent(validTablets, tabletPositions, opts.durability)
	if err != nil {
		return nil, nil, err
	}
	for _, tablet := range validTablets {
		erp.logger.Infof("finding intermediate source - sorted replica: %v", tablet.Alias)
	}

	// The first tablet in the sorted list will be the most eligible candidate unless explicitly asked for some other tablet
	winningPrimaryTablet := validTablets[0]
	winningPosition := tabletPositions[0]

	// We have already removed the tablets with errant GTIDs before calling this function. At this point our winning position must be a
	// superset of all the other valid positions. If that is not the case, then we have a split brain scenario, and we should cancel the ERS
	for i, position := range tabletPositions {
		if !winningPosition.AtLeast(position) {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "split brain detected between servers - %v and %v", winningPrimaryTablet.Alias, validTablets[i].Alias)
		}
	}

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs)
	if opts.NewPrimaryAlias != nil {
		requestedPrimaryAlias := topoproto.TabletAliasString(opts.NewPrimaryAlias)
		pos, ok := validCandidates[requestedPrimaryAlias]
		if !ok {
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "requested primary elect %v has errant GTIDs", requestedPrimaryAlias)
		}
		// if the requested tablet is as advanced as the most advanced tablet, then we can just use it for promotion.
		// otherwise, we should let it catchup to the most advanced tablet and not change the intermediate source
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
	// we reparent all the other tablets to start replication from our new source
	// we wait for all the replicas so that we can choose a better candidate from the ones that started replication later
	reachableTablets, err := erp.reparentReplicas(ctx, ev, source, tabletMap, statusMap, opts, true /* waitForAllReplicas */, false /* populateReparentJournal */)
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
	waitForAllReplicas bool,
	populateReparentJournal bool,
) ([]*topodatapb.Tablet, error) {

	var (
		replicasStartedReplication []*topodatapb.Tablet
		replicaMutex               sync.Mutex
	)

	replCtx, replCancel := context.WithTimeout(context.Background(), opts.WaitReplicasTimeout)

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
		position, err := erp.tmc.PrimaryPosition(replCtx, tablet)
		if err != nil {
			return err
		}
		if populateReparentJournal {
			erp.logger.Infof("populating reparent journal on new primary %v", alias)
			return erp.tmc.PopulateReparentJournal(replCtx, tablet, now, opts.lockAction, newPrimaryTablet.Alias, position)
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
				err = vterrors.Wrapf(err, "tablet %v could not determine StopReplicationStatus: %v", alias, err)
				rec.RecordError(err)

				return
			}

			forceStart = fs
		}

		err := erp.tmc.SetReplicationSource(replCtx, ti.Tablet, newPrimaryTablet.Alias, 0, "", forceStart, IsReplicaSemiSync(opts.durability, newPrimaryTablet, ti.Tablet))
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
	go func() {
		replWg.Wait()
		allReplicasDoneCancel()
	}()

	primaryErr := handlePrimary(topoproto.TabletAliasString(newPrimaryTablet.Alias), newPrimaryTablet)
	if primaryErr != nil {
		erp.logger.Warningf("primary failed to PopulateReparentJournal")
		replCancel()

		return nil, vterrors.Wrapf(primaryErr, "failed to PopulateReparentJournal on primary: %v", primaryErr)
	}

	// We should only cancel the context that all the replicas are using when they are done.
	// Since this function can return early when only 1 replica succeeds, if we cancel this context as a deferred call from this function,
	// then we would end up having cancelled the context for the replicas who have not yet finished running all the commands.
	// This leads to some replicas not starting replication properly. So we must wait for all the replicas to finish before cancelling this context.
	go func() {
		replWg.Wait()
		defer replCancel()
	}()

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
			return nil, vterrors.Wrapf(rec.Error(), "received more errors (= %d) than replicas (= %d), which should be impossible: %v", errCount, numReplicas, rec.Error())
		case errCount == numReplicas:
			if len(tabletMap) <= 2 {
				// If there are at most 2 tablets in the tablet map, we shouldn't be failing the promotion if the replica fails to SetReplicationSource.
				// The failing replica is probably the old primary that is down, so it is okay if it fails. We still log a warning message in the logs.
				erp.logger.Warningf("Failed to set the MySQL replication source during ERS but because there is only one other tablet we assume it is the one that had failed and will progress with the reparent. Error: %v", rec.Error())
				return nil, nil
			}
			return nil, vterrors.Wrapf(rec.Error(), "%d replica(s) failed: %v", numReplicas, rec.Error())
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
	// We should have found atleast 1 tablet in the valid list.
	// If the list is empty, then we should have errored out much sooner.
	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unreachable - did not find a valid primary candidate even though the valid candidate list was non-empty")
}

func (erp *EmergencyReparenter) promoteNewPrimary(
	ctx context.Context,
	ev *events.Reparent,
	newPrimary *topodatapb.Tablet,
	opts EmergencyReparentOptions,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
) error {
	var err error
	if ev.ShardInfo.PrimaryAlias == nil {
		erp.logger.Infof("setting up %v as new primary for an uninitialized cluster", newPrimary.Alias)
		// we call InitPrimary when the PrimaryAlias in the ShardInfo is empty. This happens when we have an uninitialized cluster.
		_, err = erp.tmc.InitPrimary(ctx, newPrimary, SemiSyncAckers(opts.durability, newPrimary) > 0)
	} else {
		erp.logger.Infof("starting promotion for the new primary - %v", newPrimary.Alias)
		// we call PromoteReplica which changes the tablet type, fixes the semi-sync, set the primary to read-write and flushes the binlogs
		_, err = erp.tmc.PromoteReplica(ctx, newPrimary, SemiSyncAckers(opts.durability, newPrimary) > 0)
	}
	if err != nil {
		return vterrors.Wrapf(err, "primary-elect tablet %v failed to be upgraded to primary: %v", newPrimary.Alias, err)
	}
	// we now reparent all the replicas to the new primary we have promoted.
	// Here we do not need to wait for all the replicas, We can finish early when even 1 succeeds.
	_, err = erp.reparentReplicas(ctx, ev, newPrimary, tabletMap, statusMap, opts, false /* waitForAllReplicas */, true /* populateReparentJournal */)
	if err != nil {
		return err
	}
	return nil
}

// filterValidCandidates filters valid tablets, keeping only the ones which can successfully be promoted without any constraint failures and can make forward progress on being promoted
func (erp *EmergencyReparenter) filterValidCandidates(validTablets []*topodatapb.Tablet, tabletsReachable []*topodatapb.Tablet, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) ([]*topodatapb.Tablet, error) {
	var restrictedValidTablets []*topodatapb.Tablet
	for _, tablet := range validTablets {
		tabletAliasStr := topoproto.TabletAliasString(tablet.Alias)
		// Remove tablets which have MustNot promote rule since they must never be promoted
		if PromotionRule(opts.durability, tablet) == promotionrule.MustNot {
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
		restrictedValidTablets = append(restrictedValidTablets, tablet)
	}
	return restrictedValidTablets, nil
}
