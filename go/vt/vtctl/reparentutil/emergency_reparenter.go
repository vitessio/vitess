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
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/stats"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/proto/vtrpc"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/event"

	"vitess.io/vitess/go/vt/logutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// EmergencyReparenter performs EmergencyReparentShard operations.
type EmergencyReparenter struct {
	ts     *topo.Server
	tmc    tmclient.TabletManagerClient
	logger logutil.Logger
}

type (
	// EmergencyReparentOptions provides optional parameters to
	// EmergencyReparentShard operations. Options are passed by value, so it is safe
	// for callers to mutate and reuse options structs for multiple calls.
	EmergencyReparentOptions struct {
		newPrimaryAlias           *topodatapb.TabletAlias
		ignoreReplicas            sets.String
		waitReplicasTimeout       time.Duration
		preventCrossCellPromotion bool

		lockAction string
	}
)

// NewEmergencyReparentOptions creates a new EmergencyReparentOptions which is used in ERS
func NewEmergencyReparentOptions(newPrimaryAlias *topodatapb.TabletAlias, ignoreReplicas sets.String, waitReplicasTimeout time.Duration, preventCrossCellPromotion bool) EmergencyReparentOptions {
	return EmergencyReparentOptions{
		newPrimaryAlias:           newPrimaryAlias,
		ignoreReplicas:            ignoreReplicas,
		waitReplicasTimeout:       waitReplicasTimeout,
		preventCrossCellPromotion: preventCrossCellPromotion,
	}
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
func (erp *EmergencyReparenter) ReparentShard(ctx context.Context, keyspace, shard string, opts EmergencyReparentOptions) (*events.Reparent, error) {
	// First step is to lock the shard for the given operation
	opts.lockAction = getLockAction(opts.newPrimaryAlias)
	ctx, unlock, err := erp.ts.LockShard(ctx, keyspace, shard, opts.lockAction)
	if err != nil {
		return nil, err
	}
	// defer the unlock-shard function
	defer unlock(&err)

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

	// run ERS with shard already locked
	err = erp.reparentShardLocked(ctx, ev, keyspace, shard, opts)

	return ev, err
}

// reparentShardLocked performs Emergency Reparent Shard operation assuming that the shard is already locked
func (erp *EmergencyReparenter) reparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, opts EmergencyReparentOptions) (err error) {
	// log the starting of the operation and increment the counter
	erp.logger.Infof("will initiate emergency reparent shard in keyspace - %s, shard - %s", keyspace, shard)
	ersCounter.Add(1)

	// get the shard information from the topology server
	var shardInfo *topo.ShardInfo
	shardInfo, err = erp.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	// get the previous primary according to the topology server,
	// we use this information to choose the best candidate in the same cell
	// and to undo promotion in case of failure
	var prevPrimary *topodatapb.Tablet
	if shardInfo.PrimaryAlias != nil {
		prevPrimaryInfo, err := erp.ts.GetTablet(ctx, shardInfo.PrimaryAlias)
		if err != nil {
			return err
		}
		prevPrimary = prevPrimaryInfo.Tablet
	}

	// read all the tablets and there information
	event.DispatchUpdate(ev, "reading all tablets")
	var tabletMap map[string]*topo.TabletInfo
	tabletMap, err = erp.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", keyspace, shard, err)
	}

	// Stop replication on all the tablets and build their status map
	var statusMap map[string]*replicationdatapb.StopReplicationStatus
	var primaryStatusMap map[string]*replicationdatapb.PrimaryStatus
	statusMap, primaryStatusMap, err = StopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, opts.waitReplicasTimeout, opts.ignoreReplicas, erp.logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	// check that we still have the shard lock. If we don't then we can terminate at this point
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// find the valid candidates for becoming the primary
	// this is where we check for errant GTIDs and remove the tablets that have them from consideration
	var validCandidates map[string]mysql.Position
	validCandidates, err = FindValidEmergencyReparentCandidates(statusMap, primaryStatusMap)
	if err != nil {
		return err
	}
	// Now, we restrict the valid candidates list, which removes some tablets from consideration
	validCandidates, err = restrictValidCandidates(validCandidates, tabletMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Wait for all candidates to apply relay logs
	if err = waitForAllRelayLogsToApply(ctx, erp.logger, erp.tmc, validCandidates, tabletMap, statusMap, opts.waitReplicasTimeout); err != nil {
		return err
	}

	// find the intermediate primary candidate that we want to replicate from. This will always be the most advanced tablet that we have
	// We let all the other tablets replicate from this primary. We will then try to choose a better candidate and let it catch up
	var intermediatePrimary *topodatapb.Tablet
	var validCandidateTablets []*topodatapb.Tablet
	intermediatePrimary, validCandidateTablets, err = findIntermediatePrimaryCandidate(erp.logger, prevPrimary, validCandidates, tabletMap, opts)
	if err != nil {
		return err
	}
	erp.logger.Infof("intermediate primary selected - %v", intermediatePrimary.Alias)

	// check weather the primary candidate selected is ideal or if it can be improved later
	isIdeal := intermediateCandidateIsIdeal(erp.logger, intermediatePrimary, prevPrimary, validCandidateTablets, tabletMap, opts)
	erp.logger.Infof("intermediate primary is ideal - %v", isIdeal)

	// Check (again) we still have the topology lock.
	if err = topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// initialize the newPrimary with the intermediate primary, override this value if it is not the ideal candidate
	newPrimary := intermediatePrimary
	if !isIdeal {
		// we now promote our intermediate primary candidate and also reparent all the other tablets to start replicating from this candidate
		// we do not promote the tablet or change the shard record. We only change the replication for all the other tablets
		// it also returns the list of the tablets that started replication successfully including itself. These are the candidates that we can use to find a replacement
		var validReplacementCandidates []*topodatapb.Tablet
		validReplacementCandidates, err = promoteIntermediatePrimary(ctx, erp.tmc, ev, erp.logger, intermediatePrimary, opts.lockAction, tabletMap, statusMap, opts)
		if err != nil {
			return err
		}

		// try to find a better candidate using the list we got back
		betterCandidate := getBetterCandidate(erp.logger, intermediatePrimary, prevPrimary, validReplacementCandidates, tabletMap, opts)

		// if our better candidate is different from our previous candidate, then we wait for it to catch up to the intermediate primary
		if !topoproto.TabletAliasEqual(betterCandidate.Alias, intermediatePrimary.Alias) {
			err = waitForCatchingUp(ctx, erp.tmc, erp.ts, ev, erp.logger, intermediatePrimary, betterCandidate, opts.lockAction, tabletMap, statusMap, opts)
			if err != nil {
				return err
			}
			newPrimary = betterCandidate
		}
	}

	// now we check if all the constraints are satisfied. If they are not, then we should abort
	constraintFailure := checkIfConstraintsSatisfied(newPrimary, prevPrimary, opts)
	if constraintFailure != nil {
		erp.logger.Errorf("have to override promotion because of constraint failure - %v", constraintFailure)
		// we want to send both the errors to the user, constraint failure and also any error encountered in undoing the promotion
		defer func() {
			if err != nil {
				err = vterrors.Errorf(vtrpc.Code_ABORTED, "error in undoing promotion - %v, constraint failure - %v", err, constraintFailure)
			} else {
				err = constraintFailure
			}
		}()
		// we now try to undo are changes. We can do so by promoting the previous primary instead of the new one we selected
		if prevPrimary == nil {
			return vterrors.Errorf(vtrpc.Code_ABORTED, "could not undo promotion, since shard record has no primary information")
		}
		newPrimary = prevPrimary
	}

	// Final step is to promote our primary candidate
	err = erp.promoteNewPrimary(ctx, ev, newPrimary, opts, tabletMap, statusMap)
	if err != nil {
		return err
	}

	ev.NewPrimary = proto.Clone(newPrimary).(*topodatapb.Tablet)
	return err
}

func (erp *EmergencyReparenter) promoteNewPrimary(ctx context.Context, ev *events.Reparent, newPrimary *topodatapb.Tablet, opts EmergencyReparentOptions, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus) error {
	erp.logger.Infof("starting promotion for the new primary - %v", newPrimary.Alias)
	// we call PromoteReplica which changes the tablet type, fixes the semi-sync, set the primary to read-write and flushes the binlogs
	_, err := erp.tmc.PromoteReplica(ctx, newPrimary)
	if err != nil {
		return vterrors.Wrapf(err, "primary-elect tablet %v failed to be upgraded to primary: %v", newPrimary.Alias, err)
	}
	// we now reparent all the replicas to the new primary we have promoted
	_, err = reparentReplicas(ctx, ev, erp.logger, erp.tmc, newPrimary, opts.lockAction, tabletMap, statusMap, opts, false, true)
	if err != nil {
		return err
	}
	return nil
}
