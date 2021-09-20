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

// counters for Emergency Reparent Shard
var (
	ersCounter = stats.NewGauge("ers_counter", "Number of time Emergency Reparent Shard has been run")
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
			event.DispatchUpdate(ev, "finished EmergencyReparentShard")
		default:
			event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
		}
	}()

	// run ERS with shard already locked
	err = erp.reparentShardLocked(ctx, ev, keyspace, shard, opts)

	opts.PostERSCompletionHook(ctx, ev, erp.logger, erp.tmc)

	return ev, err
}

// reparentShardLocked performs Emergency Reparent Shard operation assuming that the shard is already locked
func (erp *EmergencyReparenter) reparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, opts EmergencyReparentOptions) error {
	// log the starting of the operation and increment the counter
	erp.logger.Infof("will initiate emergency reparent shard in keyspace - %s, shard - %s", keyspace, shard)
	ersCounter.Add(1)

	// get the shard information from the topology server
	shardInfo, err := erp.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	// get the previous primary according to the topology server
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
	tabletMap, err := erp.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", keyspace, shard, err)
	}

	// Stop replication on all the tablets and build their status map
	statusMap, primaryStatusMap, err := StopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, opts.GetWaitReplicasTimeout(), opts.GetIgnoreReplicas(), erp.logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	// check that we still have the shard lock. If we don't then we can terminate at this point
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// find the valid candidates for becoming the primary
	// this is where we check for errant GTIDs and remove the tablets that have them from consideration
	validCandidates, err := FindValidEmergencyReparentCandidates(statusMap, primaryStatusMap)
	if err != nil {
		return err
	}
	// Now, we restrict the valid candidates list according to the ReparentFunctions implementations
	validCandidates, err = restrictValidCandidates(validCandidates, tabletMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Wait for all candidates to apply relay logs
	if err := waitForAllRelayLogsToApply(ctx, erp.logger, erp.tmc, validCandidates, tabletMap, statusMap, opts.GetWaitForRelayLogsTimeout()); err != nil {
		return err
	}

	// find the primary candidate that we want to promote
	var newPrimary *topodatapb.Tablet
	var validCandidateTablets []*topodatapb.Tablet
	newPrimary, validCandidateTablets, err = findPrimaryCandidate(erp.logger, prevPrimary, validCandidates, tabletMap, opts)
	if err != nil {
		return err
	}

	// check weather the primary candidate selected is ideal or if it can be improved later
	isIdeal := promotedReplicaIsIdeal(newPrimary, prevPrimary, validCandidateTablets, opts)

	// Check (again) we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	if !isIdeal {
		// we now promote our primary candidate and also reparent all the other tablets to start replicating from this candidate
		validReplacementCandidates, err := promoteIntermediatePrimary(ctx, erp.tmc, ev, erp.logger, newPrimary, opts.lockAction, tabletMap, statusMap, opts)
		if err != nil {
			return err
		}

		// try to find a better candidate if we do not already have the most ideal one
		betterCandidate := getBetterCandidate(newPrimary, prevPrimary, validReplacementCandidates, opts)

		// if our better candidate is different from our previous candidate, then we replace our primary
		if !topoproto.TabletAliasEqual(betterCandidate.Alias, newPrimary.Alias) {
			err = replaceWithBetterCandidate(ctx, erp.tmc, erp.ts, ev, erp.logger, newPrimary, betterCandidate, opts.lockAction, tabletMap, statusMap, opts)
			if err != nil {
				return err
			}
			newPrimary = betterCandidate
		}
	}

	// now we check if there is a need to override the promotion of the newPrimary
	errInPromotion := checkIfNeedToOverridePromotion(newPrimary, prevPrimary, opts)
	if errInPromotion != nil {
		erp.logger.Errorf("have to override promotion because of constraint failure - %v", errInPromotion)
		// we try and undo the promotion
		newPrimary, err = erp.undoPromotion(ctx, erp.ts, ev, keyspace, shard, prevPrimary, opts.lockAction, tabletMap, statusMap, opts)
		if err != nil {
			return err
		}
		if newPrimary == nil {
			return vterrors.Errorf(vtrpc.Code_ABORTED, "could not undo promotion")
		}
	}

	// Finally, we call PromoteReplica which fixes the semi-sync, set the primary to read-write and flushes the binlogs
	_, err = erp.tmc.PromoteReplica(ctx, newPrimary)
	if err != nil {
		return vterrors.Wrapf(err, "primary-elect tablet %v failed to be upgraded to primary: %v", newPrimary.Alias, err)
	}
	_, err = reparentReplicas(ctx, ev, erp.logger, erp.tmc, newPrimary, opts.lockAction, tabletMap, statusMap, opts, false, true)
	if err != nil {
		return err
	}

	ev.NewPrimary = proto.Clone(newPrimary).(*topodatapb.Tablet)
	return errInPromotion
}

func (erp *EmergencyReparenter) undoPromotion(ctx context.Context, ts *topo.Server, ev *events.Reparent, keyspace, shard string, prevPrimary *topodatapb.Tablet,
	lockAction string, tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, opts EmergencyReparentOptions) (*topodatapb.Tablet, error) {
	var err error
	if prevPrimary != nil {
		// promote the original primary back
		_, err = promotePrimaryCandidate(ctx, erp.tmc, ts, ev, erp.logger, prevPrimary, lockAction, tabletMap, statusMap, opts, true)
		if err == nil {
			return prevPrimary, nil
		}
		erp.logger.Errorf("error in undoing promotion - %v", err)
	}

	return prevPrimary, err
}
