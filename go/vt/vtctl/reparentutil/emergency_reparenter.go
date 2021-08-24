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
	tmc    tmclient.TabletManagerClient
	logger logutil.Logger
}

// NewEmergencyReparenter returns a new EmergencyReparenter object, ready to
// perform EmergencyReparentShard operations using the given
// TabletManagerClient, and logger.
//
// Providing a nil logger instance is allowed.
func NewEmergencyReparenter(tmc tmclient.TabletManagerClient, logger logutil.Logger) *EmergencyReparenter {
	erp := EmergencyReparenter{
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
func (erp *EmergencyReparenter) ReparentShard(ctx context.Context, reparentFunctions ReparentFunctions) (*events.Reparent, error) {
	ctx, unlock, err := reparentFunctions.LockShard(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock(&err)

	ev := &events.Reparent{}
	defer func() {
		switch err {
		case nil:
			event.DispatchUpdate(ev, "finished EmergencyReparentShard")
		default:
			event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
		}
	}()

	err = erp.reparentShardLocked(ctx, ev, reparentFunctions)

	return ev, err
}

func (erp *EmergencyReparenter) reparentShardLocked(ctx context.Context, ev *events.Reparent, reparentFunctions ReparentFunctions) error {

	if reparentFunctions.CheckIfFixed() {
		return nil
	}

	ts := reparentFunctions.GetTopoServer()
	keyspace := reparentFunctions.GetKeyspace()
	shard := reparentFunctions.GetShard()
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo
	event.DispatchUpdate(ev, "reading all tablets")

	if err := reparentFunctions.PreRecoveryProcesses(ctx); err != nil {
		return err
	}

	if err := reparentFunctions.CheckPrimaryRecoveryType(); err != nil {
		return err
	}

	tabletMap, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", keyspace, shard, err)
	}

	statusMap, primaryStatusMap, err := StopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, reparentFunctions.GetWaitReplicasTimeout(), reparentFunctions.GetIgnoreReplicas(), erp.logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// TODO: remove this entirely
	reparentFunctions.SetMaps(tabletMap, statusMap, primaryStatusMap)

	validCandidates, err := FindValidEmergencyReparentCandidates(statusMap, primaryStatusMap)
	if err != nil {
		return err
	}
	validCandidates, err = reparentFunctions.RestrictValidCandidates(validCandidates, tabletMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Wait for all candidates to apply relay logs
	if err := waitForAllRelayLogsToApply(ctx, erp.logger, erp.tmc, validCandidates, tabletMap, statusMap, reparentFunctions.GetWaitReplicasTimeout()); err != nil {
		return err
	}

	newPrimary, err := reparentFunctions.FindPrimaryCandidates(ctx, erp.logger, erp.tmc, validCandidates, tabletMap)
	if err != nil {
		return err
	}

	isIdeal := reparentFunctions.PromotedReplicaIsIdeal(newPrimary, tabletMap, primaryStatusMap, validCandidates)

	// TODO := LockAction and RP
	if err := PromotePrimaryCandidateAndStartReplication(ctx, erp.tmc, ts, ev, erp.logger, newPrimary, "", "", tabletMap, reparentFunctions, isIdeal); err != nil {
		return err
	}

	if err := reparentFunctions.CheckIfNeedToOverridePrimary(); err != nil {
		return err
	}

	// Check (again) we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	if err := reparentFunctions.StartReplication(ctx, ev, erp.logger, erp.tmc); err != nil {
		return err
	}

	ev.NewPrimary = proto.Clone(reparentFunctions.GetNewPrimary()).(*topodatapb.Tablet)

	return nil
}
