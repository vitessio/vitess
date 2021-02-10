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
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

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
	NewPrimaryAlias     *topodatapb.TabletAlias
	IgnoreReplicas      sets.String
	WaitReplicasTimeout time.Duration

	// Private options managed internally. We use value passing to avoid leaking
	// these details back out.

	lockAction string
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
	opts.lockAction = erp.getLockAction(opts.NewPrimaryAlias)

	ctx, unlock, err := erp.ts.LockShard(ctx, keyspace, shard, opts.lockAction)
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

func (erp *EmergencyReparenter) promoteNewPrimary(
	ctx context.Context,
	ev *events.Reparent,
	keyspace string,
	shard string,
	newPrimaryTabletAlias string,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	opts EmergencyReparentOptions,
) error {
	erp.logger.Infof("promoting tablet %v to master", newPrimaryTabletAlias)
	event.DispatchUpdate(ev, "promoting replica")

	newPrimaryTabletInfo, ok := tabletMap[newPrimaryTabletAlias]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "attempted to promote master-elect %v that was not in the tablet map; this an impossible situation", newPrimaryTabletAlias)
	}

	rp, err := erp.tmc.PromoteReplica(ctx, newPrimaryTabletInfo.Tablet)
	if err != nil {
		return vterrors.Wrapf(err, "master-elect tablet %v failed to be upgraded to master: %v", newPrimaryTabletAlias, err)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	replCtx, replCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer replCancel()

	event.DispatchUpdate(ev, "reparenting all tablets")

	// (@ajm188) - A question while migrating: Is this by design? By my read,
	// there's nothing consuming that error channel, meaning any replica that
	// fails to SetMaster will actually block trying to send to the errCh. In
	// addition, the only way an operator will ever notice these errors will be
	// in the logs on the tablet, and not from any error propagation in
	// vtctl/wrangler, so a shard will continue to attempt to serve (probably?)
	// after a partially-failed ERS.
	now := time.Now().UnixNano()
	errCh := make(chan error)

	handlePrimary := func(alias string, ti *topo.TabletInfo) error {
		erp.logger.Infof("populating reparent journal on new master %v", alias)
		return erp.tmc.PopulateReparentJournal(replCtx, ti.Tablet, now, opts.lockAction, newPrimaryTabletInfo.Alias, rp)
	}

	handleReplica := func(alias string, ti *topo.TabletInfo) {
		erp.logger.Infof("setting new master on replica %v", alias)

		var err error
		defer func() { errCh <- err }()

		forceStart := false
		if status, ok := statusMap[alias]; ok {
			forceStart = ReplicaWasRunning(status)
		}

		err = erp.tmc.SetMaster(replCtx, ti.Tablet, newPrimaryTabletInfo.Alias, now, "", forceStart)
		if err != nil {
			err = vterrors.Wrapf(err, "tablet %v SetMaster failed: %v", alias, err)
		}
	}

	for alias, ti := range tabletMap {
		switch {
		case alias == newPrimaryTabletAlias:
			continue
		case !opts.IgnoreReplicas.Has(alias):
			go handleReplica(alias, ti)
		}
	}

	primaryErr := handlePrimary(newPrimaryTabletAlias, newPrimaryTabletInfo)
	if primaryErr != nil {
		erp.logger.Warningf("master failed to PopulateReparentJournal")
		replCancel()

		return vterrors.Wrapf(primaryErr, "failed to PopulateReparentJournal on master: %v", primaryErr)
	}

	return nil
}

func (erp *EmergencyReparenter) reparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace string, shard string, opts EmergencyReparentOptions) error {
	shardInfo, err := erp.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading all tablets")

	tabletMap, err := erp.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v: %v", keyspace, shard, err)
	}

	statusMap, primaryStatusMap, err := StopReplicationAndBuildStatusMaps(ctx, erp.tmc, ev, tabletMap, opts.WaitReplicasTimeout, opts.IgnoreReplicas, erp.logger)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop replication and build status maps: %v", err)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	validCandidates, err := FindValidEmergencyReparentCandidates(statusMap, primaryStatusMap)
	if err != nil {
		return err
	} else if len(validCandidates) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no valid candidates for emergency reparent")
	}

	// Wait for all candidates to apply relay logs
	if err := erp.waitForAllRelayLogsToApply(ctx, validCandidates, tabletMap, statusMap, opts); err != nil {
		return err
	}

	// Elect the candidate with the most up-to-date position.
	var (
		winningPosition              mysql.Position
		winningPrimaryTabletAliasStr string
	)

	for alias, position := range validCandidates {
		if winningPosition.IsZero() || position.AtLeast(winningPosition) {
			winningPosition = position
			winningPrimaryTabletAliasStr = alias
		}
	}

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs) and is at least as
	// advanced as the winning position.
	if opts.NewPrimaryAlias != nil {
		winningPrimaryTabletAliasStr = topoproto.TabletAliasString(opts.NewPrimaryAlias)
		pos, ok := validCandidates[winningPrimaryTabletAliasStr]
		switch {
		case !ok:
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v has errant GTIDs", winningPrimaryTabletAliasStr)
		case !pos.AtLeast(winningPosition):
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "master elect %v at position %v is not fully caught up. Winning position: %v", winningPrimaryTabletAliasStr, pos, winningPosition)
		}
	}

	// Check (again) we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrapf(err, "lost topology lock, aborting: %v", err)
	}

	// Do the promotion.
	if err := erp.promoteNewPrimary(ctx, ev, keyspace, shard, winningPrimaryTabletAliasStr, tabletMap, statusMap, opts); err != nil {
		return err
	}

	return nil
}

func (erp *EmergencyReparenter) waitForAllRelayLogsToApply(
	ctx context.Context,
	validCandidates map[string]mysql.Position,
	tabletMap map[string]*topo.TabletInfo,
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	opts EmergencyReparentOptions,
) error {
	errCh := make(chan error)
	defer close(errCh)

	groupCtx, groupCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer groupCancel()

	for candidate := range validCandidates {
		go func(alias string) {
			var err error
			defer func() { errCh <- err }()
			err = WaitForRelayLogsToApply(groupCtx, erp.tmc, tabletMap[alias], statusMap[alias])
		}(candidate)
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        len(validCandidates),
		NumRequiredSuccesses: len(validCandidates),
		NumAllowedErrors:     0,
	}
	rec := errgroup.Wait(groupCancel, errCh)

	if len(rec.Errors) != 0 {
		return vterrors.Wrapf(rec.Error(), "could not apply all relay logs within the provided WaitReplicasTimeout (%s): %v", opts.WaitReplicasTimeout, rec.Error())
	}

	return nil
}
