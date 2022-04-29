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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// FindValidEmergencyReparentCandidates will find candidates for an emergency
// reparent, and, if successful, return a mapping of those tablet aliases (as
// raw strings) to their replication positions for later comparison.
func FindValidEmergencyReparentCandidates(
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	primaryStatusMap map[string]*replicationdatapb.PrimaryStatus,
) (map[string]mysql.Position, error) {
	replicationStatusMap := make(map[string]*mysql.ReplicationStatus, len(statusMap))
	positionMap := make(map[string]mysql.Position)

	// Build out replication status list from proto types.
	for alias, statuspb := range statusMap {
		status := mysql.ProtoToReplicationStatus(statuspb.After)
		replicationStatusMap[alias] = &status
	}

	// Determine if we're GTID-based. If we are, we'll need to look for errant
	// GTIDs below.
	var (
		isGTIDBased                bool
		isNonGTIDBased             bool
		emptyRelayPosErrorRecorder concurrency.FirstErrorRecorder
	)

	for alias, status := range replicationStatusMap {
		if _, ok := status.RelayLogPosition.GTIDSet.(mysql.Mysql56GTIDSet); ok {
			isGTIDBased = true
		} else {
			isNonGTIDBased = true
		}

		if status.RelayLogPosition.IsZero() {
			// Potentially bail. If any other tablet is detected to have
			// GTID-based relay log positions, we will return the error recorded
			// here.
			emptyRelayPosErrorRecorder.RecordError(vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "encountered tablet %v with no relay log position, when at least one other tablet in the status map has GTID based relay log positions", alias))
		}
	}

	if isGTIDBased && emptyRelayPosErrorRecorder.HasErrors() {
		return nil, emptyRelayPosErrorRecorder.Error()
	}

	if isGTIDBased && isNonGTIDBased {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "encountered mix of GTID-based and non GTID-based relay logs")
	}

	// Create relevant position list of errant GTID-based positions for later
	// comparison.
	for alias, status := range replicationStatusMap {
		// If we're not GTID-based, no need to search for errant GTIDs, so just
		// add the position to the map and continue.
		if !isGTIDBased {
			positionMap[alias] = status.Position

			continue
		}

		// This condition should really never happen, since we did the same cast
		// in the earlier loop, but let's be doubly sure.
		relayLogGTIDSet, ok := status.RelayLogPosition.GTIDSet.(mysql.Mysql56GTIDSet)
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "we got a filled-in relay log position, but it's not of type Mysql56GTIDSet, even though we've determined we need to use GTID based assesment")
		}

		// We need to remove this alias's status from the list, otherwise the
		// GTID diff will always be empty.
		statusList := make([]*mysql.ReplicationStatus, 0, len(replicationStatusMap)-1)

		for a, s := range replicationStatusMap {
			if a != alias {
				statusList = append(statusList, s)
			}
		}

		errantGTIDs, err := status.FindErrantGTIDs(statusList)
		switch {
		case err != nil:
			// Could not look up GTIDs to determine if we have any. It's not
			// safe to continue.
			return nil, err
		case len(errantGTIDs) != 0:
			// This tablet has errant GTIDs. It's not a valid candidate for
			// reparent, so don't insert it into the final mapping.
			continue
		}

		pos := mysql.Position{GTIDSet: relayLogGTIDSet}
		positionMap[alias] = pos
	}

	for alias, primaryStatus := range primaryStatusMap {
		executedPosition, err := mysql.DecodePosition(primaryStatus.Position)
		if err != nil {
			return nil, vterrors.Wrapf(err, "could not decode a primary status executed position for tablet %v: %v", alias, err)
		}

		positionMap[alias] = executedPosition
	}

	return positionMap, nil
}

// ReplicaWasRunning returns true if a StopReplicationStatus indicates that the
// replica had running replication threads before being stopped. It returns an
// error if the Before state of replication is nil.
func ReplicaWasRunning(stopStatus *replicationdatapb.StopReplicationStatus) (bool, error) {
	if stopStatus == nil || stopStatus.Before == nil {
		return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "could not determine Before state of StopReplicationStatus %v", stopStatus)
	}

	// v13 and older tablets will only provide the binary IoThreadRunning and
	// SqlThreadRunning values while v14 and newer tablets will provide the
	// non-binary replication states.
	// This backwards compatible check can be removed in v15+.
	return ((stopStatus.Before.IoState == int32(mysql.ReplicationStateRunning) || stopStatus.Before.IoThreadRunning) ||
		(stopStatus.Before.SqlState == int32(mysql.ReplicationStateRunning) || stopStatus.Before.SqlThreadRunning)), nil
}

// SetReplicationSource is used to set the replication source on the specified
// tablet to the current shard primary (if available). It also figures out if
// the tablet should be sending semi-sync ACKs or not and passes that to the
// tabletmanager RPC.
//
// It does not start the replication forcefully.
// If we are unable to find the shard primary of the tablet from the topo server
// we exit out without any error.
func SetReplicationSource(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, tablet *topodatapb.Tablet) error {
	shardPrimary, err := topotools.GetShardPrimaryForTablet(ctx, ts, tablet)
	if err != nil {
		// If we didn't find the shard primary, we return without any error
		return nil
	}

	isSemiSync := IsReplicaSemiSync(shardPrimary.Tablet, tablet)
	return tmc.SetReplicationSource(ctx, tablet, shardPrimary.Alias, 0, "", false, isSemiSync)
}

// replicationSnapshot stores the status maps and the tablets that were reachable
// when trying to stopReplicationAndBuildStatusMaps.
type replicationSnapshot struct {
	statusMap        map[string]*replicationdatapb.StopReplicationStatus
	primaryStatusMap map[string]*replicationdatapb.PrimaryStatus
	reachableTablets []*topodatapb.Tablet
}

// stopReplicationAndBuildStatusMaps stops replication on all replicas, then
// collects and returns a mapping of TabletAlias (as string) to their current
// replication positions.
// Apart from the status maps, it also returns the tablets reached as a list
func stopReplicationAndBuildStatusMaps(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	ev *events.Reparent,
	tabletMap map[string]*topo.TabletInfo,
	waitReplicasTimeout time.Duration,
	ignoredTablets sets.String,
	tabletToWaitFor *topodatapb.TabletAlias,
	logger logutil.Logger,
) (*replicationSnapshot, error) {
	event.DispatchUpdate(ev, "stop replication on all replicas")

	var (
		m          sync.Mutex
		errChan    = make(chan concurrency.Error)
		allTablets []*topodatapb.Tablet
		res        = &replicationSnapshot{
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			reachableTablets: []*topodatapb.Tablet{},
		}
	)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	fillStatus := func(alias string, tabletInfo *topo.TabletInfo, mustWaitForTablet bool) {
		var concurrencyErr concurrency.Error
		var err error
		defer func() {
			concurrencyErr.Err = err
			concurrencyErr.MustWaitFor = mustWaitForTablet
			errChan <- concurrencyErr
		}()

		logger.Infof("getting replication position from %v", alias)

		_, stopReplicationStatus, err := tmc.StopReplicationAndGetStatus(groupCtx, tabletInfo.Tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
		if err != nil {
			sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
			if isSQLErr && sqlErr != nil && sqlErr.Number() == mysql.ERNotReplica {
				var primaryStatus *replicationdatapb.PrimaryStatus

				primaryStatus, err = tmc.DemotePrimary(groupCtx, tabletInfo.Tablet)
				if err != nil {
					msg := "replica %v thinks it's primary but we failed to demote it"
					err = vterrors.Wrapf(err, msg+": %v", alias, err)

					logger.Warningf(msg, alias)
					return
				}

				m.Lock()
				res.primaryStatusMap[alias] = primaryStatus
				res.reachableTablets = append(res.reachableTablets, tabletInfo.Tablet)
				m.Unlock()
			} else {
				logger.Warningf("failed to get replication status from %v: %v", alias, err)
				err = vterrors.Wrapf(err, "error when getting replication status for alias %v: %v", alias, err)
			}
		} else {
			m.Lock()
			res.statusMap[alias] = stopReplicationStatus
			res.reachableTablets = append(res.reachableTablets, tabletInfo.Tablet)
			m.Unlock()
		}
	}

	tabletAliasToWaitFor := ""
	numErrorsToWaitFor := 0
	if tabletToWaitFor != nil {
		tabletAliasToWaitFor = topoproto.TabletAliasString(tabletToWaitFor)
	}
	for alias, tabletInfo := range tabletMap {
		allTablets = append(allTablets, tabletInfo.Tablet)
		if !ignoredTablets.Has(alias) {
			mustWaitFor := tabletAliasToWaitFor == alias
			if mustWaitFor {
				numErrorsToWaitFor++
			}
			go fillStatus(alias, tabletInfo, mustWaitFor)
		}
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        len(tabletMap) - ignoredTablets.Len(),
		NumRequiredSuccesses: len(tabletMap) - ignoredTablets.Len() - 1,
		NumAllowedErrors:     1,
		NumErrorsToWaitFor:   numErrorsToWaitFor,
	}

	errRecorder := errgroup.Wait(groupCancel, errChan)
	if len(errRecorder.Errors) <= 1 {
		return res, nil
	}
	// check that the tablets we were able to reach are sufficient for us to guarantee that no new write will be accepted by any tablet
	revokeSuccessful := haveRevoked(res.reachableTablets, allTablets)
	if !revokeSuccessful {
		return nil, vterrors.Wrapf(errRecorder.Error(), "could not reach sufficient tablets to guarantee safety: %v", errRecorder.Error())
	}

	return res, nil
}

// WaitForRelayLogsToApply blocks execution waiting for the given tablet's relay
// logs to apply, unless the specified context is canceled or exceeded.
// Typically a caller will set a timeout of WaitReplicasTimeout on a context and
// use that context with this function.
func WaitForRelayLogsToApply(ctx context.Context, tmc tmclient.TabletManagerClient, tabletInfo *topo.TabletInfo, status *replicationdatapb.StopReplicationStatus) error {
	switch status.After.RelayLogPosition {
	case "":
		return tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.FileRelayLogPosition)
	default:
		return tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.RelayLogPosition)
	}
}
