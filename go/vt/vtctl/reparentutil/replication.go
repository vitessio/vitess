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
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// FindValidEmergencyReparentCandidates will find candidates for an emergency
// reparent, and, if successful, return a mapping of those tablet aliases (as
// raw strings) to their replication positions for later comparison.
func FindValidEmergencyReparentCandidates(
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	primaryStatusMap map[string]*replicationdatapb.MasterStatus,
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
			return nil, vterrors.Wrapf(err, "could not decode a master status executed position for tablet %v: %v", alias, err)
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

	return stopStatus.Before.IoThreadRunning || stopStatus.Before.SqlThreadRunning, nil
}

// StopReplicationAndBuildStatusMaps stops replication on all replicas, then
// collects and returns a mapping of TabletAlias (as string) to their current
// replication positions.
func StopReplicationAndBuildStatusMaps(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	ev *events.Reparent,
	tabletMap map[string]*topo.TabletInfo,
	waitReplicasTimeout time.Duration,
	ignoredTablets sets.String,
	logger logutil.Logger,
) (map[string]*replicationdatapb.StopReplicationStatus, map[string]*replicationdatapb.MasterStatus, error) {
	event.DispatchUpdate(ev, "stop replication on all replicas")

	var (
		statusMap       = map[string]*replicationdatapb.StopReplicationStatus{}
		masterStatusMap = map[string]*replicationdatapb.MasterStatus{}
		m               sync.Mutex
		errChan         = make(chan error)
	)

	groupCtx, groupCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer groupCancel()

	fillStatus := func(alias string, tabletInfo *topo.TabletInfo) {
		err := vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "fillStatus did not successfully complete")
		defer func() { errChan <- err }()

		logger.Infof("getting replication position from %v", alias)

		_, stopReplicationStatus, err := tmc.StopReplicationAndGetStatus(groupCtx, tabletInfo.Tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
		switch err {
		case mysql.ErrNotReplica:
			var masterStatus *replicationdatapb.MasterStatus

			masterStatus, err = tmc.DemoteMaster(groupCtx, tabletInfo.Tablet)
			if err != nil {
				msg := "replica %v thinks it's master but we failed to demote it"
				err = vterrors.Wrapf(err, msg+": %v", alias, err)

				logger.Warningf(msg, alias)
				return
			}

			m.Lock()
			masterStatusMap[alias] = masterStatus
			m.Unlock()
		case nil:
			m.Lock()
			statusMap[alias] = stopReplicationStatus
			m.Unlock()
		default:
			logger.Warningf("failed to get replication status from %v: %v", alias, err)

			err = vterrors.Wrapf(err, "error when getting replication status for alias %v: %v", alias, err)
		}
	}

	for alias, tabletInfo := range tabletMap {
		if !ignoredTablets.Has(alias) {
			go fillStatus(alias, tabletInfo)
		}
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        len(tabletMap) - ignoredTablets.Len(),
		NumRequiredSuccesses: len(tabletMap) - ignoredTablets.Len() - 1,
		NumAllowedErrors:     1,
	}

	errRecorder := errgroup.Wait(groupCancel, errChan)
	if len(errRecorder.Errors) > 1 {
		return nil, nil, vterrors.Wrapf(errRecorder.Error(), "encountered more than one error when trying to stop replication and get positions: %v", errRecorder.Error())
	}

	return statusMap, masterStatusMap, nil
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
