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
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// RelayLogPositions contains the positions of the relay log.
type RelayLogPositions struct {
	// Combined represents the entire range of the relay
	// log with the retrieved + executed GTID sets
	// combined.
	Combined replication.Position

	// Executed represents the executed GTID set of the
	// relay log/SQL thread.
	Executed replication.Position
}

// AtLeast returns true if the RelayLogPositions object contains at least the positions provided
// as pos. If the combined positions are equal, prioritize the position where more events have
// been executed/applied, as this avoids picking tablets with SQL delay (intended or not) that
// can delay/timeout the reparent. Otherwise, pick the larger of the two combined positions as
// it contains more changes, irrespective of how many changes are executed/applied.
func (rlp *RelayLogPositions) AtLeast(pos *RelayLogPositions) bool {
	if pos == nil {
		return false
	}

	if rlp.Combined.Equal(pos.Combined) {
		return rlp.Executed.AtLeast(pos.Executed)
	}
	return rlp.Combined.AtLeast(pos.Combined)
}

// Equal returns true if the RelayLogPositions object is equal to
// the positions provided as pos.
func (rlp *RelayLogPositions) Equal(pos *RelayLogPositions) bool {
	if pos == nil {
		return false
	}
	return rlp.Combined.Equal(pos.Combined) && rlp.Executed.Equal(pos.Executed)
}

// IsZero returns true if the RelayLogPositions is zero.
func (rlp *RelayLogPositions) IsZero() bool {
	return rlp.Combined.IsZero()
}

// hasDominantPosition returns true if position a is strictly ahead of position b, meaning
// a contains everything b has, plus more.
func hasDominantPosition(a, b replication.Position) bool {
	return a.AtLeast(b) && !b.AtLeast(a)
}

// haveIncomparablePositions returns true if neither position contains the other. Replication
// lag alone can't cause this, it takes writes that no other tablet has seen (split brain
// or errant GTIDs).
func haveIncomparablePositions(a, b replication.Position) bool {
	return !a.AtLeast(b) && !b.AtLeast(a)
}

// haveReciprocallyContainedPositions returns true if two unequal positions contain each
// other. Containment can't order these: MariaDB GTID containment ignores the origin
// server, so positions like 0-1-10 and 0-2-10 "contain" each other while holding
// different histories (a split brain).
func haveReciprocallyContainedPositions(a, b replication.Position) bool {
	return a.AtLeast(b) && b.AtLeast(a) && !a.Equal(b)
}

func describeCombinedPositions(candidates map[string]*RelayLogPositions) string {
	parts := make([]string, 0, len(candidates))
	for alias, pos := range candidates {
		parts = append(parts, fmt.Sprintf("%s=%s", alias, pos.Combined.String()))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

// hasUniformCombinedPosition returns true when every candidate has the same Combined position. On the
// output of filterToMostAdvancedCombined a false result means the leading candidates have
// incomparable positions, as the filter already removed anything dominated.
func hasUniformCombinedPosition(candidates map[string]*RelayLogPositions) bool {
	var ref replication.Position
	var set bool
	for _, pos := range candidates {
		if !set {
			ref = pos.Combined
			set = true
			continue
		}
		if !pos.Combined.Equal(ref) {
			return false
		}
	}
	return true
}

// filterToMostAdvancedCombined returns the candidates that no other candidate dominates on
// the Combined position. GTID positions are partially ordered, so each candidate is
// compared against all of the others; two incomparable leaders don't dominate each other
// and both must be kept, which hasUniformCombinedPosition relies on. The returned map shares the
// caller's RelayLogPositions structs.
func filterToMostAdvancedCombined(candidates map[string]*RelayLogPositions, logger logutil.Logger) map[string]*RelayLogPositions {
	if len(candidates) == 0 {
		return candidates
	}

	result := make(map[string]*RelayLogPositions, len(candidates))
	for alias, pos := range candidates {
		var dominated bool
		for otherAlias, otherPos := range candidates {
			if otherAlias == alias {
				continue
			}
			if hasDominantPosition(otherPos.Combined, pos.Combined) {
				dominated = true
				break
			}
		}
		if !dominated {
			result[alias] = pos
		}
	}

	if len(result) < len(candidates) {
		excluded := make([]string, 0, len(candidates)-len(result))
		kept := make([]string, 0, len(result))
		for alias := range candidates {
			if _, ok := result[alias]; !ok {
				excluded = append(excluded, alias)
			}
		}
		for alias := range result {
			kept = append(kept, alias)
		}
		slices.Sort(excluded)
		slices.Sort(kept)
		logger.Infof("excluding %d candidate(s) strictly behind the most-advanced received relay log position from the relay-log-apply wait: %s (still repointed after promotion); waiting on: %s",
			len(excluded), strings.Join(excluded, ", "), strings.Join(kept, ", "))
	}

	return result
}

// hasMysql56GTIDSet reports whether pos carries a MySQL 5.6-style GTID set, as
// opposed to a MariaDB GTID set or a file position. This is the definition of
// "GTID-based" that ERS uses: its GTID-specific handling (the relay-log reconcile
// and errant-GTID detection) assumes MySQL56 semantics, where the retrieved
// (Combined) set is distinct from the executed set. A zero position has a nil
// GTIDSet and is therefore not MySQL56 here — callers that need to treat an empty
// position as flavor-agnostic must check IsZero separately.
func hasMysql56GTIDSet(pos replication.Position) bool {
	_, ok := pos.GTIDSet.(replication.Mysql56GTIDSet)
	return ok
}

// FindPositionsOfAllCandidates will find candidates for an emergency
// reparent, and, if successful, return a mapping of those tablet aliases (as
// raw strings) to their replication positions for later comparison.
func FindPositionsOfAllCandidates(
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	primaryStatusMap map[string]*replicationdatapb.PrimaryStatus,
) (map[string]*RelayLogPositions, bool, error) {
	replicationStatusMap := make(map[string]*replication.ReplicationStatus, len(statusMap))
	primaryPositions := make(map[string]replication.Position, len(primaryStatusMap))
	positionMap := make(map[string]*RelayLogPositions, len(statusMap)+len(primaryStatusMap))

	// Build out replication status list from proto types.
	for alias, statuspb := range statusMap {
		status := replication.ProtoToReplicationStatus(statuspb.After)
		replicationStatusMap[alias] = &status
	}
	// Decode former-primary executed positions up front so their flavor can
	// participate in the GTID/non-GTID detection below. A former primary was not
	// replicating, so it has no relay-log position; its executed position is
	// authoritative and is what we key its flavor off. It is exempt from the
	// empty-relay-log check that applies to replicas.
	for alias, primaryStatus := range primaryStatusMap {
		executedPosition, err := replication.DecodePosition(primaryStatus.Position)
		if err != nil {
			return nil, false, vterrors.Wrapf(err, "could not decode a primary status executed position for tablet %v", alias)
		}
		primaryPositions[alias] = executedPosition
	}

	// Determine if we're GTID-based. If we are, we'll need to look for errant
	// GTIDs below. Both replicas and former primaries contribute to detection so
	// that a shard whose only reachable candidates are former primaries is still
	// correctly classified.
	var (
		isGTIDBased                bool
		isNonGTIDBased             bool
		emptyRelayPosErrorRecorder concurrency.FirstErrorRecorder
	)

	for alias, status := range replicationStatusMap {
		if hasMysql56GTIDSet(status.RelayLogPosition) {
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
	// Fold former-primary flavors into detection. A position with no decoded GTID
	// set at all (a former primary whose executed position was empty, e.g. "") is
	// flavor-agnostic, so skip it rather than treating it as non-GTID. A typed but
	// empty set (e.g. "MySQL56/" with no transactions) still identifies the flavor
	// and is counted.
	for _, pos := range primaryPositions {
		if pos.GTIDSet == nil {
			continue
		}
		if hasMysql56GTIDSet(pos) {
			isGTIDBased = true
		} else {
			isNonGTIDBased = true
		}
	}

	if isGTIDBased && emptyRelayPosErrorRecorder.HasErrors() {
		return nil, false, emptyRelayPosErrorRecorder.Error()
	}

	if isGTIDBased && isNonGTIDBased {
		return nil, false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "encountered mix of GTID-based and non GTID-based relay logs")
	}

	// Store the final positions in the map.
	for alias, status := range replicationStatusMap {
		if !isGTIDBased {
			positionMap[alias] = &RelayLogPositions{Combined: status.Position}

			continue
		}
		positionMap[alias] = &RelayLogPositions{
			Combined: status.RelayLogPosition,
			Executed: status.Position,
		}
	}

	for alias, executedPosition := range primaryPositions {
		// A demoted/former primary applies no relay log, so its executed position
		// is authoritative. For GTID-based shards, initialize Executed as well as
		// Combined so that in RelayLogPositions.AtLeast it compares equal to an
		// equally-advanced replica (whose Executed is reconciled up to Combined
		// after its relay-log wait), letting election fall through to the
		// promotion-rule/version tiebreakers instead of ranking the former primary
		// behind. On non-GTID shards there is no such Executed reconcile — replicas
		// keep Executed zero (their executed position lives in Combined) — so leave
		// Executed zero here too, matching the replicas and preserving the prior
		// non-GTID position ordering.
		positionMap[alias] = &RelayLogPositions{Combined: executedPosition}
		if isGTIDBased {
			positionMap[alias].Executed = executedPosition
		}
	}

	return positionMap, isGTIDBased, nil
}

// ReplicaWasRunning returns true if a StopReplicationStatus indicates that the
// replica had running replication threads before being stopped. It returns an
// error if the Before state of replication is nil.
func ReplicaWasRunning(stopStatus *replicationdatapb.StopReplicationStatus) (bool, error) {
	if stopStatus == nil || stopStatus.Before == nil {
		return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "could not determine Before state of StopReplicationStatus %v", stopStatus)
	}

	replStatus := replication.ProtoToReplicationStatus(stopStatus.Before)
	return (replStatus.IOState == replication.ReplicationStateRunning) ||
		(replStatus.SQLState == replication.ReplicationStateRunning), nil
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

	durabilityName, err := ts.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("Getting a new durability policy for %v", durabilityName))
	durability, err := policy.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return err
	}

	isSemiSync := policy.IsReplicaSemiSync(durability, shardPrimary.Tablet, tablet)
	return tmc.SetReplicationSource(ctx, tablet, shardPrimary.Alias, 0, "", false, isSemiSync, 0)
}

// replicationSnapshot stores the status maps and the tablets that were reachable
// when trying to stopReplicationAndBuildStatusMaps.
type replicationSnapshot struct {
	statusMap          map[string]*replicationdatapb.StopReplicationStatus
	primaryStatusMap   map[string]*replicationdatapb.PrimaryStatus
	reachableTablets   []*topodatapb.Tablet
	tabletsBackupState map[string]bool
	mysqlVersions      map[string]mysqlctl.ServerVersion
	mysqlFlavors       map[string]mysqlctl.MySQLFlavor
}

// replicasWithStoppedIO returns the reachable replicas whose IO threads ERS
// stopped and should restart during cleanup.
func (rs *replicationSnapshot) replicasWithStoppedIO(tabletMap map[string]*topo.TabletInfo) []*topodatapb.Tablet {
	replicas := make([]*topodatapb.Tablet, 0, len(rs.statusMap))

	for alias, stopStatus := range rs.statusMap {
		ioThreadWasRunning, err := replicaIOThreadWasRunning(stopStatus)
		if err != nil || !ioThreadWasRunning {
			continue
		}

		tabletInfo := tabletMap[alias]
		if tabletInfo == nil || tabletInfo.Tablet == nil {
			continue
		}

		replicas = append(replicas, tabletInfo.Tablet)
	}

	return replicas
}

// replicaIOThreadWasRunning returns true if a StopReplicationStatus indicates
// that ERS stopped a healthy IO thread that should restart during cleanup.
func replicaIOThreadWasRunning(stopStatus *replicationdatapb.StopReplicationStatus) (bool, error) {
	if stopStatus == nil || stopStatus.Before == nil {
		return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "could not determine Before state of StopReplicationStatus %v", stopStatus)
	}

	replStatus := replication.ProtoToReplicationStatus(stopStatus.Before)

	return replStatus.IOHealthy(), nil
}

// tabletAliasError wraps an error with the tablet alias that produced it.
type tabletAliasError struct {
	alias *topodatapb.TabletAlias
	err   error
}

// Error returns the wrapped error.
func (e *tabletAliasError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

// GetAlias returns the tablet alias that produced the error.
func (e *tabletAliasError) GetAlias() *topodatapb.TabletAlias {
	return e.alias
}

// Unwrap returns the underlying error.
func (e *tabletAliasError) Unwrap() error {
	return e.err
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
	primaryAlias *topodatapb.TabletAlias,
	stopReplicationTimeout time.Duration,
	ignoredTablets sets.Set[string],
	tabletToWaitFor *topodatapb.TabletAlias,
	durability policy.Durabler,
	waitForAllTablets bool,
	logger logutil.Logger,
) (*replicationSnapshot, error) {
	event.DispatchUpdate(ev, "stop replication on all replicas")

	var (
		m          sync.Mutex
		errChan    = make(chan concurrency.Error)
		allTablets = make([]*topodatapb.Tablet, 0, len(tabletMap))
		res        = &replicationSnapshot{
			statusMap:          map[string]*replicationdatapb.StopReplicationStatus{},
			primaryStatusMap:   map[string]*replicationdatapb.PrimaryStatus{},
			reachableTablets:   make([]*topodatapb.Tablet, 0, len(tabletMap)),
			tabletsBackupState: map[string]bool{},
			mysqlVersions:      map[string]mysqlctl.ServerVersion{},
			mysqlFlavors:       map[string]mysqlctl.MySQLFlavor{},
		}
	)

	groupCtx, groupCancel := context.WithTimeout(ctx, stopReplicationTimeout)
	defer groupCancel()

	fillStatus := func(alias string, tabletInfo *topo.TabletInfo, mustWaitForTablet bool) {
		var concurrencyErr concurrency.Error
		var err error
		defer func() {
			if err != nil {
				concurrencyErr.Err = &tabletAliasError{
					alias: tabletInfo.GetAlias(),
					err:   err,
				}
			}
			concurrencyErr.MustWaitFor = mustWaitForTablet
			errChan <- concurrencyErr
		}()

		logger.Infof("getting replication position from %v", alias)

		stopReplicationStatus, err := tmc.StopReplicationAndGetStatus(groupCtx, tabletInfo.Tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
		if err != nil {
			sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			if isSQLErr && sqlErr != nil && sqlErr.Number() == sqlerror.ERNotReplica {
				var primaryStatus *replicationdatapb.PrimaryStatus

				primaryStatus, err = tmc.DemotePrimary(groupCtx, tabletInfo.Tablet, true /* force */)
				if err != nil {
					err = vterrors.Wrapf(err, "replica %v thinks it's primary but we failed to demote it", alias)

					logger.Warningf("replica %v thinks it's primary but we failed to demote it: %v", alias, err)
					return
				}

				m.Lock()
				res.primaryStatusMap[alias] = primaryStatus
				res.reachableTablets = append(res.reachableTablets, tabletInfo.Tablet)
				if primaryStatus.ServerVersion != "" {
					if flavor, v, parseErr := mysqlctl.ParseVersionString(primaryStatus.ServerVersion); parseErr == nil {
						res.mysqlVersions[alias] = v
						res.mysqlFlavors[alias] = flavor
					} else {
						logger.Warningf("failed to parse MySQL version %q for tablet %v: %v", primaryStatus.ServerVersion, alias, parseErr)
					}
				} else {
					logger.Warningf("could not determine MySQL version for tablet %v; it will not be preferred by version-aware election", alias)
				}
				m.Unlock()
			} else {
				logger.Warningf("failed to get replication status from %v: %v", alias, err)
				err = vterrors.Wrapf(err, "error when getting replication status for alias %v", alias)
			}
		} else {
			isTakingBackup := false

			// Prefer the most up-to-date information regarding whether the tablet is taking a backup from the After
			// replication status, but fall back to the Before status if After is nil.
			if stopReplicationStatus.After != nil {
				isTakingBackup = stopReplicationStatus.After.BackupRunning
			} else if stopReplicationStatus.Before != nil {
				isTakingBackup = stopReplicationStatus.Before.BackupRunning
			}

			m.Lock()
			res.tabletsBackupState[alias] = isTakingBackup
			res.statusMap[alias] = stopReplicationStatus
			res.reachableTablets = append(res.reachableTablets, tabletInfo.Tablet)
			if stopReplicationStatus.Before != nil && stopReplicationStatus.Before.ServerVersion != "" {
				if flavor, v, parseErr := mysqlctl.ParseVersionString(stopReplicationStatus.Before.ServerVersion); parseErr == nil {
					res.mysqlVersions[alias] = v
					res.mysqlFlavors[alias] = flavor
				} else {
					logger.Warningf("failed to parse MySQL version %q for tablet %v: %v", stopReplicationStatus.Before.ServerVersion, alias, parseErr)
				}
			} else {
				logger.Warningf("could not determine MySQL version for tablet %v; it will not be preferred by version-aware election", alias)
			}
			m.Unlock()
		}
	}

	// For the tablets that we want to get a response from necessarily, we
	// get them to set the MustWaitFor boolean as part of the concurrency.Error message
	// that we send to the waitGroup below.
	//
	// numErrorsToWaitFor corresponds to how many such tablets there are. This is the number
	// of special messages with MustWaitFor set that the call errgroup.Wait will wait for.
	tabletAliasToWaitFor := ""
	numErrorsToWaitFor := 0
	if tabletToWaitFor != nil {
		tabletAliasToWaitFor = topoproto.TabletAliasString(tabletToWaitFor)
	}
	numGoRoutines := 0
	for alias, tabletInfo := range tabletMap {
		allTablets = append(allTablets, tabletInfo.Tablet)
		if !ignoredTablets.Has(alias) {
			mustWaitFor := tabletAliasToWaitFor == alias
			// If this is a tablet that we must wait for
			// we increment numErrorsToWaitFor and pass in this to the
			// fillStatus function to indicate we must send this with the boolean
			// MustWaitFor specified.
			if mustWaitFor {
				numErrorsToWaitFor++
			}
			numGoRoutines++
			go fillStatus(alias, tabletInfo, mustWaitFor)
		}
	}

	if numGoRoutines == 0 && len(tabletMap) > 0 {
		return res, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no tablets available to stop replication on (%d tablets in map, %d ignored)", len(tabletMap), ignoredTablets.Len())
	}
	// In general we want to wait for n-1 tablets to respond, since we know the primary tablet is down.
	requiredSuccesses := numGoRoutines - 1
	if waitForAllTablets {
		// In the special case, where we are explicitly told to wait for all the tablets to return,
		// we set the required success to all the go-routines.
		requiredSuccesses = numGoRoutines
	}

	errgroup := concurrency.ErrorGroup{
		NumGoroutines:        numGoRoutines,
		NumRequiredSuccesses: requiredSuccesses,
		NumAllowedErrors:     len(tabletMap), // We set the number of allowed errors to a very high value, because we don't want to exit early
		// even in case of multiple failures. We rely on the revoke function below to determine if we have more failures than we can tolerate
		NumErrorsToWaitFor: numErrorsToWaitFor,
	}
	errRecorder := errgroup.Wait(groupCancel, errChan)

	// Exit early if we encountered no errors.
	if len(errRecorder.Errors) == 0 {
		return res, nil
	}

	// If there are recorded errors, confirm there is a single error from the PRIMARY.
	// We intentionally do not check for specific error types here because the nature
	// of ERS means we expect any number of possible errors from the PRIMARY we are
	// abandoning (e.g. connection refused, context deadline, MySQL down, etc.) and
	// we don't need to handle them differently — the goal is simply to confirm the
	// error came from the PRIMARY tablet, not to diagnose why it failed.
	if primaryAlias != nil && len(errRecorder.Errors) == 1 {
		var tabletErr *tabletAliasError
		if errors.As(errRecorder.Errors[0], &tabletErr) {
			// Failure to reach the PRIMARY tablet is expected, return early.
			if topoproto.TabletAliasEqual(primaryAlias, tabletErr.GetAlias()) {
				return res, nil
			}
		}
	}

	// check that the tablets we were able to reach are sufficient for us to guarantee that no new write will be accepted by any tablet
	revokeSuccessful := haveRevoked(durability, res.reachableTablets, allTablets)
	if !revokeSuccessful {
		return res, vterrors.Wrapf(errRecorder.Error(), "could not reach sufficient tablets to guarantee safety")
	}

	return res, nil
}

// WaitForRelayLogsToApply blocks execution waiting for the given tablet's relay
// logs to apply, unless the specified context is canceled or exceeded.
// Typically a caller will set a timeout of WaitReplicasTimeout on a context and
// use that context with this function.
func WaitForRelayLogsToApply(ctx context.Context, tmc tmclient.TabletManagerClient, tabletInfo *topo.TabletInfo, status *replicationdatapb.StopReplicationStatus) error {
	return tmc.WaitForPosition(ctx, tabletInfo.Tablet, appliedPositionAfterWait(status))
}

// appliedPositionAfterWait returns the position that WaitForRelayLogsToApply
// waits for — the position the SQL thread is known to have reached once that
// wait succeeds. It is the relay-log position for GTID-based replication, or the
// relay-log-equivalent file position when relay-log positions are unavailable
// (non-GTID replication).
func appliedPositionAfterWait(status *replicationdatapb.StopReplicationStatus) string {
	if status.After.RelayLogPosition == "" {
		return status.After.RelayLogSourceBinlogEquivalentPosition
	}
	return status.After.RelayLogPosition
}
