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
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
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

// FindPositionsOfAllCandidates will find candidates for an emergency
// reparent, and, if successful, return a mapping of those tablet aliases (as
// raw strings) to their replication positions for later comparison.
func FindPositionsOfAllCandidates(
	statusMap map[string]*replicationdatapb.StopReplicationStatus,
	primaryStatusMap map[string]*replicationdatapb.PrimaryStatus,
) (map[string]*RelayLogPositions, bool, error) {
	replicationStatusMap := make(map[string]*replication.ReplicationStatus, len(statusMap))
	positionMap := make(map[string]*RelayLogPositions)

	// Build out replication status list from proto types.
	for alias, statuspb := range statusMap {
		status := replication.ProtoToReplicationStatus(statuspb.After)
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
		if _, ok := status.RelayLogPosition.GTIDSet.(replication.Mysql56GTIDSet); ok {
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

	for alias, primaryStatus := range primaryStatusMap {
		executedPosition, err := replication.DecodePosition(primaryStatus.Position)
		if err != nil {
			return nil, false, vterrors.Wrapf(err, "could not decode a primary status executed position for tablet %v", alias)
		}

		positionMap[alias] = &RelayLogPositions{Combined: executedPosition}
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
// tablet to the desired source for its keyspace and tablet type. It also
// figures out if the tablet should be sending semi-sync ACKs or not and passes
// that to the tabletmanager RPC.
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

	replicationSourceConfig, err := ts.GetKeyspaceReplicationSourceConfig(ctx, tablet.Keyspace)
	if err != nil {
		return err
	}

	var tabletMap map[string]*topo.TabletInfo
	if tablet.Type == topodatapb.TabletType_RDONLY && replicationSourceConfig.GetRdonlyPolicy() == topodatapb.ReplicationSourceConfig_REPLICA {
		tabletMap, err = ts.GetTabletMapForShard(ctx, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return err
		}
	}

	source, err := DesiredReplicationSource(shardPrimary.Tablet, tablet, tabletMap, durability, replicationSourceConfig)
	if err != nil {
		return err
	}

	isSemiSync := policy.IsReplicaSemiSync(durability, source, tablet)
	return tmc.SetReplicationSource(ctx, tablet, source.Alias, 0, "", false, isSemiSync, 0)
}

func DesiredReplicationSource(
	primary *topodatapb.Tablet,
	tablet *topodatapb.Tablet,
	tabletMap map[string]*topo.TabletInfo,
	durability policy.Durabler,
	replicationSourceConfig *topodatapb.ReplicationSourceConfig,
) (*topodatapb.Tablet, error) {
	if primary == nil || primary.Alias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "primary tablet must have an alias")
	}
	if tablet == nil || tablet.Alias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet must have an alias")
	}

	rdonlyPolicy := replicationSourceConfig.GetRdonlyPolicy()
	if tablet.Type != topodatapb.TabletType_RDONLY || rdonlyPolicy == topodatapb.ReplicationSourceConfig_UNSPECIFIED {
		return primary, nil
	}

	switch rdonlyPolicy {
	case topodatapb.ReplicationSourceConfig_REPLICA:
		return desiredRdonlyReplicaReplicationSource(primary, tablet, tabletMap)
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unsupported rdonly replication source policy %v", rdonlyPolicy)
	}
}

func detachRdonlyReplicatingFromPromotionCandidate(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	tabletMap map[string]*topo.TabletInfo,
	promotionCandidate *topodatapb.Tablet,
) error {
	if promotionCandidate == nil || promotionCandidate.Alias == nil {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "promotion candidate tablet must have an alias")
	}

	aliases := make([]string, 0, len(tabletMap))
	for alias := range tabletMap {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)

	for _, alias := range aliases {
		ti := tabletMap[alias]
		if ti == nil || ti.Tablet == nil || ti.Alias == nil || ti.Type != topodatapb.TabletType_RDONLY {
			continue
		}

		tablet := ti.Tablet
		status, err := tmc.ReplicationStatus(ctx, tablet)
		if err != nil {
			return vterrors.Wrapf(err, "failed to read rdonly replication status on %s", alias)
		}

		replStatus := replication.ProtoToReplicationStatus(status)
		replicatingFromCandidate, err := replicationStatusSourceMatchesTablet(status, promotionCandidate, replStatus.IOHealthy())
		if err != nil {
			return vterrors.Wrapf(err, "failed to inspect rdonly replication source on %s", alias)
		}
		if !replicatingFromCandidate {
			continue
		}
		if err := tmc.StopReplication(ctx, tablet); err != nil {
			return vterrors.Wrapf(err, "failed to stop rdonly replication on %s", alias)
		}
	}

	return nil
}

func replicationStatusSourceMatchesTablet(status *replicationdatapb.Status, tablet *topodatapb.Tablet, ioThreadWasRunning bool) (bool, error) {
	if status == nil {
		return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "replication status is nil")
	}
	if tablet == nil {
		return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet is nil")
	}

	if status.SourceHost == "" && status.SourcePort == 0 && !ioThreadWasRunning {
		return false, nil
	}
	if status.SourceHost == "" || status.SourcePort == 0 {
		return false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "replication source is incomplete: host %q port %d", status.SourceHost, status.SourcePort)
	}
	if status.SourcePort != tablet.MysqlPort {
		return false, nil
	}

	if tablet.MysqlHostname != "" && status.SourceHost == tablet.MysqlHostname {
		return true, nil
	}
	if tablet.Hostname != "" && status.SourceHost == tablet.Hostname {
		return true, nil
	}
	return false, nil
}

func desiredRdonlyReplicaReplicationSource(primary *topodatapb.Tablet, tablet *topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo) (*topodatapb.Tablet, error) {
	type sourceCandidate struct {
		alias  string
		tablet *topodatapb.Tablet
	}

	sortSourceCandidates := func(candidates []sourceCandidate) {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].alias < candidates[j].alias
		})
	}

	sameCellCandidates := make([]sourceCandidate, 0, len(tabletMap))
	otherCellCandidates := make([]sourceCandidate, 0, len(tabletMap))

	for _, ti := range tabletMap {
		if ti == nil || ti.Tablet == nil || ti.Alias == nil {
			continue
		}

		candidate := ti.Tablet
		if !isReplicaRdonlyReplicationSource(primary, tablet, candidate) {
			continue
		}

		candidateEntry := sourceCandidate{
			alias:  topoproto.TabletAliasString(candidate.Alias),
			tablet: candidate,
		}
		if candidate.Alias.Cell == tablet.Alias.Cell {
			sameCellCandidates = append(sameCellCandidates, candidateEntry)
			continue
		}
		otherCellCandidates = append(otherCellCandidates, candidateEntry)
	}

	sortSourceCandidates(sameCellCandidates)
	if len(sameCellCandidates) > 0 {
		return sameCellCandidates[0].tablet, nil
	}

	sortSourceCandidates(otherCellCandidates)
	if len(otherCellCandidates) > 0 {
		return otherCellCandidates[0].tablet, nil
	}

	return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no replica available as replication source for rdonly tablet %s", topoproto.TabletAliasString(tablet.Alias))
}

func isReplicaRdonlyReplicationSource(primary *topodatapb.Tablet, tablet *topodatapb.Tablet, candidate *topodatapb.Tablet) bool {
	if topoproto.TabletAliasEqual(candidate.Alias, primary.Alias) || topoproto.TabletAliasEqual(candidate.Alias, tablet.Alias) {
		return false
	}
	if candidate.Keyspace != tablet.Keyspace || candidate.Shard != tablet.Shard {
		return false
	}
	return candidate.Type == topodatapb.TabletType_REPLICA
}

// replicationSnapshot stores the status maps and the tablets that were reachable
// when trying to stopReplicationAndBuildStatusMaps.
type replicationSnapshot struct {
	statusMap          map[string]*replicationdatapb.StopReplicationStatus
	primaryStatusMap   map[string]*replicationdatapb.PrimaryStatus
	reachableTablets   []*topodatapb.Tablet
	tabletsBackupState map[string]bool
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
	switch status.After.RelayLogPosition {
	case "":
		return tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.RelayLogSourceBinlogEquivalentPosition)
	default:
		return tmc.WaitForPosition(ctx, tabletInfo.Tablet, status.After.RelayLogPosition)
	}
}
