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
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	reparentShardOpTimings = stats.NewTimings("ReparentShardOperationTimings", "Timings of reparent shard operations", "Operation")
	failureResult          = "failure"
	successResult          = "success"
)

const (
	lostTopologyLockMsg = "lost topology lock, aborting"
)

// ElectNewPrimary finds a tablet that should become a primary after reparent.
// The criteria for the new primary-elect are (preferably) to be in the same
// cell as the current primary, and to be different from avoidPrimaryAlias.
// Candidates are sorted by: promotion rules (operator intent), then the lowest
// MySQL release (major.minor) to maintain replication compatibility (replicas
// must be >= primary version), then replication position, InnoDB buffer pool
// size, and tablet alias. Tablets taking backups are not considered.
// Note that the search for the most advanced replication position will race
// with transactions being executed on the current primary, so when all tablets
// are at roughly the same position, then the choice of new primary-elect will
// be somewhat unpredictable.
func ElectNewPrimary(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	innodbBufferPoolData map[string]int,
	opts *PlannedReparentOptions,
	// (TODO:@ajm188) it's a little gross we need to pass this, maybe embed in the context?
	logger logutil.Logger,
) (*topodatapb.TabletAlias, error) {
	var primaryCell string
	if shardInfo.PrimaryAlias != nil {
		primaryCell = shardInfo.PrimaryAlias.Cell
	}

	var (
		mu                   sync.Mutex
		mysqlVersions        []mysqlctl.ServerVersion
		mysqlFlavors         []mysqlctl.MySQLFlavor
		errorGroup, groupCtx = errgroup.WithContext(ctx)
	)

	// candidates are the list of tablets that can be potentially promoted after filtering out based on preliminary checks.
	candidates := make([]*topodatapb.Tablet, 0, len(tabletMap))
	reasonsToInvalidate := strings.Builder{}
	for _, tablet := range tabletMap {
		switch {
		case opts.NewPrimaryAlias != nil:
			// If newPrimaryAlias is provided, then that is the only valid tablet, even if it is not of type replica or in a different cell.
			if !topoproto.TabletAliasEqual(tablet.Alias, opts.NewPrimaryAlias) {
				fmt.Fprintf(&reasonsToInvalidate, "\n%v does not match the new primary alias provided", topoproto.TabletAliasString(tablet.Alias))
				continue
			}
		case !opts.AllowCrossCellPromotion && primaryCell != "" && tablet.Alias.Cell != primaryCell:
			fmt.Fprintf(&reasonsToInvalidate, "\n%v is not in the same cell as the previous primary", topoproto.TabletAliasString(tablet.Alias))
			continue
		case opts.AvoidPrimaryAlias != nil && topoproto.TabletAliasEqual(tablet.Alias, opts.AvoidPrimaryAlias):
			fmt.Fprintf(&reasonsToInvalidate, "\n%v matches the primary alias to avoid", topoproto.TabletAliasString(tablet.Alias))
			continue
		case tablet.Type != topodatapb.TabletType_REPLICA:
			fmt.Fprintf(&reasonsToInvalidate, "\n%v is not a replica", topoproto.TabletAliasString(tablet.Alias))
			continue
		}

		candidates = append(candidates, tablet.Tablet)
	}

	// There is only one tablet and tolerable replication lag is unspecified,
	// then we don't need to find the position of the said tablet for sorting.
	// We can just return the tablet quickly.
	// This check isn't required, but it saves us an RPC call that is otherwise unnecessary.
	if len(candidates) == 1 && opts.TolerableReplLag == 0 {
		return candidates[0].Alias, nil
	}

	validTablets := make([]*topodatapb.Tablet, 0, len(candidates))
	tabletPositions := make([]*RelayLogPositions, 0, len(candidates))

	for _, tablet := range candidates {
		tb := tablet
		errorGroup.Go(func() error {
			// find and store the positions for the tablet
			pos, replLag, takingBackup, replUnknown, serverVersion, err := findTabletPositionLagBackupStatus(groupCtx, tb, logger, tmc, opts.WaitReplicasTimeout)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && (opts.TolerableReplLag == 0 || opts.TolerableReplLag >= replLag) {
				if takingBackup {
					fmt.Fprintf(&reasonsToInvalidate, "\n%v is taking a backup", topoproto.TabletAliasString(tablet.Alias))
				} else if replUnknown {
					fmt.Fprintf(&reasonsToInvalidate, "\n%v position known but unknown replication status", topoproto.TabletAliasString(tablet.Alias))
				} else {
					validTablets = append(validTablets, tb)
					tabletPositions = append(tabletPositions, pos)

					v := unknownVersion
					f := mysqlctl.FlavorUnknown
					if serverVersion != "" {
						flavor, parsed, parseErr := mysqlctl.ParseVersionString(serverVersion)
						if parseErr == nil {
							v = parsed
							f = flavor
						} else {
							logger.Warningf("failed to parse MySQL version %q for tablet %v: %v", serverVersion, topoproto.TabletAliasString(tb.Alias), parseErr)
						}
					} else {
						logger.Warningf("could not determine MySQL version for tablet %v; it will not be preferred by version-aware election", topoproto.TabletAliasString(tb.Alias))
					}
					mysqlVersions = append(mysqlVersions, v)
					mysqlFlavors = append(mysqlFlavors, f)
				}
			} else {
				fmt.Fprintf(&reasonsToInvalidate, "\n%v has %v replication lag which is more than the tolerable amount", topoproto.TabletAliasString(tablet.Alias), replLag)
			}
			return err
		})
	}

	err := errorGroup.Wait()
	if err != nil {
		return nil, err
	}

	// return an error if there are no valid tablets available
	if len(validTablets) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "cannot find a tablet to reparent to%v", reasonsToInvalidate.String())
	}

	// Use buffer-pool data for tiebreaking only if every valid tablet has it. A missing
	// entry (e.g. MariaDB, which doesn't expose Innodb_buffer_pool_pages_data) appended as
	// a zero would unfairly outrank a legitimately low value. We gate on validTablets
	// rather than candidates so that an ineligible candidate (taking backup, excess lag,
	// etc.) doesn't disable tiebreaking for the rest.
	var innodbBufferPool []int
	if len(innodbBufferPoolData) > 0 {
		innodbBufferPool = make([]int, 0, len(validTablets))
		for _, t := range validTablets {
			v, ok := innodbBufferPoolData[topoproto.TabletAliasString(t.Alias)]
			if !ok {
				innodbBufferPool = nil
				break
			}
			innodbBufferPool = append(innodbBufferPool, v)
		}
	}

	// Disable version-aware ordering when candidates span multiple flavor
	// families (e.g. MariaDB alongside MySQL/Percona), where version comparison
	// is meaningless. Falls through to the position/promotion ordering.
	if usableMySQLVersions(mysqlVersions, mysqlFlavors) == nil {
		logger.Warningf("reparent candidates span multiple MySQL flavor families; skipping version-aware election")
		mysqlVersions = nil
	}

	// sort preferred tablets for finding the best primary — PRS prefers version over position
	// because it always catches the elected tablet up to the old primary's exact position.
	err = sortTabletsForReparent(validTablets, tabletPositions, innodbBufferPool, mysqlVersions, opts.durability, SortForPRS)
	if err != nil {
		return nil, err
	}

	return validTablets[0].Alias, nil
}

// findTabletPositionLagBackupStatus processes the replication positions and lag for a single tablet and
// returns it. It is safe to call from multiple goroutines.
func findTabletPositionLagBackupStatus(ctx context.Context, tablet *topodatapb.Tablet, logger logutil.Logger, tmc tmclient.TabletManagerClient, waitTimeout time.Duration) (*RelayLogPositions, time.Duration, bool, bool, string, error) {
	rlp := &RelayLogPositions{}

	logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	ctx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	status, err := tmc.ReplicationStatus(ctx, tablet)
	if err != nil {
		sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
		if isSQLErr && sqlErr != nil && sqlErr.Number() == sqlerror.ERNotReplica {
			logger.Warningf("no replication statue from %v, using empty gtid set", topoproto.TabletAliasString(tablet.Alias))
			return rlp, 0, false, false, "", nil
		}
		logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return rlp, 0, false, false, "", err
	}

	rlp.Executed, err = replication.DecodePosition(status.Position)
	if err != nil {
		logger.Warningf("cannot decode replica position %v for tablet %v, ignoring tablet: %v", status.Position, topoproto.TabletAliasString(tablet.Alias), err)
		return rlp, 0, status.BackupRunning, false, "", err
	}

	rlp.Combined, err = replication.DecodePosition(status.RelayLogPosition)
	if err != nil {
		logger.Warningf("cannot decode replica position %v for tablet %v, ignoring tablet: %v", status.RelayLogPosition, topoproto.TabletAliasString(tablet.Alias), err)
		return rlp, 0, status.BackupRunning, false, "", err
	}

	return rlp, time.Second * time.Duration(status.ReplicationLagSeconds), status.BackupRunning, status.ReplicationLagUnknown, status.ServerVersion, nil
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

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func ShardReplicationStatuses(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	tabletMap, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}
	tablets := maps.Values(tabletMap)

	log.Info(fmt.Sprintf("Gathering tablet replication status for: %v", tablets))
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	result := make([]*replicationdatapb.Status, len(tablets))

	for i, ti := range tablets {
		// Don't scan tablets that won't return something
		// useful. Otherwise, you'll end up waiting for a timeout.
		if ti.Type == topodatapb.TabletType_PRIMARY {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				pos, err := tmc.PrimaryPosition(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("PrimaryPosition(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = &replicationdatapb.Status{
					Position: pos,
				}
			}(i, ti)
		} else if ti.IsReplicaType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := tmc.ReplicationStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("ReplicationStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return tablets, result, rec.Error()
}

// getValidCandidatesAndPositionsAsList converts the valid candidates from a map to a list of tablets, making it easier to sort
func getValidCandidatesAndPositionsAsList(validCandidates map[string]*RelayLogPositions, tabletMap map[string]*topo.TabletInfo) ([]*topodatapb.Tablet, []*RelayLogPositions, error) {
	var validTablets []*topodatapb.Tablet
	var tabletPositions []*RelayLogPositions
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

// restrictValidCandidates is used to restrict some candidates from being considered eligible for becoming the intermediate source or the final promotion candidate
func restrictValidCandidates(validCandidates map[string]*RelayLogPositions, tabletMap map[string]*topo.TabletInfo) (map[string]*RelayLogPositions, error) {
	restrictedValidCandidates := make(map[string]*RelayLogPositions)
	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		// We do not allow BACKUP, DRAINED or RESTORE type of tablets to be considered for being the replication source or the candidate for primary
		if topoproto.IsTypeInList(candidateInfo.Type, []topodatapb.TabletType{topodatapb.TabletType_BACKUP, topodatapb.TabletType_RESTORE, topodatapb.TabletType_DRAINED}) {
			continue
		}
		restrictedValidCandidates[candidate] = position
	}
	return restrictedValidCandidates, nil
}

// findCandidate returns the best promotion candidate from possibleCandidates.
// possibleCandidates MUST already be sorted by replication position (most
// advanced first): when there is no version data, or when the lowest-release
// candidate is on the same release as the intermediate source, findCandidate
// falls back to the position-sorted ordering (the first element, or the
// intermediate source) rather than re-checking position.
func findCandidate(
	intermediateSource *topodatapb.Tablet,
	possibleCandidates []*topodatapb.Tablet,
	versionMap map[string]mysqlctl.ServerVersion,
) *topodatapb.Tablet {
	if len(possibleCandidates) == 0 {
		return nil
	}

	if len(versionMap) == 0 {
		// No version data — fall back to preferring the intermediate source to avoid catch-up.
		for _, candidate := range possibleCandidates {
			if topoproto.TabletAliasEqual(intermediateSource.Alias, candidate.Alias) {
				return candidate
			}
		}
		return possibleCandidates[0]
	}

	// Find the candidate with the lowest MySQL version in this tier, comparing by
	// major.minor (and by patch within the pre-8.0.34 MySQL 8.0 series; see
	// ServerVersion.CompareForReplication). Among candidates that compare equal,
	// prefer the intermediate source to avoid catch-up.
	sourceAlias := topoproto.TabletAliasString(intermediateSource.Alias)
	sourceVersion, ok := versionMap[sourceAlias]
	if !ok {
		sourceVersion = unknownVersion
	}

	var best *topodatapb.Tablet
	bestVersion := unknownVersion
	for _, candidate := range possibleCandidates {
		alias := topoproto.TabletAliasString(candidate.Alias)
		v, ok := versionMap[alias]
		if !ok {
			v = unknownVersion
		}

		if best == nil {
			best = candidate
			bestVersion = v
			continue
		}

		// Keep the lower version; CompareForReplication returns < 0 when v is lower.
		if v.CompareForReplication(bestVersion) >= 0 {
			continue
		}
		best = candidate
		bestVersion = v
	}

	// The first loop finds the lowest-version candidate without bias. Now that we know
	// the best version, check if the intermediate source is equivalent for replication —
	// if so, prefer it because it already holds the most-advanced position and won't need catch-up.
	if sourceVersion.CompareForReplication(bestVersion) == 0 {
		for _, candidate := range possibleCandidates {
			if topoproto.TabletAliasEqual(intermediateSource.Alias, candidate.Alias) {
				return candidate
			}
		}
	}

	return best
}

// getTabletsWithPromotionRules gets the tablets with the given promotion rule from the list of tablets
func getTabletsWithPromotionRules(durability policy.Durabler, tablets []*topodatapb.Tablet, rule promotionrule.CandidatePromotionRule) (res []*topodatapb.Tablet) {
	for _, candidate := range tablets {
		promotionRule := policy.PromotionRule(durability, candidate)
		if promotionRule == rule {
			res = append(res, candidate)
		}
	}
	return res
}

// waitForCatchUp is used to wait for the given tablet until it has caught up to the source
func waitForCatchUp(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	logger logutil.Logger,
	newPrimary *topodatapb.Tablet,
	source *topodatapb.Tablet,
	waitTime time.Duration,
) error {
	logger.Infof("waiting for %v to catch up to %v", newPrimary.Alias, source.Alias)
	// Find the primary position of the previous primary
	pos, err := tmc.PrimaryPosition(ctx, source)
	if err != nil {
		return err
	}

	// Wait until the new primary has caught upto that position
	waitForPosCtx, cancelFunc := context.WithTimeout(ctx, waitTime)
	defer cancelFunc()
	return tmc.WaitForPosition(waitForPosCtx, newPrimary, pos)
}

// GetBackupCandidates is used to get a list of healthy tablets for backup
func GetBackupCandidates(tablets []*topo.TabletInfo, stats []*replicationdatapb.Status) (res []*topo.TabletInfo) {
	for i, stat := range stats {
		// shardTablets[i] and stats[i] is 1:1 mapping
		// Always include TabletType_PRIMARY. Healthy shardTablets[i] will be added to tablets
		if tablets[i].Type == topodatapb.TabletType_PRIMARY || stat != nil {
			res = append(res, tablets[i])
		}
	}
	return res
}
