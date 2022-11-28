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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// ChooseNewPrimary finds a tablet that should become a primary after reparent.
// The criteria for the new primary-elect are (preferably) to be in the same
// cell as the current primary, and to be different from avoidPrimaryAlias. The
// tablet with the most advanced replication position is chosen to minimize the
// amount of time spent catching up with the current primary. Further ties are
// broken by the durability rules.
// Note that the search for the most advanced replication position will race
// with transactions being executed on the current primary, so when all tablets
// are at roughly the same position, then the choice of new primary-elect will
// be somewhat unpredictable.
func ChooseNewPrimary(
	ctx context.Context,
	tmc tmclient.TabletManagerClient,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidPrimaryAlias *topodatapb.TabletAlias,
	waitReplicasTimeout time.Duration,
	durability Durabler,
	// (TODO:@ajm188) it's a little gross we need to pass this, maybe embed in the context?
	logger logutil.Logger,
) (*topodatapb.TabletAlias, error) {

	var primaryCell string
	if shardInfo.PrimaryAlias != nil {
		primaryCell = shardInfo.PrimaryAlias.Cell
	}

	var (
		wg sync.WaitGroup
		// mutex to secure the next two fields from concurrent access
		mu sync.Mutex
		// tablets that are possible candidates to be the new primary and their positions
		validTablets    []*topodatapb.Tablet
		tabletPositions []mysql.Position
	)

	for _, tablet := range tabletMap {
		switch {
		case primaryCell != "" && tablet.Alias.Cell != primaryCell:
			continue
		case avoidPrimaryAlias != nil && topoproto.TabletAliasEqual(tablet.Alias, avoidPrimaryAlias):
			continue
		case tablet.Tablet.Type != topodatapb.TabletType_REPLICA:
			continue
		}

		wg.Add(1)

		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			// find and store the positions for the tablet
			pos, err := findPositionForTablet(ctx, tablet, logger, tmc, waitReplicasTimeout)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				validTablets = append(validTablets, tablet)
				tabletPositions = append(tabletPositions, pos)
			}
		}(tablet.Tablet)
	}

	wg.Wait()

	// return nothing if there are no valid tablets available
	if len(validTablets) == 0 {
		return nil, nil
	}

	// sort the tablets for finding the best primary
	err := sortTabletsForReparent(validTablets, tabletPositions, durability)
	if err != nil {
		return nil, err
	}

	return validTablets[0].Alias, nil
}

// findPositionForTablet processes the replication position for a single tablet and
// returns it. It is safe to call from multiple goroutines.
func findPositionForTablet(ctx context.Context, tablet *topodatapb.Tablet, logger logutil.Logger, tmc tmclient.TabletManagerClient, waitTimeout time.Duration) (mysql.Position, error) {
	logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	ctx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	status, err := tmc.ReplicationStatus(ctx, tablet)
	if err != nil {
		sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
		if isSQLErr && sqlErr != nil && sqlErr.Number() == mysql.ERNotReplica {
			logger.Warningf("no replication statue from %v, using empty gtid set", topoproto.TabletAliasString(tablet.Alias))
			return mysql.Position{}, nil
		}
		logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return mysql.Position{}, err
	}

	// Use the relay log position if available, otherwise use the executed GTID set (binary log position).
	positionString := status.Position
	if status.RelayLogPosition != "" {
		positionString = status.RelayLogPosition
	}
	pos, err := mysql.DecodePosition(positionString)
	if err != nil {
		logger.Warningf("cannot decode replica position %v for tablet %v, ignoring tablet: %v", positionString, topoproto.TabletAliasString(tablet.Alias), err)
		return mysql.Position{}, err
	}

	return pos, nil
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
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)

	log.Infof("Gathering tablet replication status for: %v", tablets)
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
func getValidCandidatesAndPositionsAsList(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) ([]*topodatapb.Tablet, []mysql.Position, error) {
	var validTablets []*topodatapb.Tablet
	var tabletPositions []mysql.Position
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
func restrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)
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

func findCandidate(
	intermediateSource *topodatapb.Tablet,
	possibleCandidates []*topodatapb.Tablet,
) *topodatapb.Tablet {
	// check whether the one we have selected as the source belongs to the candidate list provided
	for _, candidate := range possibleCandidates {
		if topoproto.TabletAliasEqual(intermediateSource.Alias, candidate.Alias) {
			return candidate
		}
	}
	// return the first candidate from this list, if it isn't empty
	if len(possibleCandidates) > 0 {
		return possibleCandidates[0]
	}
	return nil
}

// getTabletsWithPromotionRules gets the tablets with the given promotion rule from the list of tablets
func getTabletsWithPromotionRules(durability Durabler, tablets []*topodatapb.Tablet, rule promotionrule.CandidatePromotionRule) (res []*topodatapb.Tablet) {
	for _, candidate := range tablets {
		promotionRule := PromotionRule(durability, candidate)
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
	err = tmc.WaitForPosition(waitForPosCtx, newPrimary, pos)
	if err != nil {
		return err
	}
	return nil
}
