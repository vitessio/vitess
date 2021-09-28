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

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// ChooseNewPrimary finds a tablet that should become a primary after reparent.
// The criteria for the new primary-elect are (preferably) to be in the same
// cell as the current primary, and to be different from avoidPrimaryAlias. The
// tablet with the most advanced replication position is chosen to minimize the
// amount of time spent catching up with the current primary.
//
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
	// (TODO:@ajm188) it's a little gross we need to pass this, maybe embed in the context?
	logger logutil.Logger,
) (*topodatapb.TabletAlias, error) {
	if avoidPrimaryAlias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet to avoid for reparent is not provided, cannot choose new primary")
	}

	var primaryCell string
	if shardInfo.PrimaryAlias != nil {
		primaryCell = shardInfo.PrimaryAlias.Cell
	}

	var (
		searcher = topotools.NewMaxReplicationPositionSearcher(tmc, logger, waitReplicasTimeout)
		wg       sync.WaitGroup
	)

	for _, tablet := range tabletMap {
		switch {
		case primaryCell != "" && tablet.Alias.Cell != primaryCell:
			continue
		case topoproto.TabletAliasEqual(tablet.Alias, avoidPrimaryAlias):
			continue
		case tablet.Tablet.Type != topodatapb.TabletType_REPLICA:
			continue
		}

		wg.Add(1)

		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			searcher.ProcessTablet(ctx, tablet)
		}(tablet.Tablet)
	}

	wg.Wait()

	if maxPosTablet := searcher.MaxPositionTablet(); maxPosTablet != nil {
		return maxPosTablet.Alias, nil
	}

	return nil, nil
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

// restrictValidCandidates is used to restrict some candidates from being considered eligible for becoming the intermediate primary
func restrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)
	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		// We do not allow BACKUP, DRAINED or RESTORE type of tablets to be considered for being the replication source or the candidate for primary
		if candidateInfo.Type == topodatapb.TabletType_BACKUP || candidateInfo.Type == topodatapb.TabletType_RESTORE || candidateInfo.Type == topodatapb.TabletType_DRAINED {
			continue
		}
		restrictedValidCandidates[candidate] = position
	}
	return restrictedValidCandidates, nil
}

func findPossibleCandidateFromListWithRestrictions(newPrimary, prevPrimary *topodatapb.Tablet, possibleCandidates []*topodatapb.Tablet, checkEqualPrimary bool, checkSameCell bool) *topodatapb.Tablet {
	for _, candidate := range possibleCandidates {
		if checkEqualPrimary && !(topoproto.TabletAliasEqual(newPrimary.Alias, candidate.Alias)) {
			continue
		}
		if checkSameCell && prevPrimary != nil && !(prevPrimary.Alias.Cell == candidate.Alias.Cell) {
			continue
		}
		return candidate
	}
	return nil
}

// waitForCatchUp promotes the newer candidate over the primary candidate that we have, but it does not set to start accepting writes
func waitForCatchUp(ctx context.Context, tmc tmclient.TabletManagerClient, logger logutil.Logger, prevPrimary, newPrimary *topodatapb.Tablet, waitTime time.Duration) error {
	logger.Infof("waiting for %v to catch up to %v", newPrimary.Alias, prevPrimary.Alias)
	// Find the primary position of the previous primary
	pos, err := tmc.MasterPosition(ctx, prevPrimary)
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
