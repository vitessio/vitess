/*
Copyright 2022 The Vitess Authors.

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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// SemiSyncAckersForPrimary returns the list of tablets which are capable of sending Semi-Sync Acks for the given primary tablet
func SemiSyncAckersForPrimary(durability Durabler, primary *topodatapb.Tablet, allTablets []*topodatapb.Tablet) (semiSyncAckers []*topodatapb.Tablet) {
	for _, tablet := range allTablets {
		if topoproto.TabletAliasEqual(primary.Alias, tablet.Alias) {
			continue
		}
		if IsReplicaSemiSync(durability, primary, tablet) {
			semiSyncAckers = append(semiSyncAckers, tablet)
		}
	}
	return
}

// haveRevokedForTablet checks whether we have reached enough tablets such that the given primary eligible tablet cannot accept any new writes
// The tablets reached should have their replication stopped and must be set to read only.
func haveRevokedForTablet(durability Durabler, primaryEligible *topodatapb.Tablet, tabletsReached []*topodatapb.Tablet, allTablets []*topodatapb.Tablet) bool {
	// if we have reached the primaryEligible tablet and stopped its replication and marked it read only, then it will not
	// accept any new writes
	if topoproto.IsTabletInList(primaryEligible, tabletsReached) {
		return true
	}

	// semiSyncAckersReached is the list of reachable tablets capable of sending semi sync Acks for the given primaryEligible tablet
	semiSyncAckersReached := SemiSyncAckersForPrimary(durability, primaryEligible, tabletsReached)

	// allSemiSyncAckers is the list of reachable tablets capable of sending semi sync Acks for the given primaryEligible tablet
	allSemiSyncAckers := SemiSyncAckersForPrimary(durability, primaryEligible, allTablets)

	// numOfSemiSyncAcksRequired is the number of semi sync Acks that the primaryEligible tablet requires
	numOfSemiSyncAcksRequired := SemiSyncAckers(durability, primaryEligible)

	// if we have reached enough semi-sync Acking tablets such that the primaryEligible cannot accept a write
	// we have revoked from the tablet
	return len(allSemiSyncAckers)-len(semiSyncAckersReached) < numOfSemiSyncAcksRequired
}

// haveRevoked checks whether we have reached enough tablets to guarantee that no tablet eligible to become a primary can accept any write
// All the tablets reached must have their replication stopped and set to read only for us to guarantee that we have revoked access
// from all the primary eligible tablets (prevent them from accepting any new writes)
func haveRevoked(durability Durabler, tabletsReached []*topodatapb.Tablet, allTablets []*topodatapb.Tablet) bool {
	for _, tablet := range allTablets {
		if PromotionRule(durability, tablet) == promotionrule.MustNot {
			continue
		}
		if !haveRevokedForTablet(durability, tablet, tabletsReached, allTablets) {
			return false
		}
	}
	return true
}

// canEstablishForTablet checks whether we have reached enough tablets to say that the given primary eligible tablet will be able to accept new writes
func canEstablishForTablet(durability Durabler, primaryEligible *topodatapb.Tablet, tabletsReached []*topodatapb.Tablet) bool {
	// if we have not reached the primaryEligible tablet, then it cannot be considered eligible to accept writes
	// since it might have been stopped
	if !topoproto.IsTabletInList(primaryEligible, tabletsReached) {
		return false
	}

	// semiSyncAckersReached is the list of reachable tablets capable of sending semi sync Acks for the given primaryEligible tablet
	semiSyncAckersReached := SemiSyncAckersForPrimary(durability, primaryEligible, tabletsReached)

	// numOfSemiSyncAcksRequired is the number of semi sync Acks that the primaryEligible tablet requires
	numOfSemiSyncAcksRequired := SemiSyncAckers(durability, primaryEligible)

	// if we have reached enough semi-sync Acking tablets such that the primaryEligible can accept a write
	// we can safely promote this tablet
	return len(semiSyncAckersReached) >= numOfSemiSyncAcksRequired
}
