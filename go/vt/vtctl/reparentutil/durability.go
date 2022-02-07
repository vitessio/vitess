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
)

// SemiSyncACKersForPrimary returns the list of tablets which are capable of sending Semi-Sync ACKs for the given primary tablet
func SemiSyncACKersForPrimary(primary *topodatapb.Tablet, allTablets []*topodatapb.Tablet) (semiSyncACKers []*topodatapb.Tablet) {
	for _, tablet := range allTablets {
		if topoproto.TabletAliasEqual(primary.Alias, tablet.Alias) {
			continue
		}
		if IsReplicaSemiSync(primary, tablet) {
			semiSyncACKers = append(semiSyncACKers, tablet)
		}
	}
	return
}

// RevokeForTablet checks whether we have reached enough tablets such that the given primary capable tablet cannot accept any new transaction
func RevokeForTablet(primaryCapable *topodatapb.Tablet, tabletsReached []*topodatapb.Tablet, allTablets []*topodatapb.Tablet) bool {
	// if we have reached the primaryCapable tablet and stopped its replication, then it will not
	// accept any new transactions
	if topoproto.TabletInList(primaryCapable, tabletsReached) {
		return true
	}

	// semiSyncACKersReached is the list of reachable tablets capable of sending semi sync ACKs for the given primaryCapable tablet
	semiSyncACKersReached := SemiSyncACKersForPrimary(primaryCapable, tabletsReached)

	// allSemiSyncACKers is the list of reachable tablets capable of sending semi sync ACKs for the given primaryCapable tablet
	allSemiSyncACKers := SemiSyncACKersForPrimary(primaryCapable, allTablets)

	// numOfSemiSyncACKsRequired is the number of semi sync ACKs that the primaryCapable tablet requires
	numOfSemiSyncACKsRequired := SemiSyncAckers(primaryCapable)

	// if we have reached enough semi-sync ACKing tablets such that the primaryCapable cannot accept a transaction
	// we have revoked from the tablet
	return len(allSemiSyncACKers)-len(semiSyncACKersReached) < numOfSemiSyncACKsRequired
}
