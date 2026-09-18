/*
Copyright 2026 The Vitess Authors.

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

package logic

import (
	"fmt"

	"vitess.io/vitess/go/mysql/replication"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// primaryPositionSnapshot holds stored data and read failures for the requirement gates.
type primaryPositionSnapshot struct {
	// durability is the stored keyspace policy.
	durability policy.Durabler
	// executedGtidSet is the GTID set from the last stored poll.
	executedGtidSet string
	// instanceFound reports whether the analyzed alias has a stored poll.
	instanceFound bool
	// policyReason explains a durability policy read failure.
	policyReason string
	// instanceReason explains a stored instance read failure.
	instanceReason string
}

// requiredPrimaryPosition computes the ERS requirement and its audit explanation without database access.
func requiredPrimaryPosition(tabletType topodatapb.TabletType, snapshot primaryPositionSnapshot) (replication.Position, string) {
	// Skip replicas, including the surviving replica analyzed after a primary tablet is deleted.
	if tabletType != topodatapb.TabletType_PRIMARY {
		return replication.Position{}, "analyzed tablet is not a primary"
	}
	if snapshot.policyReason != "" {
		return replication.Position{}, snapshot.policyReason
	}
	if !policy.HasSemiSync(snapshot.durability) {
		return replication.Position{}, "durability policy has no semi-sync"
	}
	if snapshot.instanceReason != "" {
		return replication.Position{}, snapshot.instanceReason
	}

	if !snapshot.instanceFound {
		return replication.Position{}, "no stored primary instance"
	}
	if snapshot.executedGtidSet == "" {
		return replication.Position{}, "stored primary GTID set is empty"
	}

	position, err := replication.ParsePosition(replication.Mysql56FlavorID, snapshot.executedGtidSet)
	if err != nil {
		return replication.Position{}, "stored primary GTID set is not a MySQL56 GTID set"
	}

	return position, ""
}

// readRequiredPrimaryPosition loads the last primary poll without polling the failed tablet.
// A durability policy read failure skips this opt-in requirement with a reason.
// Recovery continues with unguarded ERS instead of failing.
func readRequiredPrimaryPosition(tablet *topodatapb.Tablet) (replication.Position, string) {
	var snapshot primaryPositionSnapshot

	durability, err := inst.GetDurabilityPolicy(tablet.Keyspace)
	snapshot.durability = durability
	if err != nil {
		snapshot.policyReason = fmt.Sprintf("cannot read durability policy: %v", err)
	}

	executedGtidSet, found, err := inst.ReadExecutedGtidSet(tablet.Alias)
	snapshot.executedGtidSet = executedGtidSet
	snapshot.instanceFound = found
	if err != nil {
		snapshot.instanceReason = fmt.Sprintf("cannot read stored primary instance: %v", err)
	}

	return requiredPrimaryPosition(tablet.Type, snapshot)
}
