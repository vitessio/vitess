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
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// storedPrimaryPosition returns the last gtid_executed VTOrc stored for the
// primary as a position, or a zero position when VTOrc has no record of it.
func storedPrimaryPosition(alias *topodatapb.TabletAlias) (replication.Position, error) {
	// Read by alias. ReadInstance joins on hostname and port, which a graceful
	// vttablet shutdown clears.
	executedGtidSet, found, err := inst.ReadExecutedGtidSet(alias)
	if err != nil {
		return replication.Position{}, vterrors.Wrapf(err, "cannot read the stored GTID set of %s", topoproto.TabletAliasString(alias))
	}

	if !found || executedGtidSet == "" {
		return replication.Position{}, nil
	}

	position, err := replication.ParsePosition(replication.Mysql56FlavorID, executedGtidSet)
	if err != nil {
		return replication.Position{}, vterrors.Wrapf(err, "cannot parse the stored GTID set of %s", topoproto.TabletAliasString(alias))
	}

	return position, nil
}

// requiredPositionForRecovery returns the position ERS must require when it
// recovers tablet, or a zero position when the recovery needs none. It audits
// the decision through logger. A read failure is an error. The operator asked
// for the requirement, and an unguarded ERS is worse than no ERS.
func requiredPositionForRecovery(tablet *topodatapb.Tablet, logger logutil.Logger) (replication.Position, error) {
	if !config.EmergencyReparentRequirePrimaryPosition() {
		return replication.Position{}, nil
	}

	// A recovery analyzed on a replica, such as PrimaryTabletDeleted, has no
	// stored primary to read.
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		logger.Infof("required position: none, the analyzed tablet is not the primary")
		return replication.Position{}, nil
	}

	durability, err := inst.GetDurabilityPolicy(tablet.Keyspace)
	if err != nil {
		logger.Errorf("required position: cannot read the durability policy, aborting ERS: %v", err)
		return replication.Position{}, vterrors.Wrapf(err, "cannot read the durability policy of keyspace %s", tablet.Keyspace)
	}

	// Without semi-sync the primary can hold transactions no replica received.
	if !policy.HasSemiSync(durability) {
		logger.Infof("required position: none, the durability policy has no semi-sync")
		return replication.Position{}, nil
	}

	position, err := storedPrimaryPosition(tablet.Alias)
	if err != nil {
		logger.Errorf("required position: cannot read it, aborting ERS: %v", err)
		return replication.Position{}, err
	}

	if position.IsZero() {
		logger.Infof("required position: none, VTOrc has no stored GTID set for the primary")
		return replication.Position{}, nil
	}

	logger.Infof("required position: %s", replication.EncodePosition(position))
	return position, nil
}
