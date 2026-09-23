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

// storedPrimaryPosition returns the gtid_executed that VTOrc last stored for
// the primary at alias, or a zero position when VTOrc has none.
func storedPrimaryPosition(alias *topodatapb.TabletAlias) (replication.Position, error) {
	executedGtidSet, err := inst.ReadExecutedGtidSet(alias)
	if err != nil {
		return replication.Position{}, vterrors.Wrapf(err, "cannot read the stored GTID set of %s", topoproto.TabletAliasString(alias))
	}

	if executedGtidSet == "" {
		return replication.Position{}, nil
	}

	position, err := replication.ParsePosition(replication.Mysql56FlavorID, executedGtidSet)
	if err != nil {
		return replication.Position{}, vterrors.Wrapf(err, "cannot parse the stored GTID set of %s", topoproto.TabletAliasString(alias))
	}

	return position, nil
}

// requiredPositionForRecovery returns the position that ERS must require to
// recover tablet, or a zero position when the recovery needs none. The
// requirement keeps ERS from promoting a replica that is missing transactions
// VTOrc saw on the failed primary.
func requiredPositionForRecovery(tablet *topodatapb.Tablet, logger logutil.Logger) (replication.Position, error) {
	if !config.EmergencyReparentRequirePrimaryPosition() {
		return replication.Position{}, nil
	}

	if tablet.Type != topodatapb.TabletType_PRIMARY {
		logger.Infof("required position: none, the analyzed tablet is not the primary")
		return replication.Position{}, nil
	}

	durability, err := inst.GetDurabilityPolicy(tablet.Keyspace)
	if err != nil {
		logger.Errorf("required position: cannot read the durability policy, aborting ERS: %v", err)
		return replication.Position{}, vterrors.Wrapf(err, "cannot read the durability policy of keyspace %s", tablet.Keyspace)
	}

	if !policy.HasSemiSync(durability) {
		logger.Infof("required position: none, the durability policy has no semi-sync")
		return replication.Position{}, nil
	}

	position, err := storedPrimaryPosition(tablet.Alias)
	if err != nil {
		logger.Errorf("required position: cannot read it, aborting ERS: %v", err)
		return replication.Position{}, err
	}

	// Run ERS without the requirement when VTOrc has no stored set. A VTOrc that
	// restarted after the primary failed can never poll it.
	if position.IsZero() {
		logger.Warningf("required position: none, VTOrc has no stored GTID set for the primary, ERS runs without the requirement")
		return replication.Position{}, nil
	}

	logger.Infof("required position: %s", replication.EncodePosition(position))
	return position, nil
}
