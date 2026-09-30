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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

const requiredGtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"

// saveRequiredPositionFixture stores a keyspace, a tablet record and a poll
// record for the primary.
func saveRequiredPositionFixture(t *testing.T, durability string, tabletType topodatapb.TabletType, executedGtidSet string) *topodatapb.Tablet {
	t.Helper()

	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)

	keyspace := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: durability}}
	keyspace.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspace))

	tablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Type:          tabletType,
		Keyspace:      "ks",
		Shard:         "0",
		MysqlHostname: "analyzed",
		MysqlPort:     3306,
	}
	require.NoError(t, inst.SaveTablet(tablet))
	require.NoError(t, inst.WriteInstance(&inst.Instance{
		InstanceAlias:   tablet.Alias,
		TabletType:      tabletType,
		Hostname:        tablet.MysqlHostname,
		Port:            int(tablet.MysqlPort),
		ExecutedGtidSet: executedGtidSet,
	}, true, nil))

	return tablet
}

// TestStoredPrimaryPosition checks that a primary with no stored transactions
// or no poll record has no position to require, and that the stored set is
// found after a graceful vttablet shutdown clears the MySQL address.
func TestStoredPrimaryPosition(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		tablet := saveRequiredPositionFixture(t, policy.DurabilitySemiSync, topodatapb.TabletType_PRIMARY, "")

		position, err := storedPrimaryPosition(tablet.Alias)
		require.NoError(t, err)
		assert.True(t, position.IsZero())
	})

	t.Run("no poll record", func(t *testing.T) {
		db.ClearVTOrcDatabase()
		t.Cleanup(db.ClearVTOrcDatabase)

		position, err := storedPrimaryPosition(&topodatapb.TabletAlias{Cell: "zone1", Uid: 100})
		require.NoError(t, err)
		assert.True(t, position.IsZero())
	})

	t.Run("cleared MySQL address", func(t *testing.T) {
		tablet := saveRequiredPositionFixture(t, policy.DurabilitySemiSync, topodatapb.TabletType_PRIMARY, requiredGtid)
		tablet.MysqlHostname = ""
		tablet.MysqlPort = 0
		require.NoError(t, inst.SaveTablet(tablet))

		position, err := storedPrimaryPosition(tablet.Alias)
		require.NoError(t, err)
		assert.Equal(t, "MySQL56/"+requiredGtid, replication.EncodePosition(position))
	})
}

// TestRequiredPositionForRecovery checks that VTOrc requires the stored primary
// position only with the flag on, for a recovery of the primary of a MySQL GTID
// shard, under a semi-sync durability policy.
func TestRequiredPositionForRecovery(t *testing.T) {
	tests := []struct {
		name       string
		flag       bool
		durability string
		tabletType topodatapb.TabletType
		storedSet  string
		position   string

		// warn is true when the flag is on and VTOrc has no stored set to require.
		// The flag help promises a warning in the audit for that case.
		warn bool
	}{
		{name: "flag off", flag: false, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, storedSet: requiredGtid},
		{name: "primary with semi-sync", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, storedSet: requiredGtid, position: "MySQL56/" + requiredGtid},
		{name: "no semi-sync", flag: true, durability: policy.DurabilityNone, tabletType: topodatapb.TabletType_PRIMARY, storedSet: requiredGtid},
		{name: "analyzed tablet is a replica", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_REPLICA, storedSet: requiredGtid, warn: true},
		{name: "no stored set", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, storedSet: "", warn: true},
		{name: "MariaDB shard", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, storedSet: "0-1-100", warn: true},
		{name: "file position shard", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, storedSet: "vt-0000000101-bin.000001:4567", warn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.SetEmergencyReparentRequirePrimaryPosition(tt.flag)
			t.Cleanup(func() { config.SetEmergencyReparentRequirePrimaryPosition(false) })
			tablet := saveRequiredPositionFixture(t, tt.durability, tt.tabletType, tt.storedSet)
			logger := logutil.NewMemoryLogger()

			position, err := requiredPositionForRecovery(tablet, logger)
			require.NoError(t, err)
			assert.Equal(t, tt.position, replication.EncodePosition(position))

			var warnings []string
			for _, event := range logger.Events {
				if event.Level == logutilpb.Level_WARNING {
					warnings = append(warnings, event.Value)
				}
			}
			if !tt.warn {
				assert.Empty(t, warnings)
				return
			}
			require.Len(t, warnings, 1)
			assert.Contains(t, warnings[0], "required position: none")
			assert.Contains(t, warnings[0], "ERS runs without the requirement")
		})
	}
}

// TestRequiredPositionAbortCountsAsAttempted checks that an ERS recovery aborts
// when it cannot read its requirement, and that it reports the recovery as
// attempted. The caller counts an attempted recovery with an error as failed.
func TestRequiredPositionAbortCountsAsAttempted(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) *topodatapb.TabletAlias
		wantErr string
	}{
		{
			name: "durability read error",
			setup: func(t *testing.T) *topodatapb.TabletAlias {
				// Save the primary without a keyspace row. The durability read then fails.
				alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
				require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{Alias: alias, Type: topodatapb.TabletType_PRIMARY, Keyspace: "ks", Shard: "0"}))
				return alias
			},
			wantErr: "cannot read the durability policy of keyspace ks",
		},
		{
			name: "stored set read error",
			setup: func(t *testing.T) *topodatapb.TabletAlias {
				tablet := saveRequiredPositionFixture(t, policy.DurabilitySemiSync, topodatapb.TabletType_PRIMARY, requiredGtid)

				// Drop the table of stored sets. The stored set read then fails.
				_, err := db.ExecVTOrc("DROP TABLE database_instance")
				require.NoError(t, err)
				return tablet.Alias
			},
			wantErr: "cannot read the stored GTID set of zone1-0000000100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.ClearVTOrcDatabase()
			t.Cleanup(db.ClearVTOrcDatabase)
			config.SetEmergencyReparentRequirePrimaryPosition(true)
			t.Cleanup(func() { config.SetEmergencyReparentRequirePrimaryPosition(false) })

			alias := tt.setup(t)
			entry := &inst.DetectionAnalysis{AnalyzedInstanceAlias: alias, Analysis: inst.DeadPrimary, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
			require.NoError(t, InsertRecoveryDetection(entry))

			recoveryAttempted, _, err := runEmergencyReparentOp(t.Context(), entry, "RecoverDeadPrimary", false, log.NewPrefixedLogger("test"))
			require.ErrorContains(t, err, tt.wantErr)
			assert.True(t, recoveryAttempted)
		})
	}
}
