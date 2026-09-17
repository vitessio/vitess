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
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

const requiredGtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"

// saveRequiredPositionFixture stores a keyspace, a tablet record and a poll
// record for the primary. An empty durability omits the keyspace row.
func saveRequiredPositionFixture(t *testing.T, durability string, tabletType topodatapb.TabletType, executedGtidSet string) *topodatapb.Tablet {
	t.Helper()

	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)

	if durability != "" {
		keyspace := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: durability}}
		keyspace.SetKeyspaceName("ks")
		require.NoError(t, inst.SaveKeyspace(keyspace))
	}

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
		TabletType:      topodatapb.TabletType_PRIMARY,
		Hostname:        tablet.MysqlHostname,
		Port:            int(tablet.MysqlPort),
		ExecutedGtidSet: executedGtidSet,
	}, true, nil))

	return tablet
}

// TestStoredPrimaryPosition checks the stored GTID set to position conversion.
func TestStoredPrimaryPosition(t *testing.T) {
	tests := []struct {
		name            string
		executedGtidSet string
		position        string
		wantErr         string
	}{
		{name: "MySQL56 set", executedGtidSet: requiredGtid, position: "MySQL56/" + requiredGtid},
		{name: "empty set", executedGtidSet: ""},
		{name: "invalid set", executedGtidSet: "invalid", wantErr: "cannot parse the stored GTID set of zone1-0000000100"},
		{name: "FilePos set", executedGtidSet: "mysql-bin.000001:123", wantErr: "cannot parse the stored GTID set of zone1-0000000100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet := saveRequiredPositionFixture(t, policy.DurabilitySemiSync, topodatapb.TabletType_PRIMARY, tt.executedGtidSet)

			position, err := storedPrimaryPosition(tablet.Alias)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.position, replication.EncodePosition(position))
		})
	}

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

	t.Run("read failure", func(t *testing.T) {
		tablet := saveRequiredPositionFixture(t, policy.DurabilitySemiSync, topodatapb.TabletType_PRIMARY, requiredGtid)

		// Drop the table the read targets. ClearVTOrcDatabase recreates it in cleanup.
		_, err := db.ExecVTOrc("DROP TABLE database_instance")
		require.NoError(t, err)

		_, err = storedPrimaryPosition(tablet.Alias)
		require.ErrorContains(t, err, "cannot read the stored GTID set of zone1-0000000100")
	})
}

// TestRequiredPositionForRecovery checks the flag, the tablet type and the
// durability gates, and that read failures fail closed.
func TestRequiredPositionForRecovery(t *testing.T) {
	tests := []struct {
		name       string
		flag       bool
		durability string
		tabletType topodatapb.TabletType
		position   string
		wantErr    string
	}{
		{name: "flag off", flag: false, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY},
		{name: "primary with semi-sync", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, position: "MySQL56/" + requiredGtid},
		{name: "primary with cross cell", flag: true, durability: policy.DurabilityCrossCell, tabletType: topodatapb.TabletType_PRIMARY, position: "MySQL56/" + requiredGtid},
		{name: "no semi-sync", flag: true, durability: policy.DurabilityNone, tabletType: topodatapb.TabletType_PRIMARY},
		{name: "analyzed tablet is a replica", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_REPLICA},
		{name: "replica skips before the policy read", flag: true, durability: "", tabletType: topodatapb.TabletType_REPLICA},
		{name: "missing keyspace row fails closed", flag: true, durability: "", tabletType: topodatapb.TabletType_PRIMARY, wantErr: "cannot read the durability policy of keyspace ks"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.SetEmergencyReparentRequirePrimaryPosition(tt.flag)
			t.Cleanup(func() { config.SetEmergencyReparentRequirePrimaryPosition(false) })
			tablet := saveRequiredPositionFixture(t, tt.durability, tt.tabletType, requiredGtid)

			position, err := requiredPositionForRecovery(tablet, logutil.NewMemoryLogger())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.position, replication.EncodePosition(position))
		})
	}
}
