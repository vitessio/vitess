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
// position only with the flag on, for a recovery of the primary, under a
// semi-sync durability policy.
func TestRequiredPositionForRecovery(t *testing.T) {
	tests := []struct {
		name       string
		flag       bool
		durability string
		tabletType topodatapb.TabletType
		position   string
	}{
		{name: "flag off", flag: false, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY},
		{name: "primary with semi-sync", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_PRIMARY, position: "MySQL56/" + requiredGtid},
		{name: "no semi-sync", flag: true, durability: policy.DurabilityNone, tabletType: topodatapb.TabletType_PRIMARY},
		{name: "analyzed tablet is a replica", flag: true, durability: policy.DurabilitySemiSync, tabletType: topodatapb.TabletType_REPLICA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.SetEmergencyReparentRequirePrimaryPosition(tt.flag)
			t.Cleanup(func() { config.SetEmergencyReparentRequirePrimaryPosition(false) })
			tablet := saveRequiredPositionFixture(t, tt.durability, tt.tabletType, requiredGtid)

			position, err := requiredPositionForRecovery(tablet, logutil.NewMemoryLogger())
			require.NoError(t, err)
			assert.Equal(t, tt.position, replication.EncodePosition(position))
		})
	}
}
