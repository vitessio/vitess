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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// TestReadRequiredPrimaryPosition checks stored reads and the analyzed tablet type.
func TestReadRequiredPrimaryPosition(t *testing.T) {
	const gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	tests := []struct {
		// name identifies the stored read under test.
		name string
		// durability names the stored policy, or omits the keyspace row when empty.
		durability string
		// missingInstance omits the database_instance row.
		missingInstance bool
		// clearedAddress clears the MySQL address after the stored poll.
		clearedAddress bool
		// tabletType supplies the topology tablet type independently of the stored poll.
		tabletType topodatapb.TabletType
		// position is the expected encoded requirement.
		position string
		// reason is the expected audit explanation, or its prefix for read errors.
		reason string
	}{
		{"stored primary", policy.DurabilitySemiSync, false, false, topodatapb.TabletType_PRIMARY, "MySQL56/" + gtid, ""},
		{"cleared MySQL address", policy.DurabilitySemiSync, false, true, topodatapb.TabletType_PRIMARY, "MySQL56/" + gtid, ""},
		{"missing keyspace row", "", false, false, topodatapb.TabletType_PRIMARY, "", "cannot read durability policy:"},
		{"non semi-sync keyspace", policy.DurabilityNone, false, false, topodatapb.TabletType_PRIMARY, "", "durability policy has no semi-sync"},
		{"no database_instance row", policy.DurabilitySemiSync, true, false, topodatapb.TabletType_PRIMARY, "", "no stored primary instance"},
		{"topology tablet is not PRIMARY", policy.DurabilitySemiSync, false, false, topodatapb.TabletType_RDONLY, "", "analyzed tablet is not a primary"},
		{"deleted primary surviving replica", policy.DurabilitySemiSync, false, false, topodatapb.TabletType_REPLICA, "", "analyzed tablet is not a primary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.ClearVTOrcDatabase()
			t.Cleanup(db.ClearVTOrcDatabase)

			if tt.durability != "" {
				keyspace := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: tt.durability}}
				keyspace.SetKeyspaceName("ks")
				require.NoError(t, inst.SaveKeyspace(keyspace))
			}

			tablet := &topodatapb.Tablet{
				Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				Type:          tt.tabletType,
				Keyspace:      "ks",
				Shard:         "0",
				MysqlHostname: "analyzed",
				MysqlPort:     3306,
			}
			require.NoError(t, inst.SaveTablet(tablet))

			if !tt.missingInstance {
				// Keep the stored type PRIMARY so only the topology type can reject this tablet.
				require.NoError(t, inst.WriteInstance(&inst.Instance{
					InstanceAlias:   tablet.Alias,
					TabletType:      topodatapb.TabletType_PRIMARY,
					Hostname:        tablet.MysqlHostname,
					Port:            int(tablet.MysqlPort),
					ExecutedGtidSet: gtid,
				}, true, nil))
			}

			if tt.clearedAddress {
				tablet.MysqlHostname = ""
				tablet.MysqlPort = 0
				require.NoError(t, inst.SaveTablet(tablet))
			}

			position, reason := readRequiredPrimaryPosition(tablet)
			assert.Equal(t, tt.position, replication.EncodePosition(position))
			if tt.durability == "" {
				assert.Contains(t, reason, tt.reason)
			} else {
				assert.Equal(t, tt.reason, reason)
			}
		})
	}
}

// TestRequiredPrimaryPosition checks each gate and the audit explanation.
func TestRequiredPrimaryPosition(t *testing.T) {
	const gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	tests := []struct {
		// name identifies the gate under test.
		name string
		// durability names the keyspace policy.
		durability string
		// executedGtidSet supplies the stored GTID set.
		executedGtidSet string
		// instanceFound reports whether a stored poll exists.
		instanceFound bool
		// position is the expected encoded requirement.
		position string
		// reason is the expected audit explanation.
		reason string
	}{
		{"semi-sync", policy.DurabilitySemiSync, gtid, true, "MySQL56/" + gtid, ""},
		{"no semi-sync", policy.DurabilityNone, gtid, true, "", "durability policy has no semi-sync"},
		{"empty set", policy.DurabilitySemiSync, "", true, "", "stored primary GTID set is empty"},
		{"invalid set", policy.DurabilitySemiSync, "invalid", true, "", "stored primary GTID set is not a MySQL56 GTID set"},
		{"FilePos set", policy.DurabilitySemiSync, "mysql-bin.000001:123", true, "", "stored primary GTID set is not a MySQL56 GTID set"},
		{"MariaDB set", policy.DurabilitySemiSync, "0-1-100", true, "", "stored primary GTID set is not a MySQL56 GTID set"},
		{"missing instance", policy.DurabilitySemiSync, "", false, "", "no stored primary instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := policy.GetDurabilityPolicy(tt.durability)
			require.NoError(t, err)

			position, reason := requiredPrimaryPosition(topodatapb.TabletType_PRIMARY, primaryPositionSnapshot{
				durability:      durability,
				executedGtidSet: tt.executedGtidSet,
				instanceFound:   tt.instanceFound,
			})
			assert.Equal(t, tt.position, replication.EncodePosition(position))
			assert.Equal(t, tt.reason, reason)
		})
	}
}
