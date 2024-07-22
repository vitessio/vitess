/*
Copyright 2024 The Vitess Authors.

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

package semisync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
)

func TestSemiSyncUpgradeDowngrade(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Verify that replication is running as intended.
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	replica := tablets[1]
	// Verify we are using the correct vttablet version.
	verifyVttabletVersion(t, replica, 20)
	// Check the plugin loaded in vttablet.
	require.EqualValues(t, mysql.SemiSyncTypeMaster, semiSyncExtensionLoaded(t, replica))

	t.Run("Downgrade to v19", func(t *testing.T) {
		// change vttablet binary and downgrade it.
		changeVttabletBinary(t, replica, "vttabletold")
		// Verify we are using the older vttablet version.
		verifyVttabletVersion(t, replica, 19)
		// Verify that replication is running as intended.
		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
		// Check the plugin loaded in vttablet.
		require.EqualValues(t, mysql.SemiSyncTypeMaster, semiSyncExtensionLoaded(t, replica))
	})

	t.Run("Upgrade to v19", func(t *testing.T) {
		// change vttablet binary and downgrade it.
		changeVttabletBinary(t, replica, "vttablet")
		// Verify we are using the older vttablet version.
		verifyVttabletVersion(t, replica, 20)
		// Verify that replication is running as intended.
		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
		// Check the plugin loaded in vttablet.
		require.EqualValues(t, mysql.SemiSyncTypeMaster, semiSyncExtensionLoaded(t, replica))
	})

	t.Run("Change the semi-sync plugin", func(t *testing.T) {
		// Change MySQL plugins loaded.
		utils.RunSQLs(context.Background(), t, []string{
			`SET GLOBAL READ_ONLY=OFF`,
			`STOP REPLICA;`,
			`UNINSTALL PLUGIN rpl_semi_sync_master;`,
			`UNINSTALL PLUGIN rpl_semi_sync_slave;`,
			`INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';`,
			`INSTALL PLUGIN rpl_semi_sync_replica SONAME 'semisync_replica.so';`,
			`START REPLICA;`,
		}, replica)
		// Check the plugin loaded in vttablet.
		require.EqualValues(t, mysql.SemiSyncTypeSource, semiSyncExtensionLoaded(t, replica))
		// Verify that replication is running as intended.
		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
	})
}

// semiSyncExtensionLoaded checks if the semisync extension has been loaded.
// It should work for both MariaDB and MySQL.
func semiSyncExtensionLoaded(t *testing.T, replica *cluster.Vttablet) mysql.SemiSyncType {
	qr := utils.RunSQL(context.Background(), t, `SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'`, replica)
	for _, row := range qr.Rows {
		if row[0].ToString() == "rpl_semi_sync_source_enabled" {
			return mysql.SemiSyncTypeSource
		}
		if row[0].ToString() == "rpl_semi_sync_master_enabled" {
			return mysql.SemiSyncTypeMaster
		}
	}
	return mysql.SemiSyncTypeOff
}

func changeVttabletBinary(t *testing.T, replica *cluster.Vttablet, binary string) {
	t.Helper()
	err := replica.VttabletProcess.TearDown()
	require.NoError(t, err)
	replica.VttabletProcess.Binary = binary
	err = replica.VttabletProcess.Setup()
	require.NoError(t, err)
}

func verifyVttabletVersion(t *testing.T, replica *cluster.Vttablet, version int) {
	t.Helper()
	verGot, err := cluster.GetMajorVersion(replica.VttabletProcess.Binary)
	require.NoError(t, err)
	require.EqualValues(t, version, verGot)
}
