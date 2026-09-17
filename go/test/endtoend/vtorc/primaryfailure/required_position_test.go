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

package primaryfailure

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// TestRequiredPositionRejectsLostRelayLogs checks that VTOrc rejects stale
// candidates when every replica loses transactions that the primary stored.
func TestRequiredPositionRejectsLostRelayLogs(t *testing.T) {
	t.Cleanup(func() { utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance) })
	// Keep the ERS timeouts short. The dead primary never answers, and the
	// default 30 second waits would stretch every recovery attempt.
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 3, 0, []string{
		"--emergency-reparent-require-primary-position",
		"--remote-operation-timeout=10s",
		"--wait-replicas-timeout=5s",
	}, cluster.VTOrcConfiguration{}, cluster.DefaultVtorcsByCell, policy.DurabilitySemiSync)

	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard := &keyspace.Shards[0]
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard)
	require.NotNil(t, primary)
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtorc, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard.Name, 1)

	var replicas []*cluster.Vttablet
	for _, tablet := range shard.Vttablets {
		if tablet != primary {
			replicas = append(replicas, tablet)
		}
	}

	require.Len(t, replicas, 2)
	utils.CheckReplication(t, clusterInfo, primary, replicas, 30*time.Second)

	// Keep discovery active but prevent repairs while relay logs are changed.
	utils.DisableGlobalRecoveries(t, vtorc)
	for _, replica := range replicas {
		require.NoError(t, utils.RunSQLs(t, []string{"STOP REPLICA SQL_THREAD"}, replica, ""))
	}

	// Use an ID beyond the test writes. clusterInfo.lastUsedValue starts at 100
	// and grows by one per write.
	require.NoError(t, utils.RunSQLs(t, []string{
		"INSERT INTO vt_ks.vt_insert_test(id, msg) VALUES (1000000, 'relay log loss')",
	}, primary, ""))

	res, err := utils.RunSQL(t, "SELECT @@global.gtid_executed", primary, "")
	require.NoError(t, err)
	position := strings.ReplaceAll(res.Rows[0][0].ToString(), "\n", "")
	for _, replica := range replicas {
		waitForReceivedPosition(t, primary, replica)
		res, err = utils.RunSQL(t, fmt.Sprintf("SELECT GTID_SUBSET('%s', @@global.gtid_executed)", position), replica, "")
		require.NoError(t, err)
		require.Equal(t, "0", res.Rows[0][0].ToString(), "replica must not apply the new transaction")
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rows, readErr := readVTOrcTable(vtorc, "database_instance")
		require.NoError(c, readErr)

		for _, row := range rows {
			if row.GetString("alias") == primary.Alias {
				assert.Equal(c, position, strings.ReplaceAll(row.GetString("executed_gtid_set"), "\n", ""))
				return
			}
		}

		assert.Fail(c, "primary is missing from database_instance")
	}, 30*time.Second, time.Second, "VTOrc must store the primary position before failure")

	// Stop MySQL before the relay logs are discarded. A dead source cannot refill them.
	require.NoError(t, primary.MysqlctlProcess.Stop())
	t.Cleanup(func() { utils.PermanentlyRemoveVttablet(clusterInfo, primary) })

	for _, replica := range replicas {
		require.NoError(t, utils.RunSQLs(t, []string{"STOP REPLICA", "RESET REPLICA"}, replica, ""))
		res, err = utils.RunSQL(t, "SHOW REPLICA STATUS", replica, "")
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		retrievedColumn := -1
		for i, field := range res.Fields {
			if field.Name == "Retrieved_Gtid_Set" {
				retrievedColumn = i
			}
		}
		require.NotEqual(t, -1, retrievedColumn, "SHOW REPLICA STATUS has no Retrieved_Gtid_Set column")
		require.Empty(t, res.Rows[0][retrievedColumn].ToString(), "RESET REPLICA must discard received transactions")

		// Start replication again. ERS needs both tablets as semi-sync candidates.
		require.NoError(t, utils.RunSQLs(t, []string{"START REPLICA"}, replica, ""))
	}

	utils.EnableGlobalRecoveries(t, vtorc)

	requiredLine := "required position: MySQL56/" + position
	failureLine := "required position MySQL56/" + position
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		steps, readErr := readVTOrcTable(vtorc, "topology_recovery_steps")
		require.NoError(c, readErr)

		var messages []string
		for _, step := range steps {
			messages = append(messages, step.GetString("message"))
		}

		audit := strings.Join(messages, "\n")
		assert.Contains(c, audit, requiredLine)
		assert.Contains(c, audit, "FAILED_PRECONDITION")
		assert.Contains(c, audit, failureLine)
	}, 60*time.Second, time.Second, "VTOrc must audit the requirement and the ERS failure")

	// Watch several recovery polls. A stale promotion can follow the first failure.
	// A read error counts as a change. A broken vtctld must not pass this check.
	assert.Never(t, func() bool {
		current, readErr := clusterInfo.ClusterInstance.VtctldClientProcess.GetShard(keyspace.Name, shard.Name)
		if readErr != nil {
			return true
		}

		return topoproto.TabletAliasString(current.Shard.PrimaryAlias) != primary.Alias
	}, 30*time.Second, time.Second, "no stale replica may become primary")
	assert.Zero(t, utils.GetSuccessfulRecoveryCount(t, vtorc, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard.Name))
}
