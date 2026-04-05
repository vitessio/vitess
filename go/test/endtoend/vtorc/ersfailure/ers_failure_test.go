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

package ersfailure

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestDeadPrimaryWithDelayedRelayLogApply verifies that ERS leaves replica IO
// threads running when delayed SQL threads cannot apply all relay logs within
// wait-replicas-timeout.
func TestDeadPrimaryWithDelayedRelayLogApply(t *testing.T) {
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	tablets := shard0.Vttablets

	require.Len(t, tablets, 3)

	curPrimary := tablets[0]

	err := clusterInfo.ClusterInstance.VtctldClientProcess.InitializeShard(
		keyspace.Name,
		shard0.Name,
		clusterInfo.ClusterInstance.Cell,
		curPrimary.TabletUID,
	)
	require.NoError(t, err)

	utils.CheckPrimaryTablet(t, clusterInfo, curPrimary, true)

	replicas := make([]*cluster.Vttablet, 0, 2)
	for _, tablet := range tablets {
		if tablet.Alias == curPrimary.Alias {
			continue
		}

		replicas = append(replicas, tablet)
	}

	require.Len(t, replicas, 2, "could not find both replica tablets")

	utils.CheckReplication(t, clusterInfo, curPrimary, replicas, 10*time.Second)

	readReplicaStatus := func(tablet *cluster.Vttablet) (map[string]string, error) {
		res, err := utils.RunSQL(t, "SHOW REPLICA STATUS", tablet, "")
		if err != nil {
			return nil, err
		}

		if len(res.Rows) != 1 {
			return nil, fmt.Errorf("expected 1 SHOW REPLICA STATUS row for %s, got %d", tablet.Alias, len(res.Rows))
		}

		status := make(map[string]string, len(res.Fields))
		for idx, field := range res.Fields {
			status[strings.ToLower(field.Name)] = res.Rows[0][idx].ToString()
		}

		return status, nil
	}

	replicaStatusField := func(status map[string]string, fields ...string) string {
		for _, field := range fields {
			if value, ok := status[strings.ToLower(field)]; ok {
				return value
			}
		}

		return ""
	}

	for _, replica := range replicas {
		err := utils.RunSQLs(t, []string{
			"STOP REPLICA SQL_THREAD",
			"CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 3600",
			"START REPLICA SQL_THREAD",
		}, replica, "")
		require.NoError(t, err)
	}

	insertedID := time.Now().UnixNano()

	_, err = utils.RunSQL(
		t,
		fmt.Sprintf("INSERT INTO vt_insert_test(id, msg) VALUES (%d, 'delayed ers reproduction')", insertedID),
		curPrimary,
		"vt_ks",
	)
	require.NoError(t, err)

	for _, replica := range replicas {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status, err := readReplicaStatus(replica)
			require.NoError(c, err)

			replicaIORunning := replicaStatusField(status, "Replica_IO_Running", "Slave_IO_Running")
			replicaSQLRunning := replicaStatusField(status, "Replica_SQL_Running", "Slave_SQL_Running")

			row, err := utils.RunSQL(t, fmt.Sprintf("SELECT id FROM vt_insert_test WHERE id = %d", insertedID), replica, "vt_ks")
			require.NoError(c, err)

			require.Equal(c, "Yes", replicaIORunning)
			require.Equal(c, "Yes", replicaSQLRunning)
			require.Len(c, row.Rows, 0)
		}, 30*time.Second, 500*time.Millisecond, "timed out waiting for delayed relay log apply on %s", replica.Alias)
	}

	out, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"EmergencyReparentShard",
		fmt.Sprintf("%s/%s", keyspace.Name, shard0.Name),
		"--wait-replicas-timeout", "2s",
		"--action_timeout", "60s",
	)
	require.Error(t, err)
	require.Contains(t, out, "could not apply all relay logs within the provided waitReplicasTimeout")

	for _, replica := range replicas {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status, err := readReplicaStatus(replica)
			require.NoError(c, err)

			replicaIORunning := replicaStatusField(status, "Replica_IO_Running", "Slave_IO_Running")

			require.Equal(c, "Yes", replicaIORunning)
		}, 20*time.Second, time.Second, "timed out waiting for replication to keep running on %s", replica.Alias)
	}
}
