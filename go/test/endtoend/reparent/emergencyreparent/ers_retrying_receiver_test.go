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

package emergencyreparent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	endtoendutils "vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// replicaStatusValue returns a field of the tablet's SHOW REPLICA STATUS.
func replicaStatusValue(t *testing.T, tablet *cluster.Vttablet, field string) string {
	res := utils.RunSQL(t.Context(), t, `show replica status`, tablet)
	require.Len(t, res.Rows, 1)
	for i, f := range res.Fields {
		if f.Name == field {
			return res.Rows[0][i].ToString()
		}
	}
	require.FailNow(t, "no such replica status field", field)
	return ""
}

// startRetryingReceiver makes the replica's receiver retry its connection to the primary with a
// connection error: the replication user is locked on the primary (not binlogged), and the
// receiver is restarted. The cluster is torn down after the test, so the user stays locked.
func startRetryingReceiver(t *testing.T, primary, replica *cluster.Vttablet) {
	utils.RunSQLs(t.Context(), t, []string{`SET sql_log_bin = 0`, `ALTER USER 'vt_repl'@'%' ACCOUNT LOCK`}, primary)
	utils.RunSQLs(t.Context(), t, []string{`STOP REPLICA IO_THREAD`, `START REPLICA IO_THREAD`}, replica)
	require.Eventually(t, func() bool {
		return replicaStatusValue(t, replica, "Replica_IO_Running") == "Connecting" &&
			replicaStatusValue(t, replica, "Last_IO_Errno") != "0"
	}, 60*time.Second, time.Second, "the replica's receiver must retry its connection with an error")
}

// TestERSStopReplicationStopsRetryingReceiver checks that the replication stop of ERS stops a
// receiver that retries its connection after an error. Such a receiver is not replicating, but
// it runs: once it can connect again, it receives from the old primary and acknowledges its
// semi-sync transactions, although ERS counts the tablet as one that cannot.
func TestERSStopReplicationStopsRetryingReceiver(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vttablet")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	t.Cleanup(func() { utils.TeardownCluster(clusterInstance) })
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	primary, replica := tablets[0], tablets[1]
	utils.ConfirmReplication(t, primary, tablets[1:])

	startRetryingReceiver(t, primary, replica)

	replicaTablet, err := clusterInstance.VtctldClientProcess.GetTablet(replica.Alias)
	require.NoError(t, err)
	tmc := grpctmclient.NewClient()
	t.Cleanup(tmc.Close)
	_, err = tmc.StopReplicationAndGetStatus(t.Context(), replicaTablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
	require.NoError(t, err)

	assert.Equal(t, "No", replicaStatusValue(t, replica, "Replica_IO_Running"), "the retrying receiver must be stopped")
}

// TestRepointStopsRetryingReceiver checks that repointing a replica whose applier is stopped and
// whose receiver retries its connection after an error stops replication before changing the
// source, which MySQL refuses while the receiver runs (ERROR 3081).
func TestRepointStopsRetryingReceiver(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vttablet")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	t.Cleanup(func() { utils.TeardownCluster(clusterInstance) })
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	primary, replica := tablets[0], tablets[1]
	utils.ConfirmReplication(t, primary, tablets[1:])

	utils.RunSQL(t.Context(), t, `STOP REPLICA SQL_THREAD`, replica)
	startRetryingReceiver(t, primary, replica)

	// Repoint it with a heartbeat interval, as VTOrc's replica repair does, so that the source
	// is changed although it stays the same.
	replicaTablet, err := clusterInstance.VtctldClientProcess.GetTablet(replica.Alias)
	require.NoError(t, err)
	primaryAlias, err := topoproto.ParseTabletAlias(primary.Alias)
	require.NoError(t, err)
	tmc := grpctmclient.NewClient()
	t.Cleanup(tmc.Close)
	err = tmc.SetReplicationSource(t.Context(), replicaTablet, primaryAlias, 0, "", false, true, 5)
	require.NoError(t, err)

	res := utils.RunSQL(t.Context(), t, `select HEARTBEAT_INTERVAL from performance_schema.replication_connection_configuration`, replica)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "5.000", res.Rows[0][0].ToString(), "the source must have been changed")
	assert.Equal(t, "Yes", replicaStatusValue(t, replica, "Replica_SQL_Running"), "replication must be started again, as the receiver was running")
}
