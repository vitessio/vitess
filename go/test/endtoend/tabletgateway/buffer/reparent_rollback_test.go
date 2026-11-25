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

package buffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestBufferPlannedReparentRollback(t *testing.T) {
	bt := &BufferingTest{
		VtGateExtraArgs: []string{"--buffer-window", "30s", "--buffer-max-failover-duration", "30s"},
	}
	// Keep VTGate routing to the old primary while its transaction pool drains.
	cluster, exitCode := bt.createCluster("--health-check-interval", "20s", "--demote-primary-lock-wait-timeout", "2s")
	require.Zero(t, exitCode, "failed to start cluster")
	t.Cleanup(cluster.Teardown)

	keyspace := cluster.Keyspaces[0]
	shard := keyspace.Shards[0]
	primary, replica := shard.Vttablets[0], shard.Vttablets[1]
	params := &mysql.ConnParams{Host: cluster.Hostname, Port: cluster.VtgateMySQLPort}
	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.Eventually(t, func() bool {
		_, err := conn.ExecuteFetch("select id from buffer", 1, false)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond)
	utils.Exec(t, conn, "insert into buffer (id, msg) values (1, 'before')")

	// The table lock makes setting super_read_only fail during demotion.
	dbaConn, err := utils.GetMySQLConn(primary, "vt_"+keyspace.Name)
	require.NoError(t, err)
	t.Cleanup(dbaConn.Close)
	utils.Exec(t, dbaConn, "create table buffer_lock (id bigint primary key) engine=InnoDB")
	utils.Exec(t, dbaConn, "lock tables buffer_lock write")

	txConn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	t.Cleanup(txConn.Close)
	utils.Exec(t, txConn, "begin")
	utils.Exec(t, txConn, "update buffer set msg = 'held' where id = 1")

	shardName := keyspace.Name + "/" + shard.Name
	prsDone := make(chan struct{})
	var prsOutput string
	var prsErr error
	go func() {
		defer close(prsDone)
		prsOutput, prsErr = cluster.VtctldClientProcess.ExecuteCommandWithOutput("PlannedReparentShard", shardName, "--new-primary", replica.Alias)
	}()

	statsLabel := keyspace.Name + "." + shard.Name
	var result *sqltypes.Result
	var queryErr error
	require.Eventually(t, func() bool {
		result, queryErr = conn.ExecuteFetch("select id from buffer where id = 1", 1, false)
		buffered, _ := cluster.VtgateProcess.GetVars()["BufferRequestsBuffered"].(map[string]any)
		return queryErr != nil || buffered[statsLabel] == float64(1)
	}, time.Minute, 100*time.Millisecond, "no query was buffered")

	<-prsDone
	require.Error(t, prsErr)
	require.Contains(t, prsOutput, "Lock wait timeout exceeded")
	vars := cluster.VtgateProcess.GetVars()
	buffered, ok := vars["BufferRequestsBuffered"].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1, buffered[statsLabel])
	stops, ok := vars["BufferStops"].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1, stops[statsLabel+".NewPrimarySeen"])
	require.NoError(t, queryErr, "the buffered query must succeed on the recovered primary")
	require.Equal(t, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, result.Rows)
}
