/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletReshuffle test if a vttablet can be pointed at an existing mysql
func TestTabletReshuffle(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.NoError(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.NoError(t, err)
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	//Create new tablet
	rTablet := clusterInstance.NewVttabletInstance("replica", 0, "")

	// mycnf_server_id prevents vttablet from reading the mycnf
	// Pointing to masterTablet's socket file
	// We have to disable active reparenting to prevent the tablet from trying to fix replication.
	// We also have to disable replication reporting because we're pointed at the master.
	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-mycnf_server_id", fmt.Sprintf("%d", rTablet.TabletUID),
		"-db_socket", fmt.Sprintf("%s/mysql.sock", masterTablet.VttabletProcess.Directory),
		"-disable_active_reparents",
		"-enable_replication_reporter=false",
	}
	defer func() { clusterInstance.VtTabletExtraArgs = []string{} }()

	// SupportsBackup=False prevents vttablet from trying to restore
	// Start vttablet process
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	sql := "select value from t1"
	args := []string{
		"VtTabletExecute",
		"-options", "included_fields:TYPE_ONLY",
		"-json",
		rTablet.Alias,
		sql,
	}
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	require.NoError(t, err)
	assertExcludeFields(t, result)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", rTablet.Alias)
	assert.Error(t, err, "cannot perform backup without my.cnf")

	killTablets(t, rTablet)
}

func TestHealthCheck(t *testing.T) {
	// Add one replica that starts not initialized
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	rTablet := clusterInstance.NewVttabletInstance("replica", 0, "")

	// Start Mysql Processes and return connection
	replicaConn, err := cluster.StartMySQLAndGetConnection(ctx, rTablet, username, clusterInstance.TmpDirectory)
	require.NoError(t, err)

	defer replicaConn.Close()

	// Create database in mysql
	exec(t, replicaConn, fmt.Sprintf("create database vt_%s", keyspaceName))

	// start vttablet process, should be in SERVING state as we already have a master
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.NoError(t, err)
	defer masterConn.Close()

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// Make sure the master is still master
	checkTabletType(t, masterTablet.Alias, "MASTER")
	exec(t, masterConn, "stop slave")

	// stop replication, make sure we don't go unhealthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)

	// make sure the health stream is updated
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)
	require.NoError(t, err)
	verifyStreamHealth(t, result)

	// then restart replication, make sure we stay healthy
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", rTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// now test VtTabletStreamHealth returns the right thing
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "2", rTablet.Alias)
	require.NoError(t, err)
	scanner := bufio.NewScanner(strings.NewReader(result))
	for scanner.Scan() {
		verifyStreamHealth(t, scanner.Text())
	}

	// Manual cleanup of processes
	killTablets(t, rTablet)
}

func checkHealth(t *testing.T, port int, shouldError bool) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	if shouldError {
		assert.True(t, resp.StatusCode > 400)
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}
}

func checkTabletType(t *testing.T, tabletAlias string, typeWant string) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tabletAlias)
	require.NoError(t, err)

	var tablet topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tablet)
	require.NoError(t, err)

	actualType := tablet.GetType()
	got := fmt.Sprintf("%d", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := fmt.Sprintf("%d", tabletType)

	assert.Equal(t, want, got)
}

func verifyStreamHealth(t *testing.T, result string) {
	var streamHealthResponse querypb.StreamHealthResponse
	err := json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.NoError(t, err)
	serving := streamHealthResponse.GetServing()
	UID := streamHealthResponse.GetTabletAlias().GetUid()
	realTimeStats := streamHealthResponse.GetRealtimeStats()
	secondsBehindMaster := realTimeStats.GetSecondsBehindMaster()
	assert.True(t, serving, "Tablet should be in serving state")
	assert.True(t, UID > 0, "Tablet should contain uid")
	// secondsBehindMaster varies till 7200 so setting safe limit
	assert.True(t, secondsBehindMaster < 10000, "replica should not be behind master")
}

func TestHealthCheckDrainedStateDoesNotShutdownQueryService(t *testing.T) {
	// This test is similar to test_health_check, but has the following differences:
	// - the second tablet is an 'rdonly' and not a 'replica'
	// - the second tablet will be set to 'drained' and we expect that
	// - the query service won't be shutdown

	//Wait if tablet is not in service state
	defer cluster.PanicHandler(t)
	err := rdonlyTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)

	// Check tablet health
	checkHealth(t, rdonlyTablet.HTTPPort, false)
	assert.Equal(t, "SERVING", rdonlyTablet.VttabletProcess.GetTabletStatus())

	// Change from rdonly to drained and stop replication. (These
	// actions are similar to the SplitClone vtworker command
	// implementation.)  The tablet will stay healthy, and the
	// query service is still running.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonlyTablet.Alias, "drained")
	require.NoError(t, err)
	// Trying to drain the same tablet again, should error
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonlyTablet.Alias, "drained")
	assert.Error(t, err, "already drained")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rdonlyTablet.Alias)
	require.NoError(t, err)
	// Trigger healthcheck explicitly to avoid waiting for the next interval.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTablet.Alias)
	require.NoError(t, err)

	checkTabletType(t, rdonlyTablet.Alias, "DRAINED")

	// Query service is still running.
	err = rdonlyTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)

	// Restart replication. Tablet will become healthy again.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonlyTablet.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", rdonlyTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, rdonlyTablet.HTTPPort, false)
}

func killTablets(t *testing.T, tablets ...*cluster.Vttablet) {
	var wg sync.WaitGroup
	for _, tablet := range tablets {
		wg.Add(1)
		go func(tablet *cluster.Vttablet) {
			defer wg.Done()
			_ = tablet.VttabletProcess.TearDown()
			_ = tablet.MysqlctlProcess.Stop()
		}(tablet)
	}
	wg.Wait()
}
