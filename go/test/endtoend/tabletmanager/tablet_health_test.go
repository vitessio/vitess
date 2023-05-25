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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/strings/slices"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletReshuffle test if a vttablet can be pointed at an existing mysql
func TestTabletReshuffle(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	defer conn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.NoError(t, err)
	defer replicaConn.Close()

	// Sanity Check
	utils.Exec(t, conn, "delete from t1")
	utils.Exec(t, conn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// Create new tablet
	rTablet := clusterInstance.NewVttabletInstance("replica", 0, "")

	// mycnf_server_id prevents vttablet from reading the mycnf
	// Pointing to primaryTablet's socket file
	// We have to disable active reparenting to prevent the tablet from trying to fix replication.
	// We also have to disable replication reporting because we're pointed at the primary.
	clusterInstance.VtTabletExtraArgs = []string{
		"--lock_tables_timeout", "5s",
		"--mycnf_server_id", fmt.Sprintf("%d", rTablet.TabletUID),
		"--db_socket", fmt.Sprintf("%s/mysql.sock", primaryTablet.VttabletProcess.Directory),
		"--disable_active_reparents",
		"--enable_replication_reporter=false",
	}
	defer func() { clusterInstance.VtTabletExtraArgs = []string{} }()

	// SupportsBackup=False prevents vttablet from trying to restore
	// Start vttablet process
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	sql := "select value from t1"
	qr, err := clusterInstance.ExecOnTablet(ctx, rTablet, sql, nil, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_TYPE_ONLY})
	require.NoError(t, err)

	result, err := json.Marshal(qr)
	require.NoError(t, err)
	assertExcludeFields(t, string(result))

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", rTablet.Alias)
	assert.Error(t, err, "cannot perform backup without my.cnf")

	killTablets(rTablet)
}

func TestHealthCheck(t *testing.T) {
	// Add one replica that starts not initialized
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	clusterInstance.DisableVTOrcRecoveries(t)
	defer clusterInstance.EnableVTOrcRecoveries(t)

	rTablet := clusterInstance.NewVttabletInstance("replica", 0, "")

	// Start Mysql Processes and return connection
	replicaConn, err := cluster.StartMySQLAndGetConnection(ctx, rTablet, username, clusterInstance.TmpDirectory)
	require.NoError(t, err)

	defer replicaConn.Close()

	// start vttablet process, should be in SERVING state as we already have a primary
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	defer conn.Close()

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// Make sure the primary is still primary
	checkTabletType(t, primaryTablet.Alias, "PRIMARY")
	utils.Exec(t, conn, "stop slave")

	// stop replication, make sure we don't go unhealthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)

	// make sure the health stream is updated
	shrs, err := clusterInstance.StreamTabletHealth(ctx, rTablet, 1)
	require.NoError(t, err)
	for _, shr := range shrs {
		verifyStreamHealth(t, shr, true)
	}

	// then restart replication, make sure we stay healthy
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", rTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// now test the health stream returns the right thing
	shrs, err = clusterInstance.StreamTabletHealth(ctx, rTablet, 2)
	require.NoError(t, err)
	for _, shr := range shrs {
		verifyStreamHealth(t, shr, true)
	}

	// stop the replica's source mysqld instance to break replication
	// and test that the replica tablet becomes unhealthy and non-serving after crossing
	// the tablet's --unhealthy_threshold and the gateway's --discovery_low_replication_lag
	err = primaryTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)

	time.Sleep(tabletUnhealthyThreshold + tabletHealthcheckRefreshInterval)

	// now the replica's health stream should show it as unhealthy
	shrs, err = clusterInstance.StreamTabletHealth(ctx, rTablet, 1)
	require.NoError(t, err)
	for _, shr := range shrs {
		verifyStreamHealth(t, shr, false)
	}

	// start the primary tablet's mysqld back up
	primaryTablet.MysqlctlProcess.InitMysql = false
	err = primaryTablet.MysqlctlProcess.Start()
	primaryTablet.MysqlctlProcess.InitMysql = true
	require.NoError(t, err)

	// On a MySQL restart, it comes up as a read-only tablet (check default.cnf file).
	// We have to explicitly set it to read-write otherwise heartbeat writer is unable
	// to write the heartbeats
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", primaryTablet.Alias)
	require.NoError(t, err)

	// explicitly start replication on all of the replicas to avoid any test flakiness as they were all
	// replicating from the primary instance
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", rTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", replicaTablet.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", rdonlyTablet.Alias)
	require.NoError(t, err)

	time.Sleep(tabletHealthcheckRefreshInterval)

	// now the replica's health stream should show it as healthy again
	shrs, err = clusterInstance.StreamTabletHealth(ctx, rTablet, 1)
	require.NoError(t, err)
	for _, shr := range shrs {
		verifyStreamHealth(t, shr, true)
	}

	// Manual cleanup of processes
	killTablets(rTablet)
}

// TestHealthCheckSchemaChangeSignal tests the tables and views, which report their schemas have changed in the output of a StreamHealth.
func TestHealthCheckSchemaChangeSignal(t *testing.T) {
	// Add one replica that starts not initialized
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	vtParams := clusterInstance.GetVTParams(keyspaceName)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Make sure the primary is the primary when the test starts.
	// This state should be ensured before we actually test anything.
	checkTabletType(t, primaryTablet.Alias, "PRIMARY")

	// Run a bunch of DDL queries and verify that the tables/views changed show up in the health stream.
	// These tests are for the part where `--queryserver-enable-views` flag is not set.
	verifyHealthStreamSchemaChangeSignals(t, conn, &primaryTablet, false)

	// We start a new vttablet, this time with `--queryserver-enable-views` flag specified.
	tempTablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	// Start Mysql Processes and return connection
	_, err = cluster.StartMySQLAndGetConnection(ctx, tempTablet, username, clusterInstance.TmpDirectory)
	require.NoError(t, err)
	oldArgs := clusterInstance.VtTabletExtraArgs
	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-views")
	defer func() {
		clusterInstance.VtTabletExtraArgs = oldArgs
	}()
	// start vttablet process, should be in SERVING state as we already have a primary.
	err = clusterInstance.StartVttablet(tempTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	defer func() {
		// Restore the primary tablet back to the original.
		err = clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, primaryTablet.Alias)
		require.NoError(t, err)
		// Manual cleanup of processes
		killTablets(tempTablet)
	}()

	// Now we reparent the cluster to the new tablet we have.
	err = clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, tempTablet.Alias)
	require.NoError(t, err)

	checkTabletType(t, tempTablet.Alias, "PRIMARY")
	// Run a bunch of DDL queries and verify that the tables/views changed show up in the health stream.
	// These tests are for the part where `--queryserver-enable-views` flag is set.
	verifyHealthStreamSchemaChangeSignals(t, conn, tempTablet, true)
}

func verifyHealthStreamSchemaChangeSignals(t *testing.T, vtgateConn *mysql.Conn, primaryTablet *cluster.Vttablet, viewsEnabled bool) {
	var streamErr error
	wg := sync.WaitGroup{}
	wg.Add(1)
	ranOnce := false
	finished := false
	ch := make(chan *querypb.StreamHealthResponse)
	go func() {
		defer wg.Done()
		streamErr = clusterInstance.StreamTabletHealthUntil(context.Background(), primaryTablet, 30*time.Second, func(shr *querypb.StreamHealthResponse) bool {
			ranOnce = true
			// If we are finished, then close the channel and end the stream.
			if finished {
				close(ch)
				return true
			}
			// Put the response in the channel.
			ch <- shr
			return false
		})
	}()
	// The test becomes flaky if we run the DDL immediately after starting the above go routine because the client for the Stream
	// sometimes isn't registered by the time DDL runs, and it misses the update we get. To prevent this situation, we wait for one Stream packet
	// to have returned. Once we know we received a Stream packet, then we know that we are registered for the health stream and can execute the DDL.
	for i := 0; i < 30; i++ {
		if ranOnce {
			break
		}
		time.Sleep(1 * time.Second)
	}

	verifyTableDDLSchemaChangeSignal(t, vtgateConn, ch, "CREATE TABLE `area` (`id` int NOT NULL, `country` varchar(30), PRIMARY KEY (`id`))", "area")
	verifyTableDDLSchemaChangeSignal(t, vtgateConn, ch, "CREATE TABLE `area2` (`id` int NOT NULL, PRIMARY KEY (`id`))", "area2")
	verifyViewDDLSchemaChangeSignal(t, vtgateConn, ch, "CREATE VIEW v2 as select * from t1", viewsEnabled)
	verifyTableDDLSchemaChangeSignal(t, vtgateConn, ch, "ALTER TABLE `area` ADD COLUMN name varchar(30) NOT NULL", "area")
	verifyTableDDLSchemaChangeSignal(t, vtgateConn, ch, "DROP TABLE `area2`", "area2")
	verifyViewDDLSchemaChangeSignal(t, vtgateConn, ch, "ALTER VIEW v2 as select id from t1", viewsEnabled)
	verifyViewDDLSchemaChangeSignal(t, vtgateConn, ch, "DROP VIEW v2", viewsEnabled)
	verifyTableDDLSchemaChangeSignal(t, vtgateConn, ch, "DROP TABLE `area`", "area")

	finished = true
	wg.Wait()
	require.NoError(t, streamErr)
}

func verifyTableDDLSchemaChangeSignal(t *testing.T, vtgateConn *mysql.Conn, ch chan *querypb.StreamHealthResponse, query string, table string) {
	_, err := vtgateConn.ExecuteFetch(query, 10000, false)
	require.NoError(t, err)

	timeout := time.After(15 * time.Second)
	for {
		select {
		case shr := <-ch:
			if shr != nil && shr.RealtimeStats != nil && slices.Contains(shr.RealtimeStats.TableSchemaChanged, table) {
				return
			}
		case <-timeout:
			t.Errorf("didn't get the correct tables changed in stream response until timeout")
		}
	}
}

func verifyViewDDLSchemaChangeSignal(t *testing.T, vtgateConn *mysql.Conn, ch chan *querypb.StreamHealthResponse, query string, viewsEnabled bool) {
	_, err := vtgateConn.ExecuteFetch(query, 10000, false)
	require.NoError(t, err)

	timeout := time.After(15 * time.Second)
	for {
		select {
		case shr := <-ch:
			listToUse := shr.RealtimeStats.TableSchemaChanged
			if viewsEnabled {
				listToUse = shr.RealtimeStats.ViewSchemaChanged
			}
			if shr != nil && shr.RealtimeStats != nil && slices.Contains(listToUse, "v2") {
				return
			}
		case <-timeout:
			t.Errorf("didn't get the correct views changed in stream response until timeout")
		}
	}
}

func checkHealth(t *testing.T, port int, shouldError bool) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
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

func verifyStreamHealth(t *testing.T, streamHealthResponse *querypb.StreamHealthResponse, expectHealthy bool) {
	serving := streamHealthResponse.GetServing()
	UID := streamHealthResponse.GetTabletAlias().GetUid()
	realTimeStats := streamHealthResponse.GetRealtimeStats()
	replicationLagSeconds := realTimeStats.GetReplicationLagSeconds()
	assert.True(t, UID > 0, "Tablet should contain uid")
	if expectHealthy {
		assert.True(t, serving, "Tablet should be in serving state")
		// replicationLagSeconds varies till 7200 so setting safe limit
		assert.True(t, replicationLagSeconds < 10000, "replica should not be behind primary")
	} else {
		assert.True(t, (!serving || replicationLagSeconds >= uint32(tabletUnhealthyThreshold.Seconds())), "Tablet should not be in serving and healthy state")
	}
}

func TestHealthCheckDrainedStateDoesNotShutdownQueryService(t *testing.T) {
	// This test is similar to test_health_check, but has the following differences:
	// - the second tablet is an 'rdonly' and not a 'replica'
	// - the second tablet will be set to 'drained' and we expect that
	// - the query service won't be shutdown

	//Wait if tablet is not in service state
	defer cluster.PanicHandler(t)
	clusterInstance.DisableVTOrcRecoveries(t)
	defer clusterInstance.EnableVTOrcRecoveries(t)
	err := rdonlyTablet.VttabletProcess.WaitForTabletStatus("SERVING")
	require.NoError(t, err)

	// Check tablet health
	checkHealth(t, rdonlyTablet.HTTPPort, false)
	assert.Equal(t, "SERVING", rdonlyTablet.VttabletProcess.GetTabletStatus())

	// Change from rdonly to drained and stop replication. The tablet will stay
	// healthy, and the query service is still running.
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
	err = rdonlyTablet.VttabletProcess.WaitForTabletStatus("SERVING")
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

func killTablets(tablets ...*cluster.Vttablet) {
	var wg sync.WaitGroup
	for _, tablet := range tablets {
		wg.Add(1)
		go func(tablet *cluster.Vttablet) {
			defer wg.Done()
			_ = tablet.VttabletProcess.TearDown()
			_ = tablet.MysqlctlProcess.Stop()
			_ = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		}(tablet)
	}
	wg.Wait()
}
