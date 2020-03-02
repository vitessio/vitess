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
	"testing"
	"time"

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
	require.Nil(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	//Create new tablet
	rTablet := clusterInstance.GetVttabletInstance("replica", 0, "")

	//Init Tablets
	err = clusterInstance.VtctlclientProcess.InitTablet(rTablet, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// mycnf_server_id prevents vttablet from reading the mycnf
	// Pointing to masterTablet's socket file
	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-mycnf_server_id", fmt.Sprintf("%d", rTablet.TabletUID),
		"-db_socket", fmt.Sprintf("%s/mysql.sock", masterTablet.VttabletProcess.Directory),
	}
	// SupportsBackup=False prevents vttablet from trying to restore
	// Start vttablet process
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	sql := "select value from t1"
	args := []string{
		"VtTabletExecute",
		"-options", "included_fields:TYPE_ONLY",
		"-json",
		rTablet.Alias,
		sql,
	}
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	require.Nil(t, err)
	assertExcludeFields(t, result)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", rTablet.Alias)
	assert.Error(t, err, "cannot perform backup without my.cnf")

	// Reset the VtTabletExtraArgs
	clusterInstance.VtTabletExtraArgs = []string{}
	killTablets(t, rTablet)
}

func TestHealthCheck(t *testing.T) {
	// Add one replica that starts not initialized
	// (for the replica, we let vttablet do the InitTablet)
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	rTablet := clusterInstance.GetVttabletInstance("replica", 0, "")

	// Start Mysql Processes and return connection
	replicaConn, err := cluster.StartMySQLAndGetConnection(ctx, rTablet, username, clusterInstance.TmpDirectory)
	require.Nil(t, err)

	defer replicaConn.Close()

	// Create database in mysql
	exec(t, replicaConn, fmt.Sprintf("create database vt_%s", keyspaceName))

	//Init Replica Tablet
	err = clusterInstance.VtctlclientProcess.InitTablet(rTablet, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// start vttablet process, should be in SERVING state as we already have a master
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.Nil(t, err)
	defer masterConn.Close()

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// Make sure the master is still master
	checkTabletType(t, masterTablet.Alias, "MASTER")
	exec(t, masterConn, "stop slave")

	// stop replication, make sure we don't go unhealthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rTablet.Alias)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.Nil(t, err)

	// make sure the health stream is updated
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)
	require.Nil(t, err)
	verifyStreamHealth(t, result)

	// then restart replication, make sure we stay healthy
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rTablet.Alias)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, rTablet.HTTPPort, false)

	// now test VtTabletStreamHealth returns the right thing
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "2", rTablet.Alias)
	require.Nil(t, err)
	scanner := bufio.NewScanner(strings.NewReader(result))
	for scanner.Scan() {
		// fmt.Println() // Println will add back the final '\n'
		verifyStreamHealth(t, scanner.Text())
	}

	// Manual cleanup of processes
	killTablets(t, rTablet)
}

func checkHealth(t *testing.T, port int, shouldError bool) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	require.Nil(t, err)
	if shouldError {
		assert.True(t, resp.StatusCode > 400)
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}
}

func checkTabletType(t *testing.T, tabletAlias string, typeWant string) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tabletAlias)
	require.Nil(t, err)

	var tablet topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tablet)
	require.Nil(t, err)

	actualType := tablet.GetType()
	got := fmt.Sprintf("%d", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := fmt.Sprintf("%d", tabletType)

	assert.Equal(t, want, got)
}

func verifyStreamHealth(t *testing.T, result string) {
	var streamHealthResponse querypb.StreamHealthResponse
	err := json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.Nil(t, err)
	serving := streamHealthResponse.GetServing()
	UID := streamHealthResponse.GetTabletAlias().GetUid()
	realTimeStats := streamHealthResponse.GetRealtimeStats()
	secondsBehindMaster := realTimeStats.GetSecondsBehindMaster()
	assert.True(t, serving, "Tablet should be in serving state")
	assert.True(t, UID > 0, "Tablet should contain uid")
	// secondsBehindMaster varies till 7200 so setting safe limit
	assert.True(t, secondsBehindMaster < 10000, "Slave should not be behind master")
}

func TestHealthCheckDrainedStateDoesNotShutdownQueryService(t *testing.T) {
	// This test is similar to test_health_check, but has the following differences:
	// - the second tablet is an 'rdonly' and not a 'replica'
	// - the second tablet will be set to 'drained' and we expect that
	// - the query service won't be shutdown

	//Wait if tablet is not in service state
	defer cluster.PanicHandler(t)
	err := rdonlyTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.Nil(t, err)

	// Check tablet health
	checkHealth(t, rdonlyTablet.HTTPPort, false)
	assert.Equal(t, "SERVING", rdonlyTablet.VttabletProcess.GetTabletStatus())

	// Change from rdonly to drained and stop replication. (These
	// actions are similar to the SplitClone vtworker command
	// implementation.)  The tablet will stay healthy, and the
	// query service is still running.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTablet.Alias, "drained")
	require.Nil(t, err)
	// Trying to drain the same tablet again, should error
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTablet.Alias, "drained")
	assert.Error(t, err, "already drained")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rdonlyTablet.Alias)
	require.Nil(t, err)
	// Trigger healthcheck explicitly to avoid waiting for the next interval.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTablet.Alias)
	require.Nil(t, err)

	checkTabletType(t, rdonlyTablet.Alias, "DRAINED")

	// Query service is still running.
	err = rdonlyTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.Nil(t, err)

	// Restart replication. Tablet will become healthy again.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonlyTablet.Alias, "rdonly")
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartSlave", rdonlyTablet.Alias)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rdonlyTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, rdonlyTablet.HTTPPort, false)
}

func TestIgnoreHealthError(t *testing.T) {
	// This test verify the tablet health by Ignoring the error
	// For this case we need a healthy tablet in a shard without any master.
	// When we try to make a connection to such tablet we get "no slave status" error.
	// We will then ignore this error and verify if the status report the tablet as Healthy.

	// Create a new shard
	defer cluster.PanicHandler(t)
	newShard := &cluster.Shard{
		Name: "1",
	}

	// Start mysql process
	tablet := clusterInstance.GetVttabletInstance("replica", 0, "")
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	require.Nil(t, err)

	// start vttablet process
	tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		clusterInstance.Cell,
		newShard.Name,
		clusterInstance.Keyspaces[0].Name,
		clusterInstance.VtctldProcess.Port,
		tablet.Type,
		clusterInstance.TopoProcess.Port,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory,
		clusterInstance.VtTabletExtraArgs,
		clusterInstance.EnableSemiSync)
	tablet.Alias = tablet.VttabletProcess.TabletPath
	newShard.Vttablets = append(newShard.Vttablets, tablet)

	clusterInstance.Keyspaces[0].Shards = append(clusterInstance.Keyspaces[0].Shards, *newShard)

	// Init Tablet
	err = clusterInstance.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, newShard.Name)
	require.Nil(t, err)

	// create database
	err = tablet.VttabletProcess.CreateDB(keyspaceName)
	require.Nil(t, err)

	// Start Vttablet, it should be NOT_SERVING as there is no master
	err = clusterInstance.StartVttablet(tablet, "NOT_SERVING", false, cell, keyspaceName, hostname, newShard.Name)
	require.Nil(t, err)

	// Force it healthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("IgnoreHealthError", tablet.Alias, ".*no slave status.*")
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	require.Nil(t, err)
	err = tablet.VttabletProcess.WaitForTabletType("SERVING")
	require.Nil(t, err)
	checkHealth(t, tablet.HTTPPort, false)

	// Turn off the force-healthy.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("IgnoreHealthError", tablet.Alias, "")
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	require.Nil(t, err)
	err = tablet.VttabletProcess.WaitForTabletType("NOT_SERVING")
	require.Nil(t, err)
	checkHealth(t, tablet.HTTPPort, true)

	// Tear down custom processes
	killTablets(t, tablet)
}

func TestNoMysqlHealthCheck(t *testing.T) {
	// This test starts a vttablet with no mysql port, while mysql is down.
	// It makes sure vttablet will start properly and be unhealthy.
	// Then we start mysql, and make sure vttablet becomes healthy.
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	rTablet := clusterInstance.GetVttabletInstance("replica", 0, "")
	mTablet := clusterInstance.GetVttabletInstance("replica", 0, "")

	// Start Mysql Processes and return connection
	masterConn, err := cluster.StartMySQLAndGetConnection(ctx, mTablet, username, clusterInstance.TmpDirectory)
	require.Nil(t, err)
	defer masterConn.Close()

	replicaConn, err := cluster.StartMySQLAndGetConnection(ctx, rTablet, username, clusterInstance.TmpDirectory)
	require.Nil(t, err)
	defer replicaConn.Close()

	// Create database in mysql
	exec(t, masterConn, fmt.Sprintf("create database vt_%s", keyspaceName))
	exec(t, replicaConn, fmt.Sprintf("create database vt_%s", keyspaceName))

	//Get the gtid to ensure we bring master and slave at same position
	qr := exec(t, masterConn, "SELECT @@GLOBAL.gtid_executed")
	gtid := string(qr.Rows[0][0].Raw())

	// Ensure master ans salve are at same position
	exec(t, replicaConn, "STOP SLAVE")
	exec(t, replicaConn, "RESET MASTER")
	exec(t, replicaConn, "RESET SLAVE")
	exec(t, replicaConn, fmt.Sprintf("SET GLOBAL gtid_purged='%s'", gtid))
	exec(t, replicaConn, fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1", hostname, mTablet.MySQLPort))
	exec(t, replicaConn, "START SLAVE")

	// now shutdown all mysqld
	err = rTablet.MysqlctlProcess.Stop()
	require.Nil(t, err)
	err = mTablet.MysqlctlProcess.Stop()
	require.Nil(t, err)

	//Init Tablets
	err = clusterInstance.VtctlclientProcess.InitTablet(mTablet, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(rTablet, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// Start vttablet process, should be in NOT_SERVING state as mysqld is not running
	err = clusterInstance.StartVttablet(mTablet, "NOT_SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err, "error should be Nil")
	err = clusterInstance.StartVttablet(rTablet, "NOT_SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err, "error should be Nil")

	// Check Health should fail as Mysqld is not found
	checkHealth(t, mTablet.HTTPPort, true)
	checkHealth(t, rTablet.HTTPPort, true)

	// Tell slave to not try to repair replication in healthcheck.
	// The StopSlave will ultimately fail because mysqld is not running,
	// But vttablet should remember that it's not supposed to fix replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rTablet.Alias)
	assert.Error(t, err, "Fail as mysqld not running")

	//The above notice to not fix replication should survive tablet restart.
	err = rTablet.VttabletProcess.TearDown()
	require.Nil(t, err)
	err = rTablet.VttabletProcess.Setup()
	require.Nil(t, err)

	// restart mysqld
	rTablet.MysqlctlProcess.InitMysql = false
	err = rTablet.MysqlctlProcess.Start()
	require.Nil(t, err)
	mTablet.MysqlctlProcess.InitMysql = false
	err = mTablet.MysqlctlProcess.Start()
	require.Nil(t, err)

	// the master should still be healthy
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", mTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, mTablet.HTTPPort, false)

	// the slave will now be healthy, but report a very high replication
	// lag, because it can't figure out what it exactly is.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", rTablet.Alias)
	require.Nil(t, err)
	assert.Equal(t, "SERVING", rTablet.VttabletProcess.GetTabletStatus())
	checkHealth(t, rTablet.HTTPPort, false)

	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)
	require.Nil(t, err)
	var streamHealthResponse querypb.StreamHealthResponse
	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.Nil(t, err)
	realTimeStats := streamHealthResponse.GetRealtimeStats()
	secondsBehindMaster := realTimeStats.GetSecondsBehindMaster()
	assert.True(t, secondsBehindMaster == 7200)

	// restart replication, wait until health check goes small
	// (a value of zero is default and won't be in structure)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartSlave", rTablet.Alias)
	require.Nil(t, err)

	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", rTablet.Alias)
		require.Nil(t, err)
		var streamHealthResponse querypb.StreamHealthResponse
		err = json2.Unmarshal([]byte(result), &streamHealthResponse)
		require.Nil(t, err)
		realTimeStats := streamHealthResponse.GetRealtimeStats()
		secondsBehindMaster := realTimeStats.GetSecondsBehindMaster()
		if secondsBehindMaster < 30 {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// wait for the tablet to fix its mysql port
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", rTablet.Alias)
	require.Nil(t, err)
	var tablet topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tablet)
	require.Nil(t, err)
	portMap := tablet.GetPortMap()
	mysqlPort := int(portMap["mysql"])
	assert.True(t, mysqlPort == rTablet.MySQLPort, "mysql port in tablet record")

	// Tear down custom processes
	killTablets(t, rTablet, mTablet)
}

func killTablets(t *testing.T, tablets ...*cluster.Vttablet) {
	for _, tablet := range tablets {
		//Stop Mysqld
		tablet.MysqlctlProcess.Stop()

		//Tear down Tablet
		tablet.VttabletProcess.TearDown()
	}
}
