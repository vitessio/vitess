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

package reparent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	// ClusterInstance instance to be used for test with different params
	clusterInstance *cluster.LocalProcessCluster
	tmClient        *tmc.Client
	keyspaceName    = "ks"
	dbName          = "vt_" + keyspaceName
	username        = "vt_dba"
	hostname        = "localhost"
	insertSQL       = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	sqlSchema       = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB	
`
)

//region cluster setup/teardown
func setupRangeBasedCluster(ctx context.Context, t *testing.T) {
	tablets := setupCluster(ctx, t, shardName, []string{cell1}, []int{2})
	masterTablet, replicaTablet = tablets[0], tablets[1]
}

func setupReparentCluster(t *testing.T) {
	tablets := setupCluster(context.Background(), t, shardName, []string{cell1, cell2}, []int{3, 1})
	tab1, tab2, tab3, tab4 = tablets[0], tablets[1], tablets[2], tablets[3]
}

func teardownCluster() {
	clusterInstance.Teardown()
}

func setupCluster(ctx context.Context, t *testing.T, shardName string, cells []string, numTablets []int) []*cluster.Vttablet {
	var tablets []*cluster.Vttablet
	clusterInstance = cluster.NewCluster(cells[0], hostname)
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	// Start topo server
	err := clusterInstance.StartTopo()
	if err != nil {
		t.Fatalf("Error starting topo: %s", err.Error())
	}
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cells[0])
	if err != nil {
		t.Fatalf("Error managing topo: %s", err.Error())
	}
	numCell := 1
	for numCell < len(cells) {
		err = clusterInstance.VtctlProcess.AddCellInfo(cells[numCell])
		if err != nil {
			t.Fatalf("Error managing topo: %s", err.Error())
		}
		numCell++
	}

	// Adding another cell in the same cluster
	numCell = 0
	for numCell < len(cells) {
		i := 0
		for i < numTablets[numCell] {
			i++
			tablet := clusterInstance.NewVttabletInstance("replica", 100*(numCell+1)+i, cells[numCell])
			tablets = append(tablets, tablet)
		}
		numCell++
	}

	shard := &cluster.Shard{Name: shardName}
	shard.Vttablets = tablets

	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-enable_semi_sync",
		"-track_schema_versions=true",
	}

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	if err != nil {
		t.Fatalf("Cannot launch cluster: %s", err.Error())
	}

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				t.Fatalf("Error starting start mysql: %s", err.Error())
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			t.Fatalf("Error starting mysql: %s", err.Error())
		}
	}

	// create tablet manager client
	tmClient = tmc.NewClient()
	setupShard(ctx, t, shardName, tablets)
	return tablets
}

func setupShard(ctx context.Context, t *testing.T, shardName string, tablets []*cluster.Vttablet) {
	for _, tablet := range tablets {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.NoError(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablets[0].Alias)
	require.NoError(t, err)

	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, tablets[0])

	checkMasterTablet(t, tablets[0])

	validateTopology(t, false)
	time.Sleep(100 * time.Millisecond) // wait for replication to catchup
	strArray := getShardReplicationPositions(t, keyspaceName, shardName, true)
	assert.Equal(t, len(tablets), len(strArray))
	assert.Contains(t, strArray[0], "master") // master first
}

//endregion

//region database queries
func getMysqlConnParam(tablet *cluster.Vttablet) mysql.ConnParams {
	connParams := mysql.ConnParams{
		Uname:      username,
		DbName:     dbName,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	return connParams
}

func runSQL(ctx context.Context, t *testing.T, sql string, tablet *cluster.Vttablet) *sqltypes.Result {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()
	return execute(t, conn, sql)
}

func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}

//endregion

// region prs/ers

func prs(t *testing.T, tab *cluster.Vttablet) (string, error) {
	return prsWithTimeout(t, tab, false, "", "")
}

func prsAvoid(t *testing.T, tab *cluster.Vttablet) (string, error) {
	return prsWithTimeout(t, tab, true, "", "")
}

func prsWithTimeout(t *testing.T, tab *cluster.Vttablet, avoid bool, actionTimeout, waitTimeout string) (string, error) {
	args := []string{
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardName)}
	if actionTimeout != "" {
		args = append(args, "-action_timeout", actionTimeout)
	}
	if waitTimeout != "" {
		args = append(args, "-wait_replicas_timeout", waitTimeout)
	}
	if avoid {
		args = append(args, "-avoid_master")
	} else {
		args = append(args, "-new_master")
	}
	args = append(args, tab.Alias)
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	return out, err
}

func ers(t *testing.T, tab *cluster.Vttablet, timeout string) (string, error) {
	return ersIgnoreTablet(t, tab, timeout, nil)
}

func ersIgnoreTablet(t *testing.T, tab *cluster.Vttablet, timeout string, tabToIgnore *cluster.Vttablet) (string, error) {
	args := []string{"EmergencyReparentShard", "-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardName)}
	if tab != nil {
		args = append(args, "-new_master", tab.Alias)
	}
	if timeout != "" {
		args = append(args, "-wait_replicas_timeout", "30s")
	}
	if tabToIgnore != nil {
		args = append(args, "-ignore_replicas", tabToIgnore.Alias)
	}
	return clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
}

func checkReparentFromOutside(t *testing.T, tablet *cluster.Vttablet, downMaster bool, baseTime int64) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell1, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	if !downMaster {
		assertNodeCount(t, result, int(3))
	} else {
		assertNodeCount(t, result, int(2))
	}

	// make sure the master status page says it's the master
	status := tablet.VttabletProcess.GetStatus()
	assert.Contains(t, status, "Tablet Type: MASTER")

	// make sure the master health stream says it's the master too
	// (health check is disabled on these servers, force it first)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	require.NoError(t, err)

	streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"VtTabletStreamHealth",
		"-count", "1", tablet.Alias)
	require.NoError(t, err)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	require.NoError(t, err)
	assert.Equal(t, streamHealthResponse.Target.TabletType, topodatapb.TabletType_MASTER)
	assert.True(t, streamHealthResponse.TabletExternallyReparentedTimestamp >= baseTime)
}

// endregion

// region validations

func validateTopology(t *testing.T, pingTablets bool) {
	args := []string{"Validate"}

	if pingTablets {
		args = append(args, "-ping-tablets=true")
	}
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	require.Empty(t, out)
	require.NoError(t, err)
}

func confirmReplication(t *testing.T, master *cluster.Vttablet, replicas []*cluster.Vttablet) {
	ctx := context.Background()
	n := 2 // random value ...
	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, n, n)
	runSQL(ctx, t, insertSQL, master)
	time.Sleep(100 * time.Millisecond)
	for _, tab := range replicas {
		err := checkInsertedValues(ctx, t, tab, n)
		require.NoError(t, err)
	}
}

func confirmOldMasterIsHangingAround(t *testing.T) {
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("Validate")
	require.Error(t, err)
	require.Contains(t, out, "already has master")
}

//	Waits for tablet B to catch up to the replication position of tablet A.
func waitForReplicationPosition(t *testing.T, tabletA *cluster.Vttablet, tabletB *cluster.Vttablet) error {
	posA, _ := cluster.GetMasterPosition(t, *tabletA, hostname)
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		posB, _ := cluster.GetMasterPosition(t, *tabletB, hostname)
		if positionAtLeast(t, tabletB, posA, posB) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed to catch up on replication position")
}

func positionAtLeast(t *testing.T, tablet *cluster.Vttablet, a string, b string) bool {
	isAtleast := false
	val, err := tablet.MysqlctlProcess.ExecuteCommandWithOutput("position", "at_least", a, b)
	require.NoError(t, err)
	if strings.Contains(val, "true") {
		isAtleast = true
	}
	return isAtleast
}

func assertNodeCount(t *testing.T, result string, want int) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(result), &resultMap)
	require.NoError(t, err)

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}

func checkDBvar(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show variables like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

func checkDBstatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show status like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

func checkReplicaStatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet) {
	qr := runSQL(ctx, t, "show slave status", tablet)
	IOThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	SQLThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	assert.Equal(t, IOThreadRunning, "VARCHAR(\"No\")")
	assert.Equal(t, SQLThreadRunning, "VARCHAR(\"No\")")
}

// Makes sure the tablet type is master, and its health check agrees.
func checkMasterTablet(t *testing.T, tablet *cluster.Vttablet) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.NoError(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletInfo.GetType())

	// make sure the health stream is updated
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
	require.NoError(t, err)
	var streamHealthResponse querypb.StreamHealthResponse

	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.NoError(t, err)

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletType)
}

// isHealthyMasterTablet will return if tablet is master AND healthy.
func isHealthyMasterTablet(t *testing.T, tablet *cluster.Vttablet) bool {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.Nil(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.Nil(t, err)
	if tabletInfo.GetType() != topodatapb.TabletType_MASTER {
		return false
	}

	// make sure the health stream is updated
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
	require.Nil(t, err)
	var streamHealthResponse querypb.StreamHealthResponse

	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.Nil(t, err)

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	return tabletType == topodatapb.TabletType_MASTER
}

func checkInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, index int) error {
	// wait until it gets the data
	timeout := time.Now().Add(5 * time.Second)
	i := 0
	for time.Now().Before(timeout) {
		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
		qr := runSQL(ctx, t, selectSQL, tablet)
		if len(qr.Rows) == 1 {
			return nil
		}
		t := time.Duration(300 * i)
		time.Sleep(t * time.Millisecond)
		i++
	}
	return fmt.Errorf("data is not yet replicated on tablet %s", tablet.Alias)
}

// endregion

// region tablet operations

func stopTablet(t *testing.T, tab *cluster.Vttablet, stopDatabase bool) {
	err := tab.VttabletProcess.TearDown()
	require.NoError(t, err)
	if stopDatabase {
		err = tab.MysqlctlProcess.Stop()
		require.NoError(t, err)
	}
}

func restartTablet(t *testing.T, tab *cluster.Vttablet) {
	tab.MysqlctlProcess.InitMysql = false
	err := tab.MysqlctlProcess.Start()
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tab, tab.Cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)
}

func resurrectTablet(ctx context.Context, t *testing.T, tab *cluster.Vttablet) {
	tab.MysqlctlProcess.InitMysql = false
	err := tab.MysqlctlProcess.Start()
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tab, tab.Cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	// As there is already a master the new replica will come directly in SERVING state
	tab1.VttabletProcess.ServingStatus = "SERVING"
	// Start the tablet
	err = tab.VttabletProcess.Setup()
	require.NoError(t, err)

	err = checkInsertedValues(ctx, t, tab, 2)
	require.NoError(t, err)
}

func deleteTablet(t *testing.T, tab *cluster.Vttablet) {
	err := clusterInstance.VtctlclientProcess.ExecuteCommand(
		"DeleteTablet",
		"-allow_master",
		tab.Alias)
	require.NoError(t, err)
}

// endregion

// region get info

func getNewMaster(t *testing.T) *cluster.Vttablet {
	var newMaster *cluster.Vttablet
	for _, tablet := range []*cluster.Vttablet{tab2, tab3, tab4} {
		if isHealthyMasterTablet(t, tablet) {
			newMaster = tablet
			break
		}
	}
	require.NotNil(t, newMaster)
	return newMaster
}

func getShardReplicationPositions(t *testing.T, keyspaceName, shardName string, doPrint bool) []string {
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shardName))
	require.NoError(t, err)
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	if doPrint {
		log.Infof("Positions:")
		for _, pos := range strArray {
			log.Infof("\t%s", pos)
		}
	}
	return strArray
}

// endregion
