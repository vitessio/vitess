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

package utils

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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	KeyspaceName = "ks"
	dbName       = "vt_" + KeyspaceName
	username     = "vt_dba"
	Hostname     = "localhost"
	insertVal    = 1
	insertSQL    = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	sqlSchema    = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB	
`
	cell1         = "zone1"
	cell2         = "zone2"
	ShardName     = "0"
	KeyspaceShard = KeyspaceName + "/" + ShardName
)

//region cluster setup/teardown

// SetupReparentClusterLegacy is used to setup the reparent cluster
func SetupReparentClusterLegacy(t *testing.T, enableSemiSync bool) *cluster.LocalProcessCluster {
	return setupClusterLegacy(context.Background(), t, ShardName, []string{cell1, cell2}, []int{3, 1}, enableSemiSync)
}

// SetupReparentCluster is used to setup the reparent cluster
func SetupReparentCluster(t *testing.T, enableSemiSync bool) *cluster.LocalProcessCluster {
	return setupCluster(context.Background(), t, ShardName, []string{cell1, cell2}, []int{3, 1}, enableSemiSync)
}

// SetupRangeBasedCluster sets up the range based cluster
func SetupRangeBasedCluster(ctx context.Context, t *testing.T) *cluster.LocalProcessCluster {
	return setupClusterLegacy(ctx, t, ShardName, []string{cell1}, []int{2}, true)
}

// TeardownCluster is used to teardown the reparent cluster
func TeardownCluster(clusterInstance *cluster.LocalProcessCluster) {
	clusterInstance.Teardown()
}

func setupCluster(ctx context.Context, t *testing.T, shardName string, cells []string, numTablets []int, enableSemiSync bool) *cluster.LocalProcessCluster {
	var tablets []*cluster.Vttablet
	clusterInstance := cluster.NewCluster(cells[0], Hostname)
	keyspace := &cluster.Keyspace{Name: KeyspaceName}

	durability := "none"
	if enableSemiSync {
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--enable_semi_sync")
		durability = "semi_sync"
	}

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err, "Error starting topo")
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cells[0])
	require.NoError(t, err, "Error managing topo")
	numCell := 1
	for numCell < len(cells) {
		err = clusterInstance.VtctlProcess.AddCellInfo(cells[numCell])
		require.NoError(t, err, "Error managing topo")
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

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
		"--lock_tables_timeout", "5s",
		"--init_populate_metadata",
		"--track_schema_versions=true",
		// disabling online-ddl for reparent tests. This is done to reduce flakiness.
		// All the tests in this package reparent frequently between different tablets
		// This means that Promoting a tablet to primary is sometimes immediately followed by a DemotePrimary call.
		// In this case, the close method and initSchema method of the onlineDDL executor race.
		// If the initSchema acquires the lock, then it takes about 30 seconds for it to run during which time the
		// DemotePrimary rpc is stalled!
		"--queryserver_enable_online_ddl=false",
		// disabling active reparents on the tablet since we don't want the replication manager
		// to fix replication if it is stopped. Some tests deliberately do that. Also, we don't want
		// the replication manager to silently fix the replication in case ERS or PRS mess up. All the
		// tests in this test suite should work irrespective of this flag. Each run of ERS, PRS should be
		// setting up the replication correctly.
		"--disable_active_reparents")

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err, "Cannot launch cluster")

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			require.NoError(t, err, "Error starting start mysql")
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			clusterInstance.PrintMysqlctlLogFiles()
			require.FailNow(t, "Error starting mysql: %s", err.Error())
		}
	}
	if clusterInstance.VtctlMajorVersion >= 14 {
		vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)
		out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", KeyspaceName, fmt.Sprintf("--durability-policy=%s", durability))
		require.NoError(t, err, out)
	}

	setupShard(ctx, t, clusterInstance, shardName, tablets)
	return clusterInstance
}

func setupShard(ctx context.Context, t *testing.T, clusterInstance *cluster.LocalProcessCluster, shardName string, tablets []*cluster.Vttablet) {
	for _, tablet := range tablets {
		// Start the tablet
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Initialize shard
	err := clusterInstance.VtctlclientProcess.InitializeShard(KeyspaceName, shardName, tablets[0].Cell, tablets[0].TabletUID)
	require.NoError(t, err)

	ValidateTopology(t, clusterInstance, true)

	// create Tables
	RunSQL(ctx, t, sqlSchema, tablets[0])

	CheckPrimaryTablet(t, clusterInstance, tablets[0])

	ValidateTopology(t, clusterInstance, false)
	time.Sleep(100 * time.Millisecond) // wait for replication to catchup
	strArray := GetShardReplicationPositions(t, clusterInstance, KeyspaceName, shardName, true)
	assert.Equal(t, len(tablets), len(strArray))
	assert.Contains(t, strArray[0], "primary") // primary first
}

func setupClusterLegacy(ctx context.Context, t *testing.T, shardName string, cells []string, numTablets []int, enableSemiSync bool) *cluster.LocalProcessCluster {
	var tablets []*cluster.Vttablet
	clusterInstance := cluster.NewCluster(cells[0], Hostname)
	keyspace := &cluster.Keyspace{Name: KeyspaceName}

	durability := "none"
	if enableSemiSync {
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--enable_semi_sync")
		durability = "semi_sync"
	}

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err, "Error starting topo")
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cells[0])
	require.NoError(t, err, "Error managing topo")
	numCell := 1
	for numCell < len(cells) {
		err = clusterInstance.VtctlProcess.AddCellInfo(cells[numCell])
		require.NoError(t, err, "Error managing topo")
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

	disableReplicationFlag := "--disable_active_reparents"
	if clusterInstance.VtTabletMajorVersion >= 15 {
		disableReplicationFlag = "--disable-replication-manager"
	}
	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
		"--lock_tables_timeout", "5s",
		"--init_populate_metadata",
		"--track_schema_versions=true",
		// disabling online-ddl for reparent tests. This is done to reduce flakiness.
		// All the tests in this package reparent frequently between different tablets
		// This means that Promoting a tablet to primary is sometimes immediately followed by a DemotePrimary call.
		// In this case, the close method and initSchema method of the onlineDDL executor race.
		// If the initSchema acquires the lock, then it takes about 30 seconds for it to run during which time the
		// DemotePrimary rpc is stalled!
		"--queryserver_enable_online_ddl=false",
		// disabling active reparents on the tablet since we don't want the replication manager
		// to fix replication if it is stopped. Some tests deliberately do that. Also, we don't want
		// the replication manager to silently fix the replication in case ERS or PRS mess up. All the
		// tests in this test suite should work irrespective of this flag. Each run of ERS, PRS should be
		// setting up the replication correctly.
		disableReplicationFlag)

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err, "Cannot launch cluster")

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			require.NoError(t, err, "Error starting start mysql")
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			clusterInstance.PrintMysqlctlLogFiles()
			require.FailNow(t, "Error starting mysql: %s", err.Error())
		}
	}

	if clusterInstance.VtctlMajorVersion >= 14 {
		vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)
		out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", KeyspaceName, fmt.Sprintf("--durability-policy=%s", durability))
		require.NoError(t, err, out)
	}

	setupShardLegacy(ctx, t, clusterInstance, shardName, tablets)
	return clusterInstance
}

func setupShardLegacy(ctx context.Context, t *testing.T, clusterInstance *cluster.LocalProcessCluster, shardName string, tablets []*cluster.Vttablet) {
	for _, tablet := range tablets {
		// create database
		err := tablet.VttabletProcess.CreateDB(KeyspaceName)
		require.NoError(t, err)
		tablet.VttabletProcess.SupportsBackup = false
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardPrimary", "--",
		"--force", fmt.Sprintf("%s/%s", KeyspaceName, shardName), tablets[0].Alias)
	require.NoError(t, err)

	ValidateTopology(t, clusterInstance, true)

	// create Tables
	RunSQL(ctx, t, sqlSchema, tablets[0])

	CheckPrimaryTablet(t, clusterInstance, tablets[0])

	ValidateTopology(t, clusterInstance, false)
	time.Sleep(100 * time.Millisecond) // wait for replication to catchup
	strArray := GetShardReplicationPositions(t, clusterInstance, KeyspaceName, shardName, true)
	assert.Equal(t, len(tablets), len(strArray))
	assert.Contains(t, strArray[0], "primary") // primary first
}

//endregion

// region database queries
func getMysqlConnParam(tablet *cluster.Vttablet) mysql.ConnParams {
	connParams := mysql.ConnParams{
		Uname:      username,
		DbName:     dbName,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	return connParams
}

// RunSQL is used to run a SQL command directly on the MySQL instance of a vttablet
func RunSQL(ctx context.Context, t *testing.T, sql string, tablet *cluster.Vttablet) *sqltypes.Result {
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

// region ers, prs

// Prs runs PRS
func Prs(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet) (string, error) {
	return PrsWithTimeout(t, clusterInstance, tab, false, "", "")
}

// PrsAvoid runs PRS
func PrsAvoid(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet) (string, error) {
	return PrsWithTimeout(t, clusterInstance, tab, true, "", "")
}

// PrsWithTimeout runs PRS
func PrsWithTimeout(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet, avoid bool, actionTimeout, waitTimeout string) (string, error) {
	args := []string{
		"PlannedReparentShard", "--",
		"--keyspace_shard", fmt.Sprintf("%s/%s", KeyspaceName, ShardName)}
	if actionTimeout != "" {
		args = append(args, "--action_timeout", actionTimeout)
	}
	if waitTimeout != "" {
		args = append(args, "--wait_replicas_timeout", waitTimeout)
	}
	if avoid {
		args = append(args, "--avoid_tablet")
	} else {
		args = append(args, "--new_primary")
	}
	args = append(args, tab.Alias)
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	return out, err
}

// Ers runs the ERS
func Ers(clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet, totalTimeout, waitReplicasTimeout string) (string, error) {
	return ErsIgnoreTablet(clusterInstance, tab, totalTimeout, waitReplicasTimeout, nil, false)
}

// ErsIgnoreTablet is used to run ERS
func ErsIgnoreTablet(clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet, timeout, waitReplicasTimeout string, tabletsToIgnore []*cluster.Vttablet, preventCrossCellPromotion bool) (string, error) {
	var args []string
	if timeout != "" {
		args = append(args, "--action_timeout", timeout)
	}
	args = append(args, "EmergencyReparentShard", "--", "--keyspace_shard", fmt.Sprintf("%s/%s", KeyspaceName, ShardName))
	if tab != nil {
		args = append(args, "--new_primary", tab.Alias)
	}
	if waitReplicasTimeout != "" {
		args = append(args, "--wait_replicas_timeout", waitReplicasTimeout)
	}
	if preventCrossCellPromotion {
		args = append(args, "--prevent_cross_cell_promotion=true")
	}
	if len(tabletsToIgnore) != 0 {
		tabsString := ""
		for _, vttablet := range tabletsToIgnore {
			if tabsString == "" {
				tabsString = vttablet.Alias
			} else {
				tabsString = tabsString + "," + vttablet.Alias
			}
		}
		args = append(args, "--ignore_replicas", tabsString)
	}
	return clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
}

// ErsWithVtctl runs ERS via vtctl binary
func ErsWithVtctl(clusterInstance *cluster.LocalProcessCluster) (string, error) {
	args := []string{"EmergencyReparentShard", "--", "--keyspace_shard", fmt.Sprintf("%s/%s", KeyspaceName, ShardName)}
	return clusterInstance.VtctlProcess.ExecuteCommandWithOutput(args...)
}

// endregion

// region validations

// ValidateTopology is used to validate the topology
func ValidateTopology(t *testing.T, clusterInstance *cluster.LocalProcessCluster, pingTablets bool) {
	args := []string{"Validate"}

	if pingTablets {
		args = append(args, "--", "--ping-tablets=true")
	}
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	require.Empty(t, out)
	require.NoError(t, err)
}

// ConfirmReplication confirms that the replication is working properly
func ConfirmReplication(t *testing.T, primary *cluster.Vttablet, replicas []*cluster.Vttablet) int {
	ctx := context.Background()
	insertVal++
	n := insertVal // unique value ...
	// insert data into the new primary, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, n, n)
	RunSQL(ctx, t, insertSQL, primary)
	time.Sleep(100 * time.Millisecond)
	for _, tab := range replicas {
		err := CheckInsertedValues(ctx, t, tab, n)
		require.NoError(t, err)
	}
	return n
}

// ConfirmOldPrimaryIsHangingAround confirms that the old primary is hanging around
func ConfirmOldPrimaryIsHangingAround(t *testing.T, clusterInstance *cluster.LocalProcessCluster) {
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("Validate")
	require.Error(t, err)
	require.Contains(t, out, "already has primary")
}

// CheckPrimaryTablet makes sure the tablet type is primary, and its health check agrees.
func CheckPrimaryTablet(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.NoError(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tabletInfo.GetType())

	// make sure the health stream is updated
	shrs, err := clusterInstance.StreamTabletHealth(context.Background(), tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tabletType)
}

// isHealthyPrimaryTablet will return if tablet is primary AND healthy.
func isHealthyPrimaryTablet(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet) bool {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.Nil(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.Nil(t, err)
	if tabletInfo.GetType() != topodatapb.TabletType_PRIMARY {
		return false
	}

	// make sure the health stream is updated
	shrs, err := clusterInstance.StreamTabletHealth(context.Background(), tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	return tabletType == topodatapb.TabletType_PRIMARY
}

// CheckInsertedValues checks that the given value is present in the given tablet
func CheckInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, index int) error {
	// wait until it gets the data
	timeout := time.Now().Add(15 * time.Second)
	i := 0
	for time.Now().Before(timeout) {
		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
		qr := RunSQL(ctx, t, selectSQL, tablet)
		if len(qr.Rows) == 1 {
			return nil
		}
		t := time.Duration(300 * i)
		time.Sleep(t * time.Millisecond)
		i++
	}
	return fmt.Errorf("data is not yet replicated on tablet %s", tablet.Alias)
}

func CheckSemiSyncSetupCorrectly(t *testing.T, tablet *cluster.Vttablet, semiSyncVal string) {
	dbVar, err := tablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", "")
	require.NoError(t, err)
	require.Equal(t, semiSyncVal, dbVar)
}

// CheckCountOfInsertedValues checks that the number of inserted values matches the given count on the given tablet
func CheckCountOfInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, count int) error {
	selectSQL := "select * from vt_insert_test"
	qr := RunSQL(ctx, t, selectSQL, tablet)
	if len(qr.Rows) == count {
		return nil
	}
	return fmt.Errorf("count does not match on the tablet %s", tablet.Alias)
}

// endregion

// region tablet operations

// StopTablet stops the tablet
func StopTablet(t *testing.T, tab *cluster.Vttablet, stopDatabase bool) {
	err := tab.VttabletProcess.TearDownWithTimeout(30 * time.Second)
	require.NoError(t, err)
	if stopDatabase {
		err = tab.MysqlctlProcess.Stop()
		require.NoError(t, err)
	}
}

// RestartTablet restarts the tablet
func RestartTablet(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet) {
	tab.MysqlctlProcess.InitMysql = false
	err := tab.MysqlctlProcess.Start()
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tab, tab.Cell, KeyspaceName, Hostname, ShardName)
	require.NoError(t, err)
}

// ResurrectTablet is used to resurrect the given tablet
func ResurrectTablet(ctx context.Context, t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet) {
	tab.MysqlctlProcess.InitMysql = false
	err := tab.MysqlctlProcess.Start()
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tab, tab.Cell, KeyspaceName, Hostname, ShardName)
	require.NoError(t, err)

	// As there is already a primary the new replica will come directly in SERVING state
	tab.VttabletProcess.ServingStatus = "SERVING"
	// Start the tablet
	err = tab.VttabletProcess.Setup()
	require.NoError(t, err)

	err = CheckInsertedValues(ctx, t, tab, insertVal)
	require.NoError(t, err)
}

// DeleteTablet is used to delete the given tablet
func DeleteTablet(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tab *cluster.Vttablet) {
	err := clusterInstance.VtctlclientProcess.ExecuteCommand(
		"DeleteTablet", "--",
		"--allow_primary",
		tab.Alias)
	require.NoError(t, err)
}

// endregion

// region get info

// GetNewPrimary is used to find the new primary of the cluster.
func GetNewPrimary(t *testing.T, clusterInstance *cluster.LocalProcessCluster) *cluster.Vttablet {
	var newPrimary *cluster.Vttablet
	for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets[1:] {
		if isHealthyPrimaryTablet(t, clusterInstance, tablet) {
			newPrimary = tablet
			break
		}
	}
	require.NotNil(t, newPrimary)
	return newPrimary
}

// GetShardReplicationPositions gets the shards replication positions.
func GetShardReplicationPositions(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceName, shardName string, doPrint bool) []string {
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

// CheckReplicaStatus checks the replication status and asserts that the replication is stopped
func CheckReplicaStatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet) {
	qr := RunSQL(ctx, t, "show slave status", tablet)
	IOThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	SQLThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	assert.Equal(t, IOThreadRunning, "VARCHAR(\"No\")")
	assert.Equal(t, SQLThreadRunning, "VARCHAR(\"No\")")
}

// CheckReparentFromOutside checks that cluster was reparented from outside
func CheckReparentFromOutside(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet, downPrimary bool, baseTime int64) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell1, KeyspaceShard)
	require.Nil(t, err, "error should be Nil")
	if !downPrimary {
		assertNodeCount(t, result, int(3))
	} else {
		assertNodeCount(t, result, int(2))
	}

	// make sure the primary status page says it's the primary
	status := tablet.VttabletProcess.GetStatus()
	assert.Contains(t, status, "Tablet Type: PRIMARY")

	// make sure the primary health stream says it's the primary too
	// (health check is disabled on these servers, force it first)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	require.NoError(t, err)

	shrs, err := clusterInstance.StreamTabletHealth(context.Background(), tablet, 1)
	require.NoError(t, err)

	streamHealthResponse := shrs[0]
	assert.Equal(t, streamHealthResponse.Target.TabletType, topodatapb.TabletType_PRIMARY)
	assert.True(t, streamHealthResponse.TabletExternallyReparentedTimestamp >= baseTime)
}

// WaitForReplicationPosition waits for tablet B to catch up to the replication position of tablet A.
func WaitForReplicationPosition(t *testing.T, tabletA *cluster.Vttablet, tabletB *cluster.Vttablet) error {
	posA, _ := cluster.GetPrimaryPosition(t, *tabletA, Hostname)
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		posB, _ := cluster.GetPrimaryPosition(t, *tabletB, Hostname)
		if positionAtLeast(t, tabletB, posA, posB) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed to catch up on replication position")
}

// positionAtLeast executes the command position at_least
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
	resultMap := make(map[string]any)
	err := json.Unmarshal([]byte(result), &resultMap)
	require.NoError(t, err)

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}

// CheckDBvar checks the db var
func CheckDBvar(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show variables like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

// CheckDBstatus checks the db status
func CheckDBstatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show status like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

// SetReplicationSourceFailed returns true if the given output from PRS had failed because the given tablet was
// unable to setReplicationSource. Since some tests are used in upgrade-downgrade testing, we need this function to
// work with different versions of vtctl.
func SetReplicationSourceFailed(tablet *cluster.Vttablet, prsOut string) bool {
	return strings.Contains(prsOut, fmt.Sprintf("tablet %s failed to SetReplicationSource", tablet.Alias))
}

// CheckReplicationStatus checks that the replication for sql and io threads is setup as expected
func CheckReplicationStatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, sqlThreadRunning bool, ioThreadRunning bool) {
	res := RunSQL(ctx, t, "show slave status;", tablet)
	if ioThreadRunning {
		require.Equal(t, "Yes", res.Rows[0][10].ToString())
	} else {
		require.Equal(t, "No", res.Rows[0][10].ToString())
	}

	if sqlThreadRunning {
		require.Equal(t, "Yes", res.Rows[0][11].ToString())
	} else {
		require.Equal(t, "No", res.Rows[0][11].ToString())
	}
}

// ReplicationThreadsStatus returns the status of the IO and SQL thread. It reads the result of the replication status
// based on the vtctl major version provided. It also uses the vttabletVersion to assert on the expectation of the new fields
// being unknown for the old vttablets and that they match for the new vttablets
func ReplicationThreadsStatus(t *testing.T, status *replicationdatapb.Status, vtctlVersion, vttabletVersion int) (bool, bool) {
	if vttabletVersion == 13 {
		// If vttablet is version 13, then the new fields should be unknown
		require.Equal(t, mysql.ReplicationStateUnknown, mysql.ReplicationState(status.IoState))
		require.Equal(t, mysql.ReplicationStateUnknown, mysql.ReplicationState(status.SqlState))
	} else {
		// For the new vttablet, the new parameters should not be unknown. Moreover, the old parameters should also be provided
		// and should agree with the new ones
		require.NotEqual(t, mysql.ReplicationStateUnknown, mysql.ReplicationState(status.IoState))
		require.NotEqual(t, mysql.ReplicationStateUnknown, mysql.ReplicationState(status.SqlState))
		if vttabletVersion == 14 {
			require.Equal(t, status.IoThreadRunning, mysql.ReplicationState(status.IoState) == mysql.ReplicationStateRunning || mysql.ReplicationState(status.IoState) == mysql.ReplicationStateConnecting)
			require.Equal(t, status.SqlThreadRunning, mysql.ReplicationState(status.SqlState) == mysql.ReplicationStateRunning || mysql.ReplicationState(status.SqlState) == mysql.ReplicationStateConnecting)
		}
	}

	// if vtctlVersion provided is 13, then we should read the old parameters, since that is what old vtctl would do
	if vtctlVersion == 13 {
		return status.IoThreadRunning, status.SqlThreadRunning
	}
	// If we are at the latest vtctl version, we should read the latest parameters if provided otherwise the old ones
	ioState := mysql.ReplicationState(status.IoState)
	ioThread := status.IoThreadRunning
	if ioState != mysql.ReplicationStateUnknown {
		ioThread = ioState == mysql.ReplicationStateRunning || ioState == mysql.ReplicationStateConnecting
	}
	sqlState := mysql.ReplicationState(status.SqlState)
	sqlThread := status.SqlThreadRunning
	if sqlState != mysql.ReplicationStateUnknown {
		sqlThread = sqlState == mysql.ReplicationStateRunning || sqlState == mysql.ReplicationStateConnecting
	}
	return ioThread, sqlThread
}

// TmcFullStatus retuns the result of the TabletManagerClient RPC FullStatus
func TmcFullStatus(ctx context.Context, tablet *cluster.Vttablet) (*replicationdatapb.FullStatus, error) {
	// create tablet manager client
	tmClient := tmc.NewClient()

	vttablet := getTablet(tablet.GrpcPort)
	return tmClient.FullStatus(ctx, vttablet)
}

func getTablet(tabletGrpcPort int) *topodatapb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &topodatapb.Tablet{Hostname: Hostname, PortMap: portMap}
}
