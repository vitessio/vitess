/*
Copyright 2020 The Vitess Authors.

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

package pitr

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
)

var (
	createTable = `create table product (id bigint(20) primary key, name char(10), created bigint(20));`
	insertTable = `insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
	getCountID  = `select count(*) from product`
)

var (
	clusterInstance *cluster.LocalProcessCluster

	primary        *cluster.Vttablet
	replica1       *cluster.Vttablet
	replica2       *cluster.Vttablet
	shard0Primary  *cluster.Vttablet
	shard0Replica1 *cluster.Vttablet
	shard0Replica2 *cluster.Vttablet
	shard1Primary  *cluster.Vttablet
	shard1Replica1 *cluster.Vttablet
	shard1Replica2 *cluster.Vttablet

	cell                   = "zone1"
	hostname               = "localhost"
	binlogHost             = "127.0.0.1"
	keyspaceName           = "ks"
	restoreKS1Name         = "restoreks1"
	restoreKS2Name         = "restoreks2"
	restoreKS3Name         = "restoreks3"
	shardName              = "0"
	shard0Name             = "-80"
	shard1Name             = "80-"
	dbName                 = "vt_ks"
	mysqlUserName          = "vt_dba"
	mysqlPassword          = "VtDbaPass"
	dbCredentialFile       = ""
	initDBFileWithPassword = ""
	vSchema                = `{
		"sharded": true,
		"vindexes": {
			"hash_index": {
				"type": "hash"
			}
		},
		"tables": {
			"product": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			}
		}
	}`
	commonTabletArg = []string{
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--serving_state_grace_period", "1s"}

	defaultTimeout = 30 * time.Second
	defaultTick    = 1 * time.Second
)

// Test pitr (Point in time recovery).
// -------------------------------------------
// The following test will:
// - create a shard with primary and replica
// - run InitShardPrimary
// - point binlog server to primary
// - insert some data using vtgate (e.g. here we have inserted rows 1,2)
// - verify the replication
// - take backup of replica
// - insert some data using vtgate (e.g. we inserted rows 3 4 5 6), while inserting row-4, note down the time (restoreTime1)
// - perform a resharding to create 2 shards (-80, 80-), and delete the old shard
// - point binlog server to primary of both shards
// - insert some data using vtgate (e.g. we will insert 7 8 9 10) and verify we get required number of rows in -80, 80- shard
// - take backup of both shards
// - insert some more data using vtgate (e.g. we will insert 11 12 13 14 15), while inserting row-13, note down the time (restoreTime2)
// - note down the current time (restoreTime3)

// - Till now we did all the presetup for assertions

// - asserting that restoring to restoreTime1 (going from 2 shards to 1 shard) is working, i.e. we should get 4 rows.
// - asserting that while restoring if we give small timeout value, it will restore upto to the last available backup (asserting only -80 shard)
// - asserting that restoring to restoreTime2 (going from 2 shards to 2 shards with past time) is working, it will assert for both shards
// - asserting that restoring to restoreTime3 is working, we should get complete data after restoring,  as we have in existing shards.
func TestPITRRecovery(t *testing.T) {
	defer cluster.PanicHandler(nil)
	initializeCluster(t)
	defer clusterInstance.Teardown()

	//start the binlog server and point it to primary
	bs := startBinlogServer(t, primary)
	defer bs.stop()

	// Creating the table
	_, err := primary.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)

	insertRow(t, 1, "prd-1", false)
	insertRow(t, 2, "prd-2", false)

	cluster.VerifyRowsInTabletForTable(t, replica1, keyspaceName, 2, "product")

	// backup the replica
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.NoError(t, err)

	// check that the backup shows up in the listing
	output, err := clusterInstance.ListBackups("ks/0")
	require.NoError(t, err)
	assert.Equal(t, 1, len(output))

	// now insert some more data to simulate the changes after regular backup
	// every insert has some time lag/difference to simulate the time gap between rows
	// and when we recover to certain time, this time gap will be able to identify the exact eligible row
	var restoreTime1 string
	for counter := 3; counter <= 6; counter++ {
		if counter == 4 { // we want to recovery till this, so noting the time
			tm := time.Now().Add(1 * time.Second).UTC()
			restoreTime1 = tm.Format(time.RFC3339)
		}
		insertRow(t, counter, fmt.Sprintf("prd-%d", counter), true)
	}

	// starting resharding process
	performResharding(t)

	//start the binlog server and point it to shard0Primary
	bs0 := startBinlogServer(t, shard0Primary)
	defer bs0.stop()

	//start the binlog server and point it to shard1Primary
	bs1 := startBinlogServer(t, shard1Primary)
	defer bs1.stop()

	for counter := 7; counter <= 10; counter++ {
		insertRow(t, counter, fmt.Sprintf("prd-%d", counter), false)
	}

	// wait till all the shards have required data
	cluster.VerifyRowsInTabletForTable(t, shard0Replica1, keyspaceName, 6, "product")
	cluster.VerifyRowsInTabletForTable(t, shard1Replica1, keyspaceName, 4, "product")

	// take the backup (to simulate the regular backup)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Backup", shard0Replica1.Alias)
	require.NoError(t, err)
	// take the backup (to simulate the regular backup)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Backup", shard1Replica1.Alias)
	require.NoError(t, err)

	backups, err := clusterInstance.ListBackups(keyspaceName + "/-80")
	require.NoError(t, err)
	require.Equal(t, len(backups), 1)

	backups, err = clusterInstance.ListBackups(keyspaceName + "/80-")
	require.NoError(t, err)
	require.Equal(t, len(backups), 1)

	// now insert some more data to simulate the changes after regular backup
	// every insert has some time lag/difference to simulate the time gap between rows
	// and when we recover to certain time, this time gap will be able to identify the exact eligible row
	var restoreTime2 string
	for counter := 11; counter <= 15; counter++ {
		if counter == 13 { // we want to recovery till this, so noting the time
			tm := time.Now().Add(1 * time.Second).UTC()
			restoreTime2 = tm.Format(time.RFC3339)
		}
		insertRow(t, counter, fmt.Sprintf("prd-%d", counter), true)
	}
	restoreTime3 := time.Now().UTC().Format(time.RFC3339)

	// creating restore keyspace with snapshot time as restoreTime1
	createRestoreKeyspace(t, restoreTime1, restoreKS1Name)

	// Launching a recovery tablet which recovers data from the primary till the restoreTime1
	testTabletRecovery(t, bs, "2m", restoreKS1Name, "0", "INT64(4)")

	// create restoreKeyspace with snapshot time as restoreTime2
	createRestoreKeyspace(t, restoreTime2, restoreKS2Name)

	// test the recovery with smaller binlog_lookup_timeout for shard0
	// since we have small lookup timeout, it will just get whatever available in the backup
	// mysql> select * from product;
	// +----+--------+------------+
	// | id | name   | created    |
	// +----+--------+------------+
	// |  1 | prd-1  | 1597219030 |
	// |  2 | prd-2  | 1597219030 |
	// |  3 | prd-3  | 1597219043 |
	// |  5 | prd-5  | 1597219045 |
	// |  9 | prd-9  | 1597219130 |
	// | 10 | prd-10 | 1597219130 |
	// +----+--------+------------+
	testTabletRecovery(t, bs0, "1ms", restoreKS2Name, "-80", "INT64(6)")

	// test the recovery with valid binlog_lookup_timeout for shard0 and getting the data till the restoreTime2
	// 	mysql> select * from product;
	// +----+--------+------------+
	// | id | name   | created    |
	// +----+--------+------------+
	// |  1 | prd-1  | 1597219030 |
	// |  2 | prd-2  | 1597219030 |
	// |  3 | prd-3  | 1597219043 |
	// |  5 | prd-5  | 1597219045 |
	// |  9 | prd-9  | 1597219130 |
	// | 10 | prd-10 | 1597219130 |
	// | 13 | prd-13 | 1597219141 |
	// +----+--------+------------+
	testTabletRecovery(t, bs0, "2m", restoreKS2Name, "-80", "INT64(7)")

	// test the recovery with valid binlog_lookup_timeout for shard1 and getting the data till the restoreTime2
	// 	mysql> select * from product;
	// +----+--------+------------+
	// | id | name   | created    |
	// +----+--------+------------+
	// |  4 | prd-4  | 1597219044 |
	// |  6 | prd-6  | 1597219046 |
	// |  7 | prd-7  | 1597219130 |
	// |  8 | prd-8  | 1597219130 |
	// | 11 | prd-11 | 1597219139 |
	// | 12 | prd-12 | 1597219140 |
	// +----+--------+------------+
	testTabletRecovery(t, bs1, "2m", restoreKS2Name, "80-", "INT64(6)")

	// test the recovery with timetorecover > (timestmap of last binlog event in binlog server)
	createRestoreKeyspace(t, restoreTime3, restoreKS3Name)

	// 	mysql> select * from product;
	// +----+--------+------------+
	// | id | name   | created    |
	// +----+--------+------------+
	// |  1 | prd-1  | 1597219030 |
	// |  2 | prd-2  | 1597219030 |
	// |  3 | prd-3  | 1597219043 |
	// |  5 | prd-5  | 1597219045 |
	// |  9 | prd-9  | 1597219130 |
	// | 10 | prd-10 | 1597219130 |
	// | 13 | prd-13 | 1597219141 |
	// | 15 | prd-15 | 1597219142 |
	// +----+--------+------------+
	testTabletRecovery(t, bs0, "2m", restoreKS3Name, "-80", "INT64(8)")

	// 	mysql> select * from product;
	// +----+--------+------------+
	// | id | name   | created    |
	// +----+--------+------------+
	// |  4 | prd-4  | 1597219044 |
	// |  6 | prd-6  | 1597219046 |
	// |  7 | prd-7  | 1597219130 |
	// |  8 | prd-8  | 1597219130 |
	// | 11 | prd-11 | 1597219139 |
	// | 12 | prd-12 | 1597219140 |
	// | 14 | prd-14 | 1597219142 |
	// +----+--------+------------+
	testTabletRecovery(t, bs1, "2m", restoreKS3Name, "80-", "INT64(7)")
}

func performResharding(t *testing.T) {
	err := clusterInstance.VtctldClientProcess.ApplyVSchema(keyspaceName, vSchema)
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "create", "--source-shards=0", "--target-shards=-80,80-", "--target-keyspace", "ks", "--workflow", "reshardWorkflow")
	require.NoError(t, err)

	waitTimeout := 30 * time.Second
	shard0Primary.VttabletProcess.WaitForVReplicationToCatchup(t, "ks.reshardWorkflow", dbName, sidecar.DefaultName, waitTimeout)
	shard1Primary.VttabletProcess.WaitForVReplicationToCatchup(t, "ks.reshardWorkflow", dbName, sidecar.DefaultName, waitTimeout)

	waitForNoWorkflowLag(t, clusterInstance, "ks.reshardWorkflow")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "SwitchTraffic", "--tablet-types=rdonly", "--target-keyspace", "ks", "--workflow", "reshardWorkflow")
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "SwitchTraffic", "--tablet-types=replica", "--target-keyspace", "ks", "--workflow", "reshardWorkflow")
	require.NoError(t, err)

	// then serve primary from the split shards
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "SwitchTraffic", "--tablet-types=primary", "--target-keyspace", "ks", "--workflow", "reshardWorkflow")
	require.NoError(t, err)

	// remove the original tablets in the original shard
	removeTablets(t, []*cluster.Vttablet{primary, replica1, replica2})

	for _, tablet := range []*cluster.Vttablet{replica1, replica2} {
		err = clusterInstance.VtctldClientProcess.ExecuteCommand("DeleteTablets", tablet.Alias)
		require.NoError(t, err)
	}
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", primary.Alias)
	require.NoError(t, err)

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("RebuildKeyspaceGraph", "ks")
	require.NoError(t, err)

	// delete the original shard
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("DeleteShards", "ks/0")
	require.NoError(t, err)

	// Restart vtgate process
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)

	err = clusterInstance.VtgateProcess.Setup()
	require.NoError(t, err)

	clusterInstance.WaitForTabletsToHealthyInVtgate()
}

func startBinlogServer(t *testing.T, primaryTablet *cluster.Vttablet) *binLogServer {
	bs, err := newBinlogServer(hostname, clusterInstance.GetAndReservePort())
	require.NoError(t, err)

	err = bs.start(mysqlSource{
		hostname: binlogHost,
		port:     primaryTablet.MysqlctlProcess.MySQLPort,
		username: mysqlUserName,
		password: mysqlPassword,
	})
	require.NoError(t, err)
	return bs
}

func removeTablets(t *testing.T, tablets []*cluster.Vttablet) {
	var mysqlProcs []*exec.Cmd
	for _, tablet := range tablets {
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
	}
	for _, proc := range mysqlProcs {
		err := proc.Wait()
		require.NoError(t, err)
	}
	for _, tablet := range tablets {
		tablet.VttabletProcess.TearDown()
	}
}

func initializeCluster(t *testing.T) {
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name: keyspaceName,
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, *keyspace)

	shard := &cluster.Shard{
		Name: shardName,
	}
	shard0 := &cluster.Shard{
		Name: shard0Name,
	}
	shard1 := &cluster.Shard{
		Name: shard1Name,
	}

	// Defining all the tablets
	primary = clusterInstance.NewVttabletInstance("replica", 0, "")
	replica1 = clusterInstance.NewVttabletInstance("replica", 0, "")
	replica2 = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard0Primary = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard0Replica1 = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard0Replica2 = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard1Primary = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard1Replica1 = clusterInstance.NewVttabletInstance("replica", 0, "")
	shard1Replica2 = clusterInstance.NewVttabletInstance("replica", 0, "")

	shard.Vttablets = []*cluster.Vttablet{primary, replica1, replica2}
	shard0.Vttablets = []*cluster.Vttablet{shard0Primary, shard0Replica1, shard0Replica2}
	shard1.Vttablets = []*cluster.Vttablet{shard1Primary, shard1Replica1, shard1Replica2}

	dbCredentialFile = cluster.WriteDbCredentialToTmp(clusterInstance.TmpDirectory)
	extraArgs := []string{"--db-credentials-file", dbCredentialFile}
	commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, commonTabletArg...)
	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--restore_from_backup")

	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard, *shard0, *shard1})
	require.NoError(t, err)
	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)
	out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err, out)

	initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	sql := string(initDb)
	// The original init_db.sql does not have any passwords. Here we update the init file with passwords
	sql, err = utils.GetInitDBSQL(sql, cluster.GetPasswordUpdateSQL(clusterInstance), "")
	require.NoError(t, err, "expected to load init_db file")
	initDBFileWithPassword = path.Join(clusterInstance.TmpDirectory, "init_db_with_passwords.sql")
	err = os.WriteFile(initDBFileWithPassword, []byte(sql), 0660)
	require.NoError(t, err, "expected to load init_db file")

	// Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			tablet.MysqlctlProcess.InitDBFile = initDBFileWithPassword
			tablet.VttabletProcess.DbPassword = mysqlPassword
			tablet.MysqlctlProcess.ExtraArgs = extraArgs
			proc, err := tablet.MysqlctlProcess.StartProcess()
			require.NoError(t, err)
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		err = proc.Wait()
		require.NoError(t, err)
	}

	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			err = tablet.VttabletProcess.Setup()
			require.NoError(t, err)
		}
	}

	err = clusterInstance.VtctldClientProcess.InitShardPrimary(keyspaceName, shard.Name, cell, primary.TabletUID)
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.InitShardPrimary(keyspaceName, shard0.Name, cell, shard0Primary.TabletUID)
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.InitShardPrimary(keyspaceName, shard1.Name, cell, shard1Primary.TabletUID)
	require.NoError(t, err)

	err = clusterInstance.StartVTOrc(keyspaceName)
	require.NoError(t, err)

	// Start vtgate
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)
}

func insertRow(t *testing.T, id int, productName string, isSlow bool) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertSmt := fmt.Sprintf(insertTable, id, productName)
	_, err = conn.ExecuteFetch(insertSmt, 1000, true)
	require.NoError(t, err)

	if isSlow {
		time.Sleep(1 * time.Second)
	}
}

func createRestoreKeyspace(t *testing.T, timeToRecover, restoreKeyspaceName string) {
	output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace",
		"--type=SNAPSHOT", "--base-keyspace="+keyspaceName,
		"--snapshot-timestamp", timeToRecover, restoreKeyspaceName)
	log.Info(output)
	require.NoError(t, err)
}

func testTabletRecovery(t *testing.T, binlogServer *binLogServer, lookupTimeout, restoreKeyspaceName, shardName, expectedRows string) {
	recoveryTablet := clusterInstance.NewVttabletInstance("replica", 0, cell)
	launchRecoveryTablet(t, recoveryTablet, binlogServer, lookupTimeout, restoreKeyspaceName, shardName)

	sqlRes, err := recoveryTablet.VttabletProcess.QueryTablet(getCountID, keyspaceName, true)
	require.NoError(t, err)
	assert.Equal(t, expectedRows, sqlRes.Rows[0][0].String())

	defer recoveryTablet.MysqlctlProcess.Stop()
	defer recoveryTablet.VttabletProcess.TearDown()
}

func launchRecoveryTablet(t *testing.T, tablet *cluster.Vttablet, binlogServer *binLogServer, lookupTimeout, restoreKeyspaceName, shardName string) {
	mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	require.NoError(t, err)
	tablet.MysqlctlProcess = *mysqlctlProcess
	extraArgs := []string{"--db-credentials-file", dbCredentialFile}
	tablet.MysqlctlProcess.InitDBFile = initDBFileWithPassword
	tablet.VttabletProcess.DbPassword = mysqlPassword
	tablet.MysqlctlProcess.ExtraArgs = extraArgs
	err = tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	tablet.VttabletProcess = cluster.VttabletProcessInstance(
		tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		clusterInstance.Cell,
		shardName,
		keyspaceName,
		clusterInstance.VtctldProcess.Port,
		tablet.Type,
		clusterInstance.TopoProcess.Port,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory,
		clusterInstance.VtTabletExtraArgs,
		clusterInstance.DefaultCharset)
	tablet.Alias = tablet.VttabletProcess.TabletPath
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.Keyspace = restoreKeyspaceName
	tablet.VttabletProcess.ExtraArgs = []string{
		"--disable_active_reparents",
		"--enable_replication_reporter=false",
		"--init_db_name_override", dbName,
		"--init_tablet_type", "replica",
		"--init_keyspace", restoreKeyspaceName,
		"--init_shard", shardName,
		"--binlog_host", binlogServer.hostname,
		"--binlog_port", fmt.Sprintf("%d", binlogServer.port),
		"--binlog_user", binlogServer.username,
		"--binlog_password", binlogServer.password,
		"--pitr_gtid_lookup_timeout", lookupTimeout,
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--serving_state_grace_period", "1s",
		"--db-credentials-file", dbCredentialFile,
	}
	tablet.VttabletProcess.ServingStatus = ""

	err = tablet.VttabletProcess.Setup()
	require.NoError(t, err)

	tablet.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, 20*time.Second)
}

// waitForNoWorkflowLag waits for the VReplication workflow's MaxVReplicationTransactionLag
// value to be 0.
func waitForNoWorkflowLag(t *testing.T, vc *cluster.LocalProcessCluster, ksWorkflow string) {
	lag := int64(0)
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		output, err := vc.VtctlclientProcess.ExecuteCommandWithOutput("Workflow", "--", ksWorkflow, "show")
		require.NoError(t, err)
		lag, err = jsonparser.GetInt([]byte(output), "MaxVReplicationTransactionLag")
		require.NoError(t, err)
		if lag == 0 {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("workflow %q did not eliminate VReplication lag before the timeout of %s; last seen MaxVReplicationTransactionLag: %d",
				ksWorkflow, defaultTimeout, lag))
		default:
			time.Sleep(defaultTick)
		}
	}
}
