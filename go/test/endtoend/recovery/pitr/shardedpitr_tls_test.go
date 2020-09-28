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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// Test pitr (Point in time recovery).
// -------------------------------------------
// The following test will:
// - create a shard with master and replica
// - run InitShardMaster
// - insert some data using vtgate (e.g. here we have inserted rows 1,2)
// - verify the replication
// - take backup of replica
// - insert some data using vtgate (e.g. we inserted rows 3 4 5 6), while inserting row-4, note down the time (restoreTime1)
// - perform a resharding to create 2 shards (-80, 80-), and delete the old shard
// - insert some data using vtgate (e.g. we will insert 7 8 9 10) and verify we get required number of rows in -80, 80- shard
// - take backup of both shards
// - insert some more data using vtgate (e.g. we will insert 11 12 13 14 15), while inserting row-13, note down the time (restoreTime2)
// - note down the current time (restoreTime3)

// - Till now we did all the presetup for assertions

// - asserting that restoring to restoreTime1 (going from 2 shards to 1 shard) is working, i.e. we should get 4 rows.
// - asserting that while restoring if we give small timeout value, it will restore upto to the last available backup (asserting only -80 shard)
// - asserting that restoring to restoreTime2 (going from 2 shards to 2 shards with past time) is working, it will assert for both shards
// - asserting that restoring to restoreTime3 is working, we should get complete data after restoring,  as we have in existing shards.
func TestTLSPITRRecovery(t *testing.T) {
	defer cluster.PanicHandler(nil)
	initializeCluster(t)
	defer clusterInstance.Teardown()

	// Creating the table
	_, err := master.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)

	insertRow(t, 1, "prd-1", false)
	insertRow(t, 2, "prd-2", false)

	cluster.VerifyRowsInTabletForTable(t, replica, keyspaceName, 2, "product")

	// backup the replica
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", replica.Alias)
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

	// creating restore keyspace with snapshot time as restoreTime1
	//   Need to test this before resharding and we tear down the
	//   original mysql replica, which we use as a binlog source
	createRestoreKeyspace(t, restoreTime1, restoreKS1Name)

	// Launching a recovery tablet which recovers data from the master till the restoreTime1
	tlsTestTabletRecovery(t, replica, "2m", restoreKS1Name, "0", "INT64(4)")

	// starting resharding process
	tlsPerformResharding(t)

	for counter := 7; counter <= 10; counter++ {
		insertRow(t, counter, fmt.Sprintf("prd-%d", counter), false)
	}

	// wait till all the shards have required data
	cluster.VerifyRowsInTabletForTable(t, shard0Replica, keyspaceName, 6, "product")
	cluster.VerifyRowsInTabletForTable(t, shard1Replica, keyspaceName, 4, "product")

	// take the backup (to simulate the regular backup)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", shard0Replica.Alias)
	require.NoError(t, err)
	// take the backup (to simulate the regular backup)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", shard1Replica.Alias)
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
	tlsTestTabletRecovery(t, shard0Replica, "1ms", restoreKS2Name, "-80", "INT64(6)")

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
	tlsTestTabletRecovery(t, shard0Replica, "2m", restoreKS2Name, "-80", "INT64(7)")

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
	tlsTestTabletRecovery(t, shard1Replica, "2m", restoreKS2Name, "80-", "INT64(6)")

	// test the recovery with timetorecover > (timestamp of last binlog event in binlog server)
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
	tlsTestTabletRecovery(t, shard0Replica, "2m", restoreKS3Name, "-80", "INT64(8)")

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
	tlsTestTabletRecovery(t, shard1Replica, "2m", restoreKS3Name, "80-", "INT64(7)")
}

func tlsPerformResharding(t *testing.T) {
	err := clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	require.NoError(t, err)

	err = clusterInstance.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "ks/-80", shard0Master.Alias)
	require.NoError(t, err)

	err = clusterInstance.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "ks/80-", shard1Master.Alias)
	require.NoError(t, err)

	// we need to create the schema, and the worker will do data copying
	for _, keyspaceShard := range []string{"ks/-80", "ks/80-"} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "ks/0", keyspaceShard)
		require.NoError(t, err)
	}

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Reshard", "ks.reshardWorkflow", "0", "-80,80-")
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SwitchReads", "-tablet_type=rdonly", "ks.reshardWorkflow")
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SwitchReads", "-tablet_type=replica", "ks.reshardWorkflow")
	require.NoError(t, err)

	// then serve master from the split shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SwitchWrites", "ks.reshardWorkflow")
	require.NoError(t, err)

	// remove the original tablets in the original shard
	removeTablets(t, []*cluster.Vttablet{master, replica})

	for _, tablet := range []*cluster.Vttablet{replica} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		require.NoError(t, err)
	}
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", master.Alias)
	require.NoError(t, err)

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "ks")
	require.NoError(t, err)

	// delete the original shard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", "ks/0")
	require.NoError(t, err)

	// Restart vtgate process
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)

	err = clusterInstance.VtgateProcess.Setup()
	require.NoError(t, err)

	clusterInstance.WaitForTabletsToHealthyInVtgate()
}

func tlsTestTabletRecovery(t *testing.T, tabletForBinlogs *cluster.Vttablet, lookupTimeout, restoreKeyspaceName, shardName, expectedRows string) {
	recoveryTablet := clusterInstance.NewVttabletInstance("replica", 0, cell)
	tlsLaunchRecoveryTablet(t, recoveryTablet, tabletForBinlogs, lookupTimeout, restoreKeyspaceName, shardName)

	sqlRes, err := recoveryTablet.VttabletProcess.QueryTablet(getCountID, keyspaceName, true)
	require.NoError(t, err)
	assert.Equal(t, expectedRows, sqlRes.Rows[0][0].String())

	defer recoveryTablet.MysqlctlProcess.Stop()
	defer recoveryTablet.VttabletProcess.TearDown()
}

func tlsLaunchRecoveryTablet(t *testing.T, tablet *cluster.Vttablet, tabletForBinlogs *cluster.Vttablet, lookupTimeout, restoreKeyspaceName, shardName string) {
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
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
		clusterInstance.EnableSemiSync)
	tablet.Alias = tablet.VttabletProcess.TabletPath
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.Keyspace = restoreKeyspaceName
	tablet.VttabletProcess.EnableSemiSync = true

	certDir := tabletForBinlogs.VttabletProcess.Directory + "/data"
	tablet.VttabletProcess.ExtraArgs = []string{
		"-disable_active_reparents",
		"-enable_replication_reporter=false",
		"-init_db_name_override", dbName,
		"-init_tablet_type", "replica",
		"-init_keyspace", restoreKeyspaceName,
		"-init_shard", shardName,
		"-binlog_host", clusterInstance.Hostname,
		"-binlog_port", fmt.Sprintf("%d", tabletForBinlogs.MySQLPort),
		"-binlog_user", mysqlUserName,
		"-binlog_password", mysqlPassword,
		"-binlog_ssl_ca", certDir + "/ca.pem",
		"-binlog_ssl_server_name", getCNFromCertPEM(certDir + "/server-cert.pem"),
		"-pitr_gtid_lookup_timeout", lookupTimeout,
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_tablet_type", "replica",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-serving_state_grace_period", "1s",
	}
	tablet.VttabletProcess.ServingStatus = ""

	err = tablet.VttabletProcess.Setup()
	require.NoError(t, err)

	tablet.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 20*time.Second)
}

func getCNFromCertPEM(filename string) string {
	pemBytes, _ := ioutil.ReadFile(filename)
	block, _ := pem.Decode(pemBytes)
	cert, _ := x509.ParseCertificate(block.Bytes)
	rdn := cert.Subject.ToRDNSequence()[0][0]
	t := rdn.Type

	// 2.5.4.3 is ASN OID for "CN"
	if len(t) == 4 && t[0] == 2 && t[1] == 5 && t[2] == 4 && t[3] == 3 {
		return fmt.Sprintf("%s", rdn.Value)
	}
	// As good a fallback as any
	return "localhost"
}
