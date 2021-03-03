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

package unshardedrecovery

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/test/endtoend/sharding/initialsharding"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	master           *cluster.Vttablet
	replica1         *cluster.Vttablet
	replica2         *cluster.Vttablet
	replica3         *cluster.Vttablet
	localCluster     *cluster.LocalProcessCluster
	newInitDBFile    string
	cell             = cluster.DefaultCell
	hostname         = "localhost"
	keyspaceName     = "ks"
	dbPassword       = "VtDbaPass"
	shardKsName      = fmt.Sprintf("%s/%s", keyspaceName, shardName)
	dbCredentialFile string
	shardName        = "0"
	commonTabletArg  = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-serving_state_grace_period", "1s"}
	recoveryKS1  = "recovery_ks1"
	recoveryKS2  = "recovery_ks2"
	vtInsertTest = `create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
	vSchema = `{
    "tables": {
        "vt_insert_test": {}
    }
}`
)

// TestMainImpl creates cluster for unsharded recovery testing.
func TestMainImpl(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		// Start topo server
		err := localCluster.StartTopo()
		if err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}
		localCluster.Keyspaces = append(localCluster.Keyspaces, *keyspace)

		dbCredentialFile = initialsharding.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := ioutil.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		sql := string(initDb)
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
		sql = sql + initialsharding.GetPasswordUpdateSQL(localCluster)
		ioutil.WriteFile(newInitDBFile, []byte(sql), 0666)

		extraArgs := []string{"-db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "-db-credentials-file", dbCredentialFile)

		shard := cluster.Shard{
			Name: shardName,
		}

		var mysqlProcs []*exec.Cmd
		for i := 0; i < 4; i++ {
			tabletType := "replica"
			if i == 0 {
				tabletType = "master"
			}
			tablet := localCluster.NewVttabletInstance(tabletType, 0, cell)
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
			if recovery.UseXb {
				tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, recovery.XbArgs...)
			}
			tablet.VttabletProcess.SupportsBackup = true
			tablet.VttabletProcess.EnableSemiSync = true

			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.MysqlctlProcess.InitDBFile = newInitDBFile
			tablet.MysqlctlProcess.ExtraArgs = extraArgs
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcs = append(mysqlProcs, proc)

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}
		master = shard.Vttablets[0]
		replica1 = shard.Vttablets[1]
		replica2 = shard.Vttablets[2]
		replica3 = shard.Vttablets[3]

		for _, tablet := range []cluster.Vttablet{*master, *replica1} {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard.Name, cell, master.TabletUID); err != nil {
			return 1, err
		}
		return m.Run(), nil
	}()

	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	} else {
		os.Exit(exitCode)
	}

}

// TestRecoveryImpl does following
// - create a shard with master and replica1 only
// - run InitShardMaster
// - insert some data
// - take a backup
// - insert more data on the master
// - take another backup
// - create a recovery keyspace after first backup
// - bring up tablet_replica2 in the new keyspace
// - check that new tablet does not have data created after backup1
// - create second recovery keyspace after second backup
// - bring up tablet_replica3 in second keyspace
// - check that new tablet has data created after backup1 but not data created after backup2
// - check that vtgate queries work correctly
func TestRecoveryImpl(t *testing.T) {
	defer cluster.PanicHandler(t)
	defer tabletsTeardown()
	verifyInitialReplication(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.NoError(t, err)

	backups := listBackups(t)
	require.Equal(t, len(backups), 1)
	assert.Contains(t, backups[0], replica1.Alias)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	assert.NoError(t, err)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", keyspaceName)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	recovery.RestoreTablet(t, localCluster, replica2, recoveryKS1, "0", keyspaceName, commonTabletArg)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvVSchema", cell)
	assert.NoError(t, err)
	assert.Contains(t, output, keyspaceName)
	assert.Contains(t, output, recoveryKS1)

	err = localCluster.VtctlclientProcess.ExecuteCommand("GetSrvKeyspace", cell, keyspaceName)
	assert.NoError(t, err)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS1)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 1)

	cluster.VerifyLocalMetadata(t, replica2, recoveryKS1, shardName, cell)

	// update the original row in master
	_, err = master.VttabletProcess.QueryTablet("update vt_insert_test set msg = 'msgx1' where id = 1", keyspaceName, true)
	assert.NoError(t, err)

	//verify that master has new value
	qr, err := master.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "msgx1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	//verify that restored replica has old value
	qr, err = replica2.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "test1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.NoError(t, err)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	assert.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 3)

	recovery.RestoreTablet(t, localCluster, replica3, recoveryKS2, "0", keyspaceName, commonTabletArg)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS2)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	cluster.VerifyRowsInTablet(t, replica3, keyspaceName, 2)

	// update the original row in master
	_, err = master.VttabletProcess.QueryTablet("update vt_insert_test set msg = 'msgx2' where id = 1", keyspaceName, true)
	assert.NoError(t, err)

	//verify that master has new value
	qr, err = master.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "msgx2", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	//verify that restored replica has old value
	qr, err = replica3.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "msgx1", fmt.Sprintf("%s", qr.Rows[0][0].ToBytes()))

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.TabletTypesToWait = "REPLICA"
	err = vtgateInstance.Setup()
	localCluster.VtgateGrpcPort = vtgateInstance.GrpcPort
	assert.NoError(t, err)
	defer vtgateInstance.TearDown()
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shardName), 1)
	assert.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 1)
	assert.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS1, shardName), 1)
	assert.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS2, shardName), 1)
	assert.NoError(t, err)

	// Build vtgate grpc connection
	grpcAddress := fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateGrpcPort)
	vtgateConn, err := vtgateconn.Dial(context.Background(), grpcAddress)
	assert.NoError(t, err)
	defer vtgateConn.Close()
	session := vtgateConn.Session("@replica", nil)

	//check that vtgate doesn't route queries to new tablet
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(3)")
	recovery.VerifyQueriesUsingVtgate(t, session, "select msg from vt_insert_test where id = 1", `VARCHAR("msgx2")`)
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS1), "INT64(1)")
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS1), `VARCHAR("test1")`)
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS2), "INT64(2)")
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS2), `VARCHAR("msgx1")`)

	// check that new keyspace is accessible with 'use ks'
	cluster.ExecuteQueriesUsingVtgate(t, session, "use "+recoveryKS1+"@replica")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	cluster.ExecuteQueriesUsingVtgate(t, session, "use "+recoveryKS2+"@replica")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")

	// check that new tablet is accessible with use `ks:shard`
	cluster.ExecuteQueriesUsingVtgate(t, session, "use `"+recoveryKS1+":0@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	cluster.ExecuteQueriesUsingVtgate(t, session, "use `"+recoveryKS2+":0@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")
}

// verifyInitialReplication will create schema in master, insert some data to master and verify the same data in replica.
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.NoError(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}

func listBackups(t *testing.T) []string {
	output, err := localCluster.ListBackups(shardKsName)
	assert.NoError(t, err)
	return output
}

func tabletsTeardown() {
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{master, replica1, replica2, replica3} {
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
		tablet.VttabletProcess.TearDown()
	}
	for _, proc := range mysqlProcs {
		proc.Wait()
	}
}
