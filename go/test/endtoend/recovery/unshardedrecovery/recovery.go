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
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	primary          *cluster.Vttablet
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
		"--vreplication_healthcheck_topology_refresh", "1s",
		"--vreplication_healthcheck_retry_delay", "1s",
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--serving_state_grace_period", "1s"}
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

		// Create a new init_db.sql file that sets up passwords for all users.
		// Then we use a db-credentials-file with the passwords.
		// TODO: We could have operated with empty password here. Create a separate test for --db-credentials-file functionality (@rsajwani)
		dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		sql := string(initDb)
		// The original init_db.sql does not have any passwords. Here we update the init file with passwords
		oldAlterTableMode := `SET GLOBAL old_alter_table = ON;`
		sql, err = utils.GetInitDBSQL(sql, cluster.GetPasswordUpdateSQL(localCluster), oldAlterTableMode)
		if err != nil {
			return 1, err
		}
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
		os.WriteFile(newInitDBFile, []byte(sql), 0666)

		extraArgs := []string{"--db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

		shard := cluster.Shard{
			Name: shardName,
		}

		var mysqlProcs []*exec.Cmd
		for i := 0; i < 4; i++ {
			tabletType := "replica"
			if i == 0 {
				tabletType = "primary"
			}
			tablet := localCluster.NewVttabletInstance(tabletType, 0, cell)
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
			if recovery.UseXb {
				tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, recovery.XbArgs...)
			}
			tablet.VttabletProcess.SupportsBackup = true

			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			if err != nil {
				return 1, err
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
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
		primary = shard.Vttablets[0]
		replica1 = shard.Vttablets[1]
		replica2 = shard.Vttablets[2]
		replica3 = shard.Vttablets[3]

		for _, tablet := range []cluster.Vttablet{*primary, *replica1} {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", localCluster.VtctldProcess.GrpcPort, localCluster.TmpDirectory)
		_, err = vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
		if err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.InitializeShard(keyspaceName, shard.Name, cell, primary.TabletUID); err != nil {
			return 1, err
		}
		if err := localCluster.StartVTOrc(keyspaceName); err != nil {
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
// 1. create a shard with primary and replica1 only
//   - run InitShardPrimary
//   - insert some data
//
// 2. take a backup
// 3.create a recovery keyspace after first backup
//   - bring up tablet_replica2 in the new keyspace
//   - check that new tablet has data from backup1
//
// 4. insert more data on the primary
//
// 5. take another backup
// 6. create a recovery keyspace after second backup
//   - bring up tablet_replica3 in the new keyspace
//   - check that new tablet has data from backup2
//
// 7. check that vtgate queries work correctly
func TestRecoveryImpl(t *testing.T) {
	defer cluster.PanicHandler(t)
	defer tabletsTeardown()
	verifyInitialReplication(t)

	// take first backup of value = test1
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.NoError(t, err)

	backups := listBackups(t)
	require.Equal(t, len(backups), 1)
	assert.Contains(t, backups[0], replica1.Alias)

	err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	assert.NoError(t, err)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", keyspaceName)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	// restore with latest backup
	restoreTime := time.Now().UTC()
	recovery.RestoreTablet(t, localCluster, replica2, recoveryKS1, "0", keyspaceName, commonTabletArg, restoreTime)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvVSchema", cell)
	assert.NoError(t, err)
	assert.Contains(t, output, keyspaceName)
	assert.Contains(t, output, recoveryKS1)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS1)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 1)

	// verify that restored replica has value = test1
	qr, err := replica2.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "test1", qr.Rows[0][0].ToString())

	// insert new row on primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	// update the original row in primary
	_, err = primary.VttabletProcess.QueryTablet("update vt_insert_test set msg = 'msgx1' where id = 1", keyspaceName, true)
	assert.NoError(t, err)

	// verify that primary has new value
	qr, err = primary.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "msgx1", qr.Rows[0][0].ToString())

	// check that replica1, used for the backup, has the new value
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		qr, err = replica1.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
		assert.NoError(t, err)
		if qr.Rows[0][0].ToString() == "msgx1" {
			break
		}

		select {
		case <-ctx.Done():
			t.Error("timeout waiting for new value to be replicated on replica 1")
			break
		case <-ticker.C:
		}
	}

	// take second backup of value = msgx1
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.NoError(t, err)

	// restore to first backup
	recovery.RestoreTablet(t, localCluster, replica3, recoveryKS2, "0", keyspaceName, commonTabletArg, restoreTime)

	output, err = localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetVSchema", recoveryKS2)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	// only one row from first backup
	cluster.VerifyRowsInTablet(t, replica3, keyspaceName, 1)

	//verify that restored replica has value = test1
	qr, err = replica3.VttabletProcess.QueryTablet("select msg from vt_insert_test where id = 1", keyspaceName, true)
	assert.NoError(t, err)
	assert.Equal(t, "test1", qr.Rows[0][0].ToString())

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.TabletTypesToWait = "REPLICA"
	err = vtgateInstance.Setup()
	localCluster.VtgateGrpcPort = vtgateInstance.GrpcPort
	assert.NoError(t, err)
	defer vtgateInstance.TearDown()
	assert.NoError(t, vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second))
	assert.NoError(t, vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 1, 30*time.Second))
	assert.NoError(t, vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS1, shardName), 1, 30*time.Second))
	assert.NoError(t, vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS2, shardName), 1, 30*time.Second))

	// Build vtgate grpc connection
	grpcAddress := fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateGrpcPort)
	vtgateConn, err := vtgateconn.Dial(context.Background(), grpcAddress)
	assert.NoError(t, err)
	defer vtgateConn.Close()
	session := vtgateConn.Session("@replica", nil)

	// check that vtgate doesn't route queries to new tablet
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")
	recovery.VerifyQueriesUsingVtgate(t, session, "select msg from vt_insert_test where id = 1", `VARCHAR("msgx1")`)
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS1), "INT64(1)")
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS1), `VARCHAR("test1")`)
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS2), "INT64(1)")
	recovery.VerifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS2), `VARCHAR("test1")`)

	// check that new keyspace is accessible with 'use ks'
	cluster.ExecuteQueriesUsingVtgate(t, session, "use "+recoveryKS1+"@replica")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	cluster.ExecuteQueriesUsingVtgate(t, session, "use "+recoveryKS2+"@replica")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	// check that new tablet is accessible with use `ks:shard`
	cluster.ExecuteQueriesUsingVtgate(t, session, "use `"+recoveryKS1+":0@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	cluster.ExecuteQueriesUsingVtgate(t, session, "use `"+recoveryKS2+":0@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")
}

// verifyInitialReplication will create schema in primary, insert some data to primary and verify the same data in replica.
func verifyInitialReplication(t *testing.T) {
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
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
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2, replica3} {
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
		tablet.VttabletProcess.TearDown()
	}
	for _, proc := range mysqlProcs {
		proc.Wait()
	}
}
