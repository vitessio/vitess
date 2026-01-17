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

package clone

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	vtutils "vitess.io/vitess/go/vt/utils"
)

var (
	primary           *cluster.Vttablet
	replica1          *cluster.Vttablet
	replica2          *cluster.Vttablet
	localCluster      *cluster.LocalProcessCluster
	newInitDBFile     string
	cell              = cluster.DefaultCell
	hostname          = "localhost"
	keyspaceName      = "ks"
	shardName         = "0"
	dbPassword        = "VtDbaPass"
	shardKsName       = fmt.Sprintf("%s/%s", keyspaceName, shardName)
	dbCredentialFile  string
	vttabletExtraArgs = []string{
		vtutils.GetFlagVariantForTests("--vreplication-retry-delay"), "1s",
		vtutils.GetFlagVariantForTests("--degraded-threshold"), "5s",
		vtutils.GetFlagVariantForTests("--lock-tables-timeout"), "5s",
		vtutils.GetFlagVariantForTests("--watch-replication-stream"),
		vtutils.GetFlagVariantForTests("--enable-replication-reporter"),
		vtutils.GetFlagVariantForTests("--serving-state-grace-period"), "1s",
	}
	vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		// Setup EXTRA_MY_CNF for clone plugin
		if err := setupExtraMyCnf(); err != nil {
			log.Errorf("Failed to setup extra MySQL config: %v", err)
			return 1, err
		}

		// Start topo server
		err := localCluster.StartTopo()
		if err != nil {
			return 1, err
		}

		// Start keyspace
		localCluster.Keyspaces = []cluster.Keyspace{
			{
				Name: keyspaceName,
				Shards: []cluster.Shard{
					{
						Name: shardName,
					},
				},
			},
		}
		shard := &localCluster.Keyspaces[0].Shards[0]
		vtctldClientProcess := cluster.VtctldClientProcessInstance(localCluster.VtctldProcess.GrpcPort, localCluster.TopoPort, "localhost", localCluster.TmpDirectory)
		_, err = vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync")
		if err != nil {
			return 1, err
		}

		// Create a new init_db.sql file that sets up passwords for all users and clone user
		dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		initClone, err := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_clone.sql"))
		if err != nil {
			log.Warningf("init_clone.sql not found, clone tests may fail: %v", err)
			initClone = []byte("")
		}

		sql := string(initDb)
		// The original init_db.sql does not have any passwords. Here we update the init file with passwords
		sql, err = utils.GetInitDBSQL(sql, cluster.GetPasswordUpdateSQL(localCluster), string(initClone))
		if err != nil {
			return 1, err
		}
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords_and_clone.sql")
		err = os.WriteFile(newInitDBFile, []byte(sql), 0666)
		if err != nil {
			return 1, err
		}

		mysqlctlExtraArgs := []string{"--db-credentials-file", dbCredentialFile}
		vttabletExtraArgs = append(vttabletExtraArgs,
			"--db-credentials-file", dbCredentialFile,
			"--mysql-clone-enabled")

		primary = localCluster.NewVttabletInstance("replica", 0, "")
		replica1 = localCluster.NewVttabletInstance("replica", 0, "")
		replica2 = localCluster.NewVttabletInstance("replica", 0, "")
		shard.Vttablets = []*cluster.Vttablet{primary, replica1, replica2}

		// Start MySql processes
		var mysqlProcs []*exec.Cmd
		for _, tablet := range shard.Vttablets {
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = vttabletExtraArgs
			tablet.VttabletProcess.SupportsBackup = true

			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			if err != nil {
				return 1, err
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			tablet.MysqlctlProcess.InitDBFile = newInitDBFile
			tablet.MysqlctlProcess.ExtraArgs = mysqlctlExtraArgs
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcs = append(mysqlProcs, proc)
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}

		if localCluster.VtTabletMajorVersion >= 16 {
			// If vttablets are any lower than version 16, then they are running the replication manager.
			// Running VTOrc and replication manager sometimes creates the situation where VTOrc has set up semi-sync on the primary,
			// but the replication manager starts replication on the replica without setting semi-sync. This hangs the primary.
			// Even if VTOrc fixes it, since there is no ongoing traffic, the state remains blocked.
			if err := localCluster.StartVTOrc(cell, keyspaceName); err != nil {
				return 1, err
			}
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

// setupExtraMyCnf sets EXTRA_MY_CNF to include clone plugin configuration
func setupExtraMyCnf() error {
	cloneCnfPath := path.Join(os.Getenv("VTROOT"), "config", "mycnf", "clone.cnf")
	if _, err := os.Stat(cloneCnfPath); os.IsNotExist(err) {
		return fmt.Errorf("clone.cnf not found at %s", cloneCnfPath)
	}

	// Check if EXTRA_MY_CNF is already set
	existing := os.Getenv("EXTRA_MY_CNF")
	if existing != "" {
		// Append clone.cnf to existing
		if err := os.Setenv("EXTRA_MY_CNF", existing+":"+cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	} else {
		if err := os.Setenv("EXTRA_MY_CNF", cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	}

	log.Infof("Set EXTRA_MY_CNF to include clone plugin: %s", os.Getenv("EXTRA_MY_CNF"))
	return nil
}

// mysqlVersionSupportsClone checks if the MySQL version supports CLONE plugin
func mysqlVersionSupportsClone(t *testing.T, tablet *cluster.Vttablet) bool {
	conn, err := tablet.VttabletProcess.TabletConn(keyspaceName, false)
	require.NoError(t, err, "failed to get tablet connection")
	ok, err := conn.SupportsCapability(capabilities.MySQLClonePluginFlavorCapability)
	require.NoError(t, err, "failed to check clone capability")
	return ok
}

// clonePluginAvailable checks if the clone plugin is installed and active
func clonePluginAvailable(t *testing.T, tablet *cluster.Vttablet) bool {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'clone'",
		keyspaceName, true)
	if err != nil {
		t.Logf("Failed to check clone plugin: %v", err)
		return false
	}
	if len(qr.Rows) == 0 {
		return false
	}
	status := qr.Rows[0][0].ToString()
	return status == "ACTIVE"
}

// removeBackups removes all backups for the test shard.
func removeBackups(t *testing.T) {
	backups, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)
	for backup := range strings.SplitSeq(backups, "\n") {
		if backup != "" {
			_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("RemoveBackup", shardKsName, backup)
			require.NoError(t, err)
		}
	}
}

// waitInsertedRows checks that the specific test data we inserted on primary
// exists on the cloned replica. This proves data was actually transferred.
func waitInsertedRows(
	t *testing.T,
	tablet *cluster.Vttablet,
	expectedValues []string,
	waitFor time.Duration,
	tickInterval time.Duration,
) {
	require.Eventually(t, func() bool {
		qr, err := tablet.VttabletProcess.QueryTablet(
			"SELECT msg FROM vt_insert_test ORDER BY id",
			keyspaceName,
			true,
		)
		if err != nil {
			return false
		}
		if len(qr.Rows) != len(expectedValues) {
			return false
		}

		for i, row := range qr.Rows {
			if row[0].ToString() != expectedValues[i] {
				return false
			}
		}
		return true
	}, waitFor, tickInterval)
}
