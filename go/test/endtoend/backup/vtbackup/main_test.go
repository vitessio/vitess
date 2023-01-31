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

package vtbackup

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	primary          *cluster.Vttablet
	replica1         *cluster.Vttablet
	replica2         *cluster.Vttablet
	localCluster     *cluster.LocalProcessCluster
	newInitDBFile    string
	cell             = cluster.DefaultCell
	hostname         = "localhost"
	keyspaceName     = "ks"
	shardName        = "0"
	dbPassword       = "VtDbaPass"
	shardKsName      = fmt.Sprintf("%s/%s", keyspaceName, shardName)
	dbCredentialFile string
	commonTabletArg  = []string{
		"--vreplication_healthcheck_topology_refresh", "1s",
		"--vreplication_healthcheck_retry_delay", "1s",
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--enable_replication_reporter",
		"--serving_state_grace_period", "1s"}
)

func TestMain(m *testing.M) {
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
		vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", localCluster.VtctldProcess.GrpcPort, localCluster.TmpDirectory)
		_, err = vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync")
		if err != nil {
			return 1, err
		}

		// Create a new init_db.sql file that sets up passwords for all users.
		// Then we use a db-credentials-file with the passwords.
		dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		sql := string(initDb)
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
		sql = sql + cluster.GetPasswordUpdateSQL(localCluster)
		err = os.WriteFile(newInitDBFile, []byte(sql), 0666)
		if err != nil {
			return 1, err
		}

		extraArgs := []string{"--db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

		primary = localCluster.NewVttabletInstance("replica", 0, "")
		replica1 = localCluster.NewVttabletInstance("replica", 0, "")
		replica2 = localCluster.NewVttabletInstance("replica", 0, "")
		shard.Vttablets = []*cluster.Vttablet{primary, replica1, replica2}

		// Start MySql processes
		var mysqlProcs []*exec.Cmd
		for _, tablet := range shard.Vttablets {
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
			tablet.VttabletProcess.SupportsBackup = true

			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.MysqlctlProcess.InitDBFile = newInitDBFile
			tablet.MysqlctlProcess.ExtraArgs = extraArgs
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

		// Create database
		for _, tablet := range []cluster.Vttablet{*primary, *replica1} {
			if err := tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
				return 1, err
			}
		}

		if localCluster.VtTabletMajorVersion >= 16 {
			// If vttablets are any lower than version 16, then they are running the replication manager.
			// Running VTOrc and replication manager sometimes creates the situation where VTOrc has set up semi-sync on the primary,
			// but the replication manager starts replication on the replica without setting semi-sync. This hangs the primary.
			// Even if VTOrc fixes it, since there is no ongoing traffic, the state remains blocked.
			if err := localCluster.StartVTOrc(keyspaceName); err != nil {
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
