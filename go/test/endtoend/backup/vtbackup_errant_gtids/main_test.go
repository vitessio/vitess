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

// Package vtbackuperrantgtids tests that vtbackup refuses base and new backups
// with errant GTIDs. The cluster runs without VTOrc, so the tests control
// replication and reparents: VTOrc would restart the IO threads they stop and
// race the reparents they run.
package vtbackuperrantgtids

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
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
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
		"--serving-state-grace-period", "1s",
	}
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		if err := localCluster.StartTopo(); err != nil {
			return 1, err
		}

		localCluster.Keyspaces = []cluster.Keyspace{{Name: keyspaceName, Shards: []cluster.Shard{{Name: shardName}}}}
		shard := &localCluster.Keyspaces[0].Shards[0]
		vtctldClientProcess := cluster.VtctldClientProcessInstance(localCluster.VtctldProcess.GrpcPort, localCluster.TopoPort, "localhost", localCluster.TmpDirectory)
		if _, err := vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync"); err != nil {
			return 1, err
		}

		// Rewrite init_db.sql with passwords and use a db-credentials-file, as the vtbackup suite does.
		dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		sql, err := utils.GetInitDBSQL(string(initDb), cluster.GetPasswordUpdateSQL(localCluster), "")
		if err != nil {
			return 1, err
		}
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
		if err := os.WriteFile(newInitDBFile, []byte(sql), 0o666); err != nil {
			return 1, err
		}

		extraArgs := []string{"--db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

		primary = localCluster.NewVttabletInstance("replica", 0, "")
		replica1 = localCluster.NewVttabletInstance("replica", 0, "")
		replica2 = localCluster.NewVttabletInstance("replica", 0, "")
		shard.Vttablets = []*cluster.Vttablet{primary, replica1, replica2}

		var mysqlProcs []*exec.Cmd
		for _, tablet := range shard.Vttablets {
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
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
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}

		return m.Run(), nil
	}()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	os.Exit(exitCode)
}
