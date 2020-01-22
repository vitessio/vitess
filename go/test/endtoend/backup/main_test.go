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

package backup

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	"vitess.io/vitess/go/vt/log"
)

var (
	master           *cluster.Vttablet
	replica1         *cluster.Vttablet
	replica2         *cluster.Vttablet
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
		"-enable_replication_reporter",
		"-serving_state_grace_period", "1s"}
)

func TestMain(m *testing.M) {
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

		// update password of mysql users
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

		// start mysql process for all replicas and master
		var mysqlProcs []*exec.Cmd
		for i := 0; i < 3; i++ {
			tabletType := "replica"
			if i == 0 {
				tabletType = "master"
			}
			tablet := localCluster.GetVttabletInstance(tabletType, 0, cell)
			tablet.VttabletProcess = localCluster.GetVtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
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

		// initialize tablets
		master = shard.Vttablets[0]
		replica1 = shard.Vttablets[1]
		replica2 = shard.Vttablets[2]

		if err := localCluster.VtctlclientProcess.InitTablet(master, cell, keyspaceName, hostname, shard.Name); err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.InitTablet(replica1, cell, keyspaceName, hostname, shard.Name); err != nil {
			return 1, err
		}

		// create database direct in vtTablet
		for _, tablet := range []cluster.Vttablet{*master, *replica1} {
			if err := tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		// initialize master and start replication
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
