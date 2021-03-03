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

package mysqlctl

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	shardName       = "0"
	cell            = "zone1"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1
		}

		initCluster([]string{"0"}, 2)

		// Collect tablet paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "master" {
				masterTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initCluster(shardNames []string, totalTabletsRequired int) {
	keyspace := cluster.Keyspace{
		Name: keyspaceName,
	}
	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}
		var mysqlCtlProcessList []*exec.Cmd
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  clusterInstance.GetAndReservePort(),
				GrpcPort:  clusterInstance.GetAndReservePort(),
				MySQLPort: clusterInstance.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", clusterInstance.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as master
				tablet.Type = "master"
			}
			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)

			// start vttablet process
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

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		for _, proc := range mysqlCtlProcessList {
			if err := proc.Wait(); err != nil {
				return
			}
		}

		for _, tablet := range shard.Vttablets {
			if _, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false); err != nil {
				log.Error(err.Error())
				return
			}
		}

		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
}

func TestRestart(t *testing.T) {
	defer cluster.PanicHandler(t)
	err := masterTablet.MysqlctlProcess.Stop()
	require.Nil(t, err)
	masterTablet.MysqlctlProcess.CleanupFiles(masterTablet.TabletUID)
	err = masterTablet.MysqlctlProcess.Start()
	require.Nil(t, err)
}

func TestAutoDetect(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Start up tablets with an empty MYSQL_FLAVOR, which means auto-detect
	sqlFlavor := os.Getenv("MYSQL_FLAVOR")
	os.Setenv("MYSQL_FLAVOR", "")

	err := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.Setup()
	require.Nil(t, err, "error should be nil")
	err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].VttabletProcess.Setup()
	require.Nil(t, err, "error should be nil")

	// Reparent tablets, which requires flavor detection
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	require.Nil(t, err, "error should be nil")

	//Reset flavor
	os.Setenv("MYSQL_FLAVOR", sqlFlavor)

}
