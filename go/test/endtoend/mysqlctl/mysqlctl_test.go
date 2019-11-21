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
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance     *cluster.LocalProcessCluster
	vtParams            mysql.ConnParams
	masterTabletParams  mysql.ConnParams
	replicaTabletParams mysql.ConnParams
	masterTablet        cluster.Vttablet
	replicaTablet       cluster.Vttablet
	rdonlyTablet        cluster.Vttablet
	replicaUID          int
	masterUID           int
	hostname            = "localhost"
	keyspaceName        = "test_keyspace"
	shardName           = "0"
	keyspaceShard       = "ks/" + shardName
	dbName              = "vt_" + keyspaceName
	username            = "vt_dba"
	cell                = "zone1"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
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

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "master" {
				masterTablet = tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = tablet
			} else {
				rdonlyTablet = tablet
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
			if err := tablet.MysqlctlProcess.Start(); err != nil {
				return
			}

			// start vttablet process
			tablet.VttabletProcess = *cluster.VttabletProcessInstance(tablet.HTTPPort,
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

			if _, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false); err != nil {
				log.Error(err.Error())
				return
			}

			shard.Vttablets = append(shard.Vttablets, *tablet)
		}

		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
}

func TestRestart(t *testing.T) {
	err := masterTablet.MysqlctlProcess.Stop()
	assert.Nil(t, err)
	masterTablet.MysqlctlProcess.CleanupFiles(masterTablet.TabletUID)
	err = masterTablet.MysqlctlProcess.Start()
	assert.Nil(t, err)
}

func TestAutoDetect(t *testing.T) {

	// Start up tablets with an empty MYSQL_FLAVOR, which means auto-detect
	sqlFlavor := os.Getenv("MYSQL_FLAVOR")
	os.Setenv("MYSQL_FLAVOR", "")

	// Start vttablet process, should be in NOT_SERVING state as mysqld is not running
	err := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.Setup()
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].VttabletProcess.Setup()
	assert.Nil(t, err, "error should be Nil")

	// Reparent tablets, which requires flavor detection
	clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	assert.Nil(t, err, "error should be Nil")

	//Reset flavor
	os.Setenv("MYSQL_FLAVOR", sqlFlavor)

}
