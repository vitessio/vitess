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

package reparent

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	// ClusterInstance instance to be used for test with different params
	clusterInstance *cluster.LocalProcessCluster
	tmClient        *tmc.Client
	keyspaceName    = "ks"
	shardName       = "0"
	shard1Name      = "0000000000000000-ffffffffffffffff"
	keyspaceShard   = keyspaceName + "/" + shardName
	dbName          = "vt_" + keyspaceName
	username        = "vt_dba"
	hostname        = "localhost"
	cell1           = "zone1"
	cell2           = "zone2"
	insertSQL       = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	sqlSchema       = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB
	`
	// Tablets for shard0
	tablet62344 *cluster.Vttablet
	tablet62044 *cluster.Vttablet
	tablet41983 *cluster.Vttablet
	tablet31981 *cluster.Vttablet

	// Tablets for shard1
	masterTablet  *cluster.Vttablet
	replicaTablet *cluster.Vttablet
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell1, hostname)
		defer clusterInstance.Teardown()

		// Launch keyspace
		keyspace := &cluster.Keyspace{Name: keyspaceName}

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Adding another cell in the same cluster
		err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
		if err != nil {
			return 1
		}
		err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
		if err != nil {
			return 1
		}

		tablet62344 = clusterInstance.GetVttabletInstance("replica", 62344, "")
		tablet62044 = clusterInstance.GetVttabletInstance("replica", 62044, "")
		tablet41983 = clusterInstance.GetVttabletInstance("replica", 41983, "")
		tablet31981 = clusterInstance.GetVttabletInstance("replica", 31981, cell2)

		shard0 := &cluster.Shard{Name: shardName}
		shard0.Vttablets = []*cluster.Vttablet{tablet62344, tablet62044, tablet41983, tablet31981}

		// Initiate shard1 - required for ranged based reparenting
		masterTablet = clusterInstance.GetVttabletInstance("replica", 0, "")
		replicaTablet = clusterInstance.GetVttabletInstance("replica", 0, "")

		shard1 := &cluster.Shard{Name: shard1Name}
		shard1.Vttablets = []*cluster.Vttablet{masterTablet, replicaTablet}

		clusterInstance.VtTabletExtraArgs = []string{
			"-lock_tables_timeout", "5s",
			"-enable_semi_sync",
		}

		// Initialize Cluster
		err = clusterInstance.LaunchCluster(keyspace, []cluster.Shard{*shard0, *shard1})
		if err != nil {
			return 1
		}

		//Start MySql
		var mysqlCtlProcessList []*exec.Cmd
		for _, shard := range clusterInstance.Keyspaces[0].Shards {
			for _, tablet := range shard.Vttablets {
				fmt.Println("Starting MySql for tablet ", tablet.Alias)
				if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
					return 1
				} else {
					mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
				}
			}
		}

		// Wait for mysql processes to start
		for _, proc := range mysqlCtlProcessList {
			if err := proc.Wait(); err != nil {
				return 1
			}
		}

		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

		// create tablet manager client
		tmClient = tmc.NewClient()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func getMysqlConnParam(tablet *cluster.Vttablet) mysql.ConnParams {
	connParams := mysql.ConnParams{
		Uname:      username,
		DbName:     dbName,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	return connParams
}

func runSQL(ctx context.Context, t *testing.T, sql string, tablet *cluster.Vttablet) *sqltypes.Result {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.NoError(t, err)
	defer conn.Close()

	// runSQL
	return execute(t, conn, sql)
}

func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}
