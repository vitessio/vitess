/*
Copyright 2022 The Vitess Authors.

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

package reservedconn

import (
	"context"
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `create table test(id bigint primary key)Engine=InnoDB;`
)

var enableSettingsPool bool

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	code := runAllTests(m)
	if code != 0 {
		os.Exit(code)
	}

	println("running with settings pool enabled")
	// run again with settings pool enabled.
	enableSettingsPool = true
	code = runAllTests(m)
	os.Exit(code)
}

func runAllTests(m *testing.M) int {
	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return 1
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}
	if enableSettingsPool {
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-settings-pool")
	}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, false); err != nil {
		return 1
	}

	// Start vtgate
	if err := clusterInstance.StartVtgate(); err != nil {
		return 1
	}

	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	return m.Run()
}

func TestVttabletDownServingChange(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set default_week_format = 1")
	_ = utils.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ * from test")

	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	require.NoError(t,
		primaryTablet.MysqlctlProcess.Stop())
	// kill vttablet process
	_ = primaryTablet.VttabletProcess.TearDown()
	require.NoError(t,
		clusterInstance.VtctlclientProcess.ExecuteCommand("EmergencyReparentShard", "--", "--keyspace_shard", "ks/0"))

	// This should work without any error.
	_ = utils.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ * from test")
}
