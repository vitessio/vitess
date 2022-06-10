/*
Copyright 2021 The Vitess Authors.

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

package vtgate

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	shardedKs       = "ks"

	shardedKsShards = []string{"-19a0", "19a0-20", "20-20c0", "20c0-"}
	Cell            = "test"
	//go:embed schema.sql
	shardedSchemaSQL string

	//go:embed vschema.json
	shardedVSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		sKs := &cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}

		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartKeyspace(*sKs, shardedKsShards, 0, false)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4 // enable Gen4 planner.
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		conn, closer, err := utils.NewMySQL(clusterInstance, shardedKs, shardedSchemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = conn
		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"user", "lookup"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestEqualQuery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into user (user_id, name) values (1, 'apa'), (2,'monkey')")
	mcmp.AssertMatches("select user_id from user where name = 'apa'", "[[INT64(1)]]")
}
