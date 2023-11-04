/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance   *cluster.LocalProcessCluster
	vtParams          mysql.ConnParams
	mysqlParams       mysql.ConnParams
	vtgateGrpcAddress string
	shardedKs         = "ks"
	unshardedKs       = "uks"
	Cell              = "test"
	//go:embed sharded_schema.sql
	shardedSchemaSQL string

	//go:embed unsharded_schema.sql
	unshardedSchemaSQL string

	//go:embed sharded_vschema.json
	shardedVSchema string

	//go:embed unsharded_vschema.json
	unshardedVSchema string
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

		err = clusterInstance.StartKeyspace(*sKs, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		uKs := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSchemaSQL,
			VSchema:   unshardedVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKs, 1, false)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		vtgateGrpcAddress = fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)

		connParams, closer, err := utils.NewMySQL(clusterInstance, shardedKs, shardedSchemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = connParams
		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_ = utils.Exec(t, mcmp.VtConn, "use `ks/-80`")
		tables := []string{"t4", "t3", "t2", "t1", "multicol_tbl2", "multicol_tbl1"}
		for i := 20; i > 0; i-- {
			tables = append(tables, fmt.Sprintf("fk_t%v", i))
		}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
		_ = utils.Exec(t, mcmp.VtConn, "use `ks/80-`")
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
		_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
		tables = []string{"u_t1", "u_t2", "u_t3"}
		for i := 20; i > 0; i-- {
			tables = append(tables, fmt.Sprintf("fk_t%v", i))
		}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
		_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}
