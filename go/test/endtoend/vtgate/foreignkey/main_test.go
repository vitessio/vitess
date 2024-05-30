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
	"context"
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
	clusterInstance      *cluster.LocalProcessCluster
	vtParams             mysql.ConnParams
	mysqlParams          mysql.ConnParams
	vtgateGrpcAddress    string
	shardedKs            = "ks"
	shardScopedKs        = "sks"
	unshardedKs          = "uks"
	unshardedUnmanagedKs = "unmanaged_uks"
	Cell                 = "test"

	//go:embed schema.sql
	schemaSQL string

	//go:embed sharded_vschema.json
	shardedVSchema string

	//go:embed shard_scoped_vschema.json
	shardScopedVSchema string

	//go:embed unsharded_vschema.json
	unshardedVSchema string

	//go:embed unsharded_unmanaged_vschema.json
	unshardedUnmanagedVSchema string

	fkTables = []string{"fk_t1", "fk_t2", "fk_t3", "fk_t4", "fk_t5", "fk_t6", "fk_t7",
		"fk_t10", "fk_t11", "fk_t12", "fk_t13", "fk_t15", "fk_t16", "fk_t17", "fk_t18", "fk_t19", "fk_t20",
		"fk_multicol_t1", "fk_multicol_t2", "fk_multicol_t3", "fk_multicol_t4", "fk_multicol_t5", "fk_multicol_t6", "fk_multicol_t7",
		"fk_multicol_t10", "fk_multicol_t11", "fk_multicol_t12", "fk_multicol_t13", "fk_multicol_t15", "fk_multicol_t16", "fk_multicol_t17", "fk_multicol_t18", "fk_multicol_t19"}
	fkReferences = []fkReference{
		{parentTable: "fk_t1", childTable: "fk_t2"},
		{parentTable: "fk_t2", childTable: "fk_t7"},
		{parentTable: "fk_t2", childTable: "fk_t3"},
		{parentTable: "fk_t3", childTable: "fk_t4"},
		{parentTable: "fk_t3", childTable: "fk_t6"},
		{parentTable: "fk_t4", childTable: "fk_t5"},
		{parentTable: "fk_t10", childTable: "fk_t11"},
		{parentTable: "fk_t11", childTable: "fk_t12"},
		{parentTable: "fk_t11", childTable: "fk_t13"},
		{parentTable: "fk_t15", childTable: "fk_t16"},
		{parentTable: "fk_t16", childTable: "fk_t17"},
		{parentTable: "fk_t17", childTable: "fk_t18"},
		{parentTable: "fk_t17", childTable: "fk_t19"},
		{parentTable: "fk_multicol_t1", childTable: "fk_multicol_t2"},
		{parentTable: "fk_multicol_t2", childTable: "fk_multicol_t7"},
		{parentTable: "fk_multicol_t2", childTable: "fk_multicol_t3"},
		{parentTable: "fk_multicol_t3", childTable: "fk_multicol_t4"},
		{parentTable: "fk_multicol_t3", childTable: "fk_multicol_t6"},
		{parentTable: "fk_multicol_t4", childTable: "fk_multicol_t5"},
		{parentTable: "fk_multicol_t10", childTable: "fk_multicol_t11"},
		{parentTable: "fk_multicol_t11", childTable: "fk_multicol_t12"},
		{parentTable: "fk_multicol_t11", childTable: "fk_multicol_t13"},
		{parentTable: "fk_multicol_t15", childTable: "fk_multicol_t16"},
		{parentTable: "fk_multicol_t16", childTable: "fk_multicol_t17"},
		{parentTable: "fk_multicol_t17", childTable: "fk_multicol_t18"},
		{parentTable: "fk_multicol_t17", childTable: "fk_multicol_t19"},
	}
)

// fkReference stores a foreign key reference from one table to another.
type fkReference struct {
	parentTable string
	childTable  string
}

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
			SchemaSQL: schemaSQL,
			VSchema:   shardedVSchema,
		}

		err = clusterInstance.StartKeyspace(*sKs, []string{"-80", "80-"}, 1, false)
		if err != nil {
			return 1
		}

		// Start shard-scoped keyspace
		ssKs := &cluster.Keyspace{
			Name:      shardScopedKs,
			SchemaSQL: schemaSQL,
			VSchema:   shardScopedVSchema,
		}

		err = clusterInstance.StartKeyspace(*ssKs, []string{"-80", "80-"}, 1, false)
		if err != nil {
			return 1
		}

		uKs := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: schemaSQL,
			VSchema:   unshardedVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKs, 1, false)
		if err != nil {
			return 1
		}

		unmanagedKs := &cluster.Keyspace{
			Name:      unshardedUnmanagedKs,
			SchemaSQL: schemaSQL,
			VSchema:   unshardedUnmanagedVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*unmanagedKs, 1, false)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctldClientProcess.ExecuteCommand("RebuildVSchemaGraph")
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

		connParams, closer, err := utils.NewMySQL(clusterInstance, shardedKs, schemaSQL)
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
		clearOutAllData(t, mcmp.VtConn, mcmp.MySQLConn)
		_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func startBenchmark(b *testing.B) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(b, err)
	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(b, err)

	clearOutAllData(b, vtConn, mysqlConn)
}

func clearOutAllData(t testing.TB, vtConn *mysql.Conn, mysqlConn *mysql.Conn) {
	tables := []string{"t4", "t3", "t2", "t1", "multicol_tbl2", "multicol_tbl1"}
	tables = append(tables, fkTables...)
	keyspaces := []string{`ks/-80`, `ks/80-`, `sks/-80`, `sks/80-`}
	for _, keyspace := range keyspaces {
		_ = utils.Exec(t, vtConn, fmt.Sprintf("use `%v`", keyspace))
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, vtConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
			_, _ = utils.ExecAllowError(t, mysqlConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
		}
	}

	tables = []string{"u_t1", "u_t2", "u_t3"}
	tables = append(tables, fkTables...)
	keyspaces = []string{`uks`, `unmanaged_uks`}
	for _, keyspace := range keyspaces {
		_ = utils.Exec(t, vtConn, fmt.Sprintf("use `%v`", keyspace))
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, vtConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
			_, _ = utils.ExecAllowError(t, mysqlConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
		}
	}
}
