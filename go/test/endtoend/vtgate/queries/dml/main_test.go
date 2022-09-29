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

package dml

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	sKs             = "sks"
	uKs             = "uks"
	cell            = "test"

	//go:embed sharded_schema.sql
	sSchemaSQL string

	//go:embed vschema.json
	sVSchema string

	//go:embed unsharded_schema.sql
	uSchemaSQL string

	uVSchema = `
{
  "tables": {
    "u_tbl": {},
    "user_seq": {
       "type":   "sequence"
    },
    "auto_seq": {
       "type":   "sequence"
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start unsharded keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      uKs,
			SchemaSQL: uSchemaSQL,
			VSchema:   uVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start sharded keyspace
		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchemaSQL,
			VSchema:   sVSchema,
		}
		err = clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(sKs)
		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, sKs, sSchemaSQL, uSchemaSQL)
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

		tables := []string{
			"s_tbl", "num_vdx_tbl", "user_tbl", "order_tbl", "oevent_tbl", "oextra_tbl",
			"auto_tbl", "oid_vdx_tbl", "unq_idx", "nonunq_idx", "u_tbl",
		}
		for _, table := range tables {
			// TODO (@frouioui): following assertions produce different results between MySQL and Vitess
			//  their differences are ignored for now. Fix it.
			// delete from returns different RowsAffected and Flag values
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
