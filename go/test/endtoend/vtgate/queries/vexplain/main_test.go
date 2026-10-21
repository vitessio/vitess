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

package vexplain

import (
	_ "embed"
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	shardedKs       = "ks"
	unshardedKs     = "uks"

	shardedKsShards = []string{"-40", "40-80", "80-c0", "c0-"}
	Cell            = "test"
	//go:embed schema.sql
	shardedSchemaSQL string

	//go:embed vschema.json
	shardedVSchema string

	//go:embed unsharded_schema.sql
	unshardedSchemaSQL string

	//go:embed unsharded_vschema.json
	unshardedVSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start unsharded keyspace. VEXPLAIN MYSQLPLAN's reserved-connection test
		// creates a temporary table, which Vitess only allows on an unsharded
		// keyspace.
		uKs := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSchemaSQL,
			VSchema:   unshardedVSchema,
		}

		err = clusterInstance.StartUnshardedKeyspace(*uKs, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		// Start keyspace
		sKs := &cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}

		err = clusterInstance.StartKeyspace(*sKs, shardedKsShards, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
