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
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	shardedKs       = "ks"
	unshardedKs     = "uks"
	shardedKsShards = []string{"-19a0", "19a0-20", "20-20c0", "20c0-"}
	Cell            = "test"
	//go:embed sharded_schema.sql
	shardedSchemaSQL string

	//go:embed unsharded_schema.sql
	unshardedSchemaSQL string

	//go:embed sharded_vschema.json
	shardedVSchema string

	//go:embed unsharded_vschema.json
	unshardedVSchema string

	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
	unsharded2Ks = "uks2"

	//go:embed unsharded2_schema.sql
	unsharded2SchemaSQL string
)

func setup(t testing.TB) {
	t.Helper()

	clusterInstance = nil
	vtParams = mysql.ConnParams{}
	mysqlParams = mysql.ConnParams{}

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(Cell),
		vitesst.WithVTGateArgs("--schema-change-signal"),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
		vitesst.WithKeyspace(shardedKs).
			WithShardNames(shardedKsShards...).
			WithSchema(shardedSchemaSQL).
			WithVSchema(shardedVSchema),
		vitesst.WithKeyspace(unshardedKs).
			WithSchema(unshardedSchemaSQL).
			WithVSchema(unshardedVSchema),
		// This keyspace is used to test automatic addition of tables to global routing rules when
		// there are multiple unsharded keyspaces.
		vitesst.WithKeyspace(unsharded2Ks).
			WithSchema(unsharded2SchemaSQL),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		clusterInstance = nil
		vtParams = mysql.ConnParams{}
		require.NoError(t, cleanup(cleanupCtx))
	})

	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", routingRules))
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph"))

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	conn, closer, err := vitesst.NewMySQL(ctx, cluster, shardedKs, shardedSchemaSQL)
	require.NoError(t, err)
	mysqlParams = conn
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		mysqlParams = mysql.ConnParams{}
		require.NoError(t, closer(cleanupCtx))
	})
}

func start(t *testing.T) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)
	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t2", "t3", "user_region", "region_tbl", "multicol_tbl", "t1_id2_idx", "t2_id4_idx", "u_a", "u_b"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}
