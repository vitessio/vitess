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

// vtgateConfigPath is the config file each vtgate watches inside its
// container. It is staged world-writable so TestDynamicConfig can rewrite it
// as the unprivileged container user.
const vtgateConfigPath = "/vt/files/vtgate.json"

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string

	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
)

func setupCluster(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithVTTabletArgs(
			"--queryserver-config-max-result-size", "100",
			"--queryserver-config-terse-errors",
		),
		vitesst.WithVTGateArgs("--vschema-ddl-authorized-users", "%"),
		vitesst.WithVTGateFiles(vitesst.ContainerFile{
			Content:       []byte("{}\n"),
			ContainerPath: vtgateConfigPath,
			Mode:          0o666,
		}),
		vitesst.WithKeyspace(KeyspaceName).
			WithShards(2).
			WithReplicas(2).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", routingRules))
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph"))

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		vitesst.Exec(t, conn, "use ks")
		tables := []string{"t1", "t2", "vstream_test", "t3", "t4", "t6", "t7_xxhash", "t7_xxhash_idx", "t7_fk", "t8", "t9", "t9_id_to_keyspace_id_idx", "t10", "t10_id_to_keyspace_id_idx", "t1_id2_idx", "t2_id4_idx", "t3_id7_idx", "t4_id2_idx", "t5_null_vindex", "t6_id2_idx"}
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, conn, "delete from "+table)
		}
		vitesst.Exec(t, conn, "set workload = oltp")
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
	}
}
