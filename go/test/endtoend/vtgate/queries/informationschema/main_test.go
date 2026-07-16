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

package informationschema

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
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema      string
	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
)

func setup(t *testing.T) {
	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithVTGateArgs("--schema-change-signal"),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(schemaSQL).
			WithVSchema(vschema),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	err = cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", routingRules)
	require.NoError(t, err)
	err = cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph")
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
	conn, closer, err := vitesst.NewMySQL(ctx, cluster, keyspaceName, schemaSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := closer(cleanupCtx); err != nil {
			t.Logf("mysql teardown: %v", err)
		}
	})
	mysqlParams = conn
}
