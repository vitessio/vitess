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

package misc

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
	keyspaceName    = "ks_misc"
	uks             = "uks"

	//go:embed uschema.sql
	uschemaSQL string

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema string
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithVTTabletArgs(
			"--queryserver-config-max-result-size", "1000000",
			"--queryserver-config-query-timeout", "2s",
			"--queryserver-config-transaction-timeout", "3s",
			"--queryserver-config-query-pool-timeout", "2s",
		),
		vitesst.WithVTGateArgs("--query-timeout", "100"),
		vitesst.WithKeyspace(uks).
			WithSchema(uschemaSQL),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(schemaSQL).
			WithVSchema(vschema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	conn, closer, err := vitesst.NewMySQL(ctx, cluster, keyspaceName, schemaSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := closer(cleanupCtx); err != nil {
			t.Logf("mysql teardown: %v", err)
		}
	})
	mysqlParams = conn
}
