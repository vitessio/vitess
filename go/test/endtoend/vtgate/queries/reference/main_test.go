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

package reference

import (
	"context"
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams

	unshardedKeyspaceName = "uks"
	//go:embed uschema.sql
	unshardedSQLSchema string
	//go:embed uvschema.json
	unshardedVSchema string

	shardedKeyspaceName = "sks"
	//go:embed sschema.sql
	shardedSQLSchema string
	//go:embed svschema.json
	shardedVSchema string
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(unshardedKeyspaceName).
			WithSchema(unshardedSQLSchema).
			WithVSchema(unshardedVSchema),
		vitesst.WithKeyspace(shardedKeyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(shardedSQLSchema).
			WithVSchema(shardedVSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	output, err := cluster.Vtctld().ExecuteCommandWithOutput(
		ctx,
		"Materialize",
		"--workflow", "copy_zip_detail",
		"--target-keyspace", shardedKeyspaceName,
		"create",
		"--source-keyspace", unshardedKeyspaceName,
		"--table-settings",
		`[{"target_table": "zip_detail", "source_expression": "select * from zip_detail", "create_ddl": "copy" }]`,
		"--tablet-types", "PRIMARY",
	)
	t.Logf("output from materialize: %s", output)
	require.NoError(t, err)

	vtgateConn, err := cluster.VTGate().DialVTGate(ctx)
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session("@primary", nil)
	_, err = session.Execute(ctx, `
		INSERT INTO zip_detail(id, zip_id, discontinued_at)
		VALUES (1, 1, '2022-05-13'),
		       (2, 2, '2022-08-15')
	`, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	_, err = session.Execute(ctx, `
		INSERT INTO delivery_failure(id, zip_detail_id, reason)
		VALUES (1, 1, 'Failed delivery due to discontinued zipcode.'),
		       (2, 2, 'Failed delivery due to discontinued zipcode.'),
		       (3, 3, 'Failed delivery due to unknown reason.');
	`, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	waitForMaterialization(t, ctx, cluster)

	err = cluster.Vtctld().ExecuteCommand(
		ctx,
		"Workflow",
		"--keyspace", shardedKeyspaceName,
		"delete",
		"--workflow", "copy_zip_detail",
		"--keep-data",
	)
	require.NoError(t, err)
}

func waitForMaterialization(t *testing.T, ctx context.Context, cluster *vitesst.Cluster) {
	t.Helper()
	vtgateConn, err := cluster.VTGate().DialVTGate(ctx)
	require.NoError(t, err)
	defer vtgateConn.Close()

	deadline := time.NewTimer(300 * time.Second)
	defer deadline.Stop()

	for _, keyspace := range cluster.Keyspaces() {
		if keyspace.Name != shardedKeyspaceName {
			continue
		}
		for _, shardInfo := range keyspace.Shards() {
			shard := fmt.Sprintf("%s/%s@primary", keyspace.Name, shardInfo.Name)
			session := vtgateConn.Session(shard, nil)
			for {
				select {
				case <-deadline.C:
					require.FailNow(t, "materialization did not complete", "waited for shard %s", shard)
				default:
				}

				_, err := session.Execute(ctx, "SHOW CREATE TABLE zip_detail", map[string]*querypb.BindVariable{}, false)
				if err != nil {
					t.Logf("zip_detail is not ready on %s: %v", shard, err)
					time.Sleep(time.Second)
					continue
				}
				qr, err := session.Execute(ctx, "SELECT * FROM zip_detail", map[string]*querypb.BindVariable{}, false)
				require.NoError(t, err)
				if len(qr.Rows) != 2 {
					t.Logf("%s has %d zip_detail rows, waiting for 2", shard, len(qr.Rows))
					time.Sleep(10 * time.Second)
					continue
				}
				t.Logf("%s has expected number of zip_detail rows", shard)
				break
			}
		}
	}
}
