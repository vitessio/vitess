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

package sharded

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	clusterInstance *vitesst.Cluster
	keyspaceName    = "ks"
	sqlSchema       = `
		create table vt_select_test (
		id bigint not null,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB
		`
	sqlSchemaReverse = `
		create table vt_select_test (
		msg varchar(64),
		id bigint not null,
		primary key (id)
		) Engine=InnoDB
		`
	vSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"vt_select_test": {
			   "column_vindexes": [
				{
				  "column": "id",
				  "name": "hash_index"
				}
			  ]
			}
		  }
		}
	`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1),
		vitesst.WithVTOrc(),
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
}

func TestShardedKeyspace(t *testing.T) {
	setup(t)
	ctx := t.Context()

	shard1 := clusterInstance.Keyspaces()[0].Shards()[0]
	shard2 := clusterInstance.Keyspaces()[0].Shards()[1]

	shard1Primary := shard1.Primary()
	shard2Primary := shard2.Primary()

	// apply the schema on the first shard through vtctl, so all tablets
	// are the same.
	// apply the schema on the second shard.
	_, err := shard1Primary.QueryTablet(ctx, sqlSchema)
	require.Nil(t, err)

	_, err = shard2Primary.QueryTablet(ctx, sqlSchemaReverse)
	require.Nil(t, err)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ApplyVSchema", "--vschema", vSchema, keyspaceName)
	require.NoError(t, err)

	reloadSchemas(t,
		shard1Primary.Alias(),
		shard1.Replicas()[0].Alias(),
		shard2Primary.Alias(),
		shard2.Replicas()[0].Alias())

	_ = clusterInstance.Vtctld().ExecuteCommand(ctx, "SetWritable", shard1Primary.Alias(), "true")
	_ = clusterInstance.Vtctld().ExecuteCommand(ctx, "SetWritable", shard2Primary.Alias(), "true")

	_, _ = shard1Primary.QueryTablet(ctx, "insert into vt_select_test (id, msg) values (1, 'test 1')")
	_, _ = shard2Primary.QueryTablet(ctx, "insert into vt_select_test (id, msg) values (10, 'test 10')")

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "Validate", "--ping-tablets")
	require.Nil(t, err)

	rows, err := shard1Primary.QueryTablet(ctx, "select id, msg from vt_select_test order by id")
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(1) VARCHAR("test 1")]]`, fmt.Sprintf("%v", rows.Rows))

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ValidateSchemaShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ValidateSchemaShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)

	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "ValidateSchemaKeyspace", keyspaceName)
	require.NoError(t, err)
	// We should assert that there is a schema difference and that both the shard primaries are involved in it.
	// However, we cannot assert in which order the two primaries will occur since the underlying function does not guarantee that
	// We could have an output here like `schemas differ ... shard1Primary ... differs from: shard2Primary ...` or `schemas differ ... shard2Primary ... differs from: shard1Primary ...`
	assert.Contains(t, output, "schemas differ on table vt_select_test:")
	assert.Contains(t, output, paddedAlias(shard1Primary)+": CREATE TABLE")
	assert.Contains(t, output, paddedAlias(shard2Primary)+": CREATE TABLE")

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ValidateVersionShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "GetPermissions", shard1.Replicas()[0].Alias())
	require.Nil(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ValidatePermissionsShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ValidatePermissionsKeyspace", keyspaceName)
	require.Nil(t, err)

	rows, err = shard1Primary.QueryTablet(ctx, "select id, msg from vt_select_test order by id")
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(1) VARCHAR("test 1")]]`, fmt.Sprintf("%v", rows.Rows))

	rows, err = shard2Primary.QueryTablet(ctx, "select id, msg from vt_select_test order by id")
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(10) VARCHAR("test 10")]]`, fmt.Sprintf("%v", rows.Rows))
}

func paddedAlias(t *vitesst.Tablet) string {
	return topoproto.TabletAliasString(&topodatapb.TabletAlias{
		Cell: t.Cell,
		Uid:  uint32(t.UID),
	})
}

func reloadSchemas(t *testing.T, aliases ...string) {
	for _, alias := range aliases {
		if err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "ReloadSchema", alias); err != nil {
			assert.Fail(t, "Unable to reload schema")
		}
	}
}
