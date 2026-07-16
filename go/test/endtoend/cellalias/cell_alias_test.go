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

This test cell aliases feature

We start with no aliases and assert that vtgates can't route to replicas/rondly tablets.
Then we add an alias, and these tablets should be routable
*/

package binlog

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	clusterInstance *vitesst.Cluster
	cell1           = "zone1"
	cell2           = "zone2"
	keyspaceName    = "ks"
	tableName       = "test_table"
	sqlSchema       = `
					create table %s(
					id bigint(20) unsigned auto_increment,
					msg varchar(64),
					primary key (id),
					index by_msg (msg)
					) Engine=InnoDB
`
	insertTabletTemplateKsID = `insert into %s (id, msg) values (%d, '%s') /* id:%d */`
	commonTabletArg          = []string{
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
		"--serving-state-grace-period", "1s",
		"--binlog-player-protocol", "grpc",
	}
	vSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"%s": {
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

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell1, cell2),
		vitesst.WithVTOrc(),
		vitesst.WithVTTabletArgs(commonTabletArg...),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithRDOnly(1).
			WithDurabilityPolicy("semi_sync").
			WithSchema(fmt.Sprintf(sqlSchema, tableName)).
			WithVSchema(fmt.Sprintf(vSchema, tableName)).
			WithTabletSpec(func(spec *vitesst.TabletSpec) {
				if spec.Type == "primary" {
					spec.Cell = cell1
				} else {
					spec.Cell = cell2
				}
			}),
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

	shard1 := cluster.Keyspace(keyspaceName).Shard("-80")
	// run a health check on source replica so it responds to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, tablet := range []*vitesst.Tablet{shard1.Replicas()[0], shard1.RDOnly()[0]} {
		require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", tablet.Alias()))
	}

	_ = cluster.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceName)
}

func TestAlias(t *testing.T) {
	setup(t)
	ctx := t.Context()

	insertInitialValues(t)
	defer deleteInitialValues(t)

	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)
	shard1 := clusterInstance.Keyspace(keyspaceName).Shard("-80")
	shard2 := clusterInstance.Keyspace(keyspaceName).Shard("80-")
	allCells := fmt.Sprintf("%s,%s", cell1, cell2)

	expectedPartitions := map[topodatapb.TabletType][]string{}
	expectedPartitions[topodatapb.TabletType_PRIMARY] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodatapb.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodatapb.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	checkSrvKeyspace(t, cell1, keyspaceName, expectedPartitions)
	checkSrvKeyspace(t, cell2, keyspaceName, expectedPartitions)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "AddCellsAlias",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "UpdateCellsAlias",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	vtgateInstance, err := clusterInstance.AddVTGate(t, ctx, "--tablet-types-to-wait", "PRIMARY,REPLICA")
	require.NoError(t, err)

	waitTillAllTabletsAreHealthyInVtgate(t, vtgateInstance, shard1.Name, shard2.Name)

	testQueriesOnTabletType(t, "primary", vtgateInstance, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance, false)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance, false)

	// now, delete the alias, so that if we run above assertions again, it will fail for replica,rdonly target type
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "DeleteCellsAlias",
		"region_east_coast")
	require.NoError(t, err)

	// restarts the vtgate process
	err = vtgateInstance.Restart(t, ctx, "--tablet-types-to-wait", "PRIMARY")
	require.NoError(t, err)

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "primary", vtgateInstance, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance, true)
}

func TestAddAliasWhileVtgateUp(t *testing.T) {
	setup(t)
	ctx := t.Context()

	insertInitialValues(t)
	defer deleteInitialValues(t)

	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)
	shard1 := clusterInstance.Keyspace(keyspaceName).Shard("-80")
	shard2 := clusterInstance.Keyspace(keyspaceName).Shard("80-")
	allCells := fmt.Sprintf("%s,%s", cell1, cell2)

	expectedPartitions := map[topodatapb.TabletType][]string{}
	expectedPartitions[topodatapb.TabletType_PRIMARY] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodatapb.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodatapb.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	checkSrvKeyspace(t, cell1, keyspaceName, expectedPartitions)
	checkSrvKeyspace(t, cell2, keyspaceName, expectedPartitions)

	// only primary is in vtgate's "cell", other tablet types are not visible because they are in the other cell
	vtgateInstance, err := clusterInstance.AddVTGate(t, ctx, "--tablet-types-to-wait", "PRIMARY")
	require.NoError(t, err)

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "primary", vtgateInstance, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance, true)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "AddCellsAlias",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	testQueriesOnTabletType(t, "primary", vtgateInstance, false)
	// TODO(deepthi) change the following to shouldFail:false when fixing https://github.com/vitessio/vitess/issues/5911
	testQueriesOnTabletType(t, "replica", vtgateInstance, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance, true)
}

func waitTillAllTabletsAreHealthyInVtgate(t *testing.T, vtgateInstance *vitesst.VTGate, shards ...string) {
	for _, shard := range shards {
		waitForHealthyTablet(t, vtgateInstance, fmt.Sprintf("%s.%s.primary", keyspaceName, shard))
		waitForHealthyTablet(t, vtgateInstance, fmt.Sprintf("%s.%s.replica", keyspaceName, shard))
		waitForHealthyTablet(t, vtgateInstance, fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard))
	}
}

func waitForHealthyTablet(t *testing.T, vtgateInstance *vitesst.VTGate, name string) {
	require.Eventually(t, func() bool {
		vars, err := vtgateInstance.GetVars(t.Context())
		if err != nil {
			return false
		}
		conns, ok := vars["HealthcheckConnections"].(map[string]any)
		if !ok {
			return false
		}
		count, ok := conns[name].(float64)
		return ok && int(count) == 1
	}, 30*time.Second, 300*time.Millisecond)
}

func testQueriesOnTabletType(t *testing.T, tabletType string, vtgateInstance *vitesst.VTGate, shouldFail bool) {
	qr, err := execOnVTGate(t.Context(), vtgateInstance, "@"+tabletType, "select * from "+tableName)
	if shouldFail {
		require.Error(t, err)
		return
	}
	assert.Equal(t, len(qr.Rows), 3)
}

func execOnVTGate(ctx context.Context, vtgateInstance *vitesst.VTGate, target, sql string) (*sqltypes.Result, error) {
	conn, err := vtgateInstance.DialVTGate(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return conn.Session(target, nil).Execute(ctx, sql, nil, false)
}

func checkSrvKeyspace(t *testing.T, cell string, ksname string, expectedPartition map[topodatapb.TabletType][]string) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetSrvKeyspaces", ksname, cell)
	require.NoError(t, err)

	srvKeyspaces := map[string]*topodatapb.SrvKeyspace{}
	require.NoError(t, json2.Unmarshal([]byte(out), &srvKeyspaces))

	srvKeyspace := srvKeyspaces[cell]
	require.NotNil(t, srvKeyspace, "srvKeyspace is nil for %s", cell)

	currentPartition := map[topodatapb.TabletType][]string{}

	for _, partition := range srvKeyspace.Partitions {
		currentPartition[partition.ServedType] = []string{}
		for _, shardRef := range partition.ShardReferences {
			currentPartition[partition.ServedType] = append(currentPartition[partition.ServedType], shardRef.Name)
		}
	}

	assert.True(t, reflect.DeepEqual(currentPartition, expectedPartition))
}

func executeOnTablet(t *testing.T, query string, tablet *vitesst.Tablet, expectFail bool) {
	ctx := t.Context()
	_, _ = tablet.QueryTablet(ctx, "begin")
	_, err := tablet.QueryTablet(ctx, query)
	if expectFail {
		require.Error(t, err)
	} else {
		require.Nil(t, err)
	}
	_, _ = tablet.QueryTablet(ctx, "commit")
}

func insertInitialValues(t *testing.T) {
	ks := clusterInstance.Keyspace(keyspaceName)
	shard1Primary := ks.Shard("-80").Primary()
	shard2Primary := ks.Shard("80-").Primary()

	executeOnTablet(t,
		fmt.Sprintf(insertTabletTemplateKsID, tableName, 1, "msg1", 1),
		shard1Primary,
		false)

	executeOnTablet(t,
		fmt.Sprintf(insertTabletTemplateKsID, tableName, 2, "msg2", 2),
		shard1Primary,
		false)

	executeOnTablet(t,
		fmt.Sprintf(insertTabletTemplateKsID, tableName, 4, "msg4", 4),
		shard2Primary,
		false)
}

func deleteInitialValues(t *testing.T) {
	ks := clusterInstance.Keyspace(keyspaceName)
	shard1Primary := ks.Shard("-80").Primary()
	shard2Primary := ks.Shard("80-").Primary()

	executeOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 1),
		shard1Primary,
		false)

	executeOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 2),
		shard1Primary,
		false)

	executeOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 4),
		shard2Primary,
		false)
}
