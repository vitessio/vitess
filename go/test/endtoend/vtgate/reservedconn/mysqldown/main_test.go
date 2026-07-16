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

package mysqldown

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	sqlSchema       = `create table test(id bigint primary key)Engine=InnoDB;`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTOrc(),
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(2).
			WithSchema(sqlSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func TestMysqlDownServingChange(t *testing.T) {
	setup(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set default_week_format = 1")
	_ = vitesst.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ * from test")

	// Disable VTOrc emergency reparents to prevent VTOrc from racing with the manual ERS below.
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "SetVtorcEmergencyReparent", "--disable", keyspaceName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = clusterInstance.Vtctld().ExecuteCommandWithOutput(context.WithoutCancel(ctx), "SetVtorcEmergencyReparent", "--enable", keyspaceName)
	})

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	primaryTablet := shard.Primary()
	require.NoError(t,
		primaryTablet.StopMySQL(ctx))
	require.NoError(t,
		clusterInstance.Vtctld().ExecuteCommand(ctx, "EmergencyReparentShard", shard.Ref()))

	// This should work without any error.
	_ = vitesst.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ * from test")
}
