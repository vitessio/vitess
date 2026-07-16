/*
Copyright 2020 The Vitess Authors.

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

package unsharded

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var keyspaceName = "ks"

func setupCluster(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTGateArgs(
			"--dbddl-plugin", "noop",
			"--mysql-server-query-timeout", "60s",
			"--enable-system-settings",
		),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	return cluster, cluster.VTParams(ctx, "")
}

func TestDBDDLPlugin(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setupCluster(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	createAndDrop := func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Go(func() {
			qr := vitesst.Exec(t, conn, `create database aaa`)
			require.EqualValues(t, 1, qr.RowsAffected)
		})
		time.Sleep(300 * time.Millisecond)
		start(t, clusterInstance, "aaa")

		// wait until the create database query has returned
		wg.Wait()

		vitesst.Exec(t, conn, `use aaa`)
		vitesst.Exec(t, conn, `create table t (id bigint primary key)`)
		vitesst.Exec(t, conn, `insert into t(id) values (1),(2),(3),(4),(5)`)
		vitesst.AssertMatches(t, conn, "select count(*) from t", `[[INT64(5)]]`)

		wg.Go(func() {
			_ = vitesst.Exec(t, conn, `drop database aaa`)
		})
		time.Sleep(300 * time.Millisecond)
		shutdown(t, clusterInstance, "aaa")

		// wait until the drop database query has returned
		wg.Wait()

		_, err = conn.ExecuteFetch(`select count(*) from t`, 1000, true)
		require.Error(t, err)
	}
	t.Run("first try", func(t *testing.T) {
		createAndDrop(t)
	})
	if !t.Failed() {
		t.Run("second try", func(t *testing.T) {
			createAndDrop(t)
		})
	}
}

func start(t *testing.T, clusterInstance *vitesst.Cluster, ksName string) {
	_, err := clusterInstance.AddKeyspace(t, t.Context(), vitesst.WithKeyspace(ksName))
	require.NoError(t, err, "new database creation failed")
}

func shutdown(t *testing.T, clusterInstance *vitesst.Cluster, ksName string) {
	require.NoError(t,
		clusterInstance.Vtctld().ExecuteCommand(t.Context(), "DeleteKeyspace", "--recursive", ksName))

	require.NoError(t, clusterInstance.RemoveKeyspace(t.Context(), ksName))

	require.NoError(t,
		clusterInstance.Vtctld().ExecuteCommand(t.Context(), "RebuildVSchemaGraph"))
}
