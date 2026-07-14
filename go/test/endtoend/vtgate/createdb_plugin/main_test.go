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
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTGateArgs(
				"--dbddl-plugin", "noop",
				"--mysql-server-query-timeout", "60s",
				"--enable-system-settings",
			),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-80", "80-"),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestDBDDLPlugin(t *testing.T) {
	ctx := t.Context()
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
		start(t, "aaa")

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
		shutdown(t, "aaa")

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

func start(t *testing.T, ksName string) {
	_, err := clusterInstance.AddKeyspace(t.Context(), vitesst.WithKeyspace(ksName))
	require.NoError(t, err, "new database creation failed")
}

func shutdown(t *testing.T, ksName string) {
	require.NoError(t,
		clusterInstance.Vtctld().ExecuteCommand(t.Context(), "DeleteKeyspace", "--recursive", ksName))

	require.NoError(t, clusterInstance.RemoveKeyspace(t.Context(), ksName))

	require.NoError(t,
		clusterInstance.Vtctld().ExecuteCommand(t.Context(), "RebuildVSchemaGraph"))
}
