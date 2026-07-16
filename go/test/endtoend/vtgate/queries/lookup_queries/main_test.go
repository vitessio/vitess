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

package vtgate

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	shardedKs       = "ks"

	shardedKsShards = []string{"-19a0", "19a0-20", "20-20c0", "20c0-"}
	//go:embed schema.sql
	shardedSchemaSQL string

	//go:embed vschema.json
	shardedVSchema string
)

func setup(t *testing.T) {
	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(shardedKs).
			WithShardNames(shardedKsShards...).
			WithSchema(shardedSchemaSQL).
			WithVSchema(shardedVSchema),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	err = cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph")
	require.NoError(t, err)
	vtParams = cluster.VTParams(ctx, "")
	conn, closer, err := vitesst.NewMySQL(ctx, cluster, shardedKs, shardedSchemaSQL)
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

func start(t *testing.T) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)
	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"user", "lookup"}
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

func TestLookupQueries(t *testing.T) {
	setup(t)
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`insert into user
    (id, lookup,   lookup_unique) values
	(1, 'apa',    'apa'),
	(2, 'apa',    'bandar'),
	(3, 'monkey', 'monkey')`)

	for _, workload := range []string{"olap", "oltp"} {
		mcmp.Run(workload, func(mcmp *vitesst.MySQLCompare) {
			vitesst.Exec(t, mcmp.VtConn, "set workload = "+workload)

			mcmp.AssertMatches("select id from user where lookup = 'apa'", "[[INT64(1)] [INT64(2)]]")
			mcmp.AssertMatches("select id from user where lookup = 'not there'", "[]")
			mcmp.AssertMatchesNoOrder("select id from user where lookup in ('apa', 'monkey')", "[[INT64(1)] [INT64(2)] [INT64(3)]]")
			mcmp.AssertMatches("select count(*) from user where lookup in ('apa', 'monkey')", "[[INT64(3)]]")

			mcmp.AssertMatches("select id from user where lookup_unique = 'apa'", "[[INT64(1)]]")
			mcmp.AssertMatches("select id from user where lookup_unique = 'not there'", "[]")
			mcmp.AssertMatchesNoOrder("select id from user where lookup_unique in ('apa', 'bandar')", "[[INT64(1)] [INT64(2)]]")
			mcmp.AssertMatches("select count(*) from user where lookup_unique in ('apa', 'monkey', 'bandar')", "[[INT64(3)]]")
		})
	}
}
