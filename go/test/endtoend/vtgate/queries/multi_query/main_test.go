/*
Copyright 2025 The Vitess Authors.

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

package multi_query

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
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema string
)

func setup(t *testing.T) {
	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithVTGateArgs("--mysql-server-multi-query-protocol"),
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

func start(t *testing.T) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t2"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	// insert data
	queries := []string{
		`INSERT INTO t1 (id1, id2, id3) VALUES
	(1, 'abc123', 'xyz789'),
	(2, 'def456', 'uvw123'),
	(3, 'ghi789', 'rst456'),
	(4, 'jkl012', 'opq987'),
	(5, 'mno345', 'lmn654'),
	(6, 'pqr678', 'hij321');`,
		`INSERT INTO t2 (id5, id6, id7) VALUES
	(1, 11, 21),
	(2, 12, 22),
	(10, 3, 23),
	(4, 14, 24),
	(11, 15, 25);`,
	}
	for _, query := range queries {
		mcmp.Exec(query)
	}

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}
