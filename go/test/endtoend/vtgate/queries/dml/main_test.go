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

package dml

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
	sKs             = "sks"
	uKs             = "uks"

	//go:embed sharded_schema.sql
	sSchemaSQL string

	//go:embed vschema.json
	sVSchema string

	//go:embed unsharded_schema.sql
	uSchemaSQL string

	uVSchema = `
{
  "tables": {
    "u_tbl": {},
    "user_seq": {
       "type":   "sequence"
    },
    "auto_seq": {
       "type":   "sequence"
    },
    "mixed_seq": {
       "type":   "sequence"
    }
  }
}`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(uKs).
			WithSchema(uSchemaSQL).
			WithVSchema(uVSchema),
		vitesst.WithKeyspace(sKs).
			WithShardNames("-80", "80-").
			WithSchema(sSchemaSQL).
			WithVSchema(sVSchema).
			WithTabletArgs(
				"--queryserver-config-passthrough-dmls",
				"--queryserver-config-max-result-size", "10",
			),
		vitesst.WithVTGateArgs("--vtgate-config-terse-errors", "--planner-version", "Gen4"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
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

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	conn, closer, err := vitesst.NewMySQL(ctx, cluster, sKs, sSchemaSQL, uSchemaSQL)
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

func start(t *testing.T) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{
			"s_tbl", "num_vdx_tbl", "col_vdx_tbl", "user_tbl", "order_tbl", "oevent_tbl", "oextra_tbl",
			"auto_tbl", "oid_vdx_tbl", "unq_idx", "nonunq_idx", "u_tbl", "mixed_tbl", "j_tbl", "j_utbl",
			"t1", "t2",
		}
		for _, table := range tables {
			// TODO (@frouioui): following assertions produce different results between MySQL and Vitess
			//  their differences are ignored for now. Fix it.
			// delete from returns different RowsAffected and Flag values
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}
