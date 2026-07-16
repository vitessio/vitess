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
	"vitess.io/vitess/go/vitesst"
)

var (
	KeyspaceName = "ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func setup(t *testing.T) mysql.ConnParams {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(KeyspaceName).
			WithShards(2).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	return cluster.VTParams(ctx, "")
}

func start(t *testing.T, vtParams mysql.ConnParams) (*mysql.Conn, func()) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, conn, "set workload = oltp")

		tables := []string{"t1", "lookup_t1"}
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, conn, "delete from "+table)
		}
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
	}
}

func TestInAgainstSecondaryVindex(t *testing.T) {
	vtParams := setup(t)
	conn, closer := start(t, vtParams)
	defer closer()

	vitesst.AssertMatches(t, conn, `select 1 from t1 where c2 in ("abc")`, "[]")
}
