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
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithKeyspace(KeyspaceName).
				WithShards(2).
				WithSchema(SchemaSQL).
				WithVSchema(VSchema),
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

func start(t *testing.T) (*mysql.Conn, func()) {
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
	conn, closer := start(t)
	defer closer()

	vitesst.AssertMatches(t, conn, `select 1 from t1 where c2 in ("abc")`, "[]")
}
