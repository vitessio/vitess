/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	sKs1            = "sks1"
	sKs2            = "sks2"
	sKs3            = "sks3"

	//go:embed sharded_schema1.sql
	sSchemaSQL1 string

	//go:embed vschema1.json
	sVSchema1 string

	//go:embed sharded_schema2.sql
	sSchemaSQL2 string

	//go:embed vschema2.json
	sVSchema2 string

	//go:embed sharded_schema3.sql
	sSchemaSQL3 string

	//go:embed vschema3.json
	sVSchema3 string
)

var shards4 = []string{
	"-40", "40-80", "80-c0", "c0-",
}

func setup(b *testing.B) {
	b.Helper()
	b.StopTimer()
	ctx := b.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(sKs1).
			WithShardNames(shards4...).
			WithSchema(sSchemaSQL1).
			WithVSchema(sVSchema1),
		vitesst.WithKeyspace(sKs2).
			WithShardNames(shards4...).
			WithSchema(sSchemaSQL2).
			WithVSchema(sVSchema2),
		vitesst.WithKeyspace(sKs3).
			WithShardNames(shards4...).
			WithSchema(sSchemaSQL3).
			WithVSchema(sVSchema3),
	)
	require.NoError(b, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(b, err)
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(b.Context()), time.Minute)
		defer cancel()
		if b.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, b.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			b.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "@primary")
}

func start(b *testing.B) (*mysql.Conn, func()) {
	b.Helper()
	b.StopTimer()
	conn, err := mysql.Connect(b.Context(), &vtParams)
	require.NoError(b, err)

	deleteAll := func() {
		tables := []string{
			sKs1 + ".tbl_no_lkp_vdx",
			sKs1 + ".mirror_tbl1",
			sKs1 + ".mirror_tbl2",
			sKs2 + ".mirror_tbl1",
			sKs3 + ".mirror_tbl2",
		}
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(b, conn, "delete from "+table)
		}
	}

	// Make sure all keyspaces are serving.
	pending := map[string]string{
		sKs1: "mirror_tbl1",
		sKs2: "mirror_tbl1",
		sKs3: "mirror_tbl2",
	}
	for len(pending) > 0 {
		for ks, tbl := range pending {
			_, err := conn.ExecuteFetch(
				fmt.Sprintf("SELECT COUNT(id) FROM %s.%s", ks, tbl), 1, false,
			)
			if err != nil {
				b.Logf("waiting for keyspace %s to be serving; last error: %v", ks, err)
				time.Sleep(1 * time.Second)
			} else {
				delete(pending, ks)
			}
		}
	}

	// Delete any pre-existing data.
	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
	}
}
