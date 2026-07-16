/*
Copyright 2026 The Vitess Authors.

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

package preventcrosskeyspacereads

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
	ks1Name         = "ks1"
	ks2Name         = "ks2"

	//go:embed ks1_schema.sql
	ks1SchemaSQL string

	//go:embed ks2_schema.sql
	ks2SchemaSQL string

	//go:embed ks1_vschema.json
	ks1VSchema string

	//go:embed ks2_vschema.json
	ks2VSchema string
)

func setup(t testing.TB) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		// ks1 has prevent_cross_keyspace_reads: true in vschema.
		vitesst.WithKeyspace(ks1Name).
			WithSchema(ks1SchemaSQL).
			WithVSchema(ks1VSchema),
		// ks2 has no cross-keyspace join restriction.
		vitesst.WithKeyspace(ks2Name).
			WithSchema(ks2SchemaSQL).
			WithVSchema(ks2VSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}
