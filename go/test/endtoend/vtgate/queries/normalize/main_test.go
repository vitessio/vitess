/*
Copyright 2021 The Vitess Authors.

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

package normalize

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
	keyspaceName    = "ks_normalize"

	//go:embed schema.sql
	schemaSQL string
	//go:embed vschema.json
	_ string
)

func setup(t testing.TB) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithSchema(schemaSQL),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}
