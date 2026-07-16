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

package stress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/stress"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	sqlSchema       = `
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(sqlSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

// This test portrays an end-to-end test that runs on a loaded cluster.
// The stressor is started on its own goroutine while the end-to-end test
// is executed on the same cluster.
func TestSimpleStressTest(t *testing.T) {
	setup(t)
	cfg := stress.DefaultConfig
	cfg.ConnParams = &vtParams
	s := stress.New(t, cfg)

	s.Start()

	// Vitess end-to-end test here

	s.Stop()
}
