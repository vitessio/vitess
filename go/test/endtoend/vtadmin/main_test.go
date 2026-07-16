/*
Copyright 2024 The Vitess Authors.

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

package vtadmin

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	uks             = "uks"

	uschemaSQL = `
create table u_a
(
    id bigint,
    a  bigint,
    primary key (id)
) Engine = InnoDB;

create table u_b
(
    id bigint,
    b  varchar(50),
    primary key (id)
) Engine = InnoDB;

`
)

func setupCluster(t testing.TB) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(uks).
			WithSchema(uschemaSQL),
		vitesst.WithVTAdmin(),
		vitesst.WithVTAdminClusterID("local_test"),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cleanup(context.WithoutCancel(ctx)))
	})

	clusterInstance = cluster
}

// TestVtadminAPIs tests the vtadmin APIs.
func TestVtadminAPIs(t *testing.T) {
	setupCluster(t)
	// Test the vtadmin APIs
	t.Run("keyspaces api", func(t *testing.T) {
		_, resp, err := clusterInstance.VTAdmin().MakeAPICallRetry(t.Context(), "/api/keyspaces", 10*time.Second,
			func(status int, body string) bool {
				return status == http.StatusOK
			})
		require.NoError(t, err)
		require.Contains(t, resp, uks)
	})
}
