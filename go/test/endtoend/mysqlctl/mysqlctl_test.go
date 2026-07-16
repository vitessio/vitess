/*
Copyright 2019 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	shard           *vitesst.Shard
	primaryTablet   *vitesst.Tablet
	replicaTablet   *vitesst.Tablet
	keyspaceName    = "test_keyspace"
)

// mysqldRestartTimeout bounds how long the tablet may take to serve again
// after mysqld is stopped, wiped, and restarted.
const mysqldRestartTimeout = 3 * time.Minute

func setup(t *testing.T) {
	t.Helper()

	// The keyspace starts two tablets without electing a primary, so
	// TestAutoDetect drives the first flavor-detecting reparent itself.
	cluster, err := vitesst.NewCluster(
		vitesst.WithoutVTGate(),
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithoutPrimaryElection(),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(ctx, t.Logf)
		}
		if cleanupErr := cleanup(ctx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	shard = cluster.Keyspace(keyspaceName).Shards()[0]
	tablets := shard.Tablets()
	primaryTablet = tablets[0]
	replicaTablet = tablets[1]
}

func TestRestart(t *testing.T) {
	setup(t)

	ctx := t.Context()

	require.NoError(t, primaryTablet.StopMySQL(ctx))
	require.NoError(t, primaryTablet.RemoveFile(ctx, primaryTablet.TabletDir()))
	require.NoError(t, primaryTablet.StartMySQL(ctx))
	require.NoError(t, primaryTablet.WaitForTabletStatus(ctx, mysqldRestartTimeout, "SERVING", "NOT_SERVING"))
}

func TestAutoDetect(t *testing.T) {
	setup(t)

	ctx := t.Context()

	// Electing the shard's first primary reparents both tablets, which
	// requires MySQL flavor detection to succeed.
	err := clusterInstance.Vtctld().ExecuteCommand(
		ctx,
		"PlannedReparentShard", shard.Ref(),
		"--wait-replicas-timeout", "31s",
		"--new-primary", primaryTablet.Alias(),
	)
	require.NoError(t, err)
}
