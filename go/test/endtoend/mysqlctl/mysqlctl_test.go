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
	"flag"
	"fmt"
	"os"
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

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		// The keyspace starts two tablets without electing a primary, so
		// TestAutoDetect drives the first flavor-detecting reparent itself.
		cluster, err := vitesst.NewCluster(
			vitesst.WithoutVTGate(),
			vitesst.WithKeyspace(keyspaceName).
				WithReplicas(1).
				WithoutPrimaryElection(),
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
		shard = cluster.Keyspace(keyspaceName).Shards()[0]
		tablets := shard.Tablets()
		primaryTablet = tablets[0]
		replicaTablet = tablets[1]

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestRestart(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, primaryTablet.StopMySQL(ctx))
	require.NoError(t, primaryTablet.RemoveFile(ctx, primaryTablet.TabletDir()))
	require.NoError(t, primaryTablet.StartMySQL(ctx))
	require.NoError(t, primaryTablet.WaitForTabletStatus(ctx, mysqldRestartTimeout, "SERVING", "NOT_SERVING"))
}

func TestAutoDetect(t *testing.T) {
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
