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

package loadkeyspace

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	keyspaceName = "ks"
	sqlSchema    = `
		create table vt_user (
			id bigint,
			name varchar(64),
			primary key (id)
		) Engine=InnoDB;

		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;

		create table test_table (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestLoadKeyspaceWithNoTablet(t *testing.T) {
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(sqlSchema),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, cleanup(context.WithoutCancel(ctx)))
	})
	require.NoError(t, err)

	// teardown vttablets
	for _, tablet := range cluster.Keyspace(keyspaceName).Tablets() {
		require.NoError(t, tablet.Remove(ctx))
	}

	// Start vtgate with the schema-change-signal flag
	vtgate, err := cluster.AddVTGate(t, ctx, "--schema-change-signal")
	require.NoError(t, err)

	// After starting VTGate we need to leave enough time for resolveAndLoadKeyspace to reach
	// the schema tracking timeout (5 seconds).
	vitesst.TimeoutAction(t, 5*time.Minute, "timeout - could not find 'Unable to get initial schema reload' in vtgate logs", func() bool {
		logs, err := vtgate.Logs(t.Context())
		return err == nil && strings.Contains(logs, "Unable to get initial schema reload")
	})
}

func TestNoInitialKeyspace(t *testing.T) {
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, cleanup(context.WithoutCancel(ctx)))
	})
	require.NoError(t, err)

	// remove the keyspace so vtgate has none to load
	for _, tablet := range cluster.Keyspace(keyspaceName).Tablets() {
		require.NoError(t, tablet.Remove(ctx))
	}
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "DeleteKeyspace", "--recursive", keyspaceName))

	// Start vtgate with the schema-change-signal flag
	vtgate, err := cluster.AddVTGate(t, ctx, "--schema-change-signal")
	require.NoError(t, err)

	// check logs
	vitesst.TimeoutAction(t, 5*time.Minute, "timeout - could not find 'No keyspace to load' in vtgate logs", func() bool {
		logs, err := vtgate.Logs(t.Context())
		return err == nil && strings.Contains(logs, "No keyspace to load")
	})
}
