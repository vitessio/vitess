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

package schematracker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
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

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithSchema(sqlSchema),
		// List of users authorized to execute vschema ddl operations
		vitesst.WithVTGateArgs("--schema-change-signal"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	// restart the tablet so that the schema.Engine gets a chance to start with existing schema
	tablet := cluster.Keyspace(keyspaceName).Shard("-").Primary()
	require.NoError(t, tablet.StopVttablet(ctx))
	require.NoError(t, tablet.StartVttablet(ctx, "--queryserver-config-schema-change-signal"))
	require.NoError(t, cluster.WaitForHealthyShard(ctx, keyspaceName, "-", 5*time.Minute))

	return cluster, cluster.VTParams(ctx, "")
}

func TestVSchemaTrackerInit(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	want := `[[VARCHAR("main")] [VARCHAR("test_table")] [VARCHAR("vt_user")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		want,
		100*time.Millisecond,
		60*time.Second,
		"initial table list not complete")

	vitesst.AssertMatches(t, conn, "SHOW VSCHEMA KEYSPACES", `[[VARCHAR("ks") VARCHAR("false") VARCHAR("unmanaged") VARCHAR("")]]`)
}

// TestVSchemaTrackerKeyspaceReInit tests that the vschema tracker
// properly handles primary tablet restarts -- meaning that we maintain
// the exact same vschema state as before the restart.
func TestVSchemaTrackerKeyspaceReInit(t *testing.T) {
	ctx := t.Context()
	cluster, _ := setup(t)
	primaryTablet := cluster.Keyspace(keyspaceName).Shard("-").Primary()

	// get the vschema prior to the restarts
	originalResults := readVSchema(t, cluster)
	assert.NotNil(t, originalResults)

	// restart the primary tablet so that the vschema gets reloaded for the keyspace
	for range 5 {
		err := primaryTablet.StopVttablet(ctx)
		require.NoError(t, err)
		err = primaryTablet.StartVttablet(ctx)
		require.NoError(t, err)
		waitForHealthyInVtgate(t, cluster)

		vitesst.TimeoutAction(t, 1*time.Minute, "timeout - could not find the updated vschema in VTGate", func() bool {
			newResults := readVSchema(t, cluster)
			return assert.ObjectsAreEqual(originalResults, newResults)
		})
	}
}

// waitForHealthyInVtgate waits until vtgate's healthcheck sees the keyspace's
// primary tablet as serving again.
func waitForHealthyInVtgate(t *testing.T, cluster *vitesst.Cluster) {
	t.Helper()
	_, _, err := cluster.VTGate().MakeAPICallRetry(t.Context(), "/debug/vars", 2*time.Minute, func(status int, body string) bool {
		if status != 200 {
			return false
		}
		var vars struct {
			HealthcheckConnections map[string]float64 `json:"HealthcheckConnections"`
		}
		if err := json.Unmarshal([]byte(body), &vars); err != nil {
			return false
		}
		return vars.HealthcheckConnections[keyspaceName+".-.primary"] == 1
	})
	require.NoError(t, err)
}

func readVSchema(t *testing.T, cluster *vitesst.Cluster) any {
	results, err := cluster.VTGate().ReadVSchema(t.Context())
	require.NoError(t, err)
	require.NotNil(t, results)
	return *results
}
