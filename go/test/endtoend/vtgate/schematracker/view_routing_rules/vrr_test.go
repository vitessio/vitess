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

package viewroutingrules

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	sourceKs        = "source_ks"
	targetKs        = "target_ks"
	cell            = "zone1"

	//go:embed source_schema.sql
	sourceSchema string

	//go:embed target_schema.sql
	targetSchema string

	//go:embed source_vschema.json
	sourceVSchema string

	//go:embed target_vschema.json
	targetVSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server.
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Enable views tracking.
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable-views")
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-views")

		// Start source keyspace (unsharded, with 1 replica for tablet type routing test).
		sks := cluster.Keyspace{
			Name:      sourceKs,
			SchemaSQL: sourceSchema,
			VSchema:   sourceVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(sks, 1, false, cell)
		if err != nil {
			return 1
		}

		// Start target keyspace (sharded, with 1 replica for tablet type routing test).
		tks := cluster.Keyspace{
			Name:      targetKs,
			SchemaSQL: targetSchema,
			VSchema:   targetVSchema,
		}
		err = clusterInstance.StartKeyspace(tks, []string{"-80", "80-"}, 1, false, cell)
		if err != nil {
			return 1
		}

		// Start vtgate.
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		err = clusterInstance.WaitForVTGateAndVTTablets(1 * time.Minute)
		if err != nil {
			fmt.Println(err)
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestViewRoutingRules(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for views to be tracked by the schema tracker.
	viewExists := func(t *testing.T, ksMap map[string]any) bool {
		views, ok := ksMap["views"]
		if !ok {
			return false
		}
		viewsMap := views.(map[string]any)
		_, ok = viewsMap["view1"]
		return ok
	}
	utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, sourceKs, viewExists, "source view1 not found")
	utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, targetKs, viewExists, "target view1 not found")

	// Insert different data in each keyspace so we can distinguish which one is being queried.
	utils.Exec(t, conn, "insert into source_ks.t1(id, val) values(1, 'source_data')")
	utils.Exec(t, conn, "insert into target_ks.t1(id, val) values(1, 'target_data')")

	utils.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("source_data")]]`)
	utils.AssertMatches(t, conn, "select * from target_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	// Unqualified query should fail with ambiguous table error since view1 exists in both keyspaces.
	_, err = utils.ExecAllowError(t, conn, "select * from view1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous")

	// Apply routing rules to route to target
	routingRules := `{"rules": [
		{"from_table": "view1", "to_tables": ["target_ks.view1"]},
		{"from_table": "source_ks.view1", "to_tables": ["target_ks.view1"]},
		{"from_table": "source_ks.view1@replica", "to_tables": ["target_ks.view1"]}
	]}`
	err = clusterInstance.VtctldClientProcess.ApplyRoutingRules(routingRules)
	require.NoError(t, err)
	defer func() {
		err := clusterInstance.VtctldClientProcess.ApplyRoutingRules("{}")
		require.NoError(t, err)
	}()

	// After routing rules, all queries should return target data.

	utils.AssertMatches(t, conn, "select * from view1", `[[INT64(1) VARCHAR("target_data")]]`)
	utils.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	utils.Exec(t, conn, "use @replica")
	utils.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	utils.Exec(t, conn, "use @primary")
	utils.AssertMatches(t, conn, "select * from target_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)
}
