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

package plan_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

var (
	uks = "main"
	sks = "user"
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()
	vschema := readFile("vschemas/schema.json")
	userVs := extractUserKS(vschema)
	mainVs := extractMainKS(vschema)
	sSQL := readFile("schemas/user.sql")
	uSQL := readFile("schemas/main.sql")

	// TODO: (@GuptaManan100/@systay): Also run the tests with normalizer on.
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(uks).
			WithSchema(uSQL).
			WithVSchema(mainVs),
		vitesst.WithKeyspace(sks).
			WithShardNames("-80", "80-").
			WithSchema(sSQL).
			WithVSchema(userVs),
		vitesst.WithVTGateArgs(
			"--normalize-queries=false",
			"--schema-change-signal=true",
		),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	mysqlParams, mysqlCleanup, err := vitesst.NewMySQL(t, ctx, cluster, sks, sSQL, uSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := mysqlCleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("mysql teardown: %v", cleanupErr)
		}
	})

	return cluster, cluster.VTParams(ctx, ""), mysqlParams
}

func readFile(filename string) string {
	schema, err := os.ReadFile(locateFile(filename))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return string(schema)
}

func start(t *testing.T, vtParams, mysqlParams mysql.ConnParams) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)
	return mcmp, func() {
		mcmp.Close()
	}
}

// splitSQL statements - querySQL may be a multi-line sql blob
func splitSQL(querySQL ...string) ([]string, error) {
	parser := sqlparser.NewTestParser()
	var sqls []string
	for _, sql := range querySQL {
		split, err := parser.SplitStatementToPieces(sql)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, split...)
	}
	return sqls, nil
}

func loadSampleData(t *testing.T, mcmp vitesst.MySQLCompare) {
	sampleDataSQL := readFile("sampledata/user.sql")
	insertSQL, err := splitSQL(sampleDataSQL)
	if err != nil {
		require.NoError(t, err)
	}
	for _, sql := range insertSQL {
		mcmp.ExecNoCompare(sql)
	}
}

func readJSONTests(filename string) []planbuilder.PlanTest {
	var output []planbuilder.PlanTest
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func locateFile(name string) string {
	return "../../../../vt/vtgate/planbuilder/testdata/" + name
}

// verifyTestExpectations verifies the expectations of the test.
func verifyTestExpectations(t *testing.T, pd engine.PrimitiveDescription, test planbuilder.PlanTest) {
	// 1. Verify that the Join primitive sees atleast 1 row on the left side.
	engine.WalkPrimitiveDescription(pd, func(description engine.PrimitiveDescription) {
		if description.OperatorType == "Join" {
			assert.NotZero(t, description.Inputs[0].RowsReceived[0])
		}
	})

	// 2. Verify that the plan description matches the expected plan description.
	planBytes, err := test.Plan.MarshalJSON()
	require.NoError(t, err)
	mp := make(map[string]any)
	err = json.Unmarshal(planBytes, &mp)
	require.NoError(t, err)
	pdExpected, err := engine.PrimitiveDescriptionFromMap(mp["Instructions"].(map[string]any))
	require.NoError(t, err)
	require.Empty(t, pdExpected.Equals(pd), "Expected: %v\nGot: %v", string(planBytes), pd)
}

func extractUserKS(jsonString string) string {
	var result map[string]any
	if err := json.Unmarshal([]byte(jsonString), &result); err != nil {
		panic(err.Error())
	}

	keyspaces, ok := result["keyspaces"].(map[string]any)
	if !ok {
		panic("Keyspaces not found")
	}

	user, ok := keyspaces["user"].(map[string]any)
	if !ok {
		panic("User keyspace not found")
	}

	tables, ok := user["tables"].(map[string]any)
	if !ok {
		panic("Tables not found")
	}

	userTbl, ok := tables["user"].(map[string]any)
	if !ok {
		panic("User table not found")
	}

	delete(userTbl, "auto_increment") // TODO: we should have an unsharded keyspace where this could live

	// Marshal the inner part back to JSON string
	userJson, err := json.Marshal(user)
	if err != nil {
		panic(err.Error())
	}

	return string(userJson)
}

func extractMainKS(jsonString string) string {
	var result map[string]any
	if err := json.Unmarshal([]byte(jsonString), &result); err != nil {
		panic(err.Error())
	}

	keyspaces, ok := result["keyspaces"].(map[string]any)
	if !ok {
		panic("Keyspaces not found")
	}

	main, ok := keyspaces["main"].(map[string]any)
	if !ok {
		panic("main keyspace not found")
	}

	// Marshal the inner part back to JSON string
	mainJson, err := json.Marshal(main)
	if err != nil {
		panic(err.Error())
	}

	return string(mainJson)
}
