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
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	vtutils "vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	uks             = "main"
	sks             = "user"
	cell            = "plantests"
)

func TestMain(m *testing.M) {
	vschema := readFile("vschemas/schema.json")
	userVs := extractUserKS(vschema)
	mainVs := extractMainKS(vschema)
	sSQL := readFile("schemas/user.sql")
	uSQL := readFile("schemas/main.sql")

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			fmt.Println(err.Error())
			return 1
		}

		// Start unsharded keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      uks,
			SchemaSQL: uSQL,
			VSchema:   mainVs,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false)
		if err != nil {
			fmt.Println(err.Error())
			return 1
		}

		// Start sharded keyspace
		skeyspace := &cluster.Keyspace{
			Name:      sks,
			SchemaSQL: sSQL,
			VSchema:   userVs,
		}
		err = clusterInstance.StartKeyspace(*skeyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			fmt.Println(err.Error())
			return 1
		}

		// TODO: (@GuptaManan100/@systay): Also run the tests with normalizer on.
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			vtutils.GetFlagVariantForTests("--normalize-queries")+"=false",
			vtutils.GetFlagVariantForTests("--schema-change-signal")+"=true",
		)

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			fmt.Println(err.Error())
			return 1
		}

		vtParams = clusterInstance.GetVTParams(sks)

		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, sks, sSQL, uSQL)
		if err != nil {
			fmt.Println(err.Error())
			return 1
		}
		defer closer()
		mysqlParams = conn

		return m.Run()
	}()
	os.Exit(exitCode)
}

func readFile(filename string) string {
	schema, err := os.ReadFile(locateFile(filename))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return string(schema)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
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

func loadSampleData(t *testing.T, mcmp utils.MySQLCompare) {
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
