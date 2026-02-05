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

package vtgate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestSimpleVexplainTrace(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	u := createSandbox(KsTestUnsharded)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	u.VSchema = unshardedVSchema
	serv := newSandboxForCells(ctx, []string{cell})
	resolver := newTestResolver(ctx, hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)"},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1), sqltypes.NewInt32(int32(i % 4)), sqltypes.NULL},
				{sqltypes.NewInt32(2), sqltypes.NewInt32(int32(i % 4)), sqltypes.NULL},
			},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(ctx, serv, cell, resolver)
	defer executor.Close()

	query := "vexplain trace select count(*), col2 from music group by col2"
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	gotResult, err := executorExec(ctx, executor, session, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select count(*), col2, weight_string(col2) from music group by col2, weight_string(col2) order by col2 asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	expectedRowString := `{
	"OperatorType": "Aggregate",
	"Variant": "Ordered",
	"NoOfCalls": 1,
	"AvgNumberOfRows": 4,
	"MedianNumberOfRows": 4,
	"Aggregates": "sum_count_star(0) AS count(*)",
	"GroupBy": "(1|2)",
	"ResultColumns": 2,
	"Inputs": [
		{
			"OperatorType": "Route",
			"Variant": "Scatter",
			"Keyspace": {
				"Name": "TestExecutor",
				"Sharded": true
			},
			"NoOfCalls": 1,
			"AvgNumberOfRows": 16,
			"MedianNumberOfRows": 16,
			"ShardsQueried": 8,
			"FieldQuery": "select count(*), col2, weight_string(col2) from music where 1 != 1 group by col2, weight_string(col2)",
			"OrderBy": "(1|2) ASC",
			"Query": "select count(*), col2, weight_string(col2) from music group by col2, weight_string(col2) order by col2 asc"
		}
	]
}`

	gotRowString := gotResult.Rows[0][0].ToString()
	require.Equal(t, expectedRowString, gotRowString)
}

func TestVExplainKeys(t *testing.T) {
	type testCase struct {
		Query    string          `json:"query"`
		Expected json.RawMessage `json:"expected"`
	}

	var tests []testCase
	data, err := os.ReadFile("testdata/executor_vexplain.json")
	require.NoError(t, err)

	err = json.Unmarshal(data, &tests)
	require.NoError(t, err)

	var updatedTests []testCase

	for _, tt := range tests {
		t.Run(tt.Query, func(t *testing.T) {
			executor, _, _, _, _ := createExecutorEnv(t)
			session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
			gotResult, err := executorExecSession(context.Background(), executor, session, "vexplain keys "+tt.Query, nil)
			require.NoError(t, err)

			gotRowString := gotResult.Rows[0][0].ToString()
			assert.JSONEq(t, string(tt.Expected), gotRowString)

			updatedTests = append(updatedTests, testCase{
				Query:    tt.Query,
				Expected: json.RawMessage(gotRowString),
			})

			if t.Failed() {
				fmt.Println("Test failed for query:", tt.Query)
				fmt.Println("Got result:", gotRowString)
			}
		})
	}

	// If anything failed, write the updated test cases to a temp file
	if t.Failed() {
		tempFilePath := filepath.Join(os.TempDir(), "updated_vexplain_keys_tests.json")
		fmt.Println("Writing updated tests to:", tempFilePath)

		updatedTestsData, err := json.MarshalIndent(updatedTests, "", "\t")
		require.NoError(t, err)

		err = os.WriteFile(tempFilePath, updatedTestsData, 0644)
		require.NoError(t, err)

		fmt.Println("Updated tests written to:", tempFilePath)
	}
}
