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

// explainResultForShard builds a single-row EXPLAIN FORMAT=JSON style result
// whose only column carries a JSON document that identifies the shard, so the
// test can assert results are keyed by the shard they came from.
func explainResultForShard(shard string) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "EXPLAIN", Type: sqltypes.VarChar}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar(fmt.Sprintf(`{"shard":%q}`, shard))},
		},
	}
}

// TestVExplainMySQLPlanKeysByShard verifies that a scatter SELECT runs only the
// EXPLAIN FORMAT=JSON form of the query against each shard (never the wrapped
// query), and that the resulting plan attaches the per-shard EXPLAIN output keyed
// by the shard it came from.
func TestVExplainMySQLPlanKeysByShard(t *testing.T) {
	conns := map[string]*sandboxconn.SandboxConn{}
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		if ks == KsTestSharded && tabletType == topodatapb.TabletType_PRIMARY {
			conn.SetResults([]*sqltypes.Result{explainResultForShard(shard)})
			conns[shard] = conn
		}
	})

	session := &vtgatepb.Session{TargetString: "@primary"}
	gotResult, err := executorExec(ctx, executor, session, "vexplain mysqlplan select id from `user`", nil)
	require.NoError(t, err)

	// The wrapped query must never be executed: each shard should only ever have
	// received the EXPLAIN FORMAT=JSON form of the query.
	wantQuery := "explain format = json select id from `user`"
	for shard, conn := range conns {
		require.Len(t, conn.Queries, 1, "shard %s should receive exactly one query", shard)
		assert.Equal(t, wantQuery, conn.Queries[0].Sql, "shard %s", shard)
	}

	// The plan JSON must attach the per-shard EXPLAIN output keyed by shard.
	// PrimitiveDescription inlines the entries of Other at the top level, so
	// mysql_explain_json appears alongside OperatorType.
	var plan struct {
		OperatorType     string `json:"OperatorType"`
		MySQLExplainJSON map[string]struct {
			Shard string `json:"shard"`
		} `json:"mysql_explain_json"`
	}
	require.NoError(t, json.Unmarshal([]byte(gotResult.Rows[0][0].ToString()), &plan))
	require.Equal(t, "Route", plan.OperatorType)

	perShard := plan.MySQLExplainJSON
	require.Len(t, perShard, len(conns))
	for shard := range conns {
		require.Contains(t, perShard, shard)
		assert.Equal(t, shard, perShard[shard].Shard)
	}
}

// TestVExplainMySQLPlanShardQueriesAccounting verifies that the per-shard EXPLAIN
// queries VEXPLAIN MYSQLPLAN issues are counted in ShardQueries. They run through
// ExecuteStandalone, which does not increment the counter on its own, so a scatter
// over eight shards must report eight shard queries rather than zero.
func TestVExplainMySQLPlanShardQueriesAccounting(t *testing.T) {
	const wantShards = 8
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		if ks == KsTestSharded && tabletType == topodatapb.TabletType_PRIMARY {
			conn.SetResults([]*sqltypes.Result{explainResultForShard(shard)})
		}
	})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{TargetString: "@primary"}
	_, err := executorExec(ctx, executor, session, "vexplain mysqlplan select id from `user`", nil)
	require.NoError(t, err)

	logStats := getQueryLog(logChan)
	require.NotNil(t, logStats)
	assert.EqualValues(t, wantShards, logStats.ShardQueries)
}

// TestVExplainMySQLPlanShardQueriesAccountingSkipsEmpty verifies that a shard
// whose EXPLAIN returns no rows (so it contributes no plan and is skipped) is not
// counted in ShardQueries. Of an eight-shard scatter, one shard returns an empty
// result, so only the seven shards that produced a plan are counted.
func TestVExplainMySQLPlanShardQueriesAccountingSkipsEmpty(t *testing.T) {
	const emptyShard = "-20"
	const wantShards = 7
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		if ks == KsTestSharded && tabletType == topodatapb.TabletType_PRIMARY {
			if shard == emptyShard {
				conn.SetResults([]*sqltypes.Result{{Fields: []*querypb.Field{{Name: "EXPLAIN", Type: sqltypes.VarChar}}}})
				return
			}
			conn.SetResults([]*sqltypes.Result{explainResultForShard(shard)})
		}
	})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{TargetString: "@primary"}
	_, err := executorExec(ctx, executor, session, "vexplain mysqlplan select id from `user`", nil)
	require.NoError(t, err)

	logStats := getQueryLog(logChan)
	require.NotNil(t, logStats)
	assert.EqualValues(t, wantShards, logStats.ShardQueries)
}

// TestVExplainMySQLPlanTargetedSend verifies that a SELECT with an explicit shard
// target (which plans as a Send, not a Route) still produces per-shard EXPLAIN
// output, and only the targeted shard is queried.
func TestVExplainMySQLPlanTargetedSend(t *testing.T) {
	const targetShard = "-20"
	conns := map[string]*sandboxconn.SandboxConn{}
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		if ks == KsTestSharded && tabletType == topodatapb.TabletType_PRIMARY {
			conn.SetResults([]*sqltypes.Result{explainResultForShard(shard)})
			conns[shard] = conn
		}
	})

	session := &vtgatepb.Session{TargetString: KsTestSharded + "/" + targetShard + "@primary"}
	gotResult, err := executorExec(ctx, executor, session, "vexplain mysqlplan select id from `user`", nil)
	require.NoError(t, err)

	// Only the targeted shard should have received the EXPLAIN; no other shard is touched.
	for shard, conn := range conns {
		if shard == targetShard {
			require.Len(t, conn.Queries, 1, "target shard %s should receive exactly one query", shard)
			assert.Equal(t, "explain format = json select id from `user`", conn.Queries[0].Sql)
		} else {
			assert.Empty(t, conn.Queries, "non-target shard %s should receive no query", shard)
		}
	}

	// The plan is a Send node carrying the per-shard EXPLAIN output keyed by the
	// targeted shard.
	var plan struct {
		OperatorType     string `json:"OperatorType"`
		MySQLExplainJSON map[string]struct {
			Shard string `json:"shard"`
		} `json:"mysql_explain_json"`
	}
	require.NoError(t, json.Unmarshal([]byte(gotResult.Rows[0][0].ToString()), &plan))
	require.Equal(t, "Send", plan.OperatorType)
	require.Len(t, plan.MySQLExplainJSON, 1)
	require.Contains(t, plan.MySQLExplainJSON, targetShard)
	assert.Equal(t, targetShard, plan.MySQLExplainJSON[targetShard].Shard)
}

// TestVExplainMySQLPlanMultipleRoutes verifies that a plan with more than one
// Route node (here a UNION, which plans as Distinct over a Concatenate of two
// Routes) attaches a distinct per-shard EXPLAIN map to each Route node, keyed by
// shard, without executing the wrapped query.
func TestVExplainMySQLPlanMultipleRoutes(t *testing.T) {
	conns := map[string]*sandboxconn.SandboxConn{}
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		if ks == KsTestSharded && tabletType == topodatapb.TabletType_PRIMARY {
			// Both Routes of the UNION scatter to every shard, so each shard is
			// asked to EXPLAIN twice; queue a shard-identifying result for each.
			conn.SetResults([]*sqltypes.Result{explainResultForShard(shard), explainResultForShard(shard)})
			conns[shard] = conn
		}
	})

	session := &vtgatepb.Session{TargetString: "@primary"}
	gotResult, err := executorExec(ctx, executor, session,
		"vexplain mysqlplan select id from `user` where id = 1 union select id from `user` where id = 2", nil)
	require.NoError(t, err)

	// The wrapped query must never be executed: each shard should only ever have
	// received EXPLAIN FORMAT=JSON queries, never a plain SELECT.
	for shard, conn := range conns {
		for _, q := range conn.Queries {
			assert.Contains(t, q.Sql, "explain format = json", "shard %s received a non-explain query", shard)
		}
	}

	// The plan is Distinct -> Concatenate -> [Route, Route]. Each Route node must
	// carry its own per-shard EXPLAIN map keyed by the shard it resolved to.
	type planNode struct {
		OperatorType     string `json:"OperatorType"`
		MySQLExplainJSON map[string]struct {
			Shard string `json:"shard"`
		} `json:"mysql_explain_json"`
		Inputs []json.RawMessage `json:"Inputs"`
	}
	var top planNode
	require.NoError(t, json.Unmarshal([]byte(gotResult.Rows[0][0].ToString()), &top))
	require.Equal(t, "Distinct", top.OperatorType)
	require.Len(t, top.Inputs, 1)

	var concatenate planNode
	require.NoError(t, json.Unmarshal(top.Inputs[0], &concatenate))
	require.Equal(t, "Concatenate", concatenate.OperatorType)
	require.Len(t, concatenate.Inputs, 2, "UNION should plan as a Concatenate of two Routes")

	for i, raw := range concatenate.Inputs {
		var route planNode
		require.NoError(t, json.Unmarshal(raw, &route))
		require.Equal(t, "Route", route.OperatorType, "input %d", i)
		require.NotEmpty(t, route.MySQLExplainJSON, "Route %d must carry per-shard EXPLAIN output", i)
		// The map must be keyed by the shard the Route resolved to, and each entry
		// must carry that same shard's EXPLAIN document.
		for shard, doc := range route.MySQLExplainJSON {
			assert.Equal(t, shard, doc.Shard, "Route %d entry must be keyed by its own shard", i)
		}
	}
}

// TestVExplainMySQLPlanRejectsSequence verifies that a sequence next-value query
// is rejected at plan time: its Vitess-specific `select next ... values` syntax
// cannot be sent to MySQL as EXPLAIN. The rejection must not suggest VEXPLAIN ALL
// (which would execute the query and consume sequence values), and no tablet query
// must be sent.
func TestVExplainMySQLPlanRejectsSequence(t *testing.T) {
	conns := map[string]*sandboxconn.SandboxConn{}
	executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
		conns[ks+"/"+shard] = conn
	})

	session := &vtgatepb.Session{TargetString: "@primary"}
	_, err := executorExec(ctx, executor, session, "vexplain mysqlplan select next 2 values from user_seq", nil)
	require.ErrorContains(t, err, "does not support sequence next value queries")
	// The sequence case must not point at VEXPLAIN ALL, which would consume values.
	require.NotContains(t, err.Error(), "VEXPLAIN ALL")

	// Rejection happens at plan time, so no tablet query is ever sent.
	for target, conn := range conns {
		assert.Empty(t, conn.Queries, "no query should be sent to %s", target)
	}
}

// TestVExplainMySQLPlanRejectsSubqueries verifies that a query containing a
// subquery, derived table, or common table expression is rejected at plan time,
// before any shard RPC. Such a query can merge into a single Route that the
// primitive allowlist would accept, but EXPLAIN FORMAT=JSON of a derived table can
// execute a stored function during optimization, so MYSQLPLAN must never send it to
// the shards. A non-recursive CTE is inlined as a derived table during planning
// (unlike a recursive CTE, which is rejected by the primitive allowlist), so it
// must be caught at the AST level before that inlining runs.
func TestVExplainMySQLPlanRejectsSubqueries(t *testing.T) {
	testCases := []struct {
		name  string
		query string
	}{
		{"derived table in from", "vexplain mysqlplan select id from `user`, (select 1 as f1) as dt where user.id = dt.f1"},
		{"scalar subquery in select", "vexplain mysqlplan select id, (select 1 from `user` u2 where u2.id = `user`.id) from `user`"},
		{"subquery in where", "vexplain mysqlplan select id from `user` where id in (select id from `user` where id = 5)"},
		{"non-recursive cte", "vexplain mysqlplan with t as (select id, textcol from `user` where id = 5) select id from t"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conns := map[string]*sandboxconn.SandboxConn{}
			executor, ctx := createExecutorEnvCallback(t, createExecutorConfig(), func(shard, ks string, tabletType topodatapb.TabletType, conn *sandboxconn.SandboxConn) {
				conns[ks+"/"+shard] = conn
			})

			session := &vtgatepb.Session{TargetString: "@primary"}
			_, err := executorExec(ctx, executor, session, tc.query, nil)
			require.ErrorContains(t, err, "cannot resolve the target shards without executing the query")
			require.ErrorContains(t, err, "use VEXPLAIN ALL instead")

			// Rejection happens at plan time, so no tablet query is ever sent.
			for target, conn := range conns {
				assert.Empty(t, conn.Queries, "no query should be sent to %s", target)
			}
		})
	}
}

// TestVExplainMySQLPlanRequiresExecution verifies that plans whose target shards
// cannot be resolved without running the query (lookup vindex, cross-shard join,
// recursive CTE) and DML statements are rejected at plan time, each pointing the
// user to VEXPLAIN ALL.
func TestVExplainMySQLPlanRequiresExecution(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := &vtgatepb.Session{TargetString: "@primary"}

	testCases := []struct {
		name    string
		query   string
		wantMsg string
	}{
		{"lookup vindex", "vexplain mysqlplan select * from music where id = 5", "cannot resolve the target shards without executing the query"},
		{"cross-shard join", "vexplain mysqlplan select u.id from `user` u join user_extra ue on u.col = ue.col", "cannot resolve the target shards without executing the query"},
		{"recursive cte", "vexplain mysqlplan with recursive cte(id) as (select id from `user` where id = 1 union select u.id from `user` u join cte on u.id = cte.id) select * from cte", "cannot resolve the target shards without executing the query"},
		{"insert", "vexplain mysqlplan insert into user_extra(user_id) values (5)", "only supports SELECT statements"},
		{"insert select", "vexplain mysqlplan insert into user_extra(user_id) select id from `user`", "only supports SELECT statements"},
		{"update", "vexplain mysqlplan update user_extra set col = 1 where user_id = 5", "only supports SELECT statements"},
		{"delete", "vexplain mysqlplan delete from user_extra where user_id = 5", "only supports SELECT statements"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executorExec(ctx, executor, session, tc.query, nil)
			require.ErrorContains(t, err, tc.wantMsg)
			// All unsupported cases direct the user to VEXPLAIN ALL.
			require.ErrorContains(t, err, "use VEXPLAIN ALL instead")
		})
	}
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
			gotResult, err := executorExecSession(t.Context(), executor, session, "vexplain keys "+tt.Query, nil)
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

		err = os.WriteFile(tempFilePath, updatedTestsData, 0o644)
		require.NoError(t, err)

		fmt.Println("Updated tests written to:", tempFilePath)
	}
}
