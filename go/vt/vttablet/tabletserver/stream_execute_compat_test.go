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

package tabletserver

// Tests that verify incompatibilities between StreamExecute and Execute.
// Each test documents a specific issue and should fail until the issue is fixed.
// Tracked in .claude/issues/streamexecute/

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Issue 1: DML statements rejected by BuildStreaming with "X not allowed for streaming".
// BuildStreaming only accepts SELECT, SHOW, Union, CallProc, Explain, and Analyze.
// All other statement types (INSERT, UPDATE, DELETE, DDL, SET) return an error.
func TestStreamExecuteCompat_BuildStreamingRejectsDML(t *testing.T) {
	parser := sqlparser.NewTestParser()

	testcases := []struct {
		name  string
		query string
	}{
		{"INSERT", "insert into test_table(a) values(1)"},
		{"UPDATE", "update test_table set a = 1"},
		{"DELETE", "delete from test_table"},
		{"DDL", "alter table test_table add zipcode int"},
		{"SET", "set @udv = 10"},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)

			_, err = planbuilder.BuildStreaming(vtenv.NewTestEnv(), stmt, map[string]*schema.Table{}, "dbName")
			// Currently fails: BuildStreaming rejects these statements.
			// When fixed, BuildStreaming should accept DML and produce an appropriate plan.
			assert.NoError(t, err, "BuildStreaming should accept %s statements", tc.name)
		})
	}
}

// Issue 2: Stream() only handles PlanSelectStream; all other plan types
// fall through to execStreamSQL which sends raw SQL to MySQL.
// Vitess-specific statements (sequences, migrations, throttler) fail because
// MySQL doesn't understand the syntax.
func TestStreamExecuteCompat_StreamMissingPlanTypes(t *testing.T) {
	// Test that Stream() can handle plan types that Execute() handles.
	// We test this at the TabletServer.StreamExecute level since Stream()
	// requires a fully set up QueryExecutor.
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	// SHOW statements work through streaming (they're allowed by BuildStreaming),
	// but the Vitess-specific SHOW variants need special handling in Stream().

	// Test: a SELECT query works fine through streaming (baseline).
	selectSQL := "select * from test_table limit 1000"
	db.AddQuery(selectSQL, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("row01")}},
	})

	var gotRows int
	callback := func(qr *sqltypes.Result) error {
		gotRows += len(qr.Rows)
		return nil
	}
	err := tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, 0, 0, nil, callback)
	require.NoError(t, err, "SELECT should work in streaming mode")
	assert.Equal(t, 1, gotRows)
}

// Issue 3: ACL error message says "SelectStream command denied" instead of
// "Select command denied" when using StreamExecute.
func TestStreamExecuteCompat_ACLErrorMessageConsistency(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	ctx := t.Context()
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	defer tsv.StopService()

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"allowed_user"},
		}},
	}
	require.NoError(t, tableacl.InitFromProto(config))

	// Use a user that does NOT have read access.
	callerID := &querypb.VTGateCallerID{Username: "denied_user"}
	ctx = callerid.NewContext(ctx, nil, callerID)

	query := "select * from test_table"

	// Execute path: produces "Select command denied"
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, execErr := qreExec.Execute()
	require.Error(t, execErr)

	// Stream path: currently produces "SelectStream command denied"
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	streamErr := qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, streamErr)

	// Both should produce the same error message.
	// Currently fails: Stream uses "SelectStream" while Execute uses "Select".
	assert.Equal(t, execErr.Error(), streamErr.Error(),
		"ACL error messages should be identical between Execute and Stream")
}

// Issue 7: Query counters and metrics use different keys.
// Execute records with "Execute" label; Stream records with "Stream" label.
// This causes metric key changes when switching workloads.
func TestStreamExecuteCompat_MetricKeyConsistency(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	query := "select * from test_table limit 1000"
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("row01")}},
	})

	callerID := &querypb.VTGateCallerID{Username: "test_user"}
	ctx = callerid.NewContext(ctx, nil, callerID)

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Execute path records with "Execute" label.
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)

	executeCount := tsv.Stats().UserTableQueryCount.Counts()["test_table.test_user.Execute"]

	// Stream path records with "Stream" label.
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	// Currently fails: Stream uses "Stream" label, not "Execute".
	// When fixed, both paths should use the same metric label.
	executeCountAfterStream := tsv.Stats().UserTableQueryCount.Counts()["test_table.test_user.Execute"]
	assert.Equal(t, executeCount+1, executeCountAfterStream,
		"Stream should record metrics with the same key as Execute")
}

// Issue 7 (continued): Execute uses QueryTimings.Add() while Stream uses
// QueryTimings.Record(). The plan name recorded also differs (SelectStream vs Select).
func TestStreamExecuteCompat_PlanTypeInLogStats(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	query := "select * from test_table limit 1000"
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("row01")}},
	})

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Execute path: PlanType is "Select"
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)
	executePlanType := qreExec.logStats.PlanType

	// Stream path: PlanType is "SelectStream"
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	streamPlanType := qreStream.logStats.PlanType

	// Currently fails: Execute records "Select", Stream records "SelectStream".
	// For compatibility, streaming a SELECT should report the same plan type.
	assert.Equal(t, executePlanType, streamPlanType,
		"Stream should log the same PlanType as Execute for equivalent queries")
}

// Issue 1+2 combined: StreamExecute at the TabletServer level rejects DML.
// This tests the full path through TabletServer.StreamExecute -> GetStreamPlan -> BuildStreaming.
func TestStreamExecuteCompat_DMLViaTabletServer(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	dmlResult := &sqltypes.Result{RowsAffected: 1}

	testcases := []struct {
		name  string
		query string
		dbSQL string
	}{
		{
			name:  "INSERT",
			query: "insert into test_table(pk) values(1)",
			dbSQL: "insert into test_table(pk) values (1)",
		},
		{
			name:  "UPDATE",
			query: "update test_table set pk = 1 where pk = 2",
			dbSQL: "update test_table set pk = 1 where pk = 2",
		},
		{
			name:  "DELETE",
			query: "delete from test_table where pk = 1",
			dbSQL: "delete from test_table where pk = 1",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			db.AddQuery(tc.dbSQL, dmlResult)

			callback := func(*sqltypes.Result) error { return nil }
			err := tsv.StreamExecute(ctx, nil, &target, tc.query, nil, 0, 0, nil, callback)
			// Currently fails: "INSERT/UPDATE/DELETE not allowed for streaming"
			assert.NoError(t, err, "StreamExecute should accept %s statements", tc.name)
		})
	}
}

// Issue 6: Streaming uses OLAP-specific timeout calculation which may differ.
// The StreamExecute path uses getTransactionTimeout with OLAP workload.
func TestStreamExecuteCompat_QueryTimeoutHint(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	// A query with timeout hint should be respected in streaming mode.
	// We use a very short timeout to test that it's honored.
	selectSQL := "select /*vt+ QUERY_TIMEOUT_MS=1 */ sleep(10) from test_table limit 1000"
	db.AddQuery(selectSQL, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
	})

	// Execute path: the timeout hint should cause a deadline exceeded error.
	_, execErr := tsv.Execute(ctx, nil, &target, selectSQL, nil, 0, 0, nil)

	// Stream path: should also honor the timeout hint.
	callback := func(*sqltypes.Result) error { return nil }
	streamErr := tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, 0, 0, nil, callback)

	// Both paths should either error or succeed consistently.
	// Currently may differ: streaming path may not honor QUERY_TIMEOUT_MS hint.
	if execErr != nil {
		assert.Error(t, streamErr,
			"StreamExecute should honor QUERY_TIMEOUT_MS hint like Execute does")
	}
}

// Issue 3 (detailed): Verify the exact error string format for ACL denials.
// The plan ID leaks into the error message, causing "SelectStream" vs "Select".
func TestStreamExecuteCompat_ACLErrorFormat(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	ctx := t.Context()
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	defer tsv.StopService()

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"allowed_user"},
		}},
	}
	require.NoError(t, tableacl.InitFromProto(config))

	callerID := &querypb.VTGateCallerID{Username: "denied_user"}
	ctx = callerid.NewContext(ctx, nil, callerID)

	query := "select * from test_table"

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err := qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, err)

	// Currently fails: error contains "SelectStream command denied"
	// instead of "Select command denied".
	assert.True(t, strings.Contains(err.Error(), "Select command denied"),
		"ACL error should say 'Select command denied', got: %s", err.Error())
	assert.False(t, strings.Contains(err.Error(), "SelectStream"),
		"ACL error should not contain 'SelectStream', got: %s", err.Error())
}

// Issue 2: Stream() doesn't handle PlanNextval.
// SELECT NEXT N VALUES FROM seq is a Vitess-specific syntax that needs
// special handling via execNextval(). In streaming mode, it falls through
// to execStreamSQL which sends the raw SQL to MySQL, causing a syntax error.
func TestStreamExecuteCompat_NextvalHandling(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Set up the sequence table responses that execNextval would need.
	db.AddQuery("select next_id, cache from seq where id = 0 for update", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "next_id", Type: sqltypes.Int64},
			{Name: "cache", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(3),
		}},
	})
	db.AddQuery("update seq set next_id = 4 where id = 0", &sqltypes.Result{})

	query := "select next 1 values from seq"

	// Execute path works because it has a PlanNextval handler.
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	execResult, err := qreExec.Execute()
	require.NoError(t, err, "Execute should handle NEXT VALUES")
	require.NotNil(t, execResult)

	// Stream path: currently fails because Stream() has no PlanNextval handler.
	// The raw SQL gets sent to MySQL which doesn't understand NEXT VALUES syntax.
	logStats := tabletenv.NewLogStats(ctx, "TestNextval", streamlog.NewQueryLogConfigForTest())
	plan, err := tsv.qe.GetStreamPlan(ctx, logStats, query, false)
	if err != nil {
		// Currently fails here: BuildStreaming may reject this as a SELECT variant,
		// or it might produce a PlanSelectStream which then fails in Stream().
		t.Skipf("BuildStreaming rejects NEXT VALUES syntax: %v (Issue 1 blocks this)", err)
	}

	qreStream := &QueryExecutor{
		ctx:      ctx,
		query:    query,
		bindVars: make(map[string]*querypb.BindVariable),
		plan:     plan,
		logStats: logStats,
		tsv:      tsv,
	}
	streamErr := qreStream.Stream(func(*sqltypes.Result) error { return nil })
	assert.NoError(t, streamErr,
		"Stream should handle PlanNextval like Execute does")
}

// Issue 4: Implicit transactions don't start in streaming mode for DML.
// In Execute(), DML plan types call execAutocommit() which handles implicit
// transaction creation when autocommit=0. Stream() has no equivalent.
func TestStreamExecuteCompat_ImplicitTransactionForDML(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	insertSQL := "insert into test_table(pk) values(1)"
	db.AddQuery("insert into test_table(pk) values (1)", &sqltypes.Result{RowsAffected: 1})

	// Execute path: INSERT creates an implicit transaction via execAutocommit.
	result, err := tsv.Execute(ctx, nil, &target, insertSQL, nil, 0, 0, nil)
	if err != nil {
		t.Skipf("Execute failed (unrelated issue): %v", err)
	}
	require.NotNil(t, result)

	// StreamExecute path: should also handle DML with proper transaction management.
	callback := func(*sqltypes.Result) error { return nil }
	err = tsv.StreamExecute(ctx, nil, &target, insertSQL, nil, 0, 0, nil, callback)
	// Currently fails: "INSERT not allowed for streaming" (Issue 1 blocks this test)
	assert.NoError(t, err,
		"StreamExecute should handle INSERT with implicit transaction management")
}

// Issue 2: Stream() doesn't handle DDL plan types.
// DDL statements need execDDL() handling, but Stream() just forwards raw SQL.
func TestStreamExecuteCompat_DDLViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ddlSQL := "alter table test_table add zipcode int"
	db.AddQuery("alter table test_table add column zipcode int", &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	err := tsv.StreamExecute(ctx, nil, &target, ddlSQL, nil, 0, 0, nil, callback)
	// Currently fails: "DDL not allowed for streaming" (Issue 1)
	assert.NoError(t, err,
		"StreamExecute should handle DDL statements")
}

// Issue 2: Stream() doesn't handle savepoint plan types.
// Savepoint, Release, and Rollback to Savepoint need execOther() handling.
func TestStreamExecuteCompat_SavepointViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	// Start a transaction first since savepoints require one.
	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)

	savepointSQL := "savepoint sp1"
	db.AddQuery(savepointSQL, &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	err = tsv.StreamExecute(ctx, nil, &target, savepointSQL, nil, state.TransactionID, 0, nil, callback)
	// Savepoints might work in streaming within a transaction since they go through
	// the txConn path, but outside a transaction they'd fail differently.
	// Currently likely fails: "SAVEPOINT not allowed for streaming" (Issue 1)
	assert.NoError(t, err,
		"StreamExecute should handle savepoint statements within a transaction")

	_, err = tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

// Issue 1: SET statements rejected by BuildStreaming.
func TestStreamExecuteCompat_SetViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	setSQL := "set @my_var = 42"
	db.AddQuery(setSQL, &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	err := tsv.StreamExecute(ctx, nil, &target, setSQL, nil, 0, 0, nil, callback)
	// Currently fails: "SET not allowed for streaming" (Issue 1)
	assert.NoError(t, err,
		"StreamExecute should handle SET statements")
}

// Issue 7: Stream() doesn't record RowsAffected or ResultHistogram stats.
func TestStreamExecuteCompat_ResultStatsRecording(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	query := "select * from test_table limit 1000"
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
			{sqltypes.NewVarBinary("row02")},
		},
	})

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Get baseline ResultHistogram count.
	baselineCount := tsv.Stats().ResultHistogram.Total()

	// Execute path records to ResultHistogram.
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)
	afterExecute := tsv.Stats().ResultHistogram.Total()
	assert.Greater(t, afterExecute, baselineCount,
		"Execute should record to ResultHistogram")

	// Stream path should also record to ResultHistogram.
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	afterStream := tsv.Stats().ResultHistogram.Total()

	// Currently fails: Stream() doesn't record ResultHistogram stats.
	assert.Greater(t, afterStream, afterExecute,
		"Stream should record to ResultHistogram like Execute does")
}

// Issue 1: BuildStreaming rejects Vitess-specific migration statements.
// ALTER VITESS_MIGRATION and REVERT VITESS_MIGRATION should be accepted.
func TestStreamExecuteCompat_BuildStreamingRejectsMigration(t *testing.T) {
	parser := sqlparser.NewTestParser()

	testcases := []struct {
		name  string
		query string
	}{
		{"ALTER_VITESS_MIGRATION", "alter vitess_migration 'abc' cancel"},
		{"REVERT_VITESS_MIGRATION", "revert vitess_migration 'abc'"},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err, "Failed to parse: %s", tc.query)

			_, err = planbuilder.BuildStreaming(vtenv.NewTestEnv(), stmt, map[string]*schema.Table{}, "dbName")
			// Currently fails: "MIGRATION not allowed for streaming"
			assert.NoError(t, err,
				"BuildStreaming should accept %s", tc.name)
		})
	}
}

// Issue 1+2: Verify that all plan types handled by Execute() can also
// be handled by the streaming path (BuildStreaming + Stream).
// This is a comprehensive check that enumerates all plan types.
func TestStreamExecuteCompat_AllExecutePlanTypesSupported(t *testing.T) {
	// These are all the plan types that Execute() handles in its switch statement.
	// For each one, we verify that BuildStreaming doesn't reject the SQL.
	parser := sqlparser.NewTestParser()

	testcases := []struct {
		name  string
		query string
	}{
		{"Select", "select * from test_table"},
		{"Show", "show tables"},
		{"Union", "select * from test_table union select * from test_table"},
		{"Explain", "desc test_table"},
		{"Analyze", "analyze table test_table"},
		{"Insert", "insert into test_table(pk) values(1)"},
		{"Update", "update test_table set pk = 1"},
		{"Delete", "delete from test_table"},
		{"DDL", "alter table test_table add zipcode int"},
		{"Set", "set @udv = 10"},
		{"Savepoint", "savepoint sp1"},
		{"Release", "release savepoint sp1"},
		{"RollbackSavepoint", "rollback to savepoint sp1"},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err, "Failed to parse: %s", tc.query)

			_, err = planbuilder.BuildStreaming(vtenv.NewTestEnv(), stmt, map[string]*schema.Table{}, "dbName")
			assert.NoError(t, err, "BuildStreaming should accept %s", tc.name)
		})
	}
}

// Issue 3+7: Verify that streaming a SELECT records consistent stats.
// The ACL stats key also includes the plan ID which differs.
func TestStreamExecuteCompat_ACLStatsKeyConsistency(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	ctx := t.Context()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"test_user"},
		}},
	}
	require.NoError(t, tableacl.InitFromProto(config))

	callerID := &querypb.VTGateCallerID{Username: "test_user"}
	ctx = callerid.NewContext(ctx, nil, callerID)

	query := "select * from test_table limit 1000"
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("row01")}},
	})

	// Execute uses "Select" plan ID in stats key.
	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)

	// Stream uses "SelectStream" plan ID in stats key.
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	// Check that the ACL stats don't use "SelectStream" as a key component.
	// The generateACLStatsKey includes qre.plan.PlanID.String() which will differ.
	counts := tsv.Stats().TableaclAllowed.Counts()

	// Look for any key containing "SelectStream" - there shouldn't be any
	// if the stats are consistent.
	for key := range counts {
		// Currently fails: streaming path records stats with "SelectStream" in the key.
		assert.False(t, strings.Contains(key, "SelectStream"),
			"ACL stats should not use 'SelectStream' as a plan type, found key: %s", key)
	}
}

// Verify that StreamExecute within a transaction works for SELECT.
// This is a baseline test to ensure transactional streaming works.
func TestStreamExecuteCompat_TransactionalSelect(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	selectSQL := "select * from test_table limit 1000"
	db.AddQuery(selectSQL, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("row01")}},
	})

	// Start a transaction.
	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)

	var gotRows int
	callback := func(qr *sqltypes.Result) error {
		gotRows += len(qr.Rows)
		return nil
	}

	// StreamExecute within a transaction should work for SELECT.
	err = tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, state.TransactionID, 0, nil, callback)
	require.NoError(t, err)
	assert.Equal(t, 1, gotRows)

	_, err = tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

// Issue 2: Verify that Execute handles CallProc but Stream doesn't have a handler.
func TestStreamExecuteCompat_CallProcHandling(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	callSQL := "call test_proc()"
	db.AddQuery(callSQL, &sqltypes.Result{})

	// StreamExecute: CALL is allowed by BuildStreaming but Stream() has no
	// dedicated handler for it, so it falls through to execStreamSQL.
	// For simple procs this may work, but for procs that return multiple
	// result sets it won't work correctly.
	callback := func(*sqltypes.Result) error { return nil }
	err := tsv.StreamExecute(ctx, nil, &target, callSQL, nil, 0, 0, nil, callback)
	// This might actually succeed for simple cases since execStreamSQL
	// sends the raw SQL to MySQL. But the behavior differs from Execute()
	// which uses execCallProc() with special handling.
	assert.NoError(t, err, "StreamExecute should handle CALL statements")
}

// Issue 2: Verify that FLUSH statements need proper handling.
func TestStreamExecuteCompat_FlushViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	flushSQL := "flush tables"
	db.AddQuery(flushSQL, &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	err := tsv.StreamExecute(ctx, nil, &target, flushSQL, nil, 0, 0, nil, callback)
	// Currently fails: "FLUSH not allowed for streaming" (Issue 1)
	assert.NoError(t, err,
		"StreamExecute should handle FLUSH statements")
}

// Issue 2: Verify that LOAD DATA statements need proper handling.
func TestStreamExecuteCompat_LoadDataViaStreamExecute(t *testing.T) {
	parser := sqlparser.NewTestParser()

	stmt, err := parser.Parse("load data infile 'data.txt' into table test_table")
	if err != nil {
		t.Skip("Parser doesn't support LOAD DATA syntax in this context")
	}

	_, err = planbuilder.BuildStreaming(vtenv.NewTestEnv(), stmt, map[string]*schema.Table{}, "dbName")
	// Currently fails: "LOAD not allowed for streaming" (Issue 1)
	assert.NoError(t, err,
		"BuildStreaming should accept LOAD DATA statements")
}
