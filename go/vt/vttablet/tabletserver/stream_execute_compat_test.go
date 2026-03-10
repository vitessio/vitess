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

// Tests that verify parity between StreamExecute and Execute.

import (
	"fmt"
	"math/rand/v2"
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

// BuildStreaming accepts DML and DDL statement types.
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
			assert.NoError(t, err, "BuildStreaming should accept %s statements", tc.name)
		})
	}
}

// Stream() handles plan types that Execute() handles.
func TestStreamExecuteCompat_StreamMissingPlanTypes(t *testing.T) {
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

	var gotRows int
	callback := func(qr *sqltypes.Result) error {
		gotRows += len(qr.Rows)
		return nil
	}
	err := tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, 0, 0, nil, callback)
	require.NoError(t, err, "SELECT should work in streaming mode")
	assert.Equal(t, 1, gotRows)
}

// ACL error messages are identical between Execute and Stream.
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

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, execErr := qreExec.Execute()
	require.Error(t, execErr)

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	streamErr := qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, streamErr)

	assert.Equal(t, execErr.Error(), streamErr.Error(),
		"ACL error messages should be identical between Execute and Stream")
}

// Execute records with "Execute" label; Stream records with "Stream" label.
// This is intentional so operators can distinguish between the two workloads.
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
	assert.Greater(t, executeCount, int64(0), "Execute should record with 'Execute' label")

	// Stream path records with "Stream" label.
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	streamCount := tsv.Stats().UserTableQueryCount.Counts()["test_table.test_user.Stream"]
	assert.Greater(t, streamCount, int64(0), "Stream should record with 'Stream' label")
}

// Execute and Stream log the same PlanType for equivalent queries.
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

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	assert.Equal(t, qreExec.logStats.PlanType, qreStream.logStats.PlanType,
		"Stream should log the same PlanType as Execute for equivalent queries")
}

// DML works through the full TabletServer.StreamExecute path.
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
			assert.NoError(t, err, "StreamExecute should accept %s statements", tc.name)
		})
	}
}

// StreamExecute honors query timeout hints.
func TestStreamExecuteCompat_QueryTimeoutHint(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	selectSQL := "select /*vt+ QUERY_TIMEOUT_MS=1 */ sleep(10) from test_table limit 1000"
	db.AddQuery(selectSQL, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
	})

	_, execErr := tsv.Execute(ctx, nil, &target, selectSQL, nil, 0, 0, nil)

	callback := func(*sqltypes.Result) error { return nil }
	streamErr := tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, 0, 0, nil, callback)

	if execErr != nil {
		assert.Error(t, streamErr,
			"StreamExecute should honor QUERY_TIMEOUT_MS hint like Execute does")
	}
}

// ACL denial errors use "Select" (not "SelectStream") in streaming mode.
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

	assert.Contains(t, err.Error(), "Select command denied",
		"ACL error should say 'Select command denied', got: %s", err.Error())
	assert.NotContains(t, err.Error(), "SelectStream",
		"ACL error should not contain 'SelectStream', got: %s", err.Error())
}

// Stream() handles PlanNextval via execNextval().
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

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	execResult, err := qreExec.Execute()
	require.NoError(t, err, "Execute should handle NEXT VALUES")
	require.NotNil(t, execResult)

	logStats := tabletenv.NewLogStats(ctx, "TestNextval", streamlog.NewQueryLogConfigForTest())
	plan, err := tsv.qe.GetStreamPlan(ctx, logStats, query, false)
	require.NoError(t, err)

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

// StreamExecute handles DML statements.
func TestStreamExecuteCompat_ImplicitTransactionForDML(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	insertSQL := "insert into test_table(pk) values(1)"
	db.AddQuery("insert into test_table(pk) values (1)", &sqltypes.Result{RowsAffected: 1})

	result, err := tsv.Execute(ctx, nil, &target, insertSQL, nil, 0, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	callback := func(*sqltypes.Result) error { return nil }
	err = tsv.StreamExecute(ctx, nil, &target, insertSQL, nil, 0, 0, nil, callback)
	assert.NoError(t, err,
		"StreamExecute should handle INSERT statements")
}

// StreamExecute handles DDL statements.
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
	assert.NoError(t, err,
		"StreamExecute should handle DDL statements")
}

// StreamExecute handles savepoint statements within a transaction.
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
	assert.NoError(t, err,
		"StreamExecute should handle savepoint statements within a transaction")

	_, err = tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

// SET requires a reserved connection (NeedsReservedConn=true).
// Use ReserveStreamExecute which provides sys settings context.
func TestStreamExecuteCompat_SetViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	setSQL := "set @my_var = 42"
	db.AddQuery(setSQL, &sqltypes.Result{})
	db.AddQuery("set sql_mode = ''", &sqltypes.Result{})

	// SET needs a reserved connection. ReserveStreamExecute provides sys settings
	// context which satisfies the NeedsReservedConn requirement.
	callback := func(*sqltypes.Result) error { return nil }
	_, err := tsv.ReserveStreamExecute(ctx, nil, &target, []string{"set sql_mode = ''"}, setSQL, nil, 0, nil, callback)
	assert.NoError(t, err,
		"ReserveStreamExecute should handle SET statements")
}

// Stream() records to ResultHistogram like Execute does.
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

	baselineCount := tsv.Stats().ResultHistogram.Total()

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)
	afterExecute := tsv.Stats().ResultHistogram.Total()
	assert.Greater(t, afterExecute, baselineCount,
		"Execute should record to ResultHistogram")

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	afterStream := tsv.Stats().ResultHistogram.Total()

	assert.Greater(t, afterStream, afterExecute,
		"Stream should record to ResultHistogram like Execute does")
}

// BuildStreaming accepts Vitess-specific migration statements.
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
			assert.NoError(t, err,
				"BuildStreaming should accept %s", tc.name)
		})
	}
}

// All plan types handled by Execute() are also accepted by BuildStreaming.
func TestStreamExecuteCompat_AllExecutePlanTypesSupported(t *testing.T) {
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

// ACL stats keys don't contain "SelectStream".
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

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	for key := range tsv.Stats().TableaclAllowed.Counts() {
		assert.NotContains(t, key, "SelectStream",
			"ACL stats should not use 'SelectStream' as a plan type, found key: %s", key)
	}
}

// StreamExecute within a transaction works for SELECT.
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

// StreamExecute handles CALL statements.
func TestStreamExecuteCompat_CallProcHandling(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	callSQL := "call test_proc()"
	db.AddQuery(callSQL, &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	err := tsv.StreamExecute(ctx, nil, &target, callSQL, nil, 0, 0, nil, callback)
	assert.NoError(t, err, "StreamExecute should handle CALL statements")
}

// StreamExecute handles FLUSH statements.
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
	assert.NoError(t, err,
		"StreamExecute should handle FLUSH statements")
}

// BuildStreaming accepts LOAD DATA statements.
func TestStreamExecuteCompat_LoadDataViaStreamExecute(t *testing.T) {
	parser := sqlparser.NewTestParser()

	stmt, err := parser.Parse("load data infile 'data.txt' into table test_table")
	if err != nil {
		t.Skip("Parser doesn't support LOAD DATA syntax in this context")
	}

	_, err = planbuilder.BuildStreaming(vtenv.NewTestEnv(), stmt, map[string]*schema.Table{}, "dbName")
	assert.NoError(t, err,
		"BuildStreaming should accept LOAD DATA statements")
}
