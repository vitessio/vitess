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
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// BuildStreaming accepts DML and DDL statement types.
func TestStreamExecuteCompat_BuildStreamingAcceptsDML(t *testing.T) {
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

// SELECT streams rows through the full TabletServer StreamExecute path.
func TestStreamExecuteCompat_SelectViaTabletServer(t *testing.T) {
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
	assert.Positive(t, executeCount, "Execute should record with 'Execute' label")

	// Stream path records with "Stream" label.
	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	streamCount := tsv.Stats().UserTableQueryCount.Counts()["test_table.test_user.Stream"]
	assert.Positive(t, streamCount, "Stream should record with 'Stream' label")
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
			dbSQL: "update test_table set pk = 1 where pk = 2 limit 10001",
		},
		{
			name:  "DELETE",
			query: "delete from test_table where pk = 1",
			dbSQL: "delete from test_table where pk = 1 limit 10001",
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

// The vtgate-side QUERY_TIMEOUT_MS hint reaches the tablet as
// ExecuteOptions.Timeout, which Execute enforces. The streaming path does not
// apply the per-query timeout — StreamExecute serves OLAP queries, which are
// expected to outlive OLTP limits — and is bounded by the caller's context
// deadline instead, which vtgate derives from the hint for streams.
func TestStreamExecuteCompat_QueryTimeout(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	selectSQL := "select * from test_table limit 1000"
	db.AddQuery(selectSQL, &sqltypes.Result{
		Fields: []*querypb.Field{{Type: sqltypes.VarBinary}},
	})
	// Make the fake query stall so the timeouts below can fire.
	db.SetBeforeFunc(selectSQL, func() { time.Sleep(300 * time.Millisecond) })

	shortTimeout := &querypb.ExecuteOptions{
		Timeout: &querypb.ExecuteOptions_AuthoritativeTimeout{AuthoritativeTimeout: 1},
	}
	_, execErr := tsv.Execute(ctx, nil, &target, selectSQL, nil, 0, 0, shortTimeout)
	require.Error(t, execErr, "Execute should enforce the per-query timeout option")

	callback := func(*sqltypes.Result) error { return nil }
	streamErr := tsv.StreamExecute(ctx, nil, &target, selectSQL, nil, 0, 0, shortTimeout, callback)
	require.NoError(t, streamErr,
		"the streaming path does not apply the per-query timeout")

	deadlineCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	streamErr = tsv.StreamExecute(deadlineCtx, nil, &target, selectSQL, nil, 0, 0, nil, callback)
	require.Error(t, streamErr,
		"streaming queries are bounded by the caller's context deadline")
}

// A streamed query denied by table ACLs reports the same error message as
// buffered execution.
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

	assert.EqualError(t, err, "Select command denied to user 'denied_user' for table 'test_table' (ACL check error)")
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

// setUpSeqStreamPlan wires up the sequence table responses execNextval needs
// and returns a streaming QueryExecutor for NEXT VALUE, which is a dedicated
// executor that returns both fields and rows.
func setUpSeqStreamPlan(t *testing.T, ctx context.Context, tsv *TabletServer, options *querypb.ExecuteOptions) *QueryExecutor {
	t.Helper()
	logStats := tabletenv.NewLogStats(ctx, "TestSeqStream", streamlog.NewQueryLogConfigForTest())
	query := "select next 1 values from seq"
	plan, err := tsv.qe.GetStreamPlan(ctx, logStats, query, false)
	require.NoError(t, err)
	return &QueryExecutor{
		ctx:      ctx,
		query:    query,
		bindVars: make(map[string]*querypb.BindVariable),
		options:  options,
		plan:     plan,
		logStats: logStats,
		tsv:      tsv,
	}
}

func addSeqQueries(db *fakesqldb.DB) {
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
}

// A dedicated-executor result that carries rows is delivered as a fields-only
// packet followed by a rows packet, matching the generic streaming path. vtgate
// forwards each StreamExecute packet to gRPC clients verbatim, so a combined
// fields+rows packet would repeat the fields mid-stream on a scatter.
func TestStreamExecuteCompat_DedicatedResultStreamsFieldsThenRows(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	addSeqQueries(db)

	qre := setUpSeqStreamPlan(t, ctx, tsv, nil)

	var packets []*sqltypes.Result
	require.NoError(t, qre.Stream(func(qr *sqltypes.Result) error {
		packets = append(packets, qr)
		return nil
	}))

	require.Len(t, packets, 2, "expected a fields packet followed by a rows packet")
	assert.NotEmpty(t, packets[0].Fields, "first packet carries the fields")
	assert.Empty(t, packets[0].Rows, "first packet carries no rows")
	assert.Empty(t, packets[1].Fields, "row packet carries no fields")
	assert.NotEmpty(t, packets[1].Rows, "row packet carries the rows")
}

// A dedicated-executor result honors the caller's field-metadata setting the
// way Execute does: requesting only TYPE strips the field names.
func TestStreamExecuteCompat_DedicatedResultHonorsIncludeFields(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	addSeqQueries(db)

	qre := setUpSeqStreamPlan(t, ctx, tsv, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_TYPE_ONLY})

	var fields []*querypb.Field
	require.NoError(t, qre.Stream(func(qr *sqltypes.Result) error {
		if len(qr.Fields) > 0 {
			fields = qr.Fields
		}
		return nil
	}))

	require.NotEmpty(t, fields, "the fields packet must still be sent")
	for _, f := range fields {
		assert.Empty(t, f.Name, "TYPE_ONLY must strip the field name")
		assert.Equal(t, sqltypes.Int64, f.Type, "the field type must be retained")
	}
}

// Stream() dispatches the OnlineDDL and throttler admin plans to their
// executors, matching Execute(). These plans carry a nil FullQuery, so the
// generic SQL path would send the Vitess-internal statement (e.g. "show
// vitess_throttled_apps") to MySQL and fail with a syntax error.
func TestStreamExecuteCompat_AdminPlanDispatch(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	queries := []string{
		"show vitess_throttled_apps",
		"show vitess_throttler status",
		"show vitess_migration '9d80e3da_5e7e_11ed_b526_0a43f95f28a3' logs",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
			execResult, execErr := qreExec.Execute()

			// The streaming path delivers a result-set plan as a fields-only
			// packet followed by row packets, so reassemble the packets to
			// compare against Execute's single result.
			qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
			streamResult := &sqltypes.Result{}
			streamErr := qreStream.Stream(func(qr *sqltypes.Result) error {
				if len(qr.Fields) > 0 {
					streamResult.Fields = qr.Fields
				}
				streamResult.Rows = append(streamResult.Rows, qr.Rows...)
				return nil
			})

			if execErr != nil {
				require.EqualError(t, streamErr, execErr.Error(),
					"Stream should route %q to the same executor as Execute", query)
				return
			}

			require.NoError(t, streamErr, "Stream should handle %q like Execute", query)
			// Both paths strip field metadata to the caller's setting: Execute in
			// tabletserver.execute, Stream before invoking the callback. These
			// qre-level calls bypass Execute's wrapper, so strip its result the
			// same way to compare like with like.
			execResult = execResult.StripMetadata(sqltypes.IncludeFieldsOrDefault(nil))
			assert.Equal(t, execResult.Fields, streamResult.Fields)
			assert.Equal(t, execResult.Rows, streamResult.Rows)
		})
	}
}

// Inside a transaction, Stream dispatches the admin plans like Execute:
// SHOW VITESS_MIGRATIONS is served through txConnExec, the rest are rejected
// as unexpected plan types.
func TestStreamExecuteCompat_AdminPlanInTransaction(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)
	defer func() { _, _ = tsv.Rollback(ctx, &target, state.TransactionID) }()

	callback := func(*sqltypes.Result) error { return nil }
	for _, query := range []string{
		"show vitess_throttled_apps",
		"show vitess_throttler status",
		"show vitess_migration '9d80e3da_5e7e_11ed_b526_0a43f95f28a3' logs",
		"alter vitess_migration '9d80e3da_5e7e_11ed_b526_0a43f95f28a3' cancel",
		"revert vitess_migration '9d80e3da_5e7e_11ed_b526_0a43f95f28a3'",
	} {
		streamErr := tsv.StreamExecute(ctx, nil, &target, query, nil, state.TransactionID, 0, nil, callback)
		require.ErrorContains(t, streamErr, "unexpected plan type",
			"%q inside a transaction must be rejected like Execute", query)
	}

	showSQL := "show vitess_migrations"
	_, execErr := tsv.Execute(ctx, nil, &target, showSQL, nil, state.TransactionID, 0, nil)
	streamErr := tsv.StreamExecute(ctx, nil, &target, showSQL, nil, state.TransactionID, 0, nil, callback)
	if execErr != nil {
		require.EqualError(t, streamErr, execErr.Error(),
			"SHOW VITESS_MIGRATIONS inside a transaction must behave like Execute")
	} else {
		require.NoError(t, streamErr,
			"SHOW VITESS_MIGRATIONS inside a transaction must be served like Execute")
	}
}

// Stream() dispatches PlanLoad through streamDML, matching Execute(). The
// generic streaming path never records RowsAffected in the plan stats, so a
// LOAD that returns an OK packet would lose that count there.
func TestStreamExecuteCompat_LoadDataDispatch(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	query := "load data infile 'data.txt' into table test_table"
	db.AddQuery(query, &sqltypes.Result{RowsAffected: 3})

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err, "Execute should handle LOAD DATA")
	_, _, _, execRowsAffected, _, _ := qreExec.plan.Stats()
	require.EqualValues(t, 3, execRowsAffected)

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	var streamRowsAffected uint64
	err = qreStream.Stream(func(qr *sqltypes.Result) error {
		streamRowsAffected += qr.RowsAffected
		return nil
	})
	require.NoError(t, err, "Stream should handle LOAD DATA like Execute")
	assert.EqualValues(t, 3, streamRowsAffected, "client must see the affected-row count")

	_, _, _, planRowsAffected, _, _ := qreStream.plan.Stats()
	assert.EqualValues(t, 3, planRowsAffected,
		"Stream must record RowsAffected in the plan stats like Execute")
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

// StreamExecute dispatches DDL through execDDL, matching Execute: the DDL
// runs with a schema reload afterward, and is rejected inside a transaction,
// where MySQL would implicitly commit behind the tablet's back.
func TestStreamExecuteCompat_DDLViaStreamExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ddlSQL := "alter table test_table add zipcode int"
	rewrittenDDL := "alter table test_table add column zipcode int"
	db.AddQuery(rewrittenDDL, &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }

	reloadsBefore := db.GetQueryCalledNum(mysql.BaseShowTables)
	err := tsv.StreamExecute(ctx, nil, &target, ddlSQL, nil, 0, 0, nil, callback)
	require.NoError(t, err,
		"StreamExecute should handle DDL statements")
	require.Equal(t, 1, db.GetQueryCalledNum(rewrittenDDL))
	require.Greater(t, db.GetQueryCalledNum(mysql.BaseShowTables), reloadsBefore,
		"streamed DDL must reload the schema engine like Execute")

	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)
	err = tsv.StreamExecute(ctx, nil, &target, ddlSQL, nil, state.TransactionID, 0, nil, callback)
	require.ErrorContains(t, err, "DDL statement executed inside a transaction",
		"streamed DDL inside a transaction must be rejected like Execute")
	require.Equal(t, 1, db.GetQueryCalledNum(rewrittenDDL),
		"in-transaction streamed DDL must not reach MySQL")
	_, err = tsv.Rollback(ctx, &target, state.TransactionID)
	require.NoError(t, err)
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

	insertSQL := "insert into test_table(pk) values (2)"
	db.AddQuery("savepoint sp1", &sqltypes.Result{})
	db.AddQuery(insertSQL, &sqltypes.Result{})
	db.AddQuery("rollback to sp1", &sqltypes.Result{})

	callback := func(*sqltypes.Result) error { return nil }
	for _, sql := range []string{"savepoint sp1", insertSQL, "rollback to sp1"} {
		err = tsv.StreamExecute(ctx, nil, &target, sql, nil, state.TransactionID, 0, nil, callback)
		require.NoError(t, err,
			"StreamExecute should handle %q within a transaction", sql)
	}

	// Rolling back to the savepoint must prune the transaction's recorded
	// queries like Execute does, so 2PC recovery replay doesn't re-apply the
	// rolled-back DML.
	conn, err := tsv.te.txPool.GetAndLock(state.TransactionID, "for test inspection")
	require.NoError(t, err)
	recorded := slices.Clone(conn.TxProperties().Queries)
	conn.Unlock()
	assert.Empty(t, recorded,
		"DML rolled back to a streamed savepoint must not stay recorded for 2PC recovery replay")

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
	require.NoError(t, err,
		"ReserveStreamExecute should handle SET statements")

	// Like Execute, the SET itself must not run: the settings list carries the
	// session state, and executing the raw SET on a pooled connection would
	// leak its effect to other sessions sharing the settings pool.
	assert.Zero(t, db.GetQueryCalledNum(setSQL),
		"a streamed SET with a settings list must not reach MySQL")
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

	// A failed streamed query must not contribute a result-size sample, like
	// Execute, which would otherwise skew the histogram toward empty results.
	qreFailed := newTestQueryExecutorStreaming(ctx, tsv, "select * from unregistered_table limit 1000", 0)
	err = qreFailed.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, err)
	assert.Equal(t, afterStream, tsv.Stats().ResultHistogram.Total(),
		"a failed streamed query must not record to ResultHistogram")
}

// Dedicated-executor plans on the streaming path record the rows-affected
// stats and query-log fields that Execute records: their single result is
// fully in hand, so the streaming path's usual limitation — rows-affected
// only arrives in the final OK packet, which a stream doesn't surface —
// doesn't apply to them.
func TestStreamExecuteCompat_RowsAffectedRecording(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	query := "alter table test_table add zipcode int"
	rewritten := "alter table test_table add column zipcode int"
	db.AddQuery(rewritten, &sqltypes.Result{RowsAffected: 5})

	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	qreExec := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qreExec.Execute()
	require.NoError(t, err)
	require.Equal(t, 5, qreExec.logStats.RowsAffected,
		"Execute records the DDL result's rows-affected in the query log")

	qreStream := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	err = qreStream.Stream(func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, 5, qreStream.logStats.RowsAffected,
		"streamed DDL must record rows-affected in the query log like Execute")
	assert.Equal(t, uint64(5), atomic.LoadUint64(&qreStream.plan.RowsAffected),
		"streamed DDL must record rows-affected in the per-plan stats like Execute")
}

// BuildStreaming accepts Vitess-specific migration statements.
func TestStreamExecuteCompat_BuildStreamingAcceptsMigration(t *testing.T) {
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

// Stream() dispatches every plan type deliberately: DML through streamDML,
// dedicated-executor plans through their Execute() counterparts, and the
// remaining plain-SQL statements through the generic streaming path. Plan
// types outside those dispatch tables — unreachable through GetStreamPlan,
// like PlanMessageStream — are rejected as a bug, like Execute rejects them,
// instead of silently executing raw SQL with none of Execute's semantics.
func TestStreamExecuteCompat_StreamDispatchIsTotal(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	// The dispatch tables in Stream(), by path.
	dispatched := map[planbuilder.PlanType]string{
		planbuilder.PlanInsert:      "dml",
		planbuilder.PlanUpdate:      "dml",
		planbuilder.PlanUpdateLimit: "dml",
		planbuilder.PlanDelete:      "dml",
		planbuilder.PlanDeleteLimit: "dml",
		planbuilder.PlanLoad:        "dml",

		planbuilder.PlanNextval:             "dedicated",
		planbuilder.PlanDDL:                 "dedicated",
		planbuilder.PlanSavepoint:           "dedicated",
		planbuilder.PlanSRollback:           "dedicated",
		planbuilder.PlanRelease:             "dedicated",
		planbuilder.PlanSet:                 "dedicated",
		planbuilder.PlanUnlockTables:        "dedicated",
		planbuilder.PlanShowMigrations:      "dedicated",
		planbuilder.PlanShowMigrationLogs:   "dedicated",
		planbuilder.PlanShowThrottledApps:   "dedicated",
		planbuilder.PlanShowThrottlerStatus: "dedicated",
		planbuilder.PlanAlterMigration:      "dedicated",
		planbuilder.PlanRevertMigration:     "dedicated",

		planbuilder.PlanSelect:           "generic",
		planbuilder.PlanSelectImpossible: "generic",
		planbuilder.PlanSelectLockFunc:   "generic",
		planbuilder.PlanShow:             "generic",
		planbuilder.PlanOtherRead:        "generic",
		planbuilder.PlanOtherAdmin:       "generic",
		planbuilder.PlanFlush:            "generic",
		planbuilder.PlanCallProc:         "generic",
	}

	callback := func(*sqltypes.Result) error { return nil }
	for id := range planbuilder.NumPlans {
		if _, ok := dispatched[id]; ok {
			continue
		}
		logStats := tabletenv.NewLogStats(ctx, "TestStreamDispatchIsTotal", streamlog.NewQueryLogConfigForTest())
		plan := &TabletPlan{Plan: &planbuilder.Plan{PlanID: id}, Rules: rules.New()}
		qre := newQueryExec(ctx, tsv, "select 1 from test_table", 0, plan, logStats)
		err := qre.Stream(callback)
		require.ErrorContains(t, err, "unexpected plan type",
			"%s is not in Stream()'s dispatch tables and must not silently take the generic streaming path", id.String())
	}
}

// Stream() must dispatch every plan type Execute() dispatches. Both
// dispatchers are probed behaviorally with a synthetic plan per plan type, so
// a plan type newly wired into Execute cannot pass this test without a Stream
// dispatch decision: either Stream serves it, or the plan is recorded in
// streamUnsupported below as a conscious gap. Only the connID==0 dispatch
// tables are probed: inside a transaction Stream delegates dedicated plans to
// txConnExec — Execute's own dispatcher — so those tables cannot drift apart.
func TestStreamExecuteCompat_StreamDispatchCoversExecute(t *testing.T) {
	ctx := t.Context()
	db, tsv := setupTabletServerTest(t, ctx, "")
	defer tsv.StopService()
	defer db.Close()

	// Plan types Execute dispatches but Stream rejects.
	streamUnsupported := map[planbuilder.PlanType]string{
		// The planner never produces PlanInsertMessage (analyzeInsert always
		// returns PlanInsert); Execute's dispatch entry for it is vestigial,
		// and Stream rejects it like other planner-unreachable plan types.
		planbuilder.PlanInsertMessage: "planner-unreachable, vestigial in Execute",
	}

	// probe reports whether the executor's dispatch tables rejected the plan
	// type. Any other outcome — success, an execution error from the fake DB,
	// or a panic from running a synthetic plan with no FullQuery/FullStmt —
	// means the plan type was dispatched.
	probe := func(id planbuilder.PlanType, run func(*QueryExecutor) error) (rejected bool) {
		logStats := tabletenv.NewLogStats(ctx, "TestStreamDispatchCoversExecute", streamlog.NewQueryLogConfigForTest())
		plan := &TabletPlan{Plan: &planbuilder.Plan{PlanID: id}, Rules: rules.New()}
		qre := newQueryExec(ctx, tsv, "select 1 from test_table", 0, plan, logStats)
		defer func() {
			_ = recover()
		}()
		err := run(qre)
		return err != nil && strings.Contains(err.Error(), "unexpected plan type")
	}

	for id := range planbuilder.NumPlans {
		execRejected := probe(id, func(qre *QueryExecutor) error {
			_, err := qre.Execute()
			return err
		})
		streamRejected := probe(id, func(qre *QueryExecutor) error {
			return qre.Stream(func(*sqltypes.Result) error { return nil })
		})

		if reason, ok := streamUnsupported[id]; ok {
			require.False(t, execRejected,
				"%s is listed as stream-unsupported but Execute rejects it too; remove the entry", id.String())
			require.True(t, streamRejected,
				"%s is listed as stream-unsupported (%s) but Stream dispatches it; remove the entry", id.String(), reason)
			continue
		}
		if execRejected {
			continue
		}
		require.False(t, streamRejected,
			"%s is dispatched by Execute but rejected by Stream's dispatch tables", id.String())
	}
}

// A connection setting first applied by a streamed query on a transaction
// connection must be recorded in the transaction properties, like Execute
// records it: a 2PC recovery replays the recorded queries, and later DML on
// the same connection sees the setting already applied and does not record
// it, so the first query's bookkeeping is the only record.
func TestStreamExecuteCompat_AppliedSettingRecordedForReplay(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	const applyQuery = "set @@sql_mode = ''"
	db.AddQuery(applyQuery, &sqltypes.Result{})
	query := "select * from test_table"
	db.AddQuery(query, &sqltypes.Result{Fields: getTestTableFields()})

	target := tsv.sm.Target()
	txID := newTransaction(tsv, nil)
	defer func() { _, _ = tsv.Rollback(ctx, target, txID) }()

	qre := newTestQueryExecutorStreaming(ctx, tsv, query, txID)
	qre.setting = smartconnpool.NewSetting(applyQuery, "set @@sql_mode = default")
	require.NoError(t, qre.Stream(func(*sqltypes.Result) error { return nil }))

	conn, err := tsv.te.txPool.GetAndLock(txID, "for query inspection")
	require.NoError(t, err)
	defer conn.Unlock()
	recorded := make([]string, 0, len(conn.TxProperties().Queries))
	for _, q := range conn.TxProperties().Queries {
		recorded = append(recorded, q.Sql)
	}
	require.Contains(t, recorded, applyQuery,
		"a setting applied by a streamed query must be recorded for transaction replay")
}

// State-changing plans on the streaming path are bounded by the query
// timeout, like Execute; the unbounded streaming exemption covers only reads.
func TestStreamExecuteCompat_QueryTimeoutForStateChangingPlans(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	tsv.QueryTimeout.Store(int64(100 * time.Millisecond))

	// The DDL blocks until the test releases it, so the only way Stream can
	// return before the release is the query timeout firing. The failsafe
	// release keeps a missing timeout from blocking the whole test run: the
	// DDL then completes without an error and the assertions below fail.
	ddl := "alter table test_table add zipcode int"
	rewritten := "alter table test_table add column zipcode int"
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	db.AddQuery(rewritten, &sqltypes.Result{})
	db.SetBeforeFunc(rewritten, func() { <-release })
	defer releaseOnce()
	failsafe := time.AfterFunc(30*time.Second, releaseOnce)
	defer failsafe.Stop()

	qre := newTestQueryExecutorStreaming(ctx, tsv, ddl, 0)
	err := qre.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, err, "streamed DDL must be bounded by the query timeout")
	require.ErrorContains(t, err, "maximum statement execution time exceeded")

	// A read outlives the query timeout: it holds the connection well past
	// the timeout and must still complete.
	query := "select * from test_table"
	db.AddQuery(query, &sqltypes.Result{Fields: getTestTableFields()})
	db.SetBeforeFunc(query, func() { time.Sleep(time.Second) })
	qre = newTestQueryExecutorStreaming(ctx, tsv, query, 0)
	require.NoError(t, qre.Stream(func(*sqltypes.Result) error { return nil }),
		"streamed reads must stay exempt from the query timeout")
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
