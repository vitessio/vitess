/*
Copyright 2019 The Vitess Authors.

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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/topo"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorResultsExceeded(t *testing.T) {
	save := *warnMemoryRows
	*warnMemoryRows = 3
	defer func() { *warnMemoryRows = save }()

	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	initial := warnings.Counts()["ResultsExceeded"]

	result1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1")
	result2 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	sbclookup.SetResults([]*sqltypes.Result{result1, result2})

	_, err := executor.Execute(ctx, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial, warnings.Counts()["ResultsExceeded"], "warnings count")

	_, err = executor.Execute(ctx, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial+1, warnings.Counts()["ResultsExceeded"], "warnings count")
}

func TestExecutorMaxMemoryRowsExceeded(t *testing.T) {
	save := *maxMemoryRows
	*maxMemoryRows = 3
	defer func() { *maxMemoryRows = save }()

	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	target := querypb.Target{}
	fn := func(r *sqltypes.Result) error {
		return nil
	}
	testCases := []struct {
		query string
		err   string
	}{
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from main1", ""},
		{"select * from main1", "in-memory row count exceeded allowed limit of 3"},
	}

	for _, test := range testCases {
		sbclookup.SetResults([]*sqltypes.Result{result})
		stmt, err := sqlparser.Parse(test.query)
		require.NoError(t, err)

		_, err = executor.Execute(ctx, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil)
		if sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt) {
			require.NoError(t, err, "no error when DirectiveIgnoreMaxMemoryRows is provided")
		} else {
			assert.EqualError(t, err, test.err, "maxMemoryRows limit exceeded")
		}

		sbclookup.SetResults([]*sqltypes.Result{result})
		err = executor.StreamExecute(ctx, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil, target, fn)
		require.NoError(t, err, "maxMemoryRows limit does not apply to StreamExecute")
	}
}

func TestLegacyExecutorTransactionsNoAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 0, sbclookup.CommitCount.Get(), "commit count")
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")

	// commit.
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")

	_, err = executor.Execute(context.Background(), "TestExecute", session, "commit", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// rollback.
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.RollbackCount.Get(), "rollback count")
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// CloseSession doesn't log anything
	err = executor.CloseSession(ctx, session)
	require.NoError(t, err)
	logStats = getQueryLog(logChan)
	if logStats != nil {
		t.Errorf("logstats: expected no record for no-op rollback, got %v", logStats)
	}

	// Prevent transactions on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica", InTransaction: true})
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.Error(t, err)
	want := "transactions are supported only for master tablet types, current type: REPLICA"
	require.Contains(t, err.Error(), want)

	// Prevent begin on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica"})
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), want)

	// Prevent use of non-master if in_transaction is on.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@master", InTransaction: true})
	_, err = executor.Execute(ctx, "TestExecute", session, "use @replica", nil)
	want = "cannot change to a non-master type in the middle of a transaction: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}
}

func TestDirectTargetRewrites(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	executor.normalize = true

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@master",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "select database()"

	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql:           "select :__vtdbname as `database()` from dual",
		BindVariables: map[string]*querypb.BindVariable{"__vtdbname": sqltypes.StringBindVariable("TestUnsharded/0@master")},
	}})
}

func TestExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master", Autocommit: true}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	// commit.
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@master", Autocommit: true}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.CommitCount.Get())

	logStats := testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime)
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	assert.NotEqual(t, 0, logStats.CommitTime)

	// rollback.
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@master", Autocommit: true}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
}

func TestExecutorDeleteMetadata(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	set := "set @@vitess_metadata.app_v1= '1'"
	_, err := executor.Execute(ctx, "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show := `show vitess_metadata variables like 'app\\_%'`
	result, _ := executor.Execute(ctx, "TestExecute", session, show, nil)
	assert.Len(t, result.Rows, 1)

	// Fails if deleting key that doesn't exist
	delQuery := "set @@vitess_metadata.doesn't_exist=''"
	_, err = executor.Execute(ctx, "TestExecute", session, delQuery, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))

	// Delete existing key, show should fail given the node doesn't exist
	delQuery = "set @@vitess_metadata.app_v1=''"
	_, err = executor.Execute(ctx, "TestExecute", session, delQuery, nil)
	assert.NoError(t, err)

	show = `show vitess_metadata variables like 'app\\_%'`
	_, err = executor.Execute(ctx, "TestExecute", session, show, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Get()
	_, err := executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{TargetString: "@master", InTransaction: true, FoundRows: 1, RowCount: -1}
	testSession := *session.Session
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, &testSession, "session does not match for autocommit=0")

	logStats := testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	// autocommit = 1
	_, err = executor.Execute(ctx, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, logChan, "TestExecute", "SET", "set session autocommit = 1", 0)

	// Setting autocommit=1 commits existing transaction.
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master", FoundRows: 1, RowCount: 1}
	utils.MustMatch(t, wantSession, session.Session, "session does not match for autocommit=1")

	logStats = testQueryLog(t, logChan, "TestExecute", "UPDATE", "update main1 set id=1", 1)
	assert.NotEqual(t, time.Duration(0), logStats.CommitTime, "logstats: expected non-zero CommitTime")
	assert.NotEqual(t, uint64(0), logStats.RowsAffected, "logstats: expected non-zero RowsAffected")

	// autocommit = 1, "begin"
	session.ResetTx()
	startCount = sbclookup.CommitCount.Get()
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@master", FoundRows: 1, RowCount: 1}
	testSession = *session.Session
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, &testSession, "session does not match for autocommit=1")
	if got, want := sbclookup.CommitCount.Get(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	logStats = testQueryLog(t, logChan, "TestExecute", "UPDATE", "update main1 set id=1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)

	// transition autocommit from 0 to 1 in the middle of a transaction.
	startCount = sbclookup.CommitCount.Get()
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@master"})
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	if got, want := sbclookup.CommitCount.Get(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_, err = executor.Execute(ctx, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
}

func TestExecutorShowColumns(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: ""})

	queries := []string{
		"SHOW COLUMNS FROM `user` in `TestExecutor`",
		"show columns from `user` in `TestExecutor`",
		"ShOw CoLuMnS fRoM `user` iN `TestExecutor`",
		"SHOW columns FROM `user` in `TestExecutor`",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := executor.Execute(ctx, "TestExecute", session, query, nil)
			require.NoError(t, err)

			wantQueries := []*querypb.BoundQuery{{
				Sql:           "show columns from user",
				BindVariables: map[string]*querypb.BindVariable{},
			}}

			assert.Equal(t, wantQueries, sbc1.Queries, "sbc1.Queries")
			assert.Empty(t, sbc1.BatchQueries, "sbc1.BatchQueries")
			assert.Empty(t, sbc2.Queries, "sbc2.Queries")
			assert.Empty(t, sbc2.BatchQueries, "sbc2.BatchQueries")
			assert.Empty(t, sbclookup.Queries, "sbclookup.Queries")
			assert.Empty(t, sbclookup.BatchQueries, "sbclookup.BatchQueries")

			sbc1.Queries = nil
			sbc2.Queries = nil
			sbclookup.Queries = nil
			sbc1.BatchQueries = nil
			sbc2.BatchQueries = nil
			sbclookup.BatchQueries = nil
		})
	}

}

func TestExecutorShow(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	for _, query := range []string{"show databases", "show vitess_keyspaces", "show keyspaces", "show DATABASES"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		if err != nil {
			t.Error(err)
		}
		wantqr := &sqltypes.Result{
			Fields: buildVarCharFields("Databases"),
			Rows: [][]sqltypes.Value{
				buildVarCharRow("TestExecutor"),
				buildVarCharRow(KsTestSharded),
				buildVarCharRow(KsTestUnsharded),
				buildVarCharRow("TestXBadSharding"),
				buildVarCharRow(KsTestBadVSchema),
			},
			RowsAffected: 5,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}
	_, err := executor.Execute(ctx, "TestExecute", session, "show variables", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "show collation", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "TestExecute", session, "show tables", nil)
	if err != errNoKeyspace {
		t.Errorf("'show tables' should fail without a keyspace")
	}

	if len(sbclookup.Queries) != 0 {
		t.Errorf("sbclookup unexpectedly has queries already")
	}

	showResults := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Tables_in_keyspace", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("some_table"),
		}},
	}
	sbclookup.SetResults([]*sqltypes.Result{showResults})

	query := fmt.Sprintf("show tables from %v", KsTestUnsharded)
	qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
	require.NoError(t, err)

	if len(sbclookup.Queries) != 1 {
		t.Errorf("Tablet should have received one 'show' query. Instead received: %v", sbclookup.Queries)
	} else {
		lastQuery := sbclookup.Queries[len(sbclookup.Queries)-1].Sql
		want := "show tables"
		if lastQuery != want {
			t.Errorf("Got: %v, want %v", lastQuery, want)
		}
	}

	wantqr := showResults
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
	}

	wantErrNoTable := "table unknown_table not found"
	_, err = executor.Execute(ctx, "TestExecute", session, "show create table unknown_table", nil)
	if err.Error() != wantErrNoTable {
		t.Errorf("Got: %v. Want: %v", err, wantErrNoTable)
	}

	// SHOW CREATE table using vschema to find keyspace.
	_, err = executor.Execute(ctx, "TestExecute", session, "show create table user_seq", nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	lastQuery := sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery := "show create table user_seq"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW CREATE table with query-provided keyspace
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show create table %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show create table unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW KEYS with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show keys from %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show keys from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show keys from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show keys from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW INDEX with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show index from %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show index from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show index from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show index from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW INDEXES with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show indexes from %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show indexes from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show indexes from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show indexes from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW EXTENDED {INDEX | INDEXES | KEYS}
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended index from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show extended index from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended indexes from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show extended indexes from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended keys from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show extended keys from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// Set desitation keyspace in session
	session.TargetString = KsTestUnsharded
	_, err = executor.Execute(ctx, "TestExecute", session, "show create table unknown", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "TestExecute", session, "show full columns from table1", nil)
	require.NoError(t, err)

	// Reset target string so other tests dont fail.
	session.TargetString = "@master"
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show full columns from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	for _, query := range []string{"show charset", "show character set"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8",
					"UTF-8 Unicode",
					"utf8_general_ci"), sqltypes.NewInt32(3)),
				append(buildVarCharRow(
					"utf8mb4",
					"UTF-8 Unicode",
					"utf8mb4_general_ci"),
					sqltypes.NewInt32(4)),
			},
			RowsAffected: 2,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}
	for _, query := range []string{"show charset like '%foo'", "show character set like 'foo%'", "show charset like 'foo%'", "show character set where foo like 'utf8'", "show character set where charset like '%foo'", "show charset where charset = '%foo'"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields:       append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}),
			Rows:         [][]sqltypes.Value{},
			RowsAffected: 0,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}
	for _, query := range []string{"show charset like 'utf8'", "show character set like 'utf8'", "show charset where charset = 'utf8'", "show character set where charset = 'utf8'"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8",
					"UTF-8 Unicode",
					"utf8_general_ci"), sqltypes.NewInt32(3)),
			},
			RowsAffected: 1,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}
	for _, query := range []string{"show charset like 'utf8mb4'", "show character set like 'utf8mb4'", "show charset where charset = 'utf8mb4'", "show character set where charset = 'utf8mb4'"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8mb4",
					"UTF-8 Unicode",
					"utf8mb4_general_ci"),
					sqltypes.NewInt32(4)),
			},
			RowsAffected: 1,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}

	qr, err = executor.Execute(ctx, "TestExecute", session, "show engines", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow(
				"InnoDB",
				"DEFAULT",
				"Supports transactions, row-level locking, and foreign keys",
				"YES",
				"YES",
				"YES"),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show engines:\n%+v, want\n%+v", qr, wantqr)
	}
	qr, err = executor.Execute(ctx, "TestExecute", session, "show plugins", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Name", "Status", "Type", "Library", "License"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow(
				"InnoDB",
				"ACTIVE",
				"STORAGE ENGINE",
				"NULL",
				"GPL"),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show plugins:\n%+v, want\n%+v", qr, wantqr)
	}
	for _, query := range []string{"show session status", "show session status like 'Ssl_cipher'"} {
		qr, err = executor.Execute(ctx, "TestExecute", session, "show session status", nil)
		if err != nil {
			t.Error(err)
		}
		wantqr = &sqltypes.Result{
			Fields:       buildVarCharFields("Variable_name", "Value"),
			Rows:         make([][]sqltypes.Value, 0, 2),
			RowsAffected: 0,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("%v:\n%+v, want\n%+v", query, qr, wantqr)
		}
	}
	qr, err = executor.Execute(ctx, "TestExecute", session, "show vitess_shards", nil)
	require.NoError(t, err)

	// Test SHOW FULL COLUMNS FROM where query has a qualifier
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show full columns from %v.table1", KsTestUnsharded), nil)
	require.NoError(t, err)

	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor/-20"),
			buildVarCharRow("TestXBadVSchema/e0-"),
		},
		RowsAffected: 33,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vitess_shards:\n%+v, want\n%+v", qr, wantqr)
	}

	qr, err = executor.Execute(ctx, "TestExecute", session, "show vitess_tablets", nil)
	require.NoError(t, err)
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "MasterTermStartTime"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("FakeCell", "TestExecutor", "-20", "MASTER", "SERVING", "aa-0000000000", "-20", "1970-01-01T00:00:01Z"),
			buildVarCharRow("FakeCell", "TestUnsharded", "0", "MASTER", "SERVING", "aa-0000000000", "0", "1970-01-01T00:00:01Z"),
		},
		RowsAffected: 9,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vitess_tablets:\n%+v, want\n%+v", qr, wantqr)
	}

	qr, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor", "hash_index", "hash", "", ""),
			buildVarCharRow("TestExecutor", "idx1", "hash", "", ""),
			buildVarCharRow("TestExecutor", "idx_noauto", "hash", "", "noauto_table"),
			buildVarCharRow("TestExecutor", "insert_ignore_idx", "lookup_hash", "from=fromcol; table=ins_lookup; to=tocol", "insert_ignore_test"),
			buildVarCharRow("TestExecutor", "keyspace_id", "numeric", "", ""),
			buildVarCharRow("TestExecutor", "krcol_unique_vdx", "keyrange_lookuper_unique", "", ""),
			buildVarCharRow("TestExecutor", "krcol_vdx", "keyrange_lookuper", "", ""),
			buildVarCharRow("TestExecutor", "music_user_map", "lookup_hash_unique", "from=music_id; table=music_user_map; to=user_id", "music"),
			buildVarCharRow("TestExecutor", "name_lastname_keyspace_id_map", "lookup", "from=name,lastname; table=name_lastname_keyspace_id_map; to=keyspace_id", "user2"),
			buildVarCharRow("TestExecutor", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
		},
		RowsAffected: 10,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes:\n%+v, want\n%+v", qr, wantqr)
	}

	qr, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on TestExecutor.user", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("Id", "hash_index", "hash", "", ""),
			buildVarCharRow("name", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.user:\n%+v, want\n%+v", qr, wantqr)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on user", nil)
	wantErr := errNoKeyspace.Error()
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on TestExecutor.garbage", nil)
	wantErr = "table `garbage` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	session.TargetString = "TestExecutor"
	qr, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on user", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("Id", "hash_index", "hash", "", ""),
			buildVarCharRow("name", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on user:\n%+v, want\n%+v", qr, wantqr)
	}

	session.TargetString = "TestExecutor"
	qr, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on user2", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "hash_index", "hash", "", ""),
			buildVarCharRow("name, lastname", "name_lastname_keyspace_id_map", "lookup", "from=name,lastname; table=name_lastname_keyspace_id_map; to=keyspace_id", "user2"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on user2:\n%+v, want\n%+v", qr, wantqr)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "show vschema vindexes on garbage", nil)
	wantErr = "table `garbage` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	qr, err = executor.Execute(ctx, "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Code", Type: sqltypes.Uint16},
			{Name: "Message", Type: sqltypes.VarChar},
		},
		Rows:         [][]sqltypes.Value{},
		RowsAffected: 0,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show warnings:\n%+v, want\n%+v", qr, wantqr)

	}

	session.Warnings = []*querypb.QueryWarning{}
	qr, err = executor.Execute(ctx, "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Code", Type: sqltypes.Uint16},
			{Name: "Message", Type: sqltypes.VarChar},
		},
		Rows:         [][]sqltypes.Value{},
		RowsAffected: 0,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show warnings:\n%+v, want\n%+v", qr, wantqr)
	}

	session.Warnings = []*querypb.QueryWarning{
		{Code: mysql.ERBadTable, Message: "bad table"},
		{Code: mysql.EROutOfResources, Message: "ks/-40: query timed out"},
	}
	qr, err = executor.Execute(ctx, "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Code", Type: sqltypes.Uint16},
			{Name: "Message", Type: sqltypes.VarChar},
		},

		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("Warning"), sqltypes.NewUint32(mysql.ERBadTable), sqltypes.NewVarChar("bad table")},
			{sqltypes.NewVarChar("Warning"), sqltypes.NewUint32(mysql.EROutOfResources), sqltypes.NewVarChar("ks/-40: query timed out")},
		},
		RowsAffected: 0,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show warnings:\n%+v, want\n%+v", qr, wantqr)
	}

	// Make sure it still works when one of the keyspaces is in a bad state
	getSandbox("TestExecutor").SrvKeyspaceMustFail++
	qr, err = executor.Execute(ctx, "TestExecute", session, "show vitess_shards", nil)
	require.NoError(t, err)
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestSharded/-20"),
			buildVarCharRow("TestXBadVSchema/e0-"),
		},
		RowsAffected: 25,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
	}

	session = NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	qr, err = executor.Execute(ctx, "TestExecute", session, "show vschema tables", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Tables"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("dual"),
			buildVarCharRow("ins_lookup"),
			buildVarCharRow("main1"),
			buildVarCharRow("music_user_map"),
			buildVarCharRow("name_lastname_keyspace_id_map"),
			buildVarCharRow("name_user_map"),
			buildVarCharRow("simple"),
			buildVarCharRow("user_msgs"),
			buildVarCharRow("user_seq"),
		},
		RowsAffected: 9,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema tables:\n%+v, want\n%+v", qr, wantqr)
	}

	session = NewSafeSession(&vtgatepb.Session{})
	_, err = executor.Execute(ctx, "TestExecute", session, "show vschema tables", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "show 10", nil)
	want = "syntax error at position 8 near '10'"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}

	session = NewSafeSession(&vtgatepb.Session{TargetString: "no_such_keyspace"})
	_, err = executor.Execute(ctx, "TestExecute", session, "show vschema tables", nil)
	want = "keyspace no_such_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}
}

func TestExecutorUse(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@master"})

	stmts := []string{
		"use TestExecutor",
		"use `TestExecutor:-80@master`",
	}
	want := []string{
		"TestExecutor",
		"TestExecutor:-80@master",
	}
	for i, stmt := range stmts {
		_, err := executor.Execute(ctx, "TestExecute", session, stmt, nil)
		if err != nil {
			t.Error(err)
		}
		wantSession := &vtgatepb.Session{Autocommit: true, TargetString: want[i], RowCount: -1}
		utils.MustMatch(t, wantSession, session.Session, "session does not match")
	}

	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use 1", nil)
	wantErr := "syntax error at position 6 near '1'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use UnexistentKeyspace", nil)
	wantErr = "Unknown database 'UnexistentKeyspace' (errno 1049) (sqlstate 42000)"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}
}

func TestExecutorComment(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()

	stmts := []string{
		"/*! SET autocommit=1*/",
		"/*!50708 SET @x=5000*/",
	}
	wantResult := &sqltypes.Result{}

	for _, stmt := range stmts {
		gotResult, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotResult, wantResult) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotResult, wantResult)
		}
	}
}

func TestExecutorOther(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr       bool
		hasDestinationShardErr bool
		wantCnts               cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:              "TestExecutor[-]",
			hasDestinationShardErr: true,
		},
		{
			targetStr: KsTestUnsharded,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr: "TestExecutor",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr: "TestExecutor/-20",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr: "TestExecutor[00]",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"show tables",
		"analyze table t1",
		"describe select * from t1",
		"explain select * from t1",
		"repair table t1",
		"optimize table t1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			sbc1.ExecCount.Set(0)
			sbc2.ExecCount.Set(0)
			sbclookup.ExecCount.Set(0)

			_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				assert.Error(t, err, errNoKeyspace)
			} else if tc.hasDestinationShardErr {
				assert.Errorf(t, err, "Destination can only be a single shard for statement: %s, got: DestinationExactKeyRange(-)", stmt)
			} else {
				assert.NoError(t, err)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Get(),
				Sbc2Cnt:      sbc2.ExecCount.Get(),
				SbcLookupCnt: sbclookup.ExecCount.Get(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}
		}
	}
}

func TestExecutorDDL(t *testing.T) {
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr bool
		shardQueryCnt    int
		wantCnts         cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:     KsTestUnsharded,
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr:     "TestExecutor",
			shardQueryCnt: 8,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      1,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr:     "TestExecutor/-20",
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"create table t1(id bigint primary key)",
		"alter table t1 add primary key id",
		"rename table t1 to t2",
		"truncate table t2",
		"drop table t2",
		`create table test_partitioned (
			id bigint,
			date_create int,		
			primary key(id)
		) Engine=InnoDB	/*!50100 PARTITION BY RANGE (date_create)
		  (PARTITION p2018_06_14 VALUES LESS THAN (1528959600) ENGINE = InnoDB,
		   PARTITION p2018_06_15 VALUES LESS THAN (1529046000) ENGINE = InnoDB,
		   PARTITION p2018_06_16 VALUES LESS THAN (1529132400) ENGINE = InnoDB,
		   PARTITION p2018_06_17 VALUES LESS THAN (1529218800) ENGINE = InnoDB)*/`,
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			sbc1.ExecCount.Set(0)
			sbc2.ExecCount.Set(0)
			sbclookup.ExecCount.Set(0)
			stmtType := "DDL"
			_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				require.EqualError(t, err, "keyspace not specified", "expect query to fail")
				stmtType = "" // For error case, plan is not generated to query log will not contain any stmtType.
			} else {
				require.NoError(t, err)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Get(),
				Sbc2Cnt:      sbc2.ExecCount.Get(),
				SbcLookupCnt: sbclookup.ExecCount.Get(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}

			testQueryLog(t, logChan, "TestExecute", stmtType, stmt, tc.shardQueryCnt)
		}
	}
}

func TestExecutorAlterVSchemaKeyspace(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 2)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces["TestExecutor"].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	stmt := "alter vschema create vindex TestExecutor.test_vindex using hash"
	_, err := executor.Execute(ctx, "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, "TestExecutor", "test_vindex", vschemaUpdates, executor)
	assert.Equal(t, vindex.Type, "hash")
}

func TestExecutorCreateVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()
	ks := "TestExecutor"

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema create vindex test_vindex using hash"
	_, err := executor.Execute(ctx, "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, ks, "test_vindex", vschemaUpdates, executor)
	if vindex == nil || vindex.Type != "hash" {
		t.Errorf("updated vschema did not contain test_vindex")
	}

	_, err = executor.Execute(ctx, "TestExecute", session, stmt, nil)
	wantErr := "vindex test_vindex already exists in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("create duplicate vindex: %v, want %s", err, wantErr)
	}
	select {
	case <-vschemaUpdates:
		t.Error("vschema should not be updated on error")
	default:
	}

	// Create a new vschema keyspace implicitly by creating a vindex with a different
	// target in the session
	//ksNew := "test_new_keyspace"
	session = NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt = "alter vschema create vindex test_vindex2 using hash"
	_, err = executor.Execute(ctx, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_vindex2", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}
	keyspace, ok := vschema.Keyspaces[ks]
	if !ok || !keyspace.Sharded {
		t.Errorf("keyspace should have been created with Sharded=true")
	}

	// No queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Get(),
		sbc2.ExecCount.Get(),
		sbclookup.ExecCount.Get(),
	}
	if !reflect.DeepEqual(gotCount, wantCount) {
		t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
	}
}

func TestExecutorAddDropVschemaTableDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()
	ks := KsTestUnsharded

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Tables["test_table"]
	if ok {
		t.Fatalf("test_table should not exist in original vschema")
	}

	var vschemaTables []string
	for t := range vschema.Keyspaces[ks].Tables {
		vschemaTables = append(vschemaTables, t)
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema add table test_table"
	_, err := executor.Execute(ctx, "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append(vschemaTables, "test_table"), executor)

	stmt = "alter vschema add table test_table2"
	_, err = executor.Execute(ctx, "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append(vschemaTables, []string{"test_table", "test_table2"}...), executor)

	// Should fail adding a table on a sharded keyspace
	session = NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	stmt = "alter vschema add table test_table"
	_, err = executor.Execute(ctx, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, "add vschema table: unsupported on sharded keyspace TestExecutor")

	// No queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Get(),
		sbc2.ExecCount.Get(),
		sbclookup.ExecCount.Get(),
	}
	utils.MustMatch(t, wantCount, gotCount, "")
}

func TestExecutorVindexDDLACL(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})

	ctxRedUser := callerid.NewContext(ctx, &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "redUser"})
	ctxBlueUser := callerid.NewContext(ctx, &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "blueUser"})

	// test that by default no users can perform the operation
	stmt := "alter vschema create vindex test_hash using hash"
	authErr := "not authorized to perform vschema operations"
	_, err := executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	if err == nil || err.Error() != authErr {
		t.Errorf("expected error '%s' got '%v'", authErr, err)
	}

	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	if err == nil || err.Error() != authErr {
		t.Errorf("expected error '%s' got '%v'", authErr, err)
	}

	// test when all users are enabled
	*vschemaacl.AuthorizedDDLUsers = "%"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}
	stmt = "alter vschema create vindex test_hash2 using hash"
	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// test when only one user is enabled
	*vschemaacl.AuthorizedDDLUsers = "orangeUser, blueUser, greenUser"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	if err == nil || err.Error() != authErr {
		t.Errorf("expected error '%s' got '%v'", authErr, err)
	}
	stmt = "alter vschema create vindex test_hash3 using hash"
	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// restore the disallowed state
	*vschemaacl.AuthorizedDDLUsers = ""
}

func TestExecutorUnrecognized(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "invalid statement", nil)
	require.Error(t, err, "unrecognized statement: invalid statement'")
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()

	stats := r.VSchemaStats()

	templ := template.New("")
	templ, err := templ.Parse(VSchemaTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, stats); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
	result := wr.String()
	if !strings.Contains(result, "<td>TestXBadSharding</td>") ||
		!strings.Contains(result, "<td>TestUnsharded</td>") {
		t.Errorf("invalid html result: %v", result)
	}
}

func TestGetPlanUnnormalized(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	emptyvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	unshardedvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)

	logStats1 := NewLogStats(ctx, "Test", "", nil)
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	logStats2 := NewLogStats(ctx, "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats2)
	require.NoError(t, err)
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		"@unknown:" + query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}
	logStats3 := NewLogStats(ctx, "Test", "", nil)
	plan3, err := r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats3)
	require.NoError(t, err)
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}
	logStats4 := NewLogStats(ctx, "Test", "", nil)
	plan4, err := r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats4)
	require.NoError(t, err)
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + "@unknown:" + query1,
		"@unknown:" + query1,
	}
	if diff := cmp.Diff(want, r.plans.Keys()); diff != "" {
		t.Errorf("\n-want,+got:\n%s", diff)
	}
	//if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
	//	t.Errorf("Plan keys: %s, want %s", keys, want)
	//}
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}
}

func TestGetPlanCacheUnnormalized(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	emptyvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(ctx, "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	require.NoError(t, err)
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(ctx, "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	require.NoError(t, err)
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	wantSQL = query1 + " /* comment 2 */"
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createLegacyExecutorEnv()
	unshardedvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(ctx, "Test", "", nil)
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 0 {
		t.Errorf("Plan keys should be 0, got: %v", len(r.plans.Keys()))
	}

	query1 = "insert into user(id) values (1), (2)"
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 1 {
		t.Errorf("Plan keys should be 1, got: %v", len(r.plans.Keys()))
	}

	// the target string will be resolved and become part of the plan cache key, which adds a new entry
	ksIDVc1, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	_, err = r.getPlan(ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 2 {
		t.Errorf("Plan keys should be 2, got: %v", len(r.plans.Keys()))
	}

	// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
	ksIDVc2, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	_, err = r.getPlan(ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 2 {
		t.Errorf("Plan keys should be 2, got: %v", len(r.plans.Keys()))
	}
}

func TestGetPlanCacheNormalized(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	r.normalize = true
	emptyvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(ctx, "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	require.NoError(t, err)
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := "select * from music_user_map where id = :vtg1 /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(ctx, "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	require.NoError(t, err)
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createLegacyExecutorEnv()
	r.normalize = true
	unshardedvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(ctx, "Test", "", nil)
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 0 {
		t.Errorf("Plan keys should be 0, got: %v", len(r.plans.Keys()))
	}

	query1 = "insert into user(id) values (1), (2)"
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 1 {
		t.Errorf("Plan keys should be 1, got: %v", len(r.plans.Keys()))
	}

	// the target string will be resolved and become part of the plan cache key, which adds a new entry
	ksIDVc1, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	_, err = r.getPlan(ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 2 {
		t.Errorf("Plan keys should be 2, got: %v", len(r.plans.Keys()))
	}

	// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
	ksIDVc2, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	_, err = r.getPlan(ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	if len(r.plans.Keys()) != 2 {
		t.Errorf("Plan keys should be 2, got: %v", len(r.plans.Keys()))
	}
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	r.normalize = true
	emptyvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)
	unshardedvc, _ := newVCursorImpl(ctx, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver)

	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	logStats1 := NewLogStats(ctx, "Test", "", nil)
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment 1 */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	logStats2 := NewLogStats(ctx, "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false, logStats2)
	require.NoError(t, err)
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		"@unknown:" + normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}

	wantSQL := normalized + " /* comment 1 */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	wantSQL = normalized + " /* comment 2 */"
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	logStats3 := NewLogStats(ctx, "Test", "", nil)
	plan3, err := r.getPlan(emptyvc, query2, makeComments(" /* comment 3 */"), map[string]*querypb.BindVariable{}, false, logStats3)
	require.NoError(t, err)
	if plan1 != plan3 {
		t.Errorf("getPlan(query2): plans must be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 3 */"
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}

	logStats4 := NewLogStats(ctx, "Test", "", nil)
	plan4, err := r.getPlan(emptyvc, normalized, makeComments(" /* comment 4 */"), map[string]*querypb.BindVariable{}, false, logStats4)
	require.NoError(t, err)
	if plan1 != plan4 {
		t.Errorf("getPlan(normalized): plans must be equal: %p %p", plan1, plan4)
	}
	wantSQL = normalized + " /* comment 4 */"
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}

	logStats5 := NewLogStats(ctx, "Test", "", nil)
	plan3, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment 5 */"), map[string]*querypb.BindVariable{}, false, logStats5)
	require.NoError(t, err)
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 5 */"
	if logStats5.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats5.SQL)
	}

	logStats6 := NewLogStats(ctx, "Test", "", nil)
	plan4, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment 6 */"), map[string]*querypb.BindVariable{}, false, logStats6)
	require.NoError(t, err)
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + "@unknown:" + normalized,
		"@unknown:" + normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}

	// Errors
	logStats7 := NewLogStats(ctx, "Test", "", nil)
	_, err = r.getPlan(emptyvc, "syntax", makeComments(""), map[string]*querypb.BindVariable{}, false, logStats7)
	wantErr := "syntax error at position 7 near 'syntax'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestPassthroughDDL(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	masterSession.TargetString = "TestExecutor"

	alterDDL := "/* leading */ alter table passthrough_ddl add columne col bigint default 123 /* trailing */"
	_, err := executorExec(executor, alterDDL, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           alterDDL,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc1.Queries = nil
	sbc2.Queries = nil

	// Force the query to go to only one shard. Normalization doesn't make any difference.
	masterSession.TargetString = "TestExecutor/40-60"
	executor.normalize = true

	_, err = executorExec(executor, alterDDL, nil)
	require.NoError(t, err)
	require.Nil(t, sbc1.Queries)
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""

	// Use range query
	masterSession.TargetString = "TestExecutor[-]"
	executor.normalize = true

	_, err = executorExec(executor, alterDDL, nil)
	require.NoError(t, err)
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""
}

func TestParseEmptyTargetSingleKeyspace(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("")
	if destKeyspace != KsTestUnsharded || destTabletType != topodatapb.TabletType_MASTER {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@master",
			destKeyspace,
			destTabletType,
			KsTestUnsharded,
			topodatapb.TabletType_MASTER,
		)
	}
}

func TestParseEmptyTargetMultiKeyspace(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
			KsTestSharded:   r.vschema.Keyspaces[KsTestSharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("")
	if destKeyspace != "" || destTabletType != topodatapb.TabletType_MASTER {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@master",
			destKeyspace,
			destTabletType,
			"",
			topodatapb.TabletType_MASTER,
		)
	}
}

func TestParseTargetSingleKeyspace(t *testing.T) {
	r, _, _, _ := createLegacyExecutorEnv()
	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("@replica")
	if destKeyspace != KsTestUnsharded || destTabletType != topodatapb.TabletType_REPLICA {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@replica",
			destKeyspace,
			destTabletType,
			KsTestUnsharded,
			topodatapb.TabletType_REPLICA,
		)
	}
}

func TestDebugVSchema(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/debug/vschema", nil)

	executor, _, _, _ := createLegacyExecutorEnv()
	executor.ServeHTTP(resp, req)
	v := make(map[string]interface{})
	if err := json.Unmarshal(resp.Body.Bytes(), &v); err != nil {
		t.Fatalf("Unmarshal on %s failed: %v", resp.Body.String(), err)
	}
	if _, ok := v["routing_rules"]; !ok {
		t.Errorf("routing rules missing: %v", resp.Body.String())
	}
	if _, ok := v["keyspaces"]; !ok {
		t.Errorf("keyspaces missing: %v", resp.Body.String())
	}
}

func TestGenerateCharsetRows(t *testing.T) {
	rows := make([][]sqltypes.Value, 0, 4)
	rows0 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8",
			"UTF-8 Unicode",
			"utf8_general_ci"),
			sqltypes.NewInt32(3)),
	}
	rows1 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			"utf8mb4_general_ci"),
			sqltypes.NewInt32(4)),
	}
	rows2 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8",
			"UTF-8 Unicode",
			"utf8_general_ci"),
			sqltypes.NewInt32(3)),
		append(buildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			"utf8mb4_general_ci"),
			sqltypes.NewInt32(4)),
	}

	testcases := []struct {
		input    string
		expected [][]sqltypes.Value
	}{
		{input: "show charset", expected: rows2},
		{input: "show character set", expected: rows2},
		{input: "show charset where charset like 'foo%'", expected: rows},
		{input: "show charset where charset like 'utf8%'", expected: rows0},
		{input: "show charset where charset = 'utf8'", expected: rows0},
		{input: "show charset where charset = 'foo%'", expected: rows},
		{input: "show charset where charset = 'utf8mb4'", expected: rows1},
	}

	charsets := []string{"utf8", "utf8mb4"}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tc.input)
			require.NoError(t, err)
			match := stmt.(*sqlparser.Show)
			filter := match.ShowTablesOpt.Filter
			actual, err := generateCharsetRows(filter, charsets)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestExecutorMaxPayloadSizeExceeded(t *testing.T) {
	saveMax := *maxPayloadSize
	saveWarn := *warnPayloadSize
	*maxPayloadSize = 10
	*warnPayloadSize = 5
	defer func() {
		*maxPayloadSize = saveMax
		*warnPayloadSize = saveWarn
	}()

	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})
	warningCount := warnings.Counts()["WarnPayloadSizeExceeded"]
	testMaxPayloadSizeExceeded := []string{
		"select * from main1",
		"insert into main1(id) values (1), (2)",
		"update main1 set id=1",
		"delete from main1 where id=1",
	}
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
		require.NotNil(t, err)
		assert.EqualError(t, err, "query payload size above threshold")
	}
	assert.Equal(t, warningCount, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")

	testMaxPayloadSizeOverride := []string{
		"select /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ * from main1",
		"insert /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ into main1(id) values (1), (2)",
		"update /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ main1 set id=1",
		"delete /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ from main1 where id=1",
	}
	for _, query := range testMaxPayloadSizeOverride {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeWithOverride", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")

	*maxPayloadSize = 1000
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount+4, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")
}

func TestOlapSelectDatabase(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	executor.normalize = true

	session := &vtgatepb.Session{Autocommit: true}

	sql := `select database()`
	target := querypb.Target{}
	cbInvoked := false
	cb := func(r *sqltypes.Result) error {
		cbInvoked = true
		return nil
	}
	err := executor.StreamExecute(context.Background(), "TestExecute", NewSafeSession(session), sql, nil, target, cb)
	assert.NoError(t, err)
	assert.True(t, cbInvoked)
}

func TestExecutorClearsWarnings(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{Code: 234, Message: "oh noes"}},
	})
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select 42", nil)
	require.NoError(t, err)
	require.Empty(t, session.Warnings)
}

func TestExecutorOtherRead(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr       bool
		hasDestinationShardErr bool
		wantCnts               cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:              "TestExecutor[-]",
			hasDestinationShardErr: true,
		},
		{
			targetStr: KsTestUnsharded,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr: "TestExecutor",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"analyze table t1",
		"describe select * from t1",
		"explain select * from t1",
		"do 1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			sbc1.ExecCount.Set(0)
			sbc2.ExecCount.Set(0)
			sbclookup.ExecCount.Set(0)

			_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				assert.EqualError(t, err, "keyspace not specified")
			} else if tc.hasDestinationShardErr {
				assert.Errorf(t, err, "Destination can only be a single shard for statement: %s, got: DestinationExactKeyRange(-)", stmt)
			} else {
				assert.NoError(t, err)
			}

			utils.MustMatch(t, tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Get(),
				Sbc2Cnt:      sbc2.ExecCount.Get(),
				SbcLookupCnt: sbclookup.ExecCount.Get(),
			}, "count did not match")
		}
	}
}

func TestExecutorExplain(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	bindVars := map[string]*querypb.BindVariable{}
	result, err := executorExec(executor, "explain format = vitess select * from user", bindVars)
	require.NoError(t, err)

	require.Equal(t,
		`[[VARCHAR("Route") VARCHAR("SelectScatter") VARCHAR("TestExecutor") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("select * from user")]]`,
		fmt.Sprintf("%v", result.Rows))

	result, err = executorExec(executor, "explain format = vitess select 42", bindVars)
	require.NoError(t, err)
	expected :=
		`[[VARCHAR("Projection") VARCHAR("") VARCHAR("") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("")] ` +
			`[VARCHAR("└─ SingleRow") VARCHAR("") VARCHAR("") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("")]]`
	require.Equal(t,
		`[[VARCHAR("Projection") VARCHAR("") VARCHAR("") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("")] `+
			`[VARCHAR("└─ SingleRow") VARCHAR("") VARCHAR("") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("")]]`,
		expected,
		fmt.Sprintf("%v", result.Rows), fmt.Sprintf("%v", result.Rows))
}

func TestExecutorOtherAdmin(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr       bool
		hasDestinationShardErr bool
		wantCnts               cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:              "TestExecutor[-]",
			hasDestinationShardErr: true,
		},
		{
			targetStr: KsTestUnsharded,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr: "TestExecutor",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"repair table t1",
		"optimize table t1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			sbc1.ExecCount.Set(0)
			sbc2.ExecCount.Set(0)
			sbclookup.ExecCount.Set(0)

			_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				assert.Error(t, err, errNoKeyspace)
			} else if tc.hasDestinationShardErr {
				assert.Errorf(t, err, "Destination can only be a single shard for statement: %s, got: DestinationExactKeyRange(-)", stmt)
			} else {
				assert.NoError(t, err)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Get(),
				Sbc2Cnt:      sbc2.ExecCount.Get(),
				SbcLookupCnt: sbclookup.ExecCount.Get(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}
		}
	}
}

func TestExecutorSavepointInTx(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	logChan := QueryLogger.Subscribe("TestExecutorSavepoint")
	defer QueryLogger.Unsubscribe(logChan)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: false, TargetString: "@master"})
	_, err := exec(executor, session, "savepoint a")
	require.NoError(t, err)
	_, err = exec(executor, session, "rollback to a")
	require.NoError(t, err)
	_, err = exec(executor, session, "release savepoint a")
	require.NoError(t, err)
	_, err = exec(executor, session, "select id from user where id = 1")
	require.NoError(t, err)
	_, err = exec(executor, session, "savepoint b")
	require.NoError(t, err)
	_, err = exec(executor, session, "rollback to b")
	require.NoError(t, err)
	_, err = exec(executor, session, "release savepoint b")
	require.NoError(t, err)
	_, err = exec(executor, session, "select id from user where id = 3")
	require.NoError(t, err)
	_, err = exec(executor, session, "rollback")
	require.NoError(t, err)
	sbc1WantQueries := []*querypb.BoundQuery{{
		Sql:           "savepoint a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "rollback to a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "release savepoint a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select id from user where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "savepoint b",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "rollback to b",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "release savepoint b",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	sbc2WantQueries := []*querypb.BoundQuery{{
		Sql:           "savepoint a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "rollback to a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "release savepoint a",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "savepoint b",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "rollback to b",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "release savepoint b",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select id from user where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "")
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT", "savepoint a", 0)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to a", 0)
	testQueryLog(t, logChan, "TestExecute", "RELEASE", "release savepoint a", 0)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from user where id = 1", 1)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT", "savepoint b", 1)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to b", 1)
	testQueryLog(t, logChan, "TestExecute", "RELEASE", "release savepoint b", 1)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from user where id = 3", 1)
	testQueryLog(t, logChan, "TestExecute", "ROLLBACK", "rollback", 2)
}

func TestExecutorSavepointWithoutTx(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	logChan := QueryLogger.Subscribe("TestExecutorSavepoint")
	defer QueryLogger.Unsubscribe(logChan)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@master", InTransaction: false})
	_, err := exec(executor, session, "savepoint a")
	require.NoError(t, err)
	_, err = exec(executor, session, "rollback to a")
	require.Error(t, err)
	_, err = exec(executor, session, "release savepoint a")
	require.Error(t, err)
	_, err = exec(executor, session, "select id from user where id = 1")
	require.NoError(t, err)
	_, err = exec(executor, session, "savepoint b")
	require.NoError(t, err)
	_, err = exec(executor, session, "rollback to b")
	require.Error(t, err)
	_, err = exec(executor, session, "release savepoint b")
	require.Error(t, err)
	_, err = exec(executor, session, "select id from user where id = 3")
	require.NoError(t, err)
	sbc1WantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	sbc2WantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "")
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT", "savepoint a", 0)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to a", 0)
	testQueryLog(t, logChan, "TestExecute", "RELEASE", "release savepoint a", 0)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from user where id = 1", 1)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT", "savepoint b", 0)
	testQueryLog(t, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to b", 0)
	testQueryLog(t, logChan, "TestExecute", "RELEASE", "release savepoint b", 0)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from user where id = 3", 1)
}

func exec(executor *Executor, session *SafeSession, sql string) (*sqltypes.Result, error) {
	return executor.Execute(context.Background(), "TestExecute", session, sql, nil)
}

func makeComments(text string) sqlparser.MarginComments {
	return sqlparser.MarginComments{Trailing: text}
}
