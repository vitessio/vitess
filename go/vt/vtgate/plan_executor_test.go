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

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/topo"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestPlanExecutorResultsExceeded(t *testing.T) {
	save := *warnMemoryRows
	*warnMemoryRows = 3
	defer func() { *warnMemoryRows = save }()

	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	initial := warnings.Counts()["ResultsExceeded"]

	result1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1")
	result2 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	sbclookup.SetResults([]*sqltypes.Result{result1, result2})

	_, err := executor.Execute(context.Background(), "TestExecutorResultsExceeded", session, "select * from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := warnings.Counts()["ResultsExceeded"], initial; got != want {
		t.Errorf("warnings count: %v, want %v", got, want)
	}

	_, err = executor.Execute(context.Background(), "TestExecutorResultsExceeded", session, "select * from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := warnings.Counts()["ResultsExceeded"], initial+1; got != want {
		t.Errorf("warnings count: %v, want %v", got, want)
	}
}

func TestPlanExecutorTransactionsNoAutoCommit(t *testing.T) {
	////t.Skip("not support yet")
	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}

	// commit.
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}

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
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "rollback", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// rollback doesn't emit a logstats record when it doesn't do anything
	_, err = executor.Execute(context.Background(), "TestExecute", session, "rollback", nil)
	if err != nil {
		t.Fatal(err)
	}
	logStats = getQueryLog(logChan)
	if logStats != nil {
		t.Errorf("logstats: expected no record for no-op rollback, got %v", logStats)
	}

	// Prevent transactions on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica", InTransaction: true})
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	want := "transactions are supported only for master tablet types, current type: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent begin on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica"})
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent use of non-master if in_transaction is on.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@master", InTransaction: true})
	_, err = executor.Execute(context.Background(), "TestExecute", session, "use @replica", nil)
	want = "cannot change to a non-master type in the middle of a transaction: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}
}

func TestPlanDirectTargetRewrites(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	executor.normalize = true

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@master",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "select database()"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql:           "select :__vtdbname as `database()` from dual",
		BindVariables: map[string]*querypb.BindVariable{"__vtdbname": sqltypes.StringBindVariable("TestUnsharded")},
	}})
}

func TestPlanExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master", Autocommit: true}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	// commit.
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "commit", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master", Autocommit: true}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	logStats := testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// rollback.
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "rollback", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master", Autocommit: true}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
}

func TestPlanExecutorDeleteMetadata(t *testing.T) {
	t.Skip("not support yet")
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	set := "set @@vitess_metadata.app_v1= '1'"
	_, err := executor.Execute(context.Background(), "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show := `show vitess_metadata variables like 'app\\_%'`
	result, _ := executor.Execute(context.Background(), "TestExecute", session, show, nil)
	assert.Len(t, result.Rows, 1)

	// Fails if deleting key that doesn't exist
	delete := "set @@vitess_metadata.doesn't_exist=''"
	_, err = executor.Execute(context.Background(), "TestExecute", session, delete, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))

	// Delete existing key, show should fail given the node doesn't exist
	delete = "set @@vitess_metadata.app_v1=''"
	_, err = executor.Execute(context.Background(), "TestExecute", session, delete, nil)
	assert.NoError(t, err)

	show = `show vitess_metadata variables like 'app\\_%'`
	_, err = executor.Execute(context.Background(), "TestExecute", session, show, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))
}

func TestPlanExecutorAutocommit(t *testing.T) {
	t.Skip("not support yet")
	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Get()
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{TargetString: "@master", InTransaction: true, FoundRows: 1}
	testSession := *session.Session
	testSession.ShardSessions = nil
	if !proto.Equal(&testSession, wantSession) {
		t.Errorf("autocommit=0: %v, want %v", testSession, wantSession)
	}

	logStats := testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	// autocommit = 1
	_, err = executor.Execute(context.Background(), "TestExecute", session, "set autocommit=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "SET", "set autocommit=1", 0)

	// Setting autocommit=1 commits existing transaction.
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	// In the following section, we look at AsTransaction count instead of CommitCount because
	// the update results in a single round-trip ExecuteBatch call.
	startCount = sbclookup.AsTransactionCount.Get()
	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master", FoundRows: 1}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.AsTransactionCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	logStats = testQueryLog(t, logChan, "TestExecute", "UPDATE", "update main1 set id=1", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	// autocommit = 1, "begin"
	session.Reset()
	startCount = sbclookup.CommitCount.Get()
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@master", FoundRows: 1}
	testSession = *session.Session
	testSession.ShardSessions = nil
	if !proto.Equal(&testSession, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", &testSession, wantSession)
	}
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

	_, err = executor.Execute(context.Background(), "TestExecute", session, "commit", nil)
	if err != nil {
		t.Fatal(err)
	}
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
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "set autocommit=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
}

func TestPlanExecutorClearsWarnings(t *testing.T) {
	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{Code: 234, Message: "oh noes"}},
	})
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select 42", nil)
	require.NoError(t, err)
	require.Empty(t, session.Warnings)
}

func TestPlanExecutorShow(t *testing.T) {
	t.Skip("not support yet")
	executor, _, _, sbclookup := createExecutorEnvUsing(planAllTheThings)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	for _, query := range []string{"show databases", "show vitess_keyspaces", "show keyspaces", "show DATABASES"} {
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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
	_, err := executor.Execute(context.Background(), "TestExecute", session, "show variables", nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show collation", nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'", nil)
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show tables", nil)
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
	qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show create table unknown_table", nil)
	if err.Error() != wantErrNoTable {
		t.Errorf("Got: %v. Want: %v", err, wantErrNoTable)
	}

	// SHOW CREATE table using vschema to find keyspace.
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show create table user_seq", nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	lastQuery := sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery := "show create table user_seq"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW CREATE table with query-provided keyspace
	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show create table %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show create table unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW KEYS with two different syntax
	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show keys from %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show keys from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show keys from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show keys from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// SHOW INDEX with two different syntax
	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show index from %v.unknown", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show index from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show index from unknown from %v", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show index from unknown"
	if lastQuery != wantQuery {
		t.Errorf("Got: %v. Want: %v", lastQuery, wantQuery)
	}

	// Set desitation keyspace in session
	session.TargetString = KsTestUnsharded
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show create table unknown", nil)
	require.NoError(t, err)
	// Reset target string so other tests dont fail.
	session.TargetString = "@master"
	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show full columns from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	for _, query := range []string{"show charset", "show character set"} {
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
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

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show engines", nil)
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show plugins", nil)
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
		qr, err = executor.Execute(context.Background(), "TestExecute", session, "show session status", nil)
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vitess_shards", nil)
	require.NoError(t, err)

	// Test SHOW FULL COLUMNS FROM where query has a qualifier
	_, err = executor.Execute(context.Background(), "TestExecute", session, fmt.Sprintf("show full columns from %v.table1", KsTestUnsharded), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

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

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vitess_tablets", nil)
	require.NoError(t, err)
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("FakeCell", "TestExecutor", "-20", "MASTER", "SERVING", "aa-0000000000", "-20"),
			buildVarCharRow("FakeCell", "TestUnsharded", "0", "MASTER", "SERVING", "aa-0000000000", "0"),
		},
		RowsAffected: 9,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vitess_tablets:\n%+v, want\n%+v", qr, wantqr)
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes", nil)
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

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.user", nil)
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

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on user", nil)
	wantErr := errNoKeyspace.Error()
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.garbage", nil)
	wantErr = "table `garbage` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	session.TargetString = "TestExecutor"
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on user", nil)
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on user2", nil)
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

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on garbage", nil)
	wantErr = "table `garbage` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Errorf("show vschema vindexes on user: %v, want %v", err, wantErr)
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Type", Type: sqltypes.Uint16},
			{Name: "Message", Type: sqltypes.VarChar},
		},
		Rows:         [][]sqltypes.Value{},
		RowsAffected: 0,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show warnings:\n%+v, want\n%+v", qr, wantqr)

	}

	session.Warnings = []*querypb.QueryWarning{}
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Type", Type: sqltypes.Uint16},
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show warnings", nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Type", Type: sqltypes.Uint16},
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vitess_shards", nil)
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema tables", nil)
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
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema tables", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show 10", nil)
	want = "syntax error at position 8 near '10'"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}

	session = NewSafeSession(&vtgatepb.Session{TargetString: "no_such_keyspace"})
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema tables", nil)
	want = "keyspace no_such_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}
}

func TestPlanExecutorUse(t *testing.T) {
	//t.Skip("not support yet")
	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
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
		_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
		if err != nil {
			t.Error(err)
		}
		wantSession := &vtgatepb.Session{Autocommit: true, TargetString: want[i]}
		if !proto.Equal(session.Session, wantSession) {
			t.Errorf("%s: %v, want %v", stmt, session.Session, wantSession)
		}
	}

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use 1", nil)
	wantErr := "syntax error at position 6 near '1'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use UnexistentKeyspace", nil)
	wantErr = "invalid keyspace provided: UnexistentKeyspace"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}
}

func TestPlanExecutorComment(t *testing.T) {
	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)

	var tcs = []struct {
		sql string
		qr  *sqltypes.Result
	}{
		{
			sql: "/*! select * from t where id = 1 */",
			qr: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|value",
					"int32|varchar",
				),
				"1|foo",
			),
		},
		{
			sql: "/*! insert into t(id, value) values(1000, 'msg') */",
			qr: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|value",
					"int32|varchar",
				),
				"1|foo",
			),
		},
		{
			sql: "/*!50708 set @x = 42 */",
			qr:  &sqltypes.Result{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			gotResult, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), tc.sql, nil)
			require.NoError(t, err)
			utils.MustMatch(t, tc.qr, gotResult, "did not get expected result")
		})
	}
}

func TestPlanExecutorOtherRead(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnvUsing(planAllTheThings)

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

func TestPlanExecutorExplain(t *testing.T) {
	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "explain format = vitess select * from user"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	resultText := fmt.Sprintf("%v", result.Rows)
	utils.MustMatch(t, `[[VARCHAR("Route") VARCHAR("SelectScatter") VARCHAR("TestExecutor") VARCHAR("") VARCHAR("UNKNOWN") VARCHAR("select * from user")]]`, resultText, "")

}

func TestPlanExecutorOtherAdmin(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnvUsing(planAllTheThings)

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

func TestPlanExecutorUnrecognized(t *testing.T) {
	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "invalid statement", nil)
	require.Error(t, err, "unrecognized statement: invalid statement'")
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestPlanVSchemaStats(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)

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

func TestPlanGetPlanUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	emptyvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)
	unshardedvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)

	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
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
	logStats3 := NewLogStats(context.Background(), "Test", "", nil)
	plan3, err := r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats3)
	require.NoError(t, err)
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}
	logStats4 := NewLogStats(context.Background(), "Test", "", nil)
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

func TestPlanGetPlanCacheUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	emptyvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	require.NoError(t, err)
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
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
	r, _, _, _ = createExecutorEnvUsing(planAllTheThings)
	unshardedvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(context.Background(), "Test", "", nil)
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
}

func TestPlanGetPlanCacheNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	r.normalize = true
	emptyvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	require.NoError(t, err)
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := "select * from music_user_map where id = :vtg1 /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	require.NoError(t, err)
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createExecutorEnvUsing(planAllTheThings)
	r.normalize = true
	unshardedvc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.resolver.resolver)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(context.Background(), "Test", "", nil)
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
}

func TestPlanGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
	r.normalize = true
	emptyvc, _ := createVcursorImpl(r, "@unknown")
	unshardedvc, _ := createVcursorImpl(r, KsTestUnsharded+"@unknown")

	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment 1 */"), map[string]*querypb.BindVariable{}, false, logStats1)
	require.NoError(t, err)
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
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

	logStats3 := NewLogStats(context.Background(), "Test", "", nil)
	plan3, err := r.getPlan(emptyvc, query2, makeComments(" /* comment 3 */"), map[string]*querypb.BindVariable{}, false, logStats3)
	require.NoError(t, err)
	if plan1 != plan3 {
		t.Errorf("getPlan(query2): plans must be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 3 */"
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}

	logStats4 := NewLogStats(context.Background(), "Test", "", nil)
	plan4, err := r.getPlan(emptyvc, normalized, makeComments(" /* comment 4 */"), map[string]*querypb.BindVariable{}, false, logStats4)
	require.NoError(t, err)
	if plan1 != plan4 {
		t.Errorf("getPlan(normalized): plans must be equal: %p %p", plan1, plan4)
	}
	wantSQL = normalized + " /* comment 4 */"
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}

	logStats5 := NewLogStats(context.Background(), "Test", "", nil)
	plan3, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment 5 */"), map[string]*querypb.BindVariable{}, false, logStats5)
	require.NoError(t, err)
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 5 */"
	if logStats5.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats5.SQL)
	}

	logStats6 := NewLogStats(context.Background(), "Test", "", nil)
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
	logStats7 := NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(emptyvc, "syntax", makeComments(""), map[string]*querypb.BindVariable{}, false, logStats7)
	wantErr := "syntax error at position 7 near 'syntax'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func createVcursorImpl(r *Executor, targetString string) (*vcursorImpl, error) {
	return newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: targetString}), makeComments(""), r, nil, &fakeVSchemaOperator{vschema: r.VSchema()}, r.resolver.resolver)
}

func TestPlanParseEmptyTargetSingleKeyspace(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
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

func TestPlanParseEmptyTargetMultiKeyspace(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
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

func TestPlanParseTargetSingleKeyspace(t *testing.T) {
	r, _, _, _ := createExecutorEnvUsing(planAllTheThings)
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

func TestPlanDebugVSchema(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/debug/vschema", nil)

	executor, _, _, _ := createExecutorEnvUsing(planAllTheThings)
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

func TestPlanGenerateCharsetRows(t *testing.T) {
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

func makeComments(text string) sqlparser.MarginComments {
	return sqlparser.MarginComments{Trailing: text}
}
