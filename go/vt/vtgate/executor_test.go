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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/safehtml/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestExecutorResultsExceeded(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	save := warnMemoryRows
	warnMemoryRows = 3
	defer func() { warnMemoryRows = save }()

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	initial := warnings.Counts()["ResultsExceeded"]

	result1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1")
	result2 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	sbclookup.SetResults([]*sqltypes.Result{result1, result2})

	_, err := executor.Execute(ctx, nil, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial, warnings.Counts()["ResultsExceeded"], "warnings count")

	_, err = executor.Execute(ctx, nil, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial+1, warnings.Counts()["ResultsExceeded"], "warnings count")
}

func TestExecutorMaxMemoryRowsExceeded(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	save := maxMemoryRows
	maxMemoryRows = 3
	defer func() { maxMemoryRows = save }()

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
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
		stmt, err := sqlparser.NewTestParser().Parse(test.query)
		require.NoError(t, err)

		_, err = executor.Execute(ctx, nil, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil)
		if sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt) {
			require.NoError(t, err, "no error when DirectiveIgnoreMaxMemoryRows is provided")
		} else {
			assert.EqualError(t, err, test.err, "maxMemoryRows limit exceeded")
		}

		sbclookup.SetResults([]*sqltypes.Result{result})
		err = executor.StreamExecute(ctx, nil, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil, fn)
		require.NoError(t, err, "maxMemoryRows limit does not apply to StreamExecute")
	}
}

func TestExecutorTransactionsNoAutoCommit(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@primary", SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 0, sbclookup.CommitCount.Load(), "commit count")
	logStats := testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	_, err = executor.Execute(context.Background(), nil, "TestExecute", session, "commit", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Load(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "COMMIT", "commit", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.RollbackCount.Load(), "rollback count")
	_ = testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// CloseSession doesn't log anything
	err = executor.CloseSession(ctx, session)
	require.NoError(t, err)
	logStats = getQueryLog(logChan)
	if logStats != nil {
		t.Errorf("logstats: expected no record for no-op rollback, got %v", logStats)
	}

	// Prevent use of non-primary if in_transaction is on.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@primary", InTransaction: true})
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "use @replica", nil)
	require.EqualError(t, err, `can't execute the given command because you have an active transaction`)
}

func TestDirectTargetRewrites(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	executor.normalize = true

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@primary",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "select database()"

	_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql:           "select :__vtdbname as `database()` from dual",
		BindVariables: map[string]*querypb.BindVariable{"__vtdbname": sqltypes.StringBindVariable("TestUnsharded/0@primary")},
	}})
}

func TestExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if commitCount := sbclookup.CommitCount.Load(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.CommitCount.Load())

	logStats = testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "COMMIT", "commit", 1)
	assert.NotEqual(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if rollbackCount := sbclookup.RollbackCount.Load(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	_ = testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
}

func TestExecutorTransactionsAutoCommitStreaming(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	oltpOptions := &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLTP}
	session := NewSafeSession(&vtgatepb.Session{
		TargetString: "@primary",
		Autocommit:   true,
		Options:      oltpOptions,
		SessionUUID:  "suuid",
	})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	var results []*sqltypes.Result

	// begin.
	err := executor.StreamExecute(ctx, nil, "TestExecute", session, "begin", nil, func(result *sqltypes.Result) error {
		results = append(results, result)
		return nil
	})

	require.EqualValues(t, 1, len(results), "should get empty result from begin")
	assert.Empty(t, results[0].Rows, "should get empty result from begin")

	require.NoError(t, err)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		TargetString:  "@primary",
		Autocommit:    true,
		Options:       oltpOptions,
		SessionUUID:   "suuid",
	}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.Zero(t, sbclookup.CommitCount.Load())
	logStats := testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, Options: oltpOptions, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.CommitCount.Load())

	logStats = testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
	logStats = testQueryLog(t, executor, logChan, "TestExecute", "COMMIT", "commit", 1)
	assert.NotEqual(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, Options: oltpOptions, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.RollbackCount.Load())
}

func TestExecutorDeleteMetadata(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _, ctx := createExecutorEnv(t)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true})

	set := "set @@vitess_metadata.app_v1= '1'"
	_, err := executor.Execute(ctx, nil, "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show := `show vitess_metadata variables like 'app\\_%'`
	result, _ := executor.Execute(ctx, nil, "TestExecute", session, show, nil)
	assert.Len(t, result.Rows, 1)

	// Fails if deleting key that doesn't exist
	delQuery := "set @@vitess_metadata.doesn't_exist=''"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, delQuery, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))

	// Delete existing key, show should fail given the node doesn't exist
	delQuery = "set @@vitess_metadata.app_v1=''"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, delQuery, nil)
	assert.NoError(t, err)

	show = `show vitess_metadata variables like 'app\\_%'`
	_, err = executor.Execute(ctx, nil, "TestExecute", session, show, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Load()
	_, err := executor.Execute(ctx, nil, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{TargetString: "@primary", InTransaction: true, FoundRows: 1, RowCount: -1}
	testSession := session.Session.CloneVT()
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, testSession, "session does not match for autocommit=0")

	logStats := testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsReturned == 0 {
		t.Errorf("logstats: expected non-zero RowsReturned")
	}

	// autocommit = 1
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, executor, logChan, "TestExecute", "SET", "set @@autocommit = 1", 0)

	// Setting autocommit=1 commits existing transaction.
	if got, want := sbclookup.CommitCount.Load(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	_, err = executor.Execute(ctx, nil, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary", FoundRows: 0, RowCount: 1}
	utils.MustMatch(t, wantSession, session.Session, "session does not match for autocommit=1")

	logStats = testQueryLog(t, executor, logChan, "TestExecute", "UPDATE", "update main1 set id = 1", 1)
	assert.NotZero(t, logStats.CommitTime, "logstats: expected non-zero CommitTime")
	assert.NotEqual(t, uint64(0), logStats.RowsAffected, "logstats: expected non-zero RowsAffected")

	// autocommit = 1, "begin"
	session.ResetTx()
	startCount = sbclookup.CommitCount.Load()
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@primary", FoundRows: 0, RowCount: 1}
	testSession = session.Session.CloneVT()
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, testSession, "session does not match for autocommit=1")
	if got, want := sbclookup.CommitCount.Load(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	logStats = testQueryLog(t, executor, logChan, "TestExecute", "UPDATE", "update main1 set id = 1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	_, err = executor.Execute(ctx, nil, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Load(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_ = testQueryLog(t, executor, logChan, "TestExecute", "COMMIT", "commit", 1)

	// transition autocommit from 0 to 1 in the middle of a transaction.
	startCount = sbclookup.CommitCount.Load()
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	if got, want := sbclookup.CommitCount.Load(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Load(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
}

func TestExecutorShowColumns(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: ""})

	queries := []string{
		"SHOW COLUMNS FROM `user` in `TestExecutor`",
		"show columns from `user` in `TestExecutor`",
		"ShOw CoLuMnS fRoM `user` iN `TestExecutor`",
		"SHOW columns FROM `user` in `TestExecutor`",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
			require.NoError(t, err)

			wantQueries := []*querypb.BoundQuery{{
				Sql:           "show columns from `user`",
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

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}

func assertMatchesNoOrder(t *testing.T, expected, got string) {
	t.Helper()
	if sortString(expected) != sortString(got) {
		t.Errorf("for query: expected \n%s \nbut actual \n%s", expected, got)
	}
}

func TestExecutorShow(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})

	for _, query := range []string{"show vitess_keyspaces", "show keyspaces"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		assertMatchesNoOrder(t, `[[VARCHAR("TestUnsharded")] [VARCHAR("TestMultiCol")] [VARCHAR("TestXBadVSchema")] [VARCHAR("TestXBadSharding")] [VARCHAR("TestExecutor")]]`, fmt.Sprintf("%v", qr.Rows))
	}

	for _, query := range []string{"show databases", "show DATABASES", "show schemas", "show SCHEMAS"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		// Showing default tables (5+4[default])
		assertMatchesNoOrder(t, `[[VARCHAR("TestUnsharded")] [VARCHAR("TestMultiCol")] [VARCHAR("TestXBadVSchema")] [VARCHAR("TestXBadSharding")] [VARCHAR("TestExecutor")]] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]`, fmt.Sprintf("%v", qr.Rows))
	}

	_, err := executor.Execute(ctx, nil, "TestExecute", session, "show variables", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show collation", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, "use @primary", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show tables", nil)
	assert.EqualError(t, err, errNoKeyspace.Error(), "'show tables' should fail without a keyspace")
	assert.Empty(t, sbclookup.Queries, "sbclookup unexpectedly has queries already")

	showResults := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Tables_in_keyspace", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("some_table"),
		}},
	}
	sbclookup.SetResults([]*sqltypes.Result{showResults})

	query := fmt.Sprintf("show tables from %v", KsTestUnsharded)
	qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, len(sbclookup.Queries), "Tablet should have received one 'show' query. Instead received: %v", sbclookup.Queries)
	lastQuery := sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	want := "show tables"
	assert.Equal(t, want, lastQuery, "Got: %v, want %v", lastQuery, want)

	wantqr := showResults
	utils.MustMatch(t, wantqr, qr, fmt.Sprintf("unexpected results running query: %s", query))

	wantErrNoTable := "table unknown_table not found"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show create table unknown_table", nil)
	assert.EqualErrorf(t, err, wantErrNoTable, "Got: %v. Want: %v", wantErrNoTable)

	// SHOW CREATE table using vschema to find keyspace.
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show create table user_seq", nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery := "show create table user_seq"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW CREATE table with query-provided keyspace
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show create table %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show create table unknown"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW KEYS with two different syntax
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show keys from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show indexes from unknown"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show keys from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW INDEX with two different syntax
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show index from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show index from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW INDEXES with two different syntax
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show indexes from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show indexes from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW EXTENDED {INDEX | INDEXES | KEYS}
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show extended index from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show extended indexes from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show extended keys from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// Set destination keyspace in session
	session.TargetString = KsTestUnsharded
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show create table unknown", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, nil, "TestExecute", session, "show full columns from table1", nil)
	require.NoError(t, err)

	// Reset target string so other tests dont fail.
	session.TargetString = "@primary"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show full columns from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)

	for _, query := range []string{"show charset like 'utf8%'", "show character set like 'utf8%'"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Uint32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8mb3",
					"UTF-8 Unicode",
					"utf8mb3_general_ci"),
					sqltypes.NewUint32(3)),
				append(buildVarCharRow(
					"utf8mb4",
					"UTF-8 Unicode",
					collations.MySQL8().LookupName(collations.MySQL8().DefaultConnectionCharset())),
					sqltypes.NewUint32(4)),
			},
		}

		utils.MustMatch(t, wantqr, qr, query)
	}

	for _, query := range []string{"show charset like '%foo'", "show character set like 'foo%'", "show charset like 'foo%'", "show character set where charset like '%foo'", "show charset where charset = '%foo'"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields:       append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Uint32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)}),
			RowsAffected: 0,
		}

		utils.MustMatch(t, wantqr, qr, query)
	}

	for _, query := range []string{"show charset like 'utf8mb3'", "show character set like 'utf8mb3'", "show charset where charset = 'utf8mb3'", "show character set where charset = 'utf8mb3'"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Uint32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8mb3",
					"UTF-8 Unicode",
					"utf8mb3_general_ci"),
					sqltypes.NewUint32(3)),
			},
		}

		utils.MustMatch(t, wantqr, qr, query)
	}

	for _, query := range []string{"show charset like 'utf8mb4'", "show character set like 'utf8mb4'", "show charset where charset = 'utf8mb4'", "show character set where charset = 'utf8mb4'"} {
		qr, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.NoError(t, err)
		wantqr := &sqltypes.Result{
			Fields: append(buildVarCharFields("Charset", "Description", "Default collation"), &querypb.Field{Name: "Maxlen", Type: sqltypes.Uint32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)}),
			Rows: [][]sqltypes.Value{
				append(buildVarCharRow(
					"utf8mb4",
					"UTF-8 Unicode",
					collations.MySQL8().LookupName(collations.MySQL8().DefaultConnectionCharset())),
					sqltypes.NewUint32(4)),
			},
		}
		utils.MustMatch(t, wantqr, qr, query)
	}

	for _, query := range []string{"show character set where foo like '%foo'"} {
		_, err := executor.Execute(ctx, nil, "TestExecute", session, query, nil)
		require.Error(t, err)
	}

	query = "show engines"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
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
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show plugins"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
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
	}
	utils.MustMatch(t, wantqr, qr, query)

	for _, sql := range []string{"show session status", "show session status like 'Ssl_cipher'"} {
		qr, err = executor.Execute(ctx, nil, "TestExecute", session, sql, nil)
		require.NoError(t, err)
		wantqr = &sqltypes.Result{
			Fields: []*querypb.Field{
				{Name: "id", Type: sqltypes.Int32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
				{Name: "value", Type: sqltypes.VarChar, Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1), sqltypes.NewVarChar("foo")},
			},
		}

		utils.MustMatch(t, wantqr, qr, sql)
	}

	// Test SHOW FULL COLUMNS FROM where query has a qualifier
	_, err = executor.Execute(ctx, nil, "TestExecute", session, fmt.Sprintf("show full columns from %v.table1", KsTestUnsharded), nil)
	require.NoError(t, err)

	query = "show vitess_shards"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)

	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor/-20"),
			buildVarCharRow("TestXBadVSchema/e0-"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vitess_shards like 'TestExecutor/%'"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)

	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor/-20"),
			buildVarCharRow("TestExecutor/e0-"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vitess_shards like 'TestExec%/%'"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)

	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor/-20"),
			buildVarCharRow("TestExecutor/e0-"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vitess_replication_status"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	qr.Rows = [][]sqltypes.Value{}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Shard", "TabletType", "Alias", "Hostname", "ReplicationSource", "ReplicationHealth", "ReplicationLag", "ThrottlerStatus"),
		Rows:   [][]sqltypes.Value{},
	}
	utils.MustMatch(t, wantqr, qr, query)
	query = "show vitess_replication_status like 'x'"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	qr.Rows = [][]sqltypes.Value{}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Shard", "TabletType", "Alias", "Hostname", "ReplicationSource", "ReplicationHealth", "ReplicationLag", "ThrottlerStatus"),
		Rows:   [][]sqltypes.Value{},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vitess_tablets"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("aa", "TestExecutor", "-20", "PRIMARY", "SERVING", "aa-0000000001", "-20", "1970-01-01T00:00:01Z"),
			buildVarCharRow("aa", "TestUnsharded", "0", "REPLICA", "SERVING", "aa-0000000010", "2", "1970-01-01T00:00:01Z"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vitess_tablets like 'x'"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows:   [][]sqltypes.Value{},
	}
	utils.MustMatch(t, wantqr, qr, fmt.Sprintf("%q should be empty", query))

	query = "show vitess_tablets like '-20%'"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("aa", "TestExecutor", "-20", "PRIMARY", "SERVING", "aa-0000000001", "-20", "1970-01-01T00:00:01Z"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema vindexes"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor", "cfc", "cfc", "", ""),
			buildVarCharRow("TestExecutor", "hash_index", "hash", "", ""),
			buildVarCharRow("TestExecutor", "idx1", "hash", "", ""),
			buildVarCharRow("TestExecutor", "idx_noauto", "hash", "", "noauto_table"),
			buildVarCharRow("TestExecutor", "insert_ignore_idx", "lookup_hash", "from=fromcol; table=ins_lookup; to=tocol", "insert_ignore_test"),
			buildVarCharRow("TestExecutor", "keyspace_id", "numeric", "", ""),
			buildVarCharRow("TestExecutor", "krcol_unique_vdx", "keyrange_lookuper_unique", "", ""),
			buildVarCharRow("TestExecutor", "krcol_vdx", "keyrange_lookuper", "", ""),
			buildVarCharRow("TestExecutor", "multicol_vdx", "multicol", "column_count=2; column_vindex=xxhash,binary", ""),
			buildVarCharRow("TestExecutor", "music_user_map", "lookup_hash_unique", "from=music_id; table=music_user_map; to=user_id", "music"),
			buildVarCharRow("TestExecutor", "name_lastname_keyspace_id_map", "lookup", "from=name,lastname; table=name_lastname_keyspace_id_map; to=keyspace_id", "user2"),
			buildVarCharRow("TestExecutor", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
			buildVarCharRow("TestExecutor", "regional_vdx", "region_experimental", "region_bytes=1", ""),
			buildVarCharRow("TestExecutor", "t1_lkp_vdx", "consistent_lookup_unique", "from=unq_col; table=t1_lkp_idx; to=keyspace_id", "t1"),
			buildVarCharRow("TestExecutor", "t2_erl_lu_vdx", "lookup_unique", "from=erl_lu_col; read_lock=exclusive; table=TestUnsharded.erl_lu_idx; to=keyspace_id", "t2_lookup"),
			buildVarCharRow("TestExecutor", "t2_lu_vdx", "lookup_hash_unique", "from=lu_col; table=TestUnsharded.lu_idx; to=keyspace_id", "t2_lookup"),
			buildVarCharRow("TestExecutor", "t2_nrl_lu_vdx", "lookup_unique", "from=nrl_lu_col; read_lock=none; table=TestUnsharded.nrl_lu_idx; to=keyspace_id", "t2_lookup"),
			buildVarCharRow("TestExecutor", "t2_nv_lu_vdx", "lookup_unique", "from=nv_lu_col; no_verify=true; table=TestUnsharded.nv_lu_idx; to=keyspace_id", "t2_lookup"),
			buildVarCharRow("TestExecutor", "t2_srl_lu_vdx", "lookup_unique", "from=srl_lu_col; read_lock=shared; table=TestUnsharded.srl_lu_idx; to=keyspace_id", "t2_lookup"),
			buildVarCharRow("TestExecutor", "t2_wo_lu_vdx", "lookup_unique", "from=wo_lu_col; table=TestUnsharded.wo_lu_idx; to=keyspace_id; write_only=true", "t2_lookup"),
			buildVarCharRow("TestMultiCol", "multicol_vdx", "multicol", "column_bytes=1,3,4; column_count=3; column_vindex=hash,binary,unicode_loose_xxhash", ""),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema vindexes on TestExecutor.user"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("Id", "hash_index", "hash", "", ""),
			buildVarCharRow("name", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema vindexes on user"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	wantErr := errNoKeyspace.Error()
	assert.EqualError(t, err, wantErr, query)

	query = "show vschema vindexes on TestExecutor.garbage"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	wantErr = "VT05005: table 'garbage' does not exist in keyspace 'TestExecutor'"
	assert.EqualError(t, err, wantErr, query)

	query = "show vschema vindexes on user"
	session.TargetString = "TestExecutor"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("Id", "hash_index", "hash", "", ""),
			buildVarCharRow("name", "name_user_map", "lookup_hash", "from=name; table=name_user_map; to=user_id", "user"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema vindexes on user2"
	session.TargetString = "TestExecutor"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "hash_index", "hash", "", ""),
			buildVarCharRow("name, lastname", "name_lastname_keyspace_id_map", "lookup", "from=name,lastname; table=name_lastname_keyspace_id_map; to=keyspace_id", "user2"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema vindexes on garbage"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	wantErr = "VT05005: table 'garbage' does not exist in keyspace 'TestExecutor'"
	assert.EqualError(t, err, wantErr, query)

	query = "show warnings"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
			{Name: "Code", Type: sqltypes.Uint16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Message", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
		},
		Rows: [][]sqltypes.Value{},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show warnings"
	session.Warnings = []*querypb.QueryWarning{}
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
			{Name: "Code", Type: sqltypes.Uint16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Message", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
		},
		Rows: [][]sqltypes.Value{},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show warnings"
	session.Warnings = []*querypb.QueryWarning{
		{Code: uint32(sqlerror.ERBadTable), Message: "bad table"},
		{Code: uint32(sqlerror.EROutOfResources), Message: "ks/-40: query timed out"},
	}
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
			{Name: "Code", Type: sqltypes.Uint16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Message", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
		},

		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("Warning"), sqltypes.NewUint32(uint32(sqlerror.ERBadTable)), sqltypes.NewVarChar("bad table")},
			{sqltypes.NewVarChar("Warning"), sqltypes.NewUint32(uint32(sqlerror.EROutOfResources)), sqltypes.NewVarChar("ks/-40: query timed out")},
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	// Make sure it still works when one of the keyspaces is in a bad state
	getSandbox(KsTestSharded).SrvKeyspaceMustFail++
	query = "show vitess_shards"
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestMultiCol/-20"),
			buildVarCharRow("TestXBadVSchema/e0-"),
		},
	}
	utils.MustMatch(t, wantqr, qr, fmt.Sprintf("%s, with a bad keyspace", query))

	query = "show vschema tables"
	session = NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	qr, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	require.NoError(t, err)
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Tables"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("erl_lu_idx"),
			buildVarCharRow("ins_lookup"),
			buildVarCharRow("lu_idx"),
			buildVarCharRow("main1"),
			buildVarCharRow("music_user_map"),
			buildVarCharRow("name_lastname_keyspace_id_map"),
			buildVarCharRow("name_user_map"),
			buildVarCharRow("nrl_lu_idx"),
			buildVarCharRow("nv_lu_idx"),
			buildVarCharRow("simple"),
			buildVarCharRow("srl_lu_idx"),
			buildVarCharRow("user_msgs"),
			buildVarCharRow("user_seq"),
			buildVarCharRow("wo_lu_idx"),
			buildVarCharRow("zip_detail"),
		},
	}
	utils.MustMatch(t, wantqr, qr, query)

	query = "show vschema tables"
	session = NewSafeSession(&vtgatepb.Session{})
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	want = errNoKeyspace.Error()
	assert.EqualError(t, err, want, query)

	query = "show 10"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	want = "syntax error at position 8 near '10'"
	assert.EqualError(t, err, want, query)

	query = "show vschema tables"
	session = NewSafeSession(&vtgatepb.Session{TargetString: "no_such_keyspace"})
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	want = "VT05003: unknown database 'no_such_keyspace' in vschema"
	assert.EqualError(t, err, want, query)

	query = "show vitess_migrations"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	want = "VT05003: unknown database 'no_such_keyspace' in vschema"
	assert.EqualError(t, err, want, query)

	query = "show vitess_migrations from ks like '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90'"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, query, nil)
	want = "VT05003: unknown database 'ks' in vschema"
	assert.EqualError(t, err, want, query)
}

func TestExecutorShowTargeted(t *testing.T) {
	executor, _, sbc2, _, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor/40-60"})

	queries := []string{
		"show databases",
		"show variables like 'read_only'",
		"show collation",
		"show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
		"show tables",
		fmt.Sprintf("show tables from %v", KsTestUnsharded),
		"show create table user_seq",
		"show full columns from table1",
		"show plugins",
		"show warnings",
	}

	for _, sql := range queries {
		_, err := executor.Execute(ctx, nil, "TestExecutorShowTargeted", session, sql, nil)
		require.NoError(t, err)
		assert.NotZero(t, len(sbc2.Queries), "Tablet should have received 'show' query")
		lastQuery := sbc2.Queries[len(sbc2.Queries)-1].Sql
		assert.Equal(t, sql, lastQuery, "Got: %v, want %v", lastQuery, sql)
	}
}

func TestExecutorUse(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@primary"})

	stmts := []string{
		"use TestExecutor",
		"use `TestExecutor:-80@primary`",
	}
	want := []string{
		"TestExecutor",
		"TestExecutor:-80@primary",
	}
	for i, stmt := range stmts {
		_, err := executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
		if err != nil {
			t.Error(err)
		}
		wantSession := &vtgatepb.Session{Autocommit: true, TargetString: want[i], RowCount: -1}
		utils.MustMatch(t, wantSession, session.Session, "session does not match")
	}

	_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use 1", nil)
	wantErr := "syntax error at position 6 near '1'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use UnexistentKeyspace", nil)
	require.EqualError(t, err, "VT05003: unknown database 'UnexistentKeyspace' in vschema")
}

func TestExecutorComment(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	stmts := []string{
		"/*! SET autocommit=1*/",
		"/*!50708 SET @x=5000*/",
	}
	wantResult := &sqltypes.Result{}

	for _, stmt := range stmts {
		gotResult, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		if !gotResult.Equal(wantResult) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotResult, wantResult)
		}
	}
}

func TestExecutorDDL(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

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
		"create table t2(id bigint primary key)",
		"alter table t2 add primary key (id)",
		"rename table t2 to t3",
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
			sbc1.ExecCount.Store(0)
			sbc2.ExecCount.Store(0)
			sbclookup.ExecCount.Store(0)
			stmtType := "DDL"
			_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				require.EqualError(t, err, errNoKeyspace.Error(), "expect query to fail: %q", stmt)
				stmtType = "" // For error case, plan is not generated to query log will not contain any stmtType.
			} else {
				require.NoError(t, err, "did not expect error for query: %q", stmt)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Load(),
				Sbc2Cnt:      sbc2.ExecCount.Load(),
				SbcLookupCnt: sbclookup.ExecCount.Load(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}

			testQueryLog(t, executor, logChan, "TestExecute", stmtType, stmt, tc.shardQueryCnt)
		}
	}

	stmts2 := []struct {
		input  string
		hasErr bool
	}{
		{input: "create table t1(id bigint primary key)", hasErr: false},
		{input: "drop table t1", hasErr: false},
		{input: "drop table t2", hasErr: true},
		{input: "drop view t1", hasErr: false},
		{input: "drop view t2", hasErr: true},
		{input: "alter view t1 as select * from t1", hasErr: false},
		{input: "alter view t2 as select * from t1", hasErr: true},
	}

	for _, stmt := range stmts2 {
		sbc1.ExecCount.Store(0)
		sbc2.ExecCount.Store(0)
		sbclookup.ExecCount.Store(0)
		_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: ""}), stmt.input, nil)
		if stmt.hasErr {
			require.EqualError(t, err, errNoKeyspace.Error(), "expect query to fail")
			testQueryLog(t, executor, logChan, "TestExecute", "", stmt.input, 0)
		} else {
			require.NoError(t, err)
			testQueryLog(t, executor, logChan, "TestExecute", "DDL", stmt.input, 8)
		}
	}
}

func TestExecutorDDLFk(t *testing.T) {
	mName := "TestExecutorDDLFk"
	stmts := []string{
		"create table t1(id bigint primary key, foreign key (id) references t2(id))",
		"alter table t2 add foreign key (id) references t1(id) on delete cascade",
	}

	for _, stmt := range stmts {
		for _, fkMode := range []string{"allow", "disallow"} {
			t.Run(stmt+fkMode, func(t *testing.T) {
				executor, _, _, sbc, ctx := createExecutorEnv(t)
				sbc.ExecCount.Store(0)
				foreignKeyMode = fkMode
				_, err := executor.Execute(ctx, nil, mName, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
				if fkMode == "allow" {
					require.NoError(t, err)
					require.EqualValues(t, 1, sbc.ExecCount.Load())
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "foreign key constraints are not allowed")
				}
			})
		}
	}
}

func TestExecutorAlterVSchemaKeyspace(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _, ctx := createExecutorEnv(t)
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true})

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 2)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) bool {
		vschemaUpdates <- vschema
		return true
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces["TestExecutor"].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	stmt := "alter vschema create vindex TestExecutor.test_vindex using hash"
	_, err := executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, "TestExecutor", "test_vindex", vschemaUpdates, executor)
	assert.Equal(t, vindex.Type, "hash")
}

func TestExecutorCreateVindexDDL(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)
	ks := "TestExecutor"

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) bool {
		vschemaUpdates <- vschema
		return true
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema create vindex test_vindex using hash"
	_, err := executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, ks, "test_vindex", vschemaUpdates, executor)
	if vindex == nil || vindex.Type != "hash" {
		t.Errorf("updated vschema did not contain test_vindex")
	}

	_, err = executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
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
	// ksNew := "test_new_keyspace"
	session = NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt = "alter vschema create vindex test_vindex2 using hash"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
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
		sbc1.ExecCount.Load(),
		sbc2.ExecCount.Load(),
		sbclookup.ExecCount.Load(),
	}
	require.Equal(t, wantCount, gotCount)
}

func TestExecutorAddDropVschemaTableDDL(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)
	ks := KsTestUnsharded

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(ctx, "aa", func(vschema *vschemapb.SrvVSchema, err error) bool {
		vschemaUpdates <- vschema
		return true
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
	_, err := executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append([]string{"test_table"}, vschemaTables...), executor)

	stmt = "alter vschema add table test_table2"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append([]string{"test_table", "test_table2"}, vschemaTables...), executor)

	// Should fail adding a table on a sharded keyspace
	session = NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	stmt = "alter vschema add table test_table"
	_, err = executor.Execute(ctx, nil, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, "add vschema table: unsupported on sharded keyspace TestExecutor")

	// No queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Load(),
		sbc2.ExecCount.Load(),
		sbclookup.ExecCount.Load(),
	}
	utils.MustMatch(t, wantCount, gotCount, "")
}

func TestExecutorVindexDDLACL(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})

	ctxRedUser := callerid.NewContext(ctx, &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "redUser"})
	ctxBlueUser := callerid.NewContext(ctx, &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "blueUser"})

	// test that by default no users can perform the operation
	stmt := "alter vschema create vindex test_hash using hash"
	_, err := executor.Execute(ctxRedUser, nil, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'redUser' is not authorized to perform vschema operations`)

	_, err = executor.Execute(ctxBlueUser, nil, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'blueUser' is not authorized to perform vschema operations`)

	// test when all users are enabled
	vschemaacl.AuthorizedDDLUsers = "%"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, nil, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}
	stmt = "alter vschema create vindex test_hash2 using hash"
	_, err = executor.Execute(ctxBlueUser, nil, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// test when only one user is enabled
	vschemaacl.AuthorizedDDLUsers = "orangeUser, blueUser, greenUser"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, nil, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'redUser' is not authorized to perform vschema operations`)

	stmt = "alter vschema create vindex test_hash3 using hash"
	_, err = executor.Execute(ctxBlueUser, nil, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// restore the disallowed state
	vschemaacl.AuthorizedDDLUsers = ""
}

func TestExecutorUnrecognized(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "invalid statement", nil)
	require.Error(t, err, "unrecognized statement: invalid statement'")
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _, _ := createExecutorEnv(t)
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

var pv = querypb.ExecuteOptions_Gen4

func TestGetPlanUnnormalized(t *testing.T) {
	r, _, _, _, ctx := createExecutorEnv(t)

	emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

	query1 := "select * from music_user_map where id = 1"
	plan1, logStats1 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	plan2, logStats2 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	assertCacheContains(t, r, emptyvc, query1)
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}
	plan3, logStats3 := getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}
	plan4, logStats4 := getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	assertCacheContains(t, r, emptyvc, query1)
	assertCacheContains(t, r, unshardedvc, query1)
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}
}

func assertCacheSize(t *testing.T, c *PlanCache, expected int) {
	t.Helper()
	size := c.Len()
	if size != expected {
		t.Errorf("getPlan() expected cache to have size %d, but got: %d", expected, size)
	}
}

func assertCacheContains(t *testing.T, e *Executor, vc *vcursorImpl, sql string) *engine.Plan {
	t.Helper()

	var plan *engine.Plan
	if vc == nil {
		e.ForEachPlan(func(p *engine.Plan) bool {
			if p.Original == sql {
				plan = p
			}
			return true
		})
	} else {
		h := e.hashPlan(context.Background(), vc, sql)
		plan, _ = e.plans.Get(h, e.epoch.Load())
	}
	require.Truef(t, plan != nil, "plan not found for query: %s", sql)
	return plan
}

func getPlanCached(t *testing.T, ctx context.Context, e *Executor, vcursor *vcursorImpl, sql string, comments sqlparser.MarginComments, bindVars map[string]*querypb.BindVariable, skipQueryPlanCache bool) (*engine.Plan, *logstats.LogStats) {
	logStats := logstats.NewLogStats(ctx, "Test", "", "", nil)
	vcursor.safeSession = &SafeSession{
		Session: &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{SkipQueryPlanCache: skipQueryPlanCache}},
	}

	stmt, reservedVars, err := parseAndValidateQuery(sql, sqlparser.NewTestParser())
	require.NoError(t, err)
	plan, err := e.getPlan(context.Background(), vcursor, sql, stmt, comments, bindVars, reservedVars /* normalize */, e.normalize, logStats)
	require.NoError(t, err)

	// Wait for cache to settle
	time.Sleep(100 * time.Millisecond)
	return plan, logStats
}

func TestGetPlanCacheUnnormalized(t *testing.T) {
	t.Run("Cache", func(t *testing.T) {
		r, _, _, _, ctx := createExecutorEnv(t)

		emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
		query1 := "select * from music_user_map where id = 1"

		_, logStats1 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true)
		assertCacheSize(t, r.plans, 0)

		wantSQL := query1 + " /* comment */"
		if logStats1.SQL != wantSQL {
			t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
		}

		_, logStats2 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 1)

		wantSQL = query1 + " /* comment 2 */"
		if logStats2.SQL != wantSQL {
			t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
		}
	})

	t.Run("Skip Cache", func(t *testing.T) {
		// Skip cache using directive
		r, _, _, _, ctx := createExecutorEnv(t)

		unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

		query1 := "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
		getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 0)

		query1 = "insert into user(id) values (1), (2)"
		getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 1)

		// the target string will be resolved and become part of the plan cache key, which adds a new entry
		ksIDVc1, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
		getPlanCached(t, ctx, r, ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 2)

		// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
		ksIDVc2, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
		getPlanCached(t, ctx, r, ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 2)
	})
}

func TestGetPlanCacheNormalized(t *testing.T) {
	t.Run("Cache", func(t *testing.T) {
		r, _, _, _, ctx := createExecutorEnv(t)
		r.normalize = true
		emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

		query1 := "select * from music_user_map where id = 1"
		_, logStats1 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */)
		assertCacheSize(t, r.plans, 0)
		wantSQL := "select * from music_user_map where id = :id /* INT64 */ /* comment */"
		assert.Equal(t, wantSQL, logStats1.SQL)

		_, logStats2 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */)
		assertCacheSize(t, r.plans, 1)
		assert.Equal(t, wantSQL, logStats2.SQL)
	})

	t.Run("Skip Cache", func(t *testing.T) {
		// Skip cache using directive
		r, _, _, _, ctx := createExecutorEnv(t)
		r.normalize = true
		unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

		query1 := "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
		getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 0)

		query1 = "insert into user(id) values (1), (2)"
		getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 1)

		// the target string will be resolved and become part of the plan cache key, which adds a new entry
		ksIDVc1, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
		getPlanCached(t, ctx, r, ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 2)

		// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
		ksIDVc2, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
		getPlanCached(t, ctx, r, ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
		assertCacheSize(t, r.plans, 2)
	})
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _, ctx := createExecutorEnv(t)

	r.normalize = true
	emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :id /* INT64 */"

	plan1, logStats1 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment 1 */"), map[string]*querypb.BindVariable{}, false)
	plan2, logStats2 := getPlanCached(t, ctx, r, emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false)

	assert.Equal(t, plan1, plan2)
	assertCacheContains(t, r, emptyvc, normalized)

	wantSQL := normalized + " /* comment 1 */"
	assert.Equal(t, wantSQL, logStats1.SQL)
	wantSQL = normalized + " /* comment 2 */"
	assert.Equal(t, wantSQL, logStats2.SQL)

	plan3, logStats3 := getPlanCached(t, ctx, r, emptyvc, query2, makeComments(" /* comment 3 */"), map[string]*querypb.BindVariable{}, false)
	assert.Equal(t, plan1, plan3)
	wantSQL = normalized + " /* comment 3 */"
	assert.Equal(t, wantSQL, logStats3.SQL)

	var logStats5 *logstats.LogStats
	plan3, logStats5 = getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment 5 */"), map[string]*querypb.BindVariable{}, false)
	assert.Equal(t, plan1, plan3)
	wantSQL = normalized + " /* comment 5 */"
	assert.Equal(t, wantSQL, logStats5.SQL)

	plan4, _ := getPlanCached(t, ctx, r, unshardedvc, query1, makeComments(" /* comment 6 */"), map[string]*querypb.BindVariable{}, false)
	assert.Equal(t, plan1, plan4)
	assertCacheContains(t, r, emptyvc, normalized)
	assertCacheContains(t, r, unshardedvc, normalized)
}

func TestGetPlanPriority(t *testing.T) {

	testCases := []struct {
		name             string
		sql              string
		expectedPriority string
		expectedError    error
	}{
		{name: "Invalid priority", sql: "select /*vt+ PRIORITY=something */ * from music_user_map", expectedPriority: "", expectedError: sqlparser.ErrInvalidPriority},
		{name: "Valid priority", sql: "select /*vt+ PRIORITY=33 */ * from music_user_map", expectedPriority: "33", expectedError: nil},
		{name: "empty priority", sql: "select * from music_user_map", expectedPriority: "", expectedError: nil},
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@unknown", Options: &querypb.ExecuteOptions{}})

	for _, aTestCase := range testCases {
		testCase := aTestCase

		t.Run(testCase.name, func(t *testing.T) {
			r, _, _, _, ctx := createExecutorEnv(t)

			r.normalize = true
			logStats := logstats.NewLogStats(ctx, "Test", "", "", nil)
			vCursor, err := newVCursorImpl(session, makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
			assert.NoError(t, err)

			stmt, err := sqlparser.NewTestParser().Parse(testCase.sql)
			assert.NoError(t, err)
			crticalityFromStatement, _ := sqlparser.GetPriorityFromStatement(stmt)

			_, err = r.getPlan(context.Background(), vCursor, testCase.sql, stmt, makeComments("/* some comment */"), map[string]*querypb.BindVariable{}, nil, true, logStats)
			if testCase.expectedError != nil {
				assert.ErrorIs(t, err, testCase.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedPriority, crticalityFromStatement)
				assert.Equal(t, testCase.expectedPriority, vCursor.safeSession.Options.Priority)
			}
		})
	}

}

func TestPassthroughDDL(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)
	session := &vtgatepb.Session{
		TargetString: "TestExecutor",
	}

	alterDDL := "/* leading */ alter table passthrough_ddl add column col bigint default 123 /* trailing */"
	_, err := executorExec(ctx, executor, session, alterDDL, nil)
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
	session.TargetString = "TestExecutor/40-60"
	executor.normalize = true

	_, err = executorExec(ctx, executor, session, alterDDL, nil)
	require.NoError(t, err)
	require.Nil(t, sbc1.Queries)
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil

	// Use range query
	session.TargetString = "TestExecutor[-]"
	executor.normalize = true

	_, err = executorExec(ctx, executor, session, alterDDL, nil)
	require.NoError(t, err)
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
}

func TestParseEmptyTargetSingleKeyspace(t *testing.T) {
	r, _, _, _, _ := createExecutorEnv(t)

	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("")
	if destKeyspace != KsTestUnsharded || destTabletType != topodatapb.TabletType_PRIMARY {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@primary",
			destKeyspace,
			destTabletType,
			KsTestUnsharded,
			topodatapb.TabletType_PRIMARY,
		)
	}
}

func TestParseEmptyTargetMultiKeyspace(t *testing.T) {
	r, _, _, _, _ := createExecutorEnv(t)

	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
			KsTestSharded:   r.vschema.Keyspaces[KsTestSharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("")
	if destKeyspace != "" || destTabletType != topodatapb.TabletType_PRIMARY {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@primary",
			destKeyspace,
			destTabletType,
			"",
			topodatapb.TabletType_PRIMARY,
		)
	}
}

func TestParseTargetSingleKeyspace(t *testing.T) {
	r, _, _, _, _ := createExecutorEnv(t)

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
	executor, _, _, _, _ := createExecutorEnv(t)

	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/debug/vschema", nil)

	executor.ServeHTTP(resp, req)
	v := make(map[string]any)
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

func TestExecutorMaxPayloadSizeExceeded(t *testing.T) {
	saveMax := maxPayloadSize
	saveWarn := warnPayloadSize
	maxPayloadSize = 10
	warnPayloadSize = 5
	defer func() {
		maxPayloadSize = saveMax
		warnPayloadSize = saveWarn
	}()

	executor, _, _, _, _ := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	warningCount := warnings.Counts()["WarnPayloadSizeExceeded"]
	testMaxPayloadSizeExceeded := []string{
		"select * from main1",
		"insert into main1(id) values (1), (2)",
		"update main1 set id=1",
		"delete from main1 where id=1",
	}
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), nil, "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
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
		_, err := executor.Execute(context.Background(), nil, "TestExecutorMaxPayloadSizeWithOverride", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")

	maxPayloadSize = 1000
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), nil, "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount+4, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")
}

func TestOlapSelectDatabase(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	executor.normalize = true

	session := &vtgatepb.Session{Autocommit: true}

	sql := `select database()`
	cbInvoked := false
	cb := func(r *sqltypes.Result) error {
		cbInvoked = true
		return nil
	}
	err := executor.StreamExecute(context.Background(), nil, "TestExecute", NewSafeSession(session), sql, nil, cb)
	assert.NoError(t, err)
	assert.True(t, cbInvoked)
}

func TestExecutorClearsWarnings(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	session := NewSafeSession(&vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{Code: 234, Message: "oh noes"}},
	})
	_, err := executor.Execute(context.Background(), nil, "TestExecute", session, "select 42", nil)
	require.NoError(t, err)
	require.Empty(t, session.Warnings)
}

// TestServingKeyspaces tests that the dual queries are routed to the correct keyspaces from the list of serving keyspaces.
func TestServingKeyspaces(t *testing.T) {
	buffer.SetBufferingModeInTestingEnv(true)
	defer func() {
		buffer.SetBufferingModeInTestingEnv(false)
	}()

	executor, sbc1, _, sbclookup, ctx := createExecutorEnv(t)

	executor.pv = querypb.ExecuteOptions_Gen4
	gw, ok := executor.resolver.resolver.GetGateway().(*TabletGateway)
	require.True(t, ok)
	hc := gw.hc.(*discovery.FakeHealthCheck)

	// We broadcast twice because we want to ensure the keyspace event watcher has processed all the healthcheck updates
	// from the first broadcast. Since we use a channel for broadcasting, it is blocking and hence the second call ensures
	// all the updates (specifically the last one) has been processed by the keyspace-event-watcher.
	hc.BroadcastAll()
	hc.BroadcastAll()

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("keyspace", "varchar"), "TestExecutor"),
	})
	sbclookup.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("keyspace", "varchar"), "TestUnsharded"),
	})

	require.ElementsMatch(t, []string{"TestExecutor", "TestUnsharded"}, gw.GetServingKeyspaces())
	result, err := executor.Execute(ctx, nil, "TestServingKeyspaces", NewSafeSession(&vtgatepb.Session{}), "select keyspace_name from dual", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARCHAR("TestExecutor")]]`, fmt.Sprintf("%v", result.Rows))

	for _, tablet := range hc.GetAllTablets() {
		if tablet.Keyspace == "TestExecutor" {
			hc.SetServing(tablet, false)
		}
	}
	// Two broadcast calls for the same reason as above.
	hc.BroadcastAll()
	hc.BroadcastAll()

	// Clear plan cache, to force re-planning of the query.
	executor.ClearPlans()
	require.ElementsMatch(t, []string{"TestUnsharded"}, gw.GetServingKeyspaces())
	result, err = executor.Execute(ctx, nil, "TestServingKeyspaces", NewSafeSession(&vtgatepb.Session{}), "select keyspace_name from dual", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARCHAR("TestUnsharded")]]`, fmt.Sprintf("%v", result.Rows))
}

func TestExecutorOther(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

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
		"repair table t1",
		"optimize table t1",
		"do 1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			t.Run(fmt.Sprintf("%s-%s", stmt, tc.targetStr), func(t *testing.T) {
				sbc1.ExecCount.Store(0)
				sbc2.ExecCount.Store(0)
				sbclookup.ExecCount.Store(0)

				_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
				if tc.hasNoKeyspaceErr {
					assert.Error(t, err, errNoKeyspace)
				} else if tc.hasDestinationShardErr {
					assert.Errorf(t, err, "Destination can only be a single shard for statement: %s", stmt)
				} else {
					assert.NoError(t, err)
				}

				utils.MustMatch(t, tc.wantCnts, cnts{
					Sbc1Cnt:      sbc1.ExecCount.Load(),
					Sbc2Cnt:      sbc2.ExecCount.Load(),
					SbcLookupCnt: sbclookup.ExecCount.Load(),
				})
			})
		}
	}
}

func TestExecutorAnalyze(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, _ := createExecutorEnv(t)

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		wantCnts cnts
	}{{
		targetStr: "TestExecutor[-]",
		wantCnts:  cnts{Sbc1Cnt: 1, Sbc2Cnt: 1},
	}, {
		targetStr: KsTestUnsharded,
		wantCnts:  cnts{SbcLookupCnt: 1},
	}, {
		targetStr: "TestExecutor",
		wantCnts:  cnts{Sbc1Cnt: 1, Sbc2Cnt: 1},
	}, {
		targetStr: "TestExecutor/-20",
		wantCnts:  cnts{Sbc1Cnt: 1},
	}, {
		targetStr: "TestExecutor[00]",
		wantCnts:  cnts{Sbc1Cnt: 1},
	}}

	stmt := "analyze table t1"
	for _, tc := range tcs {
		t.Run(tc.targetStr, func(t *testing.T) {
			sbc1.ExecCount.Store(0)
			sbc2.ExecCount.Store(0)
			sbclookup.ExecCount.Store(0)

			_, err := executor.Execute(context.Background(), nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			require.NoError(t, err)

			utils.MustMatch(t, tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Load(),
				Sbc2Cnt:      sbc2.ExecCount.Load(),
				SbcLookupCnt: sbclookup.ExecCount.Load(),
			}, "count did not match")
		})
	}
}

func TestExecutorExplainStmt(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		wantCnts cnts
	}{
		{
			targetStr: "",
			wantCnts:  cnts{Sbc1Cnt: 1},
		},
		{
			targetStr: "TestExecutor[-]",
			wantCnts:  cnts{Sbc1Cnt: 1, Sbc2Cnt: 1},
		},
		{
			targetStr: KsTestUnsharded,
			wantCnts:  cnts{SbcLookupCnt: 1},
		},
		{
			targetStr: "TestExecutor",
			wantCnts:  cnts{Sbc1Cnt: 1},
		},
		{
			targetStr: "TestExecutor/-20",
			wantCnts:  cnts{Sbc1Cnt: 1},
		},
		{
			targetStr: "TestExecutor[00]",
			wantCnts:  cnts{Sbc1Cnt: 1},
		},
	}

	stmts := []string{
		"describe select * from t1",
		"explain select * from t1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			t.Run(fmt.Sprintf("%s-%s", stmt, tc.targetStr), func(t *testing.T) {
				sbc1.ExecCount.Store(0)
				sbc2.ExecCount.Store(0)
				sbclookup.ExecCount.Store(0)

				_, err := executor.Execute(ctx, nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
				assert.NoError(t, err)

				utils.MustMatch(t, tc.wantCnts, cnts{
					Sbc1Cnt:      sbc1.ExecCount.Load(),
					Sbc2Cnt:      sbc2.ExecCount.Load(),
					SbcLookupCnt: sbclookup.ExecCount.Load(),
				})
			})
		}
	}
}

func TestExecutorVExplain(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	executor.normalize = true
	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	bindVars := map[string]*querypb.BindVariable{}
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "vexplain plan select * from user", bindVars)
	require.NoError(t, err)

	require.Equal(t,
		`[[VARCHAR("{\n\t\"OperatorType\": \"Route\",\n\t\"Variant\": \"Scatter\",\n\t\"Keyspace\": {\n\t\t\"Name\": \"TestExecutor\",\n\t\t\"Sharded\": true\n\t},\n\t\"FieldQuery\": \"select * from `+"`user`"+` where 1 != 1\",\n\t\"Query\": \"select * from `+"`user`"+`\",\n\t\"Table\": \"`+"`user`"+`\"\n}")]]`,
		fmt.Sprintf("%v", result.Rows))

	result, err = executorExec(ctx, executor, session, "vexplain plan select 42", bindVars)
	require.NoError(t, err)
	expected := `[[VARCHAR("{\n\t\"OperatorType\": \"Projection\",\n\t\"Expressions\": [\n\t\t\"42 as 42\"\n\t],\n\t\"Inputs\": [\n\t\t{\n\t\t\t\"OperatorType\": \"SingleRow\"\n\t\t}\n\t]\n}")]]`
	require.Equal(t, expected, fmt.Sprintf("%v", result.Rows))
}

func TestExecutorOtherAdmin(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, _ := createExecutorEnv(t)

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
			sbc1.ExecCount.Store(0)
			sbc2.ExecCount.Store(0)
			sbclookup.ExecCount.Store(0)

			_, err := executor.Execute(context.Background(), nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {
				assert.Error(t, err, errNoKeyspace)
			} else if tc.hasDestinationShardErr {
				assert.Errorf(t, err, "Destination can only be a single shard for statement: %s, got: DestinationExactKeyRange(-)", stmt)
			} else {
				assert.NoError(t, err)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Load(),
				Sbc2Cnt:      sbc2.ExecCount.Load(),
				SbcLookupCnt: sbclookup.ExecCount.Load(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}
		}
	}
}

func TestExecutorSavepointInTx(t *testing.T) {
	executor, sbc1, sbc2, _, _ := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestExecutorSavepoint")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: false, TargetString: "@primary"})
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
		Sql:           "select id from `user` where id = 1",
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
		Sql:           "select id from `user` where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "")
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "RELEASE", "release savepoint a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 1", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint b", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to b", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "RELEASE", "release savepoint b", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 3", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "ROLLBACK", "rollback", 2)
}

func TestExecutorSavepointInTxWithReservedConn(t *testing.T) {
	executor, sbc1, sbc2, _, _ := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestExecutorSavepoint")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "TestExecutor", EnableSystemSettings: true})
	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"), "a|"),
	})
	_, err := exec(executor, session, "set sql_mode = ''")
	require.NoError(t, err)

	_, err = exec(executor, session, "begin")
	require.NoError(t, err)
	_, err = exec(executor, session, "savepoint a")
	require.NoError(t, err)
	_, err = exec(executor, session, "select id from user where id = 1")
	require.NoError(t, err)
	_, err = exec(executor, session, "savepoint b")
	require.NoError(t, err)
	_, err = exec(executor, session, "release savepoint a")
	require.NoError(t, err)
	_, err = exec(executor, session, "select id from user where id = 3")
	require.NoError(t, err)
	_, err = exec(executor, session, "commit")
	require.NoError(t, err)
	emptyBV := map[string]*querypb.BindVariable{}

	sbc1WantQueries := []*querypb.BoundQuery{{
		Sql: "select @@sql_mode orig, '' new", BindVariables: emptyBV,
	}, {
		Sql: "set sql_mode = ''", BindVariables: emptyBV,
	}, {
		Sql: "savepoint a", BindVariables: emptyBV,
	}, {
		Sql: "select id from `user` where id = 1", BindVariables: emptyBV,
	}, {
		Sql: "savepoint b", BindVariables: emptyBV,
	}, {
		Sql: "release savepoint a", BindVariables: emptyBV,
	}}

	sbc2WantQueries := []*querypb.BoundQuery{{
		Sql: "set sql_mode = ''", BindVariables: emptyBV,
	}, {
		Sql: "savepoint a", BindVariables: emptyBV,
	}, {
		Sql: "savepoint b", BindVariables: emptyBV,
	}, {
		Sql: "release savepoint a", BindVariables: emptyBV,
	}, {
		Sql: "select id from `user` where id = 3", BindVariables: emptyBV,
	}}

	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "")
	testQueryLog(t, executor, logChan, "TestExecute", "SET", "set @@sql_mode = ''", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 1", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint b", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "RELEASE", "release savepoint a", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 3", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "COMMIT", "commit", 2)
}

func TestExecutorSavepointWithoutTx(t *testing.T) {
	executor, sbc1, sbc2, _, _ := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestExecutorSavepoint")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@primary", InTransaction: false})
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
		Sql:           "select id from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	sbc2WantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "")
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "RELEASE", "release savepoint a", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 1", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT", "savepoint b", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SAVEPOINT_ROLLBACK", "rollback to b", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "RELEASE", "release savepoint b", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "SELECT", "select id from `user` where id = 3", 1)
}

func TestExecutorCallProc(t *testing.T) {
	executor, sbc1, sbc2, sbcUnsharded, _ := createExecutorEnv(t)

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcUnsharded int64
	}

	tcs := []struct {
		name, targetStr string

		hasNoKeyspaceErr bool
		unshardedOnlyErr bool
		wantCnts         cnts
	}{{
		name:             "simple call with no keyspace set",
		targetStr:        "",
		hasNoKeyspaceErr: true,
	}, {
		name:      "keyrange targeted keyspace",
		targetStr: "TestExecutor[-]",
		wantCnts: cnts{
			Sbc1Cnt:      1,
			Sbc2Cnt:      1,
			SbcUnsharded: 0,
		},
	}, {
		name:      "unsharded call proc",
		targetStr: KsTestUnsharded,
		wantCnts: cnts{
			Sbc1Cnt:      0,
			Sbc2Cnt:      0,
			SbcUnsharded: 1,
		},
	}, {
		name:             "should fail with sharded call proc",
		targetStr:        "TestExecutor",
		unshardedOnlyErr: true,
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sbc1.ExecCount.Store(0)
			sbc2.ExecCount.Store(0)
			sbcUnsharded.ExecCount.Store(0)

			_, err := executor.Execute(context.Background(), nil, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), "CALL proc()", nil)
			if tc.hasNoKeyspaceErr {
				assert.EqualError(t, err, errNoKeyspace.Error())
			} else if tc.unshardedOnlyErr {
				require.EqualError(t, err, "CALL is not supported for sharded keyspace")
			} else {
				assert.NoError(t, err)
			}

			utils.MustMatch(t, tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Load(),
				Sbc2Cnt:      sbc2.ExecCount.Load(),
				SbcUnsharded: sbcUnsharded.ExecCount.Load(),
			}, "count did not match")
		})
	}
}

func TestExecutorTempTable(t *testing.T) {
	executor, _, _, sbcUnsharded, ctx := createExecutorEnv(t)

	initialWarningsCount := warnings.Counts()["WarnUnshardedOnly"]
	executor.warnShardedOnly = true
	creatQuery := "create temporary table temp_t(id bigint primary key)"
	session := NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	_, err := executor.Execute(ctx, nil, "TestExecutorTempTable", session, creatQuery, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, sbcUnsharded.ExecCount.Load())
	assert.NotEmpty(t, session.Warnings)
	assert.Equal(t, initialWarningsCount+1, warnings.Counts()["WarnUnshardedOnly"], "warnings count")

	before := executor.plans.Len()

	_, err = executor.Execute(ctx, nil, "TestExecutorTempTable", session, "select * from temp_t", nil)
	require.NoError(t, err)

	assert.Equal(t, before, executor.plans.Len())
}

func TestExecutorShowVitessMigrations(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	showQuery := "show vitess_migrations"
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	_, err := executor.Execute(ctx, nil, "", session, showQuery, nil)
	require.NoError(t, err)
	assert.Contains(t, sbc1.StringQueries(), "show vitess_migrations")
	assert.Contains(t, sbc2.StringQueries(), "show vitess_migrations")
}

func TestExecutorDescHash(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	showQuery := "desc hash_index"
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	_, err := executor.Execute(ctx, nil, "", session, showQuery, nil)
	require.NoError(t, err)
}

func TestExecutorVExplainQueries(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	session := NewAutocommitSession(&vtgatepb.Session{})

	sbclookup.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|user_id", "varchar|int64"), "apa|1", "apa|2"),
	})
	qr, err := executor.Execute(ctx, nil, "TestExecutorVExplainQueries", session, "vexplain queries select * from user where name = 'apa'", nil)
	require.NoError(t, err)
	txt := fmt.Sprintf("%v\n", qr.Rows)
	lookupQuery := "select `name`, user_id from name_user_map where `name` in"
	require.Contains(t, txt, lookupQuery)

	// Test the streaming side as well
	var results []sqltypes.Row
	session = NewAutocommitSession(&vtgatepb.Session{})
	err = executor.StreamExecute(ctx, nil, "TestExecutorVExplainQueries", session, "vexplain queries select * from user where name = 'apa'", nil, func(result *sqltypes.Result) error {
		results = append(results, result.Rows...)
		return nil
	})
	require.NoError(t, err)
	txt = fmt.Sprintf("%v\n", results)
	require.Contains(t, txt, lookupQuery)
}

func TestExecutorStartTxnStmt(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	session := NewAutocommitSession(&vtgatepb.Session{})

	tcases := []struct {
		beginSQL        string
		expTxAccessMode []querypb.ExecuteOptions_TransactionAccessMode
	}{{
		beginSQL: "begin",
	}, {
		beginSQL: "start transaction",
	}, {
		beginSQL:        "start transaction with consistent snapshot",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT},
	}, {
		beginSQL:        "start transaction read only",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_ONLY},
	}, {
		beginSQL:        "start transaction read write",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_WRITE},
	}, {
		beginSQL:        "start transaction with consistent snapshot, read only",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT, querypb.ExecuteOptions_READ_ONLY},
	}, {
		beginSQL:        "start transaction with consistent snapshot, read write",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT, querypb.ExecuteOptions_READ_WRITE},
	}, {
		beginSQL:        "start transaction read only, with consistent snapshot",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_ONLY, querypb.ExecuteOptions_CONSISTENT_SNAPSHOT},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.beginSQL, func(t *testing.T) {
			_, err := executor.Execute(ctx, nil, "TestExecutorStartTxnStmt", session, tcase.beginSQL, nil)
			require.NoError(t, err)

			assert.Equal(t, tcase.expTxAccessMode, session.GetOrCreateOptions().TransactionAccessMode)

			_, err = executor.Execute(ctx, nil, "TestExecutorStartTxnStmt", session, "rollback", nil)
			require.NoError(t, err)

		})
	}
}

func TestExecutorPrepareExecute(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	// prepare statement.
	_, err := executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "prepare prep_user from 'select * from user where id = ?'", nil)
	require.NoError(t, err)
	prepData := session.PrepareStatement["prep_user"]
	require.NotNil(t, prepData)
	require.Equal(t, "select * from `user` where id = :v1", prepData.PrepareStatement)
	require.EqualValues(t, 1, prepData.ParamsCount)

	// prepare statement using user defined variable
	_, err = executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "set @udv_query = 'select * from user where id in (?,?,?)'", nil)
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "prepare prep_user2 from @udv_query", nil)
	require.NoError(t, err)
	prepData = session.PrepareStatement["prep_user2"]
	require.NotNil(t, prepData)
	require.Equal(t, "select * from `user` where id in (:v1, :v2, :v3)", prepData.PrepareStatement)
	require.EqualValues(t, 3, prepData.ParamsCount)

	// syntax error on prepared query
	_, err = executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "prepare prep_user2 from 'select'", nil)
	require.Error(t, err)
	require.Nil(t, session.PrepareStatement["prep_user2"]) // prepared statement is cleared from the session.

	// user defined variable does not exists on prepared query
	_, err = executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "prepare prep_user from @foo", nil)
	require.Error(t, err)
	require.Nil(t, session.PrepareStatement["prep_user"]) // prepared statement is cleared from the session.

	// empty prepared query
	_, err = executor.Execute(context.Background(), nil, "TestExecutorPrepareExecute", session, "prepare prep_user from ''", nil)
	require.Error(t, err)
}

func TestExecutorTruncateErrors(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	save := truncateErrorLen
	truncateErrorLen = 32
	defer func() { truncateErrorLen = save }()

	session := NewSafeSession(&vtgatepb.Session{})
	fn := func(r *sqltypes.Result) error {
		return nil
	}

	_, err := executor.Execute(ctx, nil, "TestExecute", session, "invalid statement", nil)
	assert.EqualError(t, err, "syntax error at posi [TRUNCATED]")

	err = executor.StreamExecute(ctx, nil, "TestExecute", session, "invalid statement", nil, fn)
	assert.EqualError(t, err, "syntax error at posi [TRUNCATED]")

	_, err = executor.Prepare(context.Background(), "TestExecute", session, "invalid statement", nil)
	assert.EqualError(t, err, "[BUG] unrecognized p [TRUNCATED]")
}

func TestExecutorFlushStmt(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	tcs := []struct {
		targetStr string
		query     string

		expectedErr string
	}{{
		targetStr: KsTestUnsharded,
		query:     "flush status",
	}, {
		targetStr: KsTestUnsharded,
		query:     "flush tables user",
	}, {
		targetStr:   "TestUnsharded@replica",
		query:       "flush tables user",
		expectedErr: "VT09012: FLUSH statement with REPLICA tablet not allowed",
	}, {
		targetStr:   "TestUnsharded@replica",
		query:       "flush binary logs",
		expectedErr: "VT09012: FLUSH statement with REPLICA tablet not allowed",
	}, {
		targetStr: "TestUnsharded@replica",
		query:     "flush NO_WRITE_TO_BINLOG binary logs",
	}, {
		targetStr: KsTestUnsharded,
		query:     "flush NO_WRITE_TO_BINLOG tables user",
	}, {
		targetStr: "TestUnsharded@replica",
		query:     "flush LOCAL binary logs",
	}, {
		targetStr: KsTestUnsharded,
		query:     "flush LOCAL tables user",
	}, {
		targetStr:   "",
		query:       "flush LOCAL binary logs",
		expectedErr: "VT09005: no database selected", // no database selected error.
	}, {
		targetStr: "",
		query:     "flush LOCAL tables user",
	}}

	for _, tc := range tcs {
		t.Run(tc.query+tc.targetStr, func(t *testing.T) {
			_, err := executor.Execute(context.Background(), nil, "TestExecutorFlushStmt", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), tc.query, nil)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}

// TestExecutorKillStmt tests the kill statements on executor.
func TestExecutorKillStmt(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	tcs := []struct {
		errStr   string
		query    string
		disallow bool

		expectedLog string
	}{{
		query:       "kill 42",
		expectedLog: "kill connection: 42",
	}, {
		query:       "kill query 42",
		expectedLog: "kill query: 42",
	}, {
		query:  "kill 42",
		errStr: "connection does not exists: 42",
	}, {
		query:  "kill query 24",
		errStr: "connection does not exists: 24",
	}, {
		query:    "kill connection 1",
		disallow: true,
		errStr:   "VT07001: kill statement execution not permitted.",
	}, {
		query:    "kill query 1",
		disallow: true,
		errStr:   "VT07001: kill statement execution not permitted.",
	}}

	for _, tc := range tcs {
		allowKillStmt = !tc.disallow
		t.Run("execute:"+tc.query+tc.errStr, func(t *testing.T) {
			mysqlCtx := &fakeMysqlConnection{ErrMsg: tc.errStr}
			_, err := executor.Execute(context.Background(), mysqlCtx, "TestExecutorKillStmt", NewAutocommitSession(&vtgatepb.Session{}), tc.query, nil)
			if tc.errStr != "" {
				require.ErrorContains(t, err, tc.errStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, mysqlCtx.Log[0], tc.expectedLog)
			}
		})
		t.Run("stream:"+tc.query+tc.errStr, func(t *testing.T) {
			mysqlCtx := &fakeMysqlConnection{ErrMsg: tc.errStr}
			err := executor.StreamExecute(context.Background(), mysqlCtx, "TestExecutorKillStmt", NewAutocommitSession(&vtgatepb.Session{}), tc.query, nil, func(result *sqltypes.Result) error {
				return nil
			})
			if tc.errStr != "" {
				require.ErrorContains(t, err, tc.errStr)
			} else {
				require.NoError(t, err)
				require.Contains(t, mysqlCtx.Log[0], tc.expectedLog)
			}
		})
	}
}

type fakeMysqlConnection struct {
	ErrMsg string
	Log    []string
}

func (f *fakeMysqlConnection) KillQuery(connID uint32) error {
	if f.ErrMsg != "" {
		return errors.New(f.ErrMsg)
	}
	f.Log = append(f.Log, fmt.Sprintf("kill query: %d", connID))
	return nil
}

func (f *fakeMysqlConnection) KillConnection(ctx context.Context, connID uint32) error {
	if f.ErrMsg != "" {
		return errors.New(f.ErrMsg)
	}
	f.Log = append(f.Log, fmt.Sprintf("kill connection: %d", connID))
	return nil
}

var _ vtgateservice.MySQLConnection = (*fakeMysqlConnection)(nil)

func exec(executor *Executor, session *SafeSession, sql string) (*sqltypes.Result, error) {
	return executor.Execute(context.Background(), nil, "TestExecute", session, sql, nil)
}

func makeComments(text string) sqlparser.MarginComments {
	return sqlparser.MarginComments{Trailing: text}
}
