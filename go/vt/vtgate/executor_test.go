/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/golang/protobuf/proto"
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
)

func TestExecutorResultsExceeded(t *testing.T) {
	save := *warnMemoryRows
	*warnMemoryRows = 3
	defer func() { *warnMemoryRows = save }()

	executor, _, _, sbclookup := createExecutorEnv()
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

func TestExecutorTransactionsNoAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
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

func TestExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
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

func TestExecutorSet(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	testcases := []struct {
		in  string
		out *vtgatepb.Session
		err string
	}{{
		in:  "set autocommit = 1",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.`autocommit` = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.'autocommit' = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.\"autocommit\" = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = on",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = ON",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = 'on'",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = `on`",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = \"on\"",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = false",
		out: &vtgatepb.Session{},
	}, {
		in:  "set autocommit = off",
		out: &vtgatepb.Session{},
	}, {
		in:  "set autocommit = OFF",
		out: &vtgatepb.Session{},
	}, {
		in:  "set AUTOCOMMIT = 0",
		out: &vtgatepb.Session{},
	}, {
		in:  "set AUTOCOMMIT = 'aa'",
		err: "unexpected value for autocommit: aa",
	}, {
		in:  "set autocommit = 2",
		err: "unexpected value for autocommit: 2",
	}, {
		in:  "set client_found_rows = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set client_found_rows = true",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set client_found_rows = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set client_found_rows = false",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set @@global.client_found_rows = 1",
		err: "unsupported in set: global",
	}, {
		in:  "set global client_found_rows = 1",
		err: "unsupported in set: global",
	}, {
		in:  "set global @@session.client_found_rows = 1",
		err: "unsupported in set: mixed using of variable scope",
	}, {
		in:  "set client_found_rows = 'aa'",
		err: "unexpected value type for client_found_rows: string",
	}, {
		in:  "set client_found_rows = 2",
		err: "unexpected value for client_found_rows: 2",
	}, {
		in:  "set transaction_mode = 'unspecified'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_UNSPECIFIED},
	}, {
		in:  "set transaction_mode = 'single'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_SINGLE},
	}, {
		in:  "set transaction_mode = 'multi'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_MULTI},
	}, {
		in:  "set transaction_mode = 'twopc'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set transaction_mode = twopc",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set transaction_mode = 'aa'",
		err: "invalid transaction_mode: aa",
	}, {
		in:  "set transaction_mode = 1",
		err: "unexpected value type for transaction_mode: int64",
	}, {
		in:  "set workload = 'unspecified'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_UNSPECIFIED}},
	}, {
		in:  "set workload = 'oltp'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLTP}},
	}, {
		in:  "set workload = 'olap'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}},
	}, {
		in:  "set workload = 'dba'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_DBA}},
	}, {
		in:  "set workload = 'aa'",
		err: "invalid workload: aa",
	}, {
		in:  "set workload = 1",
		err: "unexpected value type for workload: int64",
	}, {
		in:  "set transaction_mode = 'twopc', autocommit=1",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set sql_select_limit = 5",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SqlSelectLimit: 5}},
	}, {
		in:  "set sql_select_limit = DEFAULT",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SqlSelectLimit: 0}},
	}, {
		in:  "set sql_select_limit = 'asdfasfd'",
		err: "unexpected string value for sql_select_limit: asdfasfd",
	}, {
		in:  "set autocommit = 1+1",
		err: "invalid syntax: 1 + 1",
	}, {
		in:  "set character_set_results=null",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character_set_results='abcd'",
		err: "disallowed value for character_set_results: abcd",
	}, {
		in:  "set foo = 1",
		err: "unsupported construct: set foo = 1",
	}, {
		in:  "set names utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set names ascii",
		err: "unexpected value for charset/names: ascii",
	}, {
		in:  "set charset utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set ascii",
		err: "unexpected value for charset/names: ascii",
	}, {
		in:  "set net_write_timeout = 600",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set sql_mode = 'STRICT_ALL_TABLES'",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set net_read_timeout = 600",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set skip_query_plan_cache = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SkipQueryPlanCache: true}},
	}, {
		in:  "set skip_query_plan_cache = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set sql_auto_is_null = 0",
		out: &vtgatepb.Session{Autocommit: true}, // no effect
	}, {
		in:  "set sql_auto_is_null = 1",
		err: "sql_auto_is_null is not currently supported",
	}, {
		in:  "set tx_read_only = 2",
		err: "unexpected value for tx_read_only: 2",
	}, {
		in:  "set tx_isolation = 'invalid'",
		err: "unexpected value for tx_isolation: invalid",
	}, {
		in:  "set sql_safe_updates = 2",
		err: "unexpected value for sql_safe_updates: 2",
	}}
	for _, tcase := range testcases {
		session := NewSafeSession(&vtgatepb.Session{Autocommit: true})
		_, err := executor.Execute(context.Background(), "TestExecute", session, tcase.in, nil)
		if err != nil {
			if err.Error() != tcase.err {
				t.Errorf("%s error: %v, want %s", tcase.in, err, tcase.err)
			}
			continue
		}
		if !proto.Equal(session.Session, tcase.out) {
			t.Errorf("%s: %v, want %s", tcase.in, session.Session, tcase.out)
		}
	}
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Get()
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{TargetString: "@master", InTransaction: true}
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
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
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
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@master"}
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

func TestExecutorShow(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master"})

	for _, query := range []string{"show databases", "show schemas", "show vitess_keyspaces"} {
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
	if err != nil {
		t.Error(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show collation", nil)
	if err != nil {
		t.Error(err)
	}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'", nil)
	if err != nil {
		t.Error(err)
	}

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
	if err != nil {
		t.Error(err)
	}

	if len(sbclookup.Queries) != 1 {
		t.Errorf("Tablet should have recieved one 'show' query. Instead received: %v", sbclookup.Queries)
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

	_, err = executor.Execute(context.Background(), "TestExecute", session, "show create table unknown_table", nil)
	if err != errNoKeyspace {
		t.Errorf("Got: %v. Want: %v", err, errNoKeyspace)
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

	for _, query := range []string{"show charset", "show charset like '%foo'", "show character set", "show character set like '%foo'"} {
		qr, err := executor.Execute(context.Background(), "TestExecute", session, query, nil)
		if err != nil {
			t.Error(err)
		}
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
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show engines", nil)
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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

func TestExecutorUse(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
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

func TestExecutorComment(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	stmts := []string{
		"/*! SET max_execution_time=5000*/",
		"/*!50708 SET max_execution_time=5000*/",
	}
	wantResult := &sqltypes.Result{}

	for _, stmt := range stmts {
		gotResult, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotResult, wantResult) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotResult, wantResult)
		}
	}
}

func TestExecutorOther(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	stmts := []string{
		"show other",
		"analyze",
		"describe",
		"explain",
		"repair",
		"optimize",
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		gotCount := []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[2]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}

		_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
	}

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "analyze", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}

	// Can't target a range with handle other
	_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor[-]"}), "analyze", nil)
	want = "Destination can only be a single shard for statement: analyze, got: DestinationExactKeyRange(-)"
	if err == nil || err.Error() != want {
		t.Errorf("analyze: got %v, want %v", err, want)
	}
}

func TestExecutorDDL(t *testing.T) {
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	stmts := []string{
		"create",
		"alter",
		"rename",
		"drop",
		"truncate",
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		gotCount := []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[2]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
		testQueryLog(t, logChan, "TestExecute", "DDL", stmt, 1)

		_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		wantCount[1]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
		testQueryLog(t, logChan, "TestExecute", "DDL", stmt, 8)

		_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor/-20"}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
		testQueryLog(t, logChan, "TestExecute", "DDL", stmt, 1)
	}

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "create", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("ddl with no keyspace: %v, want %v", err, want)
	}
	testQueryLog(t, logChan, "TestExecute", "DDL", "create", 0)
}

func waitForVindex(t *testing.T, ks, name string, watch chan *vschemapb.SrvVSchema, executor *Executor) (*vschemapb.SrvVSchema, *vschemapb.Vindex) {
	t.Helper()

	// Wait up to 10ms until the watch gets notified of the update
	ok := false
	for i := 0; i < 10; i++ {
		select {
		case vschema := <-watch:
			_, ok = vschema.Keyspaces[ks].Vindexes[name]
			if !ok {
				t.Errorf("updated vschema did not contain %s", name)
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if !ok {
		t.Errorf("vschema was not updated as expected")
	}

	// Wait up to 10ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {
		vschema := executor.vm.GetCurrentSrvVschema()
		vindex, ok := vschema.Keyspaces[ks].Vindexes[name]
		if ok {
			return vschema, vindex
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("updated vschema did not contain %s", name)
	return nil, nil
}

func waitForVschemaTables(t *testing.T, ks string, tables []string, executor *Executor) *vschemapb.SrvVSchema {
	t.Helper()

	// Wait up to 10ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {
		vschema := executor.vm.GetCurrentSrvVschema()
		gotTables := []string{}
		for t := range vschema.Keyspaces[ks].Tables {
			gotTables = append(gotTables, t)
		}
		sort.Strings(tables)
		sort.Strings(gotTables)
		if reflect.DeepEqual(tables, gotTables) {
			return vschema
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("updated vschema did not contain tables %v", tables)
	return nil
}

func waitForColVindexes(t *testing.T, ks, table string, names []string, executor *Executor) *vschemapb.SrvVSchema {
	t.Helper()

	// Wait up to 10ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {

		vschema := executor.vm.GetCurrentSrvVschema()
		table, ok := vschema.Keyspaces[ks].Tables[table]

		// The table is removed from the vschema when there are no
		// vindexes defined
		if !ok == (len(names) == 0) {
			return vschema
		} else if ok && (len(names) == len(table.ColumnVindexes)) {
			match := true
			for i, name := range names {
				if name != table.ColumnVindexes[i].Name {
					match = false
					break
				}
			}
			if match {
				return vschema
			}
		}

		time.Sleep(time.Millisecond)

	}

	t.Fatalf("updated vschema did not contain vindexes %v on table %s", names, table)
	return nil
}

func TestExecutorCreateVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	ks := "TestExecutor"

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema create vindex test_vindex using hash"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Error(err)
	}

	_, vindex := waitForVindex(t, ks, "test_vindex", vschemaUpdates, executor)
	if vindex == nil || vindex.Type != "hash" {
		t.Errorf("updated vschema did not contain test_vindex")
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
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
	ksNew := "test_new_keyspace"
	session = NewSafeSession(&vtgatepb.Session{TargetString: ksNew})
	stmt = "alter vschema create vindex test_vindex2 using hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ksNew, "test_vindex2", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}
	keyspace, ok := vschema.Keyspaces[ksNew]
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
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	ks := KsTestUnsharded

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Tables["test_table"]
	if ok {
		t.Fatalf("test_table should not exist in original vschema")
	}

	vschemaTables := []string{}
	for t := range vschema.Keyspaces[ks].Tables {
		vschemaTables = append(vschemaTables, t)
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema add table test_table"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Error(err)
	}
	_ = waitForVschemaTables(t, ks, append(vschemaTables, "test_table"), executor)

	stmt = "alter vschema add table test_table2"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Error(err)
	}
	_ = waitForVschemaTables(t, ks, append(vschemaTables, []string{"test_table", "test_table2"}...), executor)

	// Should fail on a sharded keyspace
	session = NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	stmt = "alter vschema add table test_table"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr := "add vschema table: unsupported on sharded keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("want error %v got %v", wantErr, err)
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

func TestExecutorAddDropVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_hash"]
	if ok {
		t.Fatalf("test_hash should not exist in original vschema")
	}

	// Create a new vindex implicitly with the statement
	stmt := "alter vschema on test add vindex test_hash (id) using hash "
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, vindex := waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	_ = waitForColVindexes(t, ks, "test", []string{"test_hash"}, executor)
	qr, err := executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr := &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// Drop it
	stmt = "alter vschema on test drop vindex test_hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, _ = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	_ = waitForColVindexes(t, ks, "test", []string{}, executor)
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	wantErr := "table `test` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Fatalf("expected error in show vschema vindexes on TestExecutor.test %v: got %v", wantErr, err)
	}

	// add it again using the same syntax
	stmt = "alter vschema on test add vindex test_hash (id) using hash "
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, vindex = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	_ = waitForColVindexes(t, ks, "test", []string{"test_hash"}, executor)

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// add another
	stmt = "alter vschema on test add vindex test_lookup (c1,c2) using lookup with owner=`test`, from=`c1,c2`, table=test_lookup, to=keyspace_id"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_lookup", vschemaUpdates, executor)
	if vindex.Type != "lookup" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test"]; ok {
		if len(table.ColumnVindexes) != 2 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[1].Name != "test_lookup" {
			t.Fatalf("table vindexes didn't contain test_lookup")
		}
	} else {
		t.Fatalf("table test not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	stmt = "alter vschema on test add vindex test_hash_id2 (id2) using hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_hash_id2", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test"]; ok {
		if len(table.ColumnVindexes) != 3 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[2].Name != "test_hash_id2" {
			t.Fatalf("table vindexes didn't contain test_hash_id2")
		}
	} else {
		t.Fatalf("table test not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
			buildVarCharRow("id2", "test_hash_id2", "hash", "", ""),
		},
		RowsAffected: 3,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// drop one
	stmt = "alter vschema on test drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	// wait for up to 50ms for it to disappear
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
		if err != nil {
			t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
		}
		wantqr = &sqltypes.Result{
			Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
			Rows: [][]sqltypes.Value{
				buildVarCharRow("id", "test_hash", "hash", "", ""),
				buildVarCharRow("id2", "test_hash_id2", "hash", "", ""),
			},
			RowsAffected: 2,
		}
		if reflect.DeepEqual(qr, wantqr) {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("timed out waiting for test_lookup vindex to be removed")
		}
		time.Sleep(1 * time.Millisecond)
	}

	// use the newly created vindex on a new table
	stmt = "alter vschema on test2 add vindex test_hash (id)"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test2"]; ok {
		if len(table.ColumnVindexes) != 1 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[0].Name != "test_hash" {
			t.Fatalf("table vindexes didn't contain test_hash")
		}
	} else {
		t.Fatalf("table test2 not defined in vschema")
	}

	// create an identical vindex definition on a different table
	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=`test`, from=`c1,c2`, table=test_lookup, to=keyspace_id"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_lookup", vschemaUpdates, executor)
	if vindex.Type != "lookup" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test2"]; ok {
		if len(table.ColumnVindexes) != 2 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[1].Name != "test_lookup" {
			t.Fatalf("table vindexes didn't contain test_lookup")
		}
	} else {
		t.Fatalf("table test2 not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test2", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test2: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	stmt = "alter vschema on test2 add vindex nonexistent (c1,c2)"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex nonexistent does not exist in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_hash (c1,c2) using lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_hash defined with type hash not lookup"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=xyz"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup defined with owner test not xyz"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=`test`, foo=bar"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup defined with different parameters"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nonexistent drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "table TestExecutor.nonexistent not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nonexistent drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "InvalidKeyspace"}), stmt, nil)
	wantErr = "table InvalidKeyspace.nonexistent not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nowhere.nohow drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "table nowhere.nohow not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup not defined in table TestExecutor.test"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	// no queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Get(),
		sbc2.ExecCount.Get(),
		sbclookup.ExecCount.Get(),
	}
	if !reflect.DeepEqual(gotCount, wantCount) {
		t.Errorf("Exec %s: %v, want %v", "", gotCount, wantCount)
	}
}

func TestExecutorVindexDDLNewKeyspace(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	ksName := "NewKeyspace"

	vschema := executor.vm.GetCurrentSrvVschema()
	ks, ok := vschema.Keyspaces[ksName]
	if ok || ks != nil {
		t.Fatalf("keyspace should not exist before test")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ksName})
	stmt := "alter vschema create vindex test_hash using hash"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(50 * time.Millisecond)

	stmt = "alter vschema on test add vindex test_hash2 (id) using hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	time.Sleep(50 * time.Millisecond)
	vschema = executor.vm.GetCurrentSrvVschema()
	ks, ok = vschema.Keyspaces[ksName]
	if !ok || ks == nil {
		t.Fatalf("keyspace was not created as expected")
	}

	vindex := ks.Vindexes["test_hash"]
	if vindex == nil {
		t.Fatalf("vindex was not created as expected")
	}

	vindex2 := ks.Vindexes["test_hash2"]
	if vindex2 == nil {
		t.Fatalf("vindex was not created as expected")
	}

	table := ks.Tables["test"]
	if table == nil {
		t.Fatalf("column vindex was not created as expected")
	}

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

func TestExecutorVindexDDLACL(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})

	ctxRedUser := callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "redUser"})
	ctxBlueUser := callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "blueUser"})

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
	executor, _, _, _ := createExecutorEnv()
	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{}), "invalid statement", nil)
	want := "unrecognized statement: invalid statement"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema tables: %v, want %v", err, want)
	}
}

func TestExecutorMessageAckSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	// Constant in IN clause is just a number, not a bind variable.
	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	count, err := executor.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
	if !reflect.DeepEqual(sbc1.MessageIDs, ids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc1.MessageIDs, ids)
	}
	if sbc2.MessageIDs != nil {
		t.Errorf("sbc2.MessageIDs: %+v, want nil\n", sbc2.MessageIDs)
	}

	// Constants in IN clause are just numbers, not bind variables.
	// They result in two different MessageIDs on two shards.
	sbc1.MessageIDs = nil
	sbc2.MessageIDs = nil
	ids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	count, err = executor.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	if !reflect.DeepEqual(sbc1.MessageIDs, wantids) {
		t.Errorf("sbc1.MessageIDs: %+v, want %+v\n", sbc1.MessageIDs, wantids)
	}
	wantids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	if !reflect.DeepEqual(sbc2.MessageIDs, wantids) {
		t.Errorf("sbc2.MessageIDs: %+v, want %+v\n", sbc2.MessageIDs, wantids)
	}
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _ := createExecutorEnv()

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
	r, _, _, _ := createExecutorEnv()
	emptyvc := newVCursorImpl(context.Background(), nil, "", 0, makeComments(""), r, nil)
	unshardedvc := newVCursorImpl(context.Background(), nil, KsTestUnsharded, 0, makeComments(""), r, nil)

	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats2)
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}
	logStats4 := NewLogStats(context.Background(), "Test", "", nil)
	plan4, err := r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats4)
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + "@unknown:" + query1,
		"@unknown:" + query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}
}

func TestGetPlanCacheUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	emptyvc := newVCursorImpl(context.Background(), nil, "", 0, makeComments(""), r, nil)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	if err != nil {
		t.Error(err)
	}
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	if err != nil {
		t.Error(err)
	}
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	wantSQL = query1 + " /* comment 2 */"
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createExecutorEnv()
	unshardedvc := newVCursorImpl(context.Background(), nil, KsTestUnsharded, 0, makeComments(""), r, nil)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	if len(r.plans.Keys()) != 0 {
		t.Errorf("Plan keys should be 0, got: %v", len(r.plans.Keys()))
	}

	query1 = "insert into user(id) values (1), (2)"
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	if len(r.plans.Keys()) != 1 {
		t.Errorf("Plan keys should be 1, got: %v", len(r.plans.Keys()))
	}
}

func TestGetPlanCacheNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	emptyvc := newVCursorImpl(context.Background(), nil, "", 0, makeComments(""), r, nil)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
	if err != nil {
		t.Error(err)
	}
	if r.plans.Size() != 0 {
		t.Errorf("getPlan() expected cache to have size 0, but got: %b", r.plans.Size())
	}
	wantSQL := "select * from music_user_map where id = :vtg1 /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	if err != nil {
		t.Error(err)
	}
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createExecutorEnv()
	r.normalize = true
	unshardedvc := newVCursorImpl(context.Background(), nil, KsTestUnsharded, 0, makeComments(""), r, nil)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	logStats1 = NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	if len(r.plans.Keys()) != 0 {
		t.Errorf("Plan keys should be 0, got: %v", len(r.plans.Keys()))
	}

	query1 = "insert into user(id) values (1), (2)"
	_, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	if len(r.plans.Keys()) != 1 {
		t.Errorf("Plan keys should be 1, got: %v", len(r.plans.Keys()))
	}
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	emptyvc := newVCursorImpl(context.Background(), nil, "", 0, makeComments(""), r, nil)
	unshardedvc := newVCursorImpl(context.Background(), nil, KsTestUnsharded, 0, makeComments(""), r, nil)

	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	logStats1 := NewLogStats(context.Background(), "Test", "", nil)
	plan1, err := r.getPlan(emptyvc, query1, makeComments(" /* comment 1 */"), map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	logStats2 := NewLogStats(context.Background(), "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false, logStats2)
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan3 {
		t.Errorf("getPlan(query2): plans must be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 3 */"
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}

	logStats4 := NewLogStats(context.Background(), "Test", "", nil)
	plan4, err := r.getPlan(emptyvc, normalized, makeComments(" /* comment 4 */"), map[string]*querypb.BindVariable{}, false, logStats4)
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan4 {
		t.Errorf("getPlan(normalized): plans must be equal: %p %p", plan1, plan4)
	}
	wantSQL = normalized + " /* comment 4 */"
	if logStats4.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats4.SQL)
	}

	logStats5 := NewLogStats(context.Background(), "Test", "", nil)
	plan3, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment 5 */"), map[string]*querypb.BindVariable{}, false, logStats5)
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	wantSQL = normalized + " /* comment 5 */"
	if logStats5.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats5.SQL)
	}

	logStats6 := NewLogStats(context.Background(), "Test", "", nil)
	plan4, err = r.getPlan(unshardedvc, query1, makeComments(" /* comment 6 */"), map[string]*querypb.BindVariable{}, false, logStats6)
	if err != nil {
		t.Error(err)
	}
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
	logStats8 := NewLogStats(context.Background(), "Test", "", nil)
	_, err = r.getPlan(emptyvc, "create table a(id int)", makeComments(""), map[string]*querypb.BindVariable{}, false, logStats8)
	wantErr = "unsupported construct: ddl"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestPassthroughDDL(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	masterSession.TargetString = "TestExecutor"

	_, err := executorExec(executor, "/* leading */ create table passthrough_ddl (col bigint default 123) /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ create table passthrough_ddl (col bigint default 123) /* trailing */",
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

	_, err = executorExec(executor, "/* leading */ create table passthrough_ddl (col bigint default 123) /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""

	// Use range query
	masterSession.TargetString = "TestExecutor[-]"
	executor.normalize = true

	_, err = executorExec(executor, "/* leading */ create table passthrough_ddl (col bigint default 123) /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
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
	r, _, _, _ := createExecutorEnv()
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
	r, _, _, _ := createExecutorEnv()
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
	r, _, _, _ := createExecutorEnv()
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

	executor, _, _, _ := createExecutorEnv()
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

func makeComments(text string) sqlparser.MarginComments {
	return sqlparser.MarginComments{Trailing: text}
}
