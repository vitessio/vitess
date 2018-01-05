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
	"html/template"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

func TestExecutorTransactionsNoAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master"}
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

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
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
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
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
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
	session = &vtgatepb.Session{TargetString: "@replica", InTransaction: true}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	want := "transactions are supported only for master tablet types, current type: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent begin on non-master.
	session = &vtgatepb.Session{TargetString: "@replica"}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent use of non-master if in_transaction is on.
	session = &vtgatepb.Session{TargetString: "@master", InTransaction: true}
	_, err = executor.Execute(context.Background(), "TestExecute", session, "use @replica", nil)
	want = "cannot change to a non-master type in the middle of a transaction: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}
}

func TestExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master", Autocommit: true}

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master", Autocommit: true}
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

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
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
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
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
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
		in:  "set autocommit=1",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set AUTOCOMMIT = 0",
		out: &vtgatepb.Session{},
	}, {
		in:  "set AUTOCOMMIT = 'aa'",
		err: "unexpected value type for autocommit: string",
	}, {
		in:  "set autocommit = 2",
		err: "unexpected value for autocommit: 2",
	}, {
		in:  "set client_found_rows=1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set client_found_rows=0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set client_found_rows='aa'",
		err: "unexpected value type for client_found_rows: string",
	}, {
		in:  "set client_found_rows=2",
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
		in:  "set autocommit=1+1",
		err: "invalid syntax: 1 + 1",
	}, {
		in:  "set character_set_results=null",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character_set_results='abcd'",
		err: "disallowed value for character_set_results: abcd",
	}, {
		in:  "set foo=1",
		err: "unsupported construct: set foo=1",
	}, {
		in:  "set names utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set names ascii",
		err: "unexpected value for charset: ascii",
	}, {
		in:  "set charset utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set ascii",
		err: "unexpected value for charset: ascii",
	}, {
		in:  "set net_write_timeout = 600",
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
	}}
	for _, tcase := range testcases {
		session := &vtgatepb.Session{Autocommit: true}
		_, err := executor.Execute(context.Background(), "TestExecute", session, tcase.in, nil)
		if err != nil {
			if err.Error() != tcase.err {
				t.Errorf("%s error: %v, want %s", tcase.in, err, tcase.err)
			}
			continue
		}
		if !proto.Equal(session, tcase.out) {
			t.Errorf("%s: %v, want %s", tcase.in, session, tcase.out)
		}
	}
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Get()
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select id from main1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{TargetString: "@master", InTransaction: true}
	testSession := *session
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
	logStats = testQueryLog(t, logChan, "TestExecute", "SET", "set autocommit=1", 0)

	// Setting autocommit=1 commits existing transaction.
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	startCount = sbclookup.CommitCount.Get()
	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !proto.Equal(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
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
	startCount = sbclookup.CommitCount.Get()
	_, err = executor.Execute(context.Background(), "TestExecute", session, "begin", nil)
	if err != nil {
		t.Fatal(err)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@master"}
	testSession = *session
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
	if !proto.Equal(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)

	// transition autocommit from 0 to 1 in the middle of a transaction.
	startCount = sbclookup.CommitCount.Get()
	session = &vtgatepb.Session{TargetString: "@master"}
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
	if !proto.Equal(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
}

func TestExecutorLegacyAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master", Autocommit: false}

	// If legacy is on, there should be no implicit transaction.
	executor.legacyAutocommit = true
	startCount := sbclookup.BeginCount.Get()
	_, err := executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sbclookup.BeginCount.Get(), startCount; got != want {
		t.Errorf("Begin count: %d, want %d", got, want)
	}

	// If legacy is off, there should be an implicit begin.
	executor.legacyAutocommit = false
	_, err = executor.Execute(context.Background(), "TestExecute", session, "update main1 set id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sbclookup.BeginCount.Get(), startCount+1; got != want {
		t.Errorf("Begin count: %d, want %d", got, want)
	}
}

func TestExecutorShow(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	for _, query := range []string{"show databases", "show vitess_keyspaces"} {
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
			},
			RowsAffected: 4,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
		}
	}

	qr, err := executor.Execute(context.Background(), "TestExecute", session, "show vitess_shards", nil)
	if err != nil {
		t.Error(err)
	}
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr := &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestExecutor/-20"),
			buildVarCharRow("TestXBadSharding/e0-"),
		},
		RowsAffected: 25,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
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
			buildVarCharRow("TestXBadSharding/e0-"),
		},
		RowsAffected: 17,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
	}

	session = &vtgatepb.Session{TargetString: KsTestUnsharded}
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema_tables", nil)
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
			buildVarCharRow("user_seq"),
		},
		RowsAffected: 8,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema_tables:\n%+v, want\n%+v", qr, wantqr)
	}

	session = &vtgatepb.Session{}
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema_tables", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show 10", nil)
	want = "syntax error at position 8 near '10'"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}

	session = &vtgatepb.Session{TargetString: "no_such_keyspace"}
	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema_tables", nil)
	want = "keyspace no_such_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorUse(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := &vtgatepb.Session{Autocommit: true, TargetString: "@master"}

	stmts := []string{
		"use db",
		"use `ks:-80@master`",
	}
	want := []string{
		"db",
		"ks:-80@master",
	}
	for i, stmt := range stmts {
		_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
		if err != nil {
			t.Error(err)
		}
		wantSession := &vtgatepb.Session{Autocommit: true, TargetString: want[i]}
		if !proto.Equal(session, wantSession) {
			t.Errorf("%s: %v, want %v", stmt, session, wantSession)
		}
	}

	_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{}, "use 1", nil)
	wantErr := "syntax error at position 6 near '1'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("use 1: %v, want %v", err, wantErr)
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
		"truncate",
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{TargetString: KsTestUnsharded}, stmt, nil)
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

		_, err = executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{TargetString: "TestExecutor"}, stmt, nil)
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

	_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{}, "analyze", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
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
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{TargetString: KsTestUnsharded}, stmt, nil)
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

		_, err = executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{TargetString: "TestExecutor"}, stmt, nil)
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

		_, err = executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{TargetString: "TestExecutor/-20"}, stmt, nil)
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

	_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{}, "create", nil)
	want := errNoKeyspace.Error()
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
	testQueryLog(t, logChan, "TestExecute", "DDL", "create", 0)
}

func TestExecutorUnrecognized(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	_, err := executor.Execute(context.Background(), "TestExecute", &vtgatepb.Session{}, "invalid statement", nil)
	want := "unrecognized statement: invalid statement"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorMessageAckSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	// Constant in IN is just a number, not a bind variable.
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

	// Constant in IN is just a couple numbers, not bind variables.
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
	emptyvc := newVCursorImpl(context.Background(), nil, querypb.Target{}, "", r, nil)
	unshardedvc := newVCursorImpl(context.Background(), nil, querypb.Target{Keyspace: KsTestUnsharded}, "", r, nil)

	logStats1 := NewLogStats(nil, "Test", "", nil)
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(emptyvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	logStats2 := NewLogStats(nil, "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, false, logStats2)
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}
	logStats3 := NewLogStats(nil, "Test", "", nil)
	plan3, err := r.getPlan(unshardedvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, false, logStats3)
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	if logStats3.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats3.SQL)
	}
	logStats4 := NewLogStats(nil, "Test", "", nil)
	plan4, err := r.getPlan(unshardedvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, false, logStats4)
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + query1,
		query1,
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
	emptyvc := newVCursorImpl(context.Background(), nil, querypb.Target{}, "", r, nil)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(nil, "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
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
	logStats2 := NewLogStats(nil, "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, " /* comment 2 */", map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
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
}

func TestGetPlanCacheNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	emptyvc := newVCursorImpl(context.Background(), nil, querypb.Target{}, "", r, nil)
	query1 := "select * from music_user_map where id = 1"
	logStats1 := NewLogStats(nil, "Test", "", nil)
	_, err := r.getPlan(emptyvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */, logStats1)
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
	logStats2 := NewLogStats(nil, "Test", "", nil)
	_, err = r.getPlan(emptyvc, query1, " /* comment */", map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */, logStats2)
	if err != nil {
		t.Error(err)
	}
	if r.plans.Size() != 1 {
		t.Errorf("getPlan() expected cache to have size 1, but got: %b", r.plans.Size())
	}
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	emptyvc := newVCursorImpl(context.Background(), nil, querypb.Target{}, "", r, nil)
	unshardedvc := newVCursorImpl(context.Background(), nil, querypb.Target{Keyspace: KsTestUnsharded}, "", r, nil)

	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	logStats1 := NewLogStats(nil, "Test", "", nil)
	plan1, err := r.getPlan(emptyvc, query1, " /* comment 1 */", map[string]*querypb.BindVariable{}, false, logStats1)
	if err != nil {
		t.Error(err)
	}
	logStats2 := NewLogStats(nil, "Test", "", nil)
	plan2, err := r.getPlan(emptyvc, query1, " /* comment 2 */", map[string]*querypb.BindVariable{}, false, logStats2)
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		normalized,
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

	logStats3 := NewLogStats(nil, "Test", "", nil)
	plan3, err := r.getPlan(emptyvc, query2, " /* comment 3 */", map[string]*querypb.BindVariable{}, false, logStats3)
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

	logStats4 := NewLogStats(nil, "Test", "", nil)
	plan4, err := r.getPlan(emptyvc, normalized, " /* comment 4 */", map[string]*querypb.BindVariable{}, false, logStats4)
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

	logStats5 := NewLogStats(nil, "Test", "", nil)
	plan3, err = r.getPlan(unshardedvc, query1, " /* comment 5 */", map[string]*querypb.BindVariable{}, false, logStats5)
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

	logStats6 := NewLogStats(nil, "Test", "", nil)
	plan4, err = r.getPlan(unshardedvc, query1, " /* comment 6 */", map[string]*querypb.BindVariable{}, false, logStats6)
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + normalized,
		normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}

	// Errors
	logStats7 := NewLogStats(nil, "Test", "", nil)
	_, err = r.getPlan(emptyvc, "syntax", "", map[string]*querypb.BindVariable{}, false, logStats7)
	wantErr := "syntax error at position 7 near 'syntax'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	logStats8 := NewLogStats(nil, "Test", "", nil)
	_, err = r.getPlan(emptyvc, "create table a(id int)", "", map[string]*querypb.BindVariable{}, false, logStats8)
	wantErr = "unsupported construct: ddl"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestParseTarget(t *testing.T) {
	r, _, _, _ := createExecutorEnv()

	testcases := []struct {
		targetString string
		target       querypb.Target
	}{{
		targetString: "ks",
		target: querypb.Target{
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks/-80",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks:-80",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks@replica",
		target: querypb.Target{
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "ks:-80@replica",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "@replica",
		target: querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "@bad",
		target: querypb.Target{
			TabletType: topodatapb.TabletType_UNKNOWN,
		},
	}}

	for _, tcase := range testcases {
		if target := r.ParseTarget(tcase.targetString); !proto.Equal(&target, &tcase.target) {
			t.Errorf("ParseTarget(%s): %v, want %v", tcase.targetString, target, tcase.target)
		}
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

	got := r.ParseTarget("@master")
	want := querypb.Target{
		Keyspace:   KsTestUnsharded,
		TabletType: topodatapb.TabletType_MASTER,
	}
	if !proto.Equal(&got, &want) {
		t.Errorf("ParseTarget(%s): %v, want %v", "@master", got, want)
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
}

func TestParseEmptyTargetSingleKeyspace(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
		},
	}
	r.vschema = altVSchema

	got := r.ParseTarget("")
	want := querypb.Target{
		Keyspace:   KsTestUnsharded,
		TabletType: topodatapb.TabletType_MASTER,
	}
	if !proto.Equal(&got, &want) {
		t.Errorf("ParseTarget(%s): %v, want %v", "@master", got, want)
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

	got := r.ParseTarget("")
	want := querypb.Target{
		Keyspace:   "",
		TabletType: topodatapb.TabletType_MASTER,
	}
	if !proto.Equal(&got, &want) {
		t.Errorf("ParseTarget(%s): %v, want %v", "@master", got, want)
	}
}
