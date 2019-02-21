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
	"testing"

	"github.com/golang/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestExecutorTransactionsNoAutoCommitOlapWorkload(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// TODO: change all calls to Execute to use executorStream
	// begin.
	_, err := executorStreamWithSession(executor, session, "begin")
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master", Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, logChan, "TestExecuteStream", "BEGIN", "begin", 0)

	// commit.
	_, err = executorStreamWithSession(executor, session, "select id from main1")
	if err != nil {
		t.Fatal(err)
	}
	logStats = testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}

	_, err = executorStreamWithSession(executor, session, "commit")
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master", Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	logStats = testQueryLog(t, logChan, "TestExecuteStream", "COMMIT", "commit", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// rollback.
	_, err = executorStreamWithSession(executor, session, "begin")
	if err != nil {
		t.Fatal(err)
	}
	_, err = executorStreamWithSession(executor, session, "select id from main1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = executorStreamWithSession(executor, session, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master", Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	logStats = testQueryLog(t, logChan, "TestExecuteStream", "BEGIN", "begin", 0)
	logStats = testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, logChan, "TestExecuteStream", "ROLLBACK", "rollback", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}

	// rollback doesn't emit a logstats record when it doesn't do anything
	_, err = executorStreamWithSession(executor, session, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	logStats = getQueryLog(logChan)
	if logStats != nil {
		t.Errorf("logstats: expected no record for no-op rollback, got %v", logStats)
	}

	// Prevent transactions on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica", InTransaction: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}})
	_, err = executorStreamWithSession(executor, session, "select id from main1")
	want := "transactions are supported only for master tablet types, current type: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent begin on non-master.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@replica", Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}})
	_, err = executorStreamWithSession(executor, session, "begin")
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}

	// Prevent use of non-master if in_transaction is on.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@master", InTransaction: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}})
	_, err = executorStreamWithSession(executor, session, "use @replica")
	want = "cannot change to a non-master type in the middle of a transaction: REPLICA"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(@replica, in_transaction) err: %v, want %s", err, want)
	}
}
