/*
Copyright 2017 Google Inc.

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

package endtoend

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	catcher := framework.NewTxCatcher()
	defer catcher.Close()
	vstart := framework.DebugVars()

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}
	tx, err := catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want := []string{"insert into vitess_test(intval, floatval, charval, binval) values (4, null, null, null) /* _stream vitess_test (intval ) (4 ); */"}
	if !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}
	if !reflect.DeepEqual(tx.Conclusion, "commit") {
		t.Errorf("conclusion: %s, want commit", tx.Conclusion)
	}

	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	_, err = client.Execute("delete from vitess_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}

	qr, err = client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions/TotalCount",
		diff: 2,
	}, {
		tag:  "Transactions/Histograms/commit/Count",
		diff: 2,
	}, {
		tag:  "Queries/TotalCount",
		diff: 6,
	}, {
		tag:  "Queries/Histograms/BEGIN/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/COMMIT/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/INSERT_PK/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/DML_PK/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/PASS_SELECT/Count",
		diff: 2,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		if err := compareIntDiff(vend, expected.tag, vstart, expected.diff); err != nil {
			t.Error(err)
		}
	}
}

func TestRollback(t *testing.T) {
	client := framework.NewClient()

	catcher := framework.NewTxCatcher()
	defer catcher.Close()
	vstart := framework.DebugVars()

	query := "insert into vitess_test values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Rollback()
	if err != nil {
		t.Error(err)
		return
	}
	tx, err := catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want := []string{"insert into vitess_test(intval, floatval, charval, binval) values (4, null, null, null) /* _stream vitess_test (intval ) (4 ); */"}
	if !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}
	if !reflect.DeepEqual(tx.Conclusion, "rollback") {
		t.Errorf("conclusion: %s, want rollback", tx.Conclusion)
	}

	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 3", qr.RowsAffected)
	}

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions/TotalCount",
		diff: 1,
	}, {
		tag:  "Transactions/Histograms/rollback/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/BEGIN/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/ROLLBACK/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/INSERT_PK/Count",
		diff: 1,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		if err := compareIntDiff(vend, expected.tag, vstart, expected.diff); err != nil {
			t.Error(err)
		}
	}
}

func TestAutoCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	catcher := framework.NewTxCatcher()
	defer catcher.Close()
	vstart := framework.DebugVars()

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	_, err := client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	tx, err := catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want := []string{"insert into vitess_test(intval, floatval, charval, binval) values (4, null, null, null) /* _stream vitess_test (intval ) (4 ); */"}
	// Sometimes, no queries will be returned by the querylog because reliability
	// is not guaranteed. If so, just move on without verifying. The subsequent
	// rowcount check will anyway verify that the insert succeeded.
	if len(tx.Queries) != 0 && !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}
	if !reflect.DeepEqual(tx.Conclusion, "commit") {
		t.Errorf("conclusion: %s, want commit", tx.Conclusion)
	}

	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	_, err = client.Execute("delete from vitess_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}

	qr, err = client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions/TotalCount",
		diff: 2,
	}, {
		tag:  "Transactions/Histograms/commit/Count",
		diff: 2,
	}, {
		tag:  "Queries/TotalCount",
		diff: 4,
	}, {
		tag:  "Queries/Histograms/BEGIN/Count",
		diff: 0,
	}, {
		tag:  "Queries/Histograms/COMMIT/Count",
		diff: 0,
	}, {
		tag:  "Queries/Histograms/INSERT_PK/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/DML_PK/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/PASS_SELECT/Count",
		diff: 2,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		got := framework.FetchInt(vend, expected.tag)
		want := framework.FetchInt(vstart, expected.tag) + expected.diff
		// It's possible that other house-keeping transactions (like messaging)
		// can happen during this test. So, don't perform equality comparisons.
		if got < want {
			t.Errorf("%s: %d, must be at least %d", expected.tag, got, want)
		}
	}
}

func TestAutoCommitOff(t *testing.T) {
	framework.Server.SetAutoCommit(false)
	defer framework.Server.SetAutoCommit(true)

	_, err := framework.NewClient().Execute("insert into vitess_test values(4, null, null, null)", nil)
	want := "INSERT_PK disallowed outside transaction"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("%v, must start with %s", err, want)
	}
}

func TestTxPoolSize(t *testing.T) {
	vstart := framework.DebugVars()

	client1 := framework.NewClient()
	err := client1.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	defer client1.Rollback()
	if err := verifyIntValue(framework.DebugVars(), "TransactionPoolAvailable", tabletenv.Config.TransactionCap-1); err != nil {
		t.Error(err)
	}

	defer framework.Server.SetTxPoolSize(framework.Server.TxPoolSize())
	framework.Server.SetTxPoolSize(1)
	defer framework.Server.BeginTimeout.Set(framework.Server.BeginTimeout.Get())
	timeout := 1 * time.Millisecond
	framework.Server.BeginTimeout.Set(timeout)
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "TransactionPoolAvailable", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolCapacity", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "BeginTimeout", int(timeout)); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolDbaInUse", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolDbaTotal", 0); err != nil {
		t.Error(err)
	}

	client2 := framework.NewClient()
	err = client2.Begin(false)
	want := "connection limit exceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v, must contain %s", err, want)
	}
	if err := compareIntDiff(framework.DebugVars(), "Errors/RESOURCE_EXHAUSTED", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestTxPoolDba(t *testing.T) {
	client1 := framework.NewClient()

	// No DBA transactions yet
	vstart := framework.DebugVars()
	if err := verifyIntValue(vstart, "TransactionPoolDbaInUse", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vstart, "TransactionPoolDbaTotal", 0); err != nil {
		t.Error(err)
	}

	err := client1.BeginWithOptions(&querypb.ExecuteOptions{Workload: query.ExecuteOptions_DBA})
	if err != nil {
		t.Error(err)
		return
	}
	defer client1.Rollback()

	defer framework.Server.BeginTimeout.Set(framework.Server.BeginTimeout.Get())
	timeout := 1 * time.Millisecond
	framework.Server.BeginTimeout.Set(timeout)

	// DBA transaction running, but not in use
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "TransactionPoolDbaInUse", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolDbaTotal", 1); err != nil {
		t.Error(err)
	}

	// Activate transaction
	// TODO

	// DBA transaction running and in use
	// TODO check

	// Wait for transaction to stop running
	// TODO

	// DBA transaction running, but not in use
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "TransactionPoolDbaInUse", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolDbaTotal", 1); err != nil {
		t.Error(err)
	}

}

func TestTxTimeout(t *testing.T) {
	vstart := framework.DebugVars()

	defer framework.Server.SetTxTimeout(framework.Server.TxTimeout())
	framework.Server.SetTxTimeout(1 * time.Millisecond)
	if err := verifyIntValue(framework.DebugVars(), "TransactionPoolTimeout", int(1*time.Millisecond)); err != nil {
		t.Error(err)
	}

	catcher := framework.NewTxCatcher()
	defer catcher.Close()
	client := framework.NewClient()
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	tx, err := catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	if tx.Conclusion != "kill" {
		t.Errorf("Conclusion: %s, want kill", tx.Conclusion)
	}
	if err := compareIntDiff(framework.DebugVars(), "Kills/Transactions", vstart, 1); err != nil {
		t.Error(err)
	}

	// Ensure commit fails.
	err = client.Commit()
	if code := vterrors.Code(err); code != vtrpcpb.Code_ABORTED {
		t.Errorf("Commit code: %v, want %v", code, vtrpcpb.Code_ABORTED)
	}
}

func TestForUpdate(t *testing.T) {
	for _, mode := range []string{"for update", "lock in share mode"} {
		client := framework.NewClient()
		query := fmt.Sprintf("select * from vitess_test where intval=2 %s", mode)
		_, err := client.Execute(query, nil)
		want := "SELECT_LOCK disallowed outside transaction"
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("%v, must have prefix %s", err, want)
		}

		// We should not get errors here
		err = client.Begin(false)
		if err != nil {
			t.Error(err)
			return
		}
		_, err = client.Execute(query, nil)
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Commit()
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func TestPrepareRollback(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Error(err)
		return
	}
	err = client.RollbackPrepared("aa", 0)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 3", qr.RowsAffected)
	}
}

func TestPrepareCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Error(err)
		return
	}
	err = client.CommitPrepared("aa")
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}
}

func TestPrepareReparentCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Error(err)
		return
	}
	// Rollback all transactions
	err = client.SetServingType(topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Error(err)
		return
	}
	// This should resurrect the prepared transaction.
	err = client.SetServingType(topodatapb.TabletType_MASTER)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.CommitPrepared("aa")
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}
}

func TestMMCommitFlow(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
	}

	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	if err != nil {
		t.Error(err)
	}

	err = client.CreateTransaction("aa", []*querypb.Target{})
	want := "Duplicate entry"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v, must contain %s", err, want)
	}

	err = client.StartCommit("aa")
	if err != nil {
		t.Error(err)
	}

	err = client.SetRollback("aa", 0)
	want = "could not transition to ROLLBACK: aa (CallerID: dev)"
	if err == nil || err.Error() != want {
		t.Errorf("%v, must contain %s", err, want)
	}

	info, err := client.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	info.TimeCreated = 0
	wantInfo := &querypb.TransactionMetadata{
		Dtid:  "aa",
		State: 2,
		Participants: []*querypb.Target{{
			Keyspace:   "test1",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
		}, {
			Keyspace:   "test2",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}
	if !proto.Equal(info, wantInfo) {
		t.Errorf("ReadTransaction: %#v, want %#v", info, wantInfo)
	}

	err = client.ConcludeTransaction("aa")
	if err != nil {
		t.Error(err)
	}

	info, err = client.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	wantInfo = &querypb.TransactionMetadata{}
	if !proto.Equal(info, wantInfo) {
		t.Errorf("ReadTransaction: %#v, want %#v", info, wantInfo)
	}
}

func TestMMRollbackFlow(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
	}

	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	if err != nil {
		t.Error(err)
	}
	client.Rollback()

	err = client.SetRollback("aa", 0)
	if err != nil {
		t.Error(err)
	}

	info, err := client.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	info.TimeCreated = 0
	wantInfo := &querypb.TransactionMetadata{
		Dtid:  "aa",
		State: 3,
		Participants: []*querypb.Target{{
			Keyspace:   "test1",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
		}, {
			Keyspace:   "test2",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}
	if !proto.Equal(info, wantInfo) {
		t.Errorf("ReadTransaction: %#v, want %#v", info, wantInfo)
	}

	err = client.ConcludeTransaction("aa")
	if err != nil {
		t.Error(err)
	}
}

func TestWatchdog(t *testing.T) {
	client := framework.NewClient()

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
	}

	start := time.Now()
	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	if err != nil {
		t.Error(err)
	}

	// The watchdog should kick in after 1 second.
	dtid := <-framework.ResolveChan
	if dtid != "aa" {
		t.Errorf("dtid: %s, want aa", dtid)
	}
	diff := time.Now().Sub(start)
	if diff < 1*time.Second {
		t.Errorf("diff: %v, want greater than 1s", diff)
	}

	err = client.SetRollback("aa", 0)
	if err != nil {
		t.Error(err)
	}
	err = client.ConcludeTransaction("aa")
	if err != nil {
		t.Error(err)
	}

	// Make sure the watchdog stops sending messages.
	// Check twice. Sometimes, a race can still cause
	// a stray message.
	dtid = ""
	for i := 0; i < 2; i++ {
		select {
		case dtid = <-framework.ResolveChan:
			continue
		case <-time.After(2 * time.Second):
			return
		}
	}
	t.Errorf("Unexpected message: %s", dtid)
}

func TestUnresolvedTracking(t *testing.T) {
	// This is a long running test. Enable only for testing the watchdog.
	t.Skip()
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("aa")
	defer client.RollbackPrepared("aa", 0)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)
	vars := framework.DebugVars()
	if val := framework.FetchInt(vars, "Unresolved/Prepares"); val != 1 {
		t.Errorf("Unresolved: %d, want 1", val)
	}
}

func TestManualTwopcz(t *testing.T) {
	// This is a manual test. Uncomment the Skip to perform this test.
	// The test will print the twopcz URL. Navigate to that location
	// and perform all the operations allowed. They should all succeed
	// and cause the transactions to be resolved.
	t.Skip()
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	// Successful prepare.
	err = client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(4, null, null, null)", nil)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(5, null, null, null)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("dtidsuccess")
	defer client.RollbackPrepared("dtidsuccess", 0)
	if err != nil {
		t.Error(err)
		return
	}

	// Failed transaction.
	err = client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(6, null, null, null)", nil)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(7, null, null, null)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Prepare("dtidfail")
	defer client.RollbackPrepared("dtidfail", 0)
	if err != nil {
		t.Error(err)
		return
	}
	conn.ExecuteFetch(fmt.Sprintf("update _vt.redo_state set state = %d where dtid = 'dtidfail'", tabletserver.RedoStateFailed), 10, false)
	conn.ExecuteFetch("commit", 10, false)

	// Distributed transaction.
	err = client.CreateTransaction("distributed", []*querypb.Target{{
		Keyspace: "k1",
		Shard:    "s1",
	}, {
		Keyspace: "k2",
		Shard:    "s2",
	}})
	defer client.ConcludeTransaction("distributed")

	fmt.Printf("%s/twopcz\n", framework.ServerAddress)
	fmt.Print("Sleeping for 30 seconds\n")
	time.Sleep(30 * time.Second)
}
