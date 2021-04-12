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

package endtoend

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	"context"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	vstart := framework.DebugVars()

	query := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)

	_, err = client.Execute(query, nil)
	require.NoError(t, err)

	err = client.Commit()
	require.NoError(t, err)

	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	require.Equal(t, 4, len(qr.Rows), "rows affected")

	_, err = client.Execute("delete from vitess_test where intval=4", nil)
	require.NoError(t, err)

	qr, err = client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(qr.Rows), "rows affected")

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
		tag:  "Queries/Histograms/Insert/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/DeleteLimit/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/Select/Count",
		diff: 2,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		compareIntDiff(t, vend, expected.tag, vstart, expected.diff)
	}
}

func TestRollback(t *testing.T) {
	client := framework.NewClient()

	vstart := framework.DebugVars()

	query := "insert into vitess_test values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)
	err = client.Rollback()
	require.NoError(t, err)

	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(qr.Rows))

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
		tag:  "Queries/Histograms/Insert/Count",
		diff: 1,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		compareIntDiff(t, vend, expected.tag, vstart, expected.diff)
	}
}

func TestAutoCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	vstart := framework.DebugVars()

	query := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	_, err := client.Execute(query, nil)
	require.NoError(t, err)

	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(qr.Rows))

	_, err = client.Execute("delete from vitess_test where intval=4", nil)
	require.NoError(t, err)

	qr, err = client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(qr.Rows))

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
		tag:  "Queries/Histograms/Insert/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/DeleteLimit/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/Select/Count",
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

func TestTxPoolSize(t *testing.T) {
	vstart := framework.DebugVars()

	client1 := framework.NewClient()
	err := client1.Begin(false)
	require.NoError(t, err)
	defer client1.Rollback()
	verifyIntValue(t, framework.DebugVars(), "TransactionPoolAvailable", tabletenv.NewCurrentConfig().TxPool.Size-1)

	revert := changeVar(t, "TxPoolSize", "1")
	defer revert()
	vend := framework.DebugVars()
	verifyIntValue(t, vend, "TransactionPoolAvailable", 0)
	verifyIntValue(t, vend, "TransactionPoolCapacity", 1)

	client2 := framework.NewClient()
	err = client2.Begin(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection limit exceeded")
	compareIntDiff(t, framework.DebugVars(), "Errors/RESOURCE_EXHAUSTED", vstart, 1)
}

func TestForUpdate(t *testing.T) {
	for _, mode := range []string{"for update", "lock in share mode"} {
		client := framework.NewClient()
		query := fmt.Sprintf("select * from vitess_test where intval=2 %s", mode)
		_, err := client.Execute(query, nil)
		require.NoError(t, err)

		// We should not get errors here
		err = client.Begin(false)
		require.NoError(t, err)
		_, err = client.Execute(query, nil)
		require.NoError(t, err)
		err = client.Commit()
		require.NoError(t, err)
	}
}

func TestPrepareRollback(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Fatalf(err.Error())
	}
	err = client.RollbackPrepared("aa", 0)
	require.NoError(t, err)
	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(qr.Rows))
}

func TestPrepareCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Fatal(err)
	}
	err = client.CommitPrepared("aa")
	require.NoError(t, err)
	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(qr.Rows))
}

func TestPrepareReparentCommit(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)
	err = client.Prepare("aa")
	if err != nil {
		client.RollbackPrepared("aa", 0)
		t.Fatal(err)
	}
	// Rollback all transactions
	err = client.SetServingType(topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	// This should resurrect the prepared transaction.
	err = client.SetServingType(topodatapb.TabletType_MASTER)
	require.NoError(t, err)
	err = client.CommitPrepared("aa")
	require.NoError(t, err)
	qr, err := client.Execute("select * from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(qr.Rows))
}

func TestShutdownGracePeriod(t *testing.T) {
	client := framework.NewClient()

	err := client.Begin(false)
	require.NoError(t, err)
	go func() {
		_, err = client.Execute("select sleep(10) from dual", nil)
		assert.Error(t, err)
	}()

	started := false
	for i := 0; i < 10; i++ {
		queries := framework.LiveQueryz()
		if len(queries) == 1 {
			started = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, started)

	start := time.Now()
	err = client.SetServingType(topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.True(t, time.Since(start) < 5*time.Second, time.Since(start))
	client.Rollback()

	client = framework.NewClientWithTabletType(topodatapb.TabletType_REPLICA)
	err = client.Begin(false)
	require.NoError(t, err)
	go func() {
		_, err = client.Execute("select sleep(11) from dual", nil)
		assert.Error(t, err)
	}()

	started = false
	for i := 0; i < 10; i++ {
		queries := framework.LiveQueryz()
		if len(queries) == 1 {
			started = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, started)
	start = time.Now()
	err = client.SetServingType(topodatapb.TabletType_MASTER)
	require.NoError(t, err)
	assert.True(t, time.Since(start) < 1*time.Second, time.Since(start))
	client.Rollback()
}

func TestShortTxTimeout(t *testing.T) {
	client := framework.NewClient()
	defer framework.Server.SetTxTimeout(framework.Server.TxTimeout())
	framework.Server.SetTxTimeout(10 * time.Millisecond)

	err := client.Begin(false)
	require.NoError(t, err)
	start := time.Now()
	_, err = client.Execute("select sleep(10) from dual", nil)
	assert.Error(t, err)
	assert.True(t, time.Since(start) < 5*time.Second, time.Since(start))
	client.Rollback()
}

func TestMMCommitFlow(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval=4", nil)

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)

	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	require.NoError(t, err)

	err = client.CreateTransaction("aa", []*querypb.Target{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate entry")

	err = client.StartCommit("aa")
	require.NoError(t, err)

	err = client.SetRollback("aa", 0)
	require.EqualError(t, err, "could not transition to ROLLBACK: aa (CallerID: dev)")

	info, err := client.ReadTransaction("aa")
	require.NoError(t, err)
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
	utils.MustMatch(t, wantInfo, info, "ReadTransaction")

	err = client.ConcludeTransaction("aa")
	require.NoError(t, err)

	info, err = client.ReadTransaction("aa")
	require.NoError(t, err)
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
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)

	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	require.NoError(t, err)
	client.Rollback()

	err = client.SetRollback("aa", 0)
	require.NoError(t, err)

	info, err := client.ReadTransaction("aa")
	require.NoError(t, err)
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
	require.NoError(t, err)
}

func TestWatchdog(t *testing.T) {
	client := framework.NewClient()

	query := "insert into vitess_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)

	start := time.Now()
	err = client.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "test1",
		Shard:    "0",
	}, {
		Keyspace: "test2",
		Shard:    "1",
	}})
	require.NoError(t, err)

	// The watchdog should kick in after 1 second.
	dtid := <-framework.ResolveChan
	if dtid != "aa" {
		t.Errorf("dtid: %s, want aa", dtid)
	}
	diff := time.Since(start)
	if diff < 1*time.Second {
		t.Errorf("diff: %v, want greater than 1s", diff)
	}

	err = client.SetRollback("aa", 0)
	require.NoError(t, err)
	err = client.ConcludeTransaction("aa")
	require.NoError(t, err)

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
	require.NoError(t, err)
	_, err = client.Execute(query, nil)
	require.NoError(t, err)
	err = client.Prepare("aa")
	defer client.RollbackPrepared("aa", 0)
	require.NoError(t, err)
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
	require.NoError(t, err)
	defer conn.Close()

	// Successful prepare.
	err = client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(4, null, null, null)", nil)
	require.NoError(t, err)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(5, null, null, null)", nil)
	require.NoError(t, err)
	err = client.Prepare("dtidsuccess")
	defer client.RollbackPrepared("dtidsuccess", 0)
	require.NoError(t, err)

	// Failed transaction.
	err = client.Begin(false)
	require.NoError(t, err)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(6, null, null, null)", nil)
	require.NoError(t, err)
	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values(7, null, null, null)", nil)
	require.NoError(t, err)
	err = client.Prepare("dtidfail")
	defer client.RollbackPrepared("dtidfail", 0)
	require.NoError(t, err)
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

	require.NoError(t, err)
	fmt.Printf("%s/twopcz\n", framework.ServerAddress)
	fmt.Print("Sleeping for 30 seconds\n")
	time.Sleep(30 * time.Second)
}
