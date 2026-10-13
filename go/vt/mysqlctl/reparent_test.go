/*
Copyright 2024 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
)

func TestPopulateReparentJournal(t *testing.T) {
	input := `MySQL replica position: filename 'vt-0476396352-bin.000005', position '310088991', GTID of the last change '145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,
	1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,
	47b59de1-b368-11e9-b48b-624401d35560:1-152981,
	557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,
	599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,
	b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262'
	MySQL replica binlog position: master host '10.128.0.43', purge list '145e508e-ae54-11e9-8ce6-46824dd1815e:1-3, 1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3, 47b59de1-b368-11e9-b48b-624401d35560:1-152981, 557def0a-b368-11e9-84ed-f6fffd91cc57:1-3, 599ef589-ae55-11e9-9688-ca1f44501925:1-14857169, b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262', channel name: ''
	
	190809 00:15:44 [00] Streaming <STDOUT>
	190809 00:15:44 [00]        ...done
	190809 00:15:44 [00] Streaming <STDOUT>
	190809 00:15:44 [00]        ...done
	xtrabackup: Transaction log of lsn (405344842034) to (406364859653) was copied.
	190809 00:16:14 completed OK!`

	pos, err := findReplicationPosition(input, "MySQL56", logutil.NewConsoleLogger())
	require.NoError(t, err)

	res := PopulateReparentJournal(1, "action", "primaryAlias", pos)
	want := `INSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES (1, 'action', 'primaryAlias', 'MySQL56/145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,47b59de1-b368-11e9-b48b-624401d35560:1-152981,557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262')`
	assert.Equal(t, want, res)
}

func TestWaitForReparentJournal(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT action_name, primary_alias, replication_position FROM _vt.reparent_journal WHERE time_created_ns=5", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_row"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	err := testMysqld.WaitForReparentJournal(ctx, 5)
	assert.NoError(t, err)
}

func TestPromote(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	db.AddQuery("FLUSH BINARY LOGS", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	pos, err := testMysqld.Promote(t.Context(), map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17", pos.String())
}

// replicaStatusResult is a SHOW REPLICA STATUS row of a replica of source uuid.
func replicaStatusResult(ioRunning, sqlRunning, executed, retrieved string) *sqltypes.Result {
	return sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Source_Port|Source_UUID|Replica_IO_Running|Replica_SQL_Running|Executed_Gtid_Set|Retrieved_Gtid_Set",
			"varchar|int64|varchar|varchar|varchar|varchar|varchar"),
		"old-primary|3306|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9|"+ioRunning+"|"+sqlRunning+"|"+executed+"|"+retrieved)
}

// Promotion must apply the final received GTID set before resetting replication.
func TestPromoteAppliesReceivedTransactions(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.OrderMatters()
	t.Cleanup(db.VerifyAllExecutedOrFail)
	db.AddExpectedQuery("SELECT 1", nil)
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SHOW REPLICA STATUS",
		QueryResult: replicaStatusResult("Yes", "Yes", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"),
	})
	db.AddExpectedQuery("STOP REPLICA IO_THREAD", nil)
	// More transactions arrived between the initial status read and the receiver stop.
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SHOW REPLICA STATUS",
		QueryResult: replicaStatusResult("No", "Yes", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10"),
	})
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SELECT WAIT_FOR_EXECUTED_GTID_SET('8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10', *",
		QueryResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("w", "int64"), "0"),
	})
	db.AddExpectedQuery("STOP REPLICA", nil)
	db.AddExpectedQuery("RESET REPLICA ALL", nil)
	db.AddExpectedQuery("FLUSH BINARY LOGS", nil)
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SELECT @@global.gtid_executed",
		QueryResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10"),
	})

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	pos, err := testMysqld.Promote(ctx, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10", pos.String())
}

// A failed applier wait must leave the relay log intact.
func TestPromotePreservesRelayLogWhenApplierWaitFails(t *testing.T) {
	for _, tc := range []struct {
		name      string
		result    *sqltypes.Result
		err       error
		wantError string
	}{
		{name: "timeout", result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("w", "int64"), "1"), wantError: "timed out waiting for position"},
		{name: "query failure", err: errors.New("injected applier wait failure"), wantError: "injected applier wait failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			t.Cleanup(db.Close)
			db.OrderMatters()
			t.Cleanup(db.VerifyAllExecutedOrFail)
			db.AddExpectedQuery("SELECT 1", nil)
			db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
				Query:       "SHOW REPLICA STATUS",
				QueryResult: replicaStatusResult("No", "Yes", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"),
			})
			// Any STOP, RESET or FLUSH after the failed wait is unexpected.
			db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
				Query:       "SELECT WAIT_FOR_EXECUTED_GTID_SET('8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8', *",
				QueryResult: tc.result,
				Error:       tc.err,
			})
			cp := *db.ConnParams()
			mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
			t.Cleanup(mysqld.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)

			pos, err := mysqld.Promote(ctx, nil)
			require.ErrorContains(t, err, tc.wantError)
			assert.True(t, pos.IsZero())
		})
	}
}

// TestPromoteRefusesUnappliedTransactionsWithStoppedApplier checks that a promotion fails rather
// than discarding received transactions that its stopped applier cannot execute.
func TestPromoteRefusesUnappliedTransactionsWithStoppedApplier(t *testing.T) {
	for _, receiverRunning := range []string{"No", "Yes"} {
		t.Run("receiver_running="+receiverRunning, func(t *testing.T) {
			db := fakesqldb.New(t)
			t.Cleanup(db.Close)
			db.OrderMatters()
			t.Cleanup(db.VerifyAllExecutedOrFail)
			db.AddExpectedQuery("SELECT 1", nil)
			db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
				Query:       "SHOW REPLICA STATUS",
				QueryResult: replicaStatusResult(receiverRunning, "No", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"),
			})
			if receiverRunning == "Yes" {
				db.AddExpectedQuery("STOP REPLICA IO_THREAD", nil)
				db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
					Query:       "SHOW REPLICA STATUS",
					QueryResult: replicaStatusResult("No", "No", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10"),
				})
			}
			// No wait, STOP REPLICA, RESET or FLUSH is allowed after refusal.
			cp := *db.ConnParams()
			mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
			t.Cleanup(mysqld.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)

			pos, err := mysqld.Promote(ctx, nil)
			require.ErrorContains(t, err, "has not applied")
			assert.True(t, pos.IsZero())
		})
	}
}

// TestPromoteSkipsWaitForEmptyReceivedPosition checks that promotion stops the
// receiver but does not wait or require a running applier when no GTIDs were received.
func TestPromoteSkipsWaitForEmptyReceivedPosition(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	db.OrderMatters()
	t.Cleanup(db.VerifyAllExecutedOrFail)
	db.AddExpectedQuery("SELECT 1", nil)
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SHOW REPLICA STATUS",
		QueryResult: replicaStatusResult("Yes", "No", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", ""),
	})
	db.AddExpectedQuery("STOP REPLICA IO_THREAD", nil)
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SHOW REPLICA STATUS",
		QueryResult: replicaStatusResult("No", "No", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", ""),
	})
	// A GTID wait is unexpected; promotion can proceed with the applier stopped.
	db.AddExpectedQuery("STOP REPLICA", nil)
	db.AddExpectedQuery("RESET REPLICA ALL", nil)
	db.AddExpectedQuery("FLUSH BINARY LOGS", nil)
	db.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
		Query:       "SELECT @@global.gtid_executed",
		QueryResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("gtid_executed", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5"),
	})
	cp := *db.ConnParams()
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	pos, err := mysqld.Promote(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", pos.String())
}
