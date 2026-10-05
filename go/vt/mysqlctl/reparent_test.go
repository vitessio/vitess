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

// TestPromoteAppliesReceivedTransactions checks that a promotion stops the receiver and waits for
// the applier to execute everything the replica received before RESET REPLICA ALL discards the
// relay log: the replica may have ACKed those transactions, and they may be acknowledged writes.
func TestPromoteAppliesReceivedTransactions(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", replicaStatusResult("Yes", "Yes", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"))
	db.AddQuery("STOP REPLICA IO_THREAD", &sqltypes.Result{})
	db.AddQueryPattern(`SELECT WAIT_FOR_EXECUTED_GTID_SET\('8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8', \d+\)`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("w", "int64"), "0"))
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	db.AddQuery("FLUSH BINARY LOGS", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"))

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pos, err := testMysqld.Promote(ctx, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8", pos.String())
	assert.Equal(t, 1, db.GetQueryCalledNum("STOP REPLICA IO_THREAD"))
	assert.Equal(t, 1, db.GetQueryCalledNum("RESET REPLICA ALL"))
}

// TestPromoteRefusesUnappliedTransactionsWithStoppedApplier checks that a promotion fails rather
// than discarding received transactions that its stopped applier cannot execute.
func TestPromoteRefusesUnappliedTransactionsWithStoppedApplier(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", replicaStatusResult("No", "No", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"))
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	_, err := testMysqld.Promote(t.Context(), map[string]string{})
	require.ErrorContains(t, err, "has not applied")
	assert.Zero(t, db.GetQueryCalledNum("RESET REPLICA ALL"))
}
