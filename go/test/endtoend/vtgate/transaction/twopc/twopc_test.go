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

package transaction

import (
	"context"
	_ "embed"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestDTCommit tests distributed transaction commit for insert, update and delete operations
// It verifies the binlog events for the same with transaction state changes and redo statements.
func TestDTCommit(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(8,'bar')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(9,'baz')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(10,'apa')")
	utils.Exec(t, conn, "commit")

	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitions(t, ch, tableMap, dtMap)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.redo_state:-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		},
		"ks.twopc_user:-80": {
			`insert:[INT64(8) VARCHAR("bar")]`,
			`insert:[INT64(10) VARCHAR("apa")]`,
		},
		"ks.twopc_user:80-": {
			`insert:[INT64(7) VARCHAR("foo")]`,
			`insert:[INT64(9) VARCHAR("baz")]`,
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 8")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.redo_state:-80": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-80": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:-80": {"update:[INT64(8) VARCHAR(\"newfoo\")]"},
		"ks.twopc_user:80-": {"update:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where id = 9")
	utils.Exec(t, conn, "delete from twopc_user where id = 10")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.redo_state:-80": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-80": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:-80": {"delete:[INT64(10) VARCHAR(\"apa\")]"},
		"ks.twopc_user:80-": {"delete:[INT64(9) VARCHAR(\"baz\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTRollback tests distributed transaction rollback for insert, update and delete operations
// There would not be any binlog events for rollback
func TestDTRollback(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// Insert initial Data
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo'), (8,'bar')")

	// run vstream to stream binlogs
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(9,'baz')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(10,'apa')")
	utils.Exec(t, conn, "rollback")

	tableMap := make(map[string][]*querypb.Field)
	logTable := retrieveTransitions(t, ch, tableMap, nil)
	assert.Zero(t, len(logTable),
		"no change in binlog expected: got: %s", prettyPrint(logTable))

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 8")
	utils.Exec(t, conn, "rollback")

	logTable = retrieveTransitions(t, ch, tableMap, nil)
	assert.Zero(t, len(logTable),
		"no change in binlog expected: got: %s", prettyPrint(logTable))

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where id = 7")
	utils.Exec(t, conn, "delete from twopc_user where id = 8")
	utils.Exec(t, conn, "rollback")

	logTable = retrieveTransitions(t, ch, tableMap, nil)
	assert.Zero(t, len(logTable),
		"no change in binlog expected: got: %s", prettyPrint(logTable))
}

// TestDTCommitMultiShardTxSingleShardDML tests distributed transaction commit for insert, update and delete operations
// There is DML operation only on single shard but transaction open on multiple shards.
// Metdata Manager is the one which executed the DML operation on the shard.
func TestDTCommitDMLOnlyOnMM(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "select * from twopc_user")
	utils.Exec(t, conn, "commit")

	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitions(t, ch, tableMap, dtMap)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.twopc_user:80-": {"insert:[INT64(7) VARCHAR(\"foo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "select * from twopc_user")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.twopc_user:80-": {"update:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where id = 7")
	utils.Exec(t, conn, "select * from twopc_user")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.twopc_user:80-": {"delete:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTCommitMultiShardTxSingleShardDML tests distributed transaction commit for insert, update and delete operations
// There is DML operation only on single shard but transaction open on multiple shards.
// Resource Manager is the one which executed the DML operation on the shard.
func TestDTCommitDMLOnlyOnRM(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "commit")

	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitions(t, ch, tableMap, dtMap)
	expectations := map[string][]string{
		"ks.dt_state:-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
		},
		"ks.twopc_user:80-": {"insert:[INT64(7) VARCHAR(\"foo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:-80": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:-80": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 7 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 7 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:80-": {"update:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "delete from twopc_user where id = 7")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:-80": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:-80": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 7 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 7 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:80-": {"delete:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTPrepareFailOnRM tests distributed transaction prepare failure on resource manager
func TestDTPrepareFailOnRM(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(8,'bar')")

	ctx2 := context.Background()
	conn2, err := mysql.Connect(ctx2, &vtParams)
	require.NoError(t, err)

	utils.Exec(t, conn2, "begin")
	utils.Exec(t, conn2, "insert into twopc_user(id, name) values(9,'baz')")
	utils.Exec(t, conn2, "insert into twopc_user(id, name) values(10,'apa')")

	var wg sync.WaitGroup
	wg.Add(2)
	var commitErr error
	go func() {
		_, err := utils.ExecAllowError(t, conn, "commit")
		if err != nil {
			commitErr = err
		}
		wg.Done()
	}()
	go func() {
		_, err := utils.ExecAllowError(t, conn2, "commit")
		wg.Done()
		if err != nil {
			commitErr = err
		}
	}()
	wg.Wait()
	require.ErrorContains(t, commitErr, "ResourceExhausted desc = prepare failed")

	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitions(t, ch, tableMap, dtMap)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"ROLLBACK\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"ROLLBACK\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-80\")]",
		},
		"ks.redo_state:-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-80": { /* flexi Expectation */ },
		"ks.twopc_user:-80":     { /* flexi Expectation */ },
		"ks.twopc_user:80-":     { /* flexi Expectation */ },
	}
	flexiExpectations := map[string][2][]string{
		"ks.redo_statement:-80": {{
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		}, {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		}},
		"ks.twopc_user:-80": {{
			"insert:[INT64(8) VARCHAR(\"bar\")]",
		}, {
			"insert:[INT64(10) VARCHAR(\"apa\")]",
		}},
		"ks.twopc_user:80-": {{
			"insert:[INT64(7) VARCHAR(\"foo\")]",
		}, {
			"insert:[INT64(9) VARCHAR(\"baz\")]",
		}},
	}

	compareMaps(t, expectations, logTable, flexiExpectations)
}

func compareMaps(t *testing.T, expected, actual map[string][]string, flexibleExp map[string][2][]string) {
	assert.Equal(t, len(expected), len(actual), "mismatch in number of keys: expected: %d, got: %d", len(expected), len(actual))

	for key, expectedValue := range expected {
		actualValue, ok := actual[key]
		require.Truef(t, ok, "key %s not found in actual map", key)

		if validValues, isFlexi := flexibleExp[key]; isFlexi {
			// For the flexible key, check if the actual value matches one of the valid values
			if !reflect.DeepEqual(actualValue, validValues[0]) && !reflect.DeepEqual(actualValue, validValues[1]) {
				t.Fatalf("mismatch in values for key '%s': expected one of: %v, got: %v", key, validValues, actualValue)
			}
		} else {
			// Sort the slices before comparison
			sort.Strings(expectedValue)
			sort.Strings(actualValue)
			assert.Equal(t, expectedValue, actualValue, "mismatch in values for key %s: expected: %v, got: %v", key, expectedValue, actualValue)
		}
	}
}
