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
	"fmt"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const (
	DebugDelayCommitShard = "VT_DELAY_COMMIT_SHARD"
	DebugDelayCommitTime  = "VT_DELAY_COMMIT_TIME"
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
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:-40": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-40": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
		"ks.twopc_user:-40": {
			`insert:[INT64(10) VARCHAR("apa")]`,
		},
		"ks.twopc_user:40-80": {
			`insert:[INT64(8) VARCHAR("bar")]`,
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
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:40-80": {"update:[INT64(8) VARCHAR(\"newfoo\")]"},
		"ks.twopc_user:80-":   {"update:[INT64(7) VARCHAR(\"newfoo\")]"},
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
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:-40": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-40": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:-40": {"delete:[INT64(10) VARCHAR(\"apa\")]"},
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
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "select * from twopc_user where id = 10")
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
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.twopc_user:80-": {"insert:[INT64(7) VARCHAR(\"foo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "select * from twopc_user where id = 10")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-2\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.twopc_user:80-": {"update:[INT64(7) VARCHAR(\"newfoo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where id = 7")
	utils.Exec(t, conn, "select * from twopc_user where id = 8")
	utils.Exec(t, conn, "select * from twopc_user where id = 10")
	utils.Exec(t, conn, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-3\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
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
		"ks.dt_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:40-80": {
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
		"ks.dt_state:40-80": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:40-80": {
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
		"ks.dt_state:40-80": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:40-80": {
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
	utils.Exec(t, conn2, "insert into twopc_user(id, name) values(18,'apa')")

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
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": { /* flexi Expectation */ },
		"ks.twopc_user:40-80":     { /* flexi Expectation */ },
		"ks.twopc_user:80-":       { /* flexi Expectation */ },
	}
	flexiExpectations := map[string][2][]string{
		"ks.redo_statement:40-80": {{
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		}, {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (18, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (18, 'apa')\")]",
		}},
		"ks.twopc_user:40-80": {{
			"insert:[INT64(8) VARCHAR(\"bar\")]",
		}, {
			"insert:[INT64(18) VARCHAR(\"apa\")]",
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

// TestDTResolveAfterMMCommit tests that transaction is committed on recovery
// failure after MM commit.
func TestDTResolveAfterMMCommit(t *testing.T) {
	defer cleanup(t)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	conn := vtgateConn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Insert into multiple shards
	_, err = conn.Execute(qCtx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(9,'baz')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'apa')", nil)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail After MM commit")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during metadata manager commit; transaction will be committed/rollbacked based on the state on recovery",
		false, "COMMIT", "ks:40-80,ks:-40")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:-40": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-40": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
		"ks.twopc_user:-40": {
			`insert:[INT64(10) VARCHAR("apa")]`,
		},
		"ks.twopc_user:40-80": {
			`insert:[INT64(8) VARCHAR("bar")]`,
		},
		"ks.twopc_user:80-": {
			`insert:[INT64(7) VARCHAR("foo")]`,
			`insert:[INT64(9) VARCHAR("baz")]`,
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveAfterRMPrepare tests that transaction is rolled back on recovery
// failure after RM prepare and before MM commit.
func TestDTResolveAfterRMPrepare(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	conn := vtgateConn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Insert into multiple shards
	_, err = conn.Execute(qCtx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMPrepared_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail After RM prepared")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during transaction prepare phase; prepare transaction rollback attempted; conclude on recovery",
		true /* transaction concluded */, "", "")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveDuringRMPrepare tests that transaction is rolled back on recovery
// failure after semi RM prepare.
func TestDTResolveDuringRMPrepare(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	conn := vtgateConn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Insert into multiple shards
	_, err = conn.Execute(qCtx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'bar')", nil)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMPrepare_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail During RM prepare")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during transaction prepare phase; prepare transaction rollback attempted; conclude on recovery",
		true, "", "")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveDuringRMCommit tests that transaction is committed on recovery
// failure after semi RM commit.
func TestDTResolveDuringRMCommit(t *testing.T) {
	defer cleanup(t)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	conn := vtgateConn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Insert into multiple shards
	_, err = conn.Execute(qCtx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'apa')", nil)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMCommit_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail During RM commit")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during resource manager commit; transaction will be committed on recovery",
		false, "COMMIT", "ks:40-80,ks:-40")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:-40": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:-40": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
		"ks.twopc_user:-40": {
			`insert:[INT64(10) VARCHAR("apa")]`,
		},
		"ks.twopc_user:40-80": {
			`insert:[INT64(8) VARCHAR("bar")]`,
		},
		"ks.twopc_user:80-": {
			`insert:[INT64(7) VARCHAR("foo")]`,
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveAfterTransactionRecord tests that transaction is rolled back on recovery
// failure after TR created and before RM prepare.
func TestDTResolveAfterTransactionRecord(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	conn := vtgateConn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Insert into multiple shards
	_, err = conn.Execute(qCtx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("TRCreated_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail After TR created")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during transaction record creation; rollback attempted; conclude on recovery",
		false, "PREPARE", "ks:40-80")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"ROLLBACK\")]",
		},
		"ks.dt_participant:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

type warn struct {
	level string
	code  uint16
	msg   string
}

func toWarn(row sqltypes.Row) warn {
	code, _ := row[1].ToUint16()
	return warn{
		level: row[0].ToString(),
		code:  code,
		msg:   row[2].ToString(),
	}
}

type txStatus struct {
	dtid         string
	state        string
	rTime        string
	participants string
}

func toTxStatus(row sqltypes.Row) txStatus {
	return txStatus{
		dtid:         row[0].ToString(),
		state:        row[1].ToString(),
		rTime:        row[2].ToString(),
		participants: row[3].ToString(),
	}
}

func testWarningAndTransactionStatus(t *testing.T, conn *vtgateconn.VTGateSession, warnMsg string,
	txConcluded bool, txState string, txParticipants string) {
	t.Helper()

	qr, err := conn.Execute(context.Background(), "show warnings", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)

	// validate warning output
	w := toWarn(qr.Rows[0])
	assert.Equal(t, "Warning", w.level)
	assert.EqualValues(t, 302, w.code)
	assert.Contains(t, w.msg, warnMsg)

	// extract transaction ID
	indx := strings.Index(w.msg, " ")
	require.Greater(t, indx, 0)
	dtid := w.msg[:indx]

	qr, err = conn.Execute(context.Background(), fmt.Sprintf(`show transaction status for '%v'`, dtid), nil)
	require.NoError(t, err)

	// validate transaction status
	if txConcluded {
		require.Empty(t, qr.Rows)
	} else {
		tx := toTxStatus(qr.Rows[0])
		assert.Equal(t, dtid, tx.dtid)
		assert.Equal(t, txState, tx.state)
		assert.Equal(t, txParticipants, tx.participants)
	}
}

// TestReadingUnresolvedTransactions tests the reading of unresolved transactions
func TestReadingUnresolvedTransactions(t *testing.T) {
	testcases := []struct {
		name    string
		queries []string
	}{
		{
			name: "show transaction status for explicit keyspace",
			queries: []string{
				fmt.Sprintf("show unresolved transactions for %v", keyspaceName),
			},
		},
		{
			name: "show transaction status with use command",
			queries: []string{
				fmt.Sprintf("use %v", keyspaceName),
				"show unresolved transactions",
			},
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			conn, closer := start(t)
			defer closer()
			// Start an atomic transaction.
			utils.Exec(t, conn, "begin")
			// Insert rows such that they go to all the three shards. Given that we have sharded the table `twopc_t1` on reverse_bits
			// it is very easy to figure out what value will end up in which shard.
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(4, 4)")
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(6, 4)")
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(9, 4)")
			// We want to delay the commit on one of the shards to simulate slow commits on a shard.
			writeTestCommunicationFile(t, DebugDelayCommitShard, "80-")
			defer deleteFile(DebugDelayCommitShard)
			writeTestCommunicationFile(t, DebugDelayCommitTime, "5")
			defer deleteFile(DebugDelayCommitTime)
			// We will execute a commit in a go routine, because we know it will take some time to complete.
			// While the commit is ongoing, we would like to check that we see the unresolved transaction.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := utils.ExecAllowError(t, conn, "commit")
				if err != nil {
					log.Errorf("Error in commit - %v", err)
				}
			}()
			// Allow enough time for the commit to have started.
			time.Sleep(1 * time.Second)
			var lastRes *sqltypes.Result
			newConn, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			defer newConn.Close()
			for _, query := range testcase.queries {
				lastRes = utils.Exec(t, newConn, query)
			}
			require.NotNil(t, lastRes)
			require.Len(t, lastRes.Rows, 1)
			// This verifies that we already decided to commit the transaction, but it is still unresolved.
			assert.Contains(t, fmt.Sprintf("%v", lastRes.Rows), `VARCHAR("COMMIT")`)
			// Wait for the commit to have returned.
			wg.Wait()
		})
	}
}

// TestDisruptions tests that atomic transactions persevere through various disruptions.
func TestDisruptions(t *testing.T) {
	testcases := []struct {
		disruptionName  string
		commitDelayTime string
		disruption      func() error
	}{
		{
			disruptionName:  "No Disruption",
			commitDelayTime: "1",
			disruption: func() error {
				return nil
			},
		},
		{
			disruptionName:  "PlannedReparentShard",
			commitDelayTime: "5",
			disruption:      prsShard3,
		},
	}
	for _, tt := range testcases {
		t.Run(fmt.Sprintf("%s-%ss timeout", tt.disruptionName, tt.commitDelayTime), func(t *testing.T) {
			// Reparent all the shards to first tablet being the primary.
			reparentToFistTablet(t)
			// cleanup all the old data.
			conn, closer := start(t)
			defer closer()
			// Start an atomic transaction.
			utils.Exec(t, conn, "begin")
			// Insert rows such that they go to all the three shards. Given that we have sharded the table `twopc_t1` on reverse_bits
			// it is very easy to figure out what value will end up in which shard.
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(4, 4)")
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(6, 4)")
			utils.Exec(t, conn, "insert into twopc_t1(id, col) values(9, 4)")
			// We want to delay the commit on one of the shards to simulate slow commits on a shard.
			writeTestCommunicationFile(t, DebugDelayCommitShard, "80-")
			defer deleteFile(DebugDelayCommitShard)
			writeTestCommunicationFile(t, DebugDelayCommitTime, tt.commitDelayTime)
			defer deleteFile(DebugDelayCommitTime)
			// We will execute a commit in a go routine, because we know it will take some time to complete.
			// While the commit is ongoing, we would like to run the disruption.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := utils.ExecAllowError(t, conn, "commit")
				if err != nil {
					log.Errorf("Error in commit - %v", err)
				}
			}()
			// Allow enough time for the commit to have started.
			time.Sleep(1 * time.Second)
			// Run the disruption.
			err := tt.disruption()
			require.NoError(t, err)
			// Wait for the commit to have returned. We don't actually check for an error in the commit because the user might receive an error.
			// But since we are waiting in CommitPrepared, the decision to commit the transaction should have already been taken.
			wg.Wait()
			// Check the data in the table.
			waitForResults(t, "select id, col from twopc_t1 where col = 4 order by id", `[[INT64(4) INT64(4)] [INT64(6) INT64(4)] [INT64(9) INT64(4)]]`, 10*time.Second)
		})
	}
}

// reparentToFistTablet reparents all the shards to first tablet being the primary.
func reparentToFistTablet(t *testing.T) {
	ks := clusterInstance.Keyspaces[0]
	for _, shard := range ks.Shards {
		primary := shard.Vttablets[0]
		err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, primary.Alias)
		require.NoError(t, err)
	}
}

// writeTestCommunicationFile writes the content to the file with the given name.
// We use these files to coordinate with the vttablets running in the debug mode.
func writeTestCommunicationFile(t *testing.T, fileName string, content string) {
	err := os.WriteFile(path.Join(os.Getenv("VTDATAROOT"), fileName), []byte(content), 0644)
	require.NoError(t, err)
}

// deleteFile deletes the file specified.
func deleteFile(fileName string) {
	_ = os.Remove(path.Join(os.Getenv("VTDATAROOT"), fileName))
}

// waitForResults waits for the results of the query to be as expected.
func waitForResults(t *testing.T, query string, resultExpected string, waitTime time.Duration) {
	timeout := time.After(waitTime)
	for {
		select {
		case <-timeout:
			t.Fatalf("didn't reach expected results for %s", query)
		default:
			ctx := context.Background()
			conn, err := mysql.Connect(ctx, &vtParams)
			require.NoError(t, err)
			res := utils.Exec(t, conn, query)
			conn.Close()
			if fmt.Sprintf("%v", res.Rows) == resultExpected {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

/*
Cluster Level Disruptions for the fuzzer
*/

// prsShard3 runs a PRS in shard 3 of the keyspace. It promotes the second tablet to be the new primary.
func prsShard3() error {
	shard := clusterInstance.Keyspaces[0].Shards[2]
	newPrimary := shard.Vttablets[1]
	return clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, newPrimary.Alias)
}
