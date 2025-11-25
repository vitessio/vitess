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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/callerid"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

// TestDynamicConfig tests that transaction mode is dynamically configurable.
func TestDynamicConfig(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	defer conn.Close()

	// Ensure that initially running a distributed transaction is possible.
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(4, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(6, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(9, 4)")
	utils.Exec(t, conn, "commit")

	clusterInstance.VtgateProcess.Config.TransactionMode = "SINGLE"
	defer func() {
		clusterInstance.VtgateProcess.Config.TransactionMode = "TWOPC"
		err := clusterInstance.VtgateProcess.RewriteConfiguration()
		require.NoError(t, err)
	}()
	err := clusterInstance.VtgateProcess.RewriteConfiguration()
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForConfig(`"transaction_mode":"SINGLE"`)
	require.NoError(t, err)

	// After the config changes verify running a distributed transaction fails.
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(20, 4)")
	_, err = utils.ExecAllowError(t, conn, "insert into twopc_t1(id, col) values(22, 4)")
	require.ErrorContains(t, err, "multi-db transaction attempted")
	utils.Exec(t, conn, "rollback")
}

// TestDTCommit tests distributed transaction commit for insert, update and delete operations
// It verifies the binlog events for the same with transaction state changes and redo statements.
func TestDTCommit(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
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
	utils.Exec(t, conn, `set time_zone="+10:30"`)
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
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"set time_zone = '+10:30'\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"set time_zone = '+10:30'\")]",
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
			"insert:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"insert:[VARCHAR(\"dtid-2\") INT64(2) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(2) BLOB(\"update twopc_user set `name` = 'newfoo' where id = 8 limit 10001 /* INT64 */\")]",
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
			"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"insert:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"set time_zone = '+10:30'\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from twopc_user where id = 10 limit 10001 /* INT64 */\")]",
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
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
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

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
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

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
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

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
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
	initconn, closer := start(t)
	defer closer()

	// Do an insertion into a table that has a consistent lookup vindex.
	utils.Exec(t, initconn, "insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)")

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
	_, err = conn.Execute(qCtx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(9,'baz')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'apa')", nil, false)
	require.NoError(t, err)
	// Also do an update to a table that has a consistent lookup vindex.
	// We expect to see only the pre-session changes in the logs.
	_, err = conn.Execute(qCtx, "update twopc_consistent_lookup set col = 22 where id = 4", nil, false)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
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
			"insert:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"update twopc_consistent_lookup set col = 22 where id = 4 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (10, 'apa')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) BLOB(\"update twopc_consistent_lookup set col = 22 where id = 4 limit 10001 /* INT64 */\")]",
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
		"ks.consistent_lookup:-40": {
			"insert:[INT64(22) INT64(4) VARBINARY(\" \\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
		},
		"ks.twopc_consistent_lookup:-40": {
			"update:[INT64(4) INT64(22) INT64(6)]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveAfterRMPrepare tests that transaction is rolled back on recovery
// failure after RM prepare and before MM commit.
func TestDTResolveAfterRMPrepare(t *testing.T) {
	initconn, closer := start(t)
	defer closer()

	// Do an insertion into a table that has a consistent lookup vindex.
	utils.Exec(t, initconn, "insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)")

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
	_, err = conn.Execute(qCtx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil, false)
	require.NoError(t, err)
	// Also do an update to a table that has a consistent lookup vindex.
	// We expect to see only the pre-session changes in the logs.
	_, err = conn.Execute(qCtx, "update twopc_consistent_lookup set col = 22 where id = 4", nil, false)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMPrepared_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
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
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"-40\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_state:-40": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
		"ks.redo_statement:-40": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"update twopc_consistent_lookup set col = 22 where id = 4 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"update twopc_consistent_lookup set col = 22 where id = 4 limit 10001 /* INT64 */\")]",
		},
		"ks.consistent_lookup:-40": {
			"insert:[INT64(22) INT64(4) VARBINARY(\" \\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestDTResolveDuringRMPrepare tests that transaction is rolled back on recovery
// failure after semi RM prepare.
func TestDTResolveDuringRMPrepare(t *testing.T) {
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
	_, err = conn.Execute(qCtx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'bar')", nil, false)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMPrepare_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
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
	_, err = conn.Execute(qCtx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(10,'apa')", nil, false)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("RMCommit_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
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
	_, err = conn.Execute(qCtx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(7,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(qCtx, "insert into twopc_user(id, name) values(8,'bar')", nil, false)
	require.NoError(t, err)

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("TRCreated_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
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

	qr, err := conn.Execute(context.Background(), "show warnings", nil, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)

	// validate warning output
	w := twopcutil.ToWarn(qr.Rows[0])
	assert.Equal(t, "Warning", w.Level)
	assert.EqualValues(t, 302, w.Code)
	assert.Contains(t, w.Msg, warnMsg)

	// extract transaction ID
	indx := strings.Index(w.Msg, " ")
	require.Greater(t, indx, 0)
	dtid := w.Msg[:indx]

	qr, err = conn.Execute(context.Background(), fmt.Sprintf(`show transaction status for '%v'`, dtid), nil, false)
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
	defer cleanup(t)

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
			twopcutil.WriteTestCommunicationFile(t, twopcutil.DebugDelayCommitShard, "80-")
			defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitShard)
			twopcutil.WriteTestCommunicationFile(t, twopcutil.DebugDelayCommitTime, "5")
			defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitTime)
			// We will execute a commit in a go routine, because we know it will take some time to complete.
			// While the commit is ongoing, we would like to check that we see the unresolved transaction.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := utils.ExecAllowError(t, conn, "commit")
				if err != nil {
					fmt.Println("Error in commit: ", err.Error())
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

// TestDTSavepointWithVanilaMySQL ensures that distributed transactions should work with savepoint as with vanila MySQL
func TestDTSavepointWithVanilaMySQL(t *testing.T) {
	mcmp, closer := startWithMySQL(t)
	defer closer()

	// internal savepoint
	mcmp.Exec("begin")
	mcmp.Exec("insert into twopc_user(id, name) values(7,'foo'), (8,'bar')")
	mcmp.Exec("commit")
	mcmp.Exec("select * from twopc_user order by id")

	// external savepoint, single shard transaction.
	mcmp.Exec("begin")
	mcmp.Exec("savepoint a")
	mcmp.Exec("insert into twopc_user(id, name) values(9,'baz')")
	mcmp.Exec("savepoint b")
	mcmp.Exec("rollback to b")
	mcmp.Exec("commit")
	mcmp.Exec("select * from twopc_user order by id")

	// external savepoint, multi-shard transaction.
	mcmp.Exec("begin")
	mcmp.Exec("savepoint a")
	mcmp.Exec("insert into twopc_user(id, name) values(10,'apa')")
	mcmp.Exec("savepoint b")
	mcmp.Exec("update twopc_user set name = 'temp' where id = 7")
	mcmp.Exec("rollback to a")
	mcmp.Exec("commit")
	mcmp.Exec("select * from twopc_user order by id")

	// external savepoint, multi-shard transaction.
	mcmp.Exec("begin")
	mcmp.Exec("savepoint a")
	mcmp.Exec("insert into twopc_user(id, name) values(10,'apa')")
	mcmp.Exec("savepoint b")
	mcmp.Exec("update twopc_user set name = 'temp' where id = 7")
	mcmp.Exec("rollback to b")
	mcmp.Exec("commit")
	mcmp.Exec("select * from twopc_user order by id")

	// external savepoint, multi-shard transaction.
	mcmp.Exec("begin")
	mcmp.Exec("update twopc_user set name = 'temp1' where id = 10")
	mcmp.Exec("savepoint b")
	mcmp.Exec("update twopc_user set name = 'temp2' where id = 7")
	mcmp.Exec("commit")
	mcmp.Exec("select * from twopc_user order by id")
}

// TestDTSavepoint tests distributed transaction should work with savepoint.
func TestDTSavepoint(t *testing.T) {
	defer cleanup(t)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	ss := vtgateConn.Session("", nil)

	// internal savepoint
	execute(ctx, t, ss, "begin")
	execute(ctx, t, ss, "insert into twopc_user(id, name) values(7,'foo'), (8,'bar')")
	execute(ctx, t, ss, "commit")

	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	logTable := retrieveTransitions(t, ch, tableMap, dtMap)
	expectations := map[string][]string{
		"ks.dt_participant:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.dt_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
		},
		"ks.twopc_user:40-80": {"insert:[INT64(8) VARCHAR(\"bar\")]"},
		"ks.twopc_user:80-":   {"insert:[INT64(7) VARCHAR(\"foo\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// external savepoint, single shard transaction.
	execute(ctx, t, ss, "begin")
	execute(ctx, t, ss, "savepoint a")
	execute(ctx, t, ss, "insert into twopc_user(id, name) values(9,'baz')")
	execute(ctx, t, ss, "savepoint b")
	execute(ctx, t, ss, "rollback to b")
	execute(ctx, t, ss, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.twopc_user:80-": {"insert:[INT64(9) VARCHAR(\"baz\")]"}}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// external savepoint, multi-shard transaction - rollback to a savepoint that leaves no change.
	execute(ctx, t, ss, "begin")
	execute(ctx, t, ss, "savepoint a")
	execute(ctx, t, ss, "insert into twopc_user(id, name) values(10,'apa')")
	execute(ctx, t, ss, "savepoint b")
	execute(ctx, t, ss, "update twopc_user set name = 'temp' where id = 7")
	execute(ctx, t, ss, "rollback to a")
	execute(ctx, t, ss, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_participant:-40": {
			"insert:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-2\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.dt_state:-40": {
			"insert:[VARCHAR(\"dtid-2\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-2\") VARCHAR(\"COMMIT\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// external savepoint, multi-shard transaction - rollback to a savepoint that leaves a change.
	execute(ctx, t, ss, "begin")
	execute(ctx, t, ss, "savepoint a")
	execute(ctx, t, ss, "insert into twopc_user(id, name) values(10,'apa')")
	execute(ctx, t, ss, "savepoint b")
	execute(ctx, t, ss, "update twopc_user set name = 'temp' where id = 7")
	execute(ctx, t, ss, "rollback to b")
	execute(ctx, t, ss, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_participant:-40": {
			"insert:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-3\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.dt_state:-40": {
			"insert:[VARCHAR(\"dtid-3\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-3\") VARCHAR(\"COMMIT\")]",
		},
		"ks.twopc_user:-40": {"insert:[INT64(10) VARCHAR(\"apa\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))

	// external savepoint, multi-shard transaction - savepoint added later and rollback to it.
	execute(ctx, t, ss, "begin")
	execute(ctx, t, ss, "update twopc_user set name = 'temp1' where id = 7")
	execute(ctx, t, ss, "savepoint c")
	execute(ctx, t, ss, "update twopc_user set name = 'temp2' where id = 8")
	execute(ctx, t, ss, "rollback to c")
	execute(ctx, t, ss, "commit")

	logTable = retrieveTransitions(t, ch, tableMap, dtMap)
	expectations = map[string][]string{
		"ks.dt_participant:40-80": {
			"insert:[VARCHAR(\"dtid-4\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-4\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.dt_state:40-80": {
			"insert:[VARCHAR(\"dtid-4\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-4\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-4\") VARCHAR(\"COMMIT\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-4\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-4\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-4\") INT64(1) BLOB(\"update twopc_user set `name` = 'temp1' where id = 7 limit 10001 /* INT64 */\")]",
			"delete:[VARCHAR(\"dtid-4\") INT64(1) BLOB(\"update twopc_user set `name` = 'temp1' where id = 7 limit 10001 /* INT64 */\")]",
		},
		"ks.twopc_user:80-": {"update:[INT64(7) VARCHAR(\"temp1\")]"},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

func execute(ctx context.Context, t *testing.T, ss *vtgateconn.VTGateSession, sql string) {
	t.Helper()

	err := executeReturnError(ctx, t, ss, sql)
	require.NoError(t, err)
}

func executeReturnError(ctx context.Context, t *testing.T, ss *vtgateconn.VTGateSession, sql string) error {
	t.Helper()

	if sql == "commit" {
		// sort by shard
		sortShard(ss)
	}
	_, err := ss.Execute(ctx, sql, nil, false)
	return err
}

func sortShard(ss *vtgateconn.VTGateSession) {
	sort.Slice(ss.SessionPb().ShardSessions, func(i, j int) bool {
		return ss.SessionPb().ShardSessions[i].Target.Shard < ss.SessionPb().ShardSessions[j].Target.Shard
	})
}

// TestDTSavepointResolveAfterMMCommit tests that transaction is committed on recovery
// failure after MM commit involving savepoint.
func TestDTSavepointResolveAfterMMCommit(t *testing.T) {
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

	// initial insert
	for i := 1; i <= 100; i++ {
		execute(qCtx, t, conn, fmt.Sprintf("insert into twopc_user(id, name) values(%d,'foo')", 10*i))
	}

	// ignore initial change
	tableMap := make(map[string][]*querypb.Field)
	dtMap := make(map[string]string)
	_ = retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)

	// Insert into multiple shards
	execute(qCtx, t, conn, "begin")
	execute(qCtx, t, conn, "insert into twopc_user(id, name) values(7,'foo'),(8,'bar')")
	execute(qCtx, t, conn, "savepoint a")
	for i := 1; i <= 100; i++ {
		execute(qCtx, t, conn, fmt.Sprintf("insert ignore into twopc_user(id, name) values(%d,'baz')", 12+i))
	}
	execute(qCtx, t, conn, "savepoint b")
	execute(qCtx, t, conn, "insert into twopc_user(id, name) values(11,'apa')")
	execute(qCtx, t, conn, "rollback to a")

	// The caller ID is used to simulate the failure at the desired point.
	newCtx := callerid.NewContext(qCtx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	err = executeReturnError(newCtx, t, conn, "commit")
	require.ErrorContains(t, err, "Fail After MM commit")

	testWarningAndTransactionStatus(t, conn,
		"distributed transaction ID failed during metadata manager commit; transaction will be committed/rollbacked based on the state on recovery",
		false, "COMMIT", "ks:40-80,ks:80-")

	// 2nd session to write something on different primary key, this should continue to work.
	conn2 := vtgateConn.Session("", nil)
	execute(qCtx, t, conn2, "insert into twopc_user(id, name) values(190001,'mysession')")
	execute(qCtx, t, conn2, "insert into twopc_user(id, name) values(290001,'mysession')")

	// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
	logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
	expectations := map[string][]string{
		"ks.dt_participant:-40": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"insert:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) VARCHAR(\"ks\") VARCHAR(\"40-80\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(2) VARCHAR(\"ks\") VARCHAR(\"80-\")]",
		},
		"ks.dt_state:-40": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"update:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"COMMIT\")]",
		},
		"ks.redo_state:40-80": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_state:80-": {
			"insert:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
			"delete:[VARCHAR(\"dtid-1\") VARCHAR(\"PREPARE\")]",
		},
		"ks.redo_statement:40-80": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (8, 'bar')\")]",
		},
		"ks.redo_statement:80-": {
			"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
			"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"insert into twopc_user(id, `name`) values (7, 'foo')\")]",
		},
		"ks.twopc_user:-40": {
			"insert:[INT64(290001) VARCHAR(\"mysession\")]",
		},
		"ks.twopc_user:40-80": {
			"insert:[INT64(190001) VARCHAR(\"mysession\")]",
			"insert:[INT64(8) VARCHAR(\"bar\")]",
		},
		"ks.twopc_user:80-": {
			"insert:[INT64(7) VARCHAR(\"foo\")]",
		},
	}
	assert.Equal(t, expectations, logTable,
		"mismatch expected: \n got: %s, want: %s", prettyPrint(logTable), prettyPrint(expectations))
}

// TestSemiSyncRequiredWithTwoPC tests that semi-sync is required when using two-phase commit.
func TestSemiSyncRequiredWithTwoPC(t *testing.T) {
	// cleanup all the old data.
	conn, closer := start(t)
	defer closer()
	defer func() {
		reparentAllShards(t, clusterInstance, 0)
	}()

	reparentAllShards(t, clusterInstance, 0)
	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=none")
	require.NoError(t, err, out)
	// After changing the durability policy for the given keyspace to none, we run PRS to ensure the changes have taken effect.
	reparentAllShards(t, clusterInstance, 1)

	// A new distributed transaction should fail.
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(4, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(6, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(9, 4)")
	_, err = utils.ExecAllowError(t, conn, "commit")
	require.Error(t, err)
	require.ErrorContains(t, err, "two-pc is enabled, but semi-sync is not")

	_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err)
	reparentAllShards(t, clusterInstance, 0)

	// Transaction should now succeed.
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(4, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(6, 4)")
	utils.Exec(t, conn, "insert into twopc_t1(id, col) values(9, 4)")
	_, err = utils.ExecAllowError(t, conn, "commit")
	require.NoError(t, err)
}

// reparentAllShards reparents all the shards to the given tablet index for that shard.
func reparentAllShards(t *testing.T, clusterInstance *cluster.LocalProcessCluster, idx int) {
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, shard.Vttablets[idx].Alias)
		require.NoError(t, err)
	}
}

// TestReadTransactionStatus tests that read transaction state rpc works as expected.
func TestReadTransactionStatus(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	defer conn.Close()
	defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitShard)
	defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitTime)

	// We create a multi shard commit and delay its commit on one of the shards.
	// This allows us to query to transaction status and actually get some data back.
	var wg sync.WaitGroup
	twopcutil.RunMultiShardCommitWithDelay(t, conn, "10", &wg, []string{
		"begin",
		"insert into twopc_t1(id, col) values(4, 4)",
		"insert into twopc_t1(id, col) values(6, 4)",
		"insert into twopc_t1(id, col) values(9, 4)",
	})

	// Create a tablet manager client and use it to read the transaction state.
	tmc := grpctmclient.NewClient()
	defer tmc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	primaryTablet := getTablet(clusterInstance.Keyspaces[0].Shards[2].FindPrimaryTablet().GrpcPort)
	// Wait for the transaction to show up in the unresolved list.
	var unresTransaction *querypb.TransactionMetadata
	timeout := time.After(10 * time.Second)
	for {
		for _, shard := range clusterInstance.Keyspaces[0].Shards {
			urtRes, err := tmc.GetUnresolvedTransactions(ctx, getTablet(shard.FindPrimaryTablet().GrpcPort), 1)
			require.NoError(t, err)
			if len(urtRes) > 0 {
				unresTransaction = urtRes[0]
			}
		}
		if unresTransaction != nil {
			break
		}
		select {
		case <-timeout:
			require.Fail(t, "timed out waiting for unresolved transaction")
		default:
		}
	}
	require.NotNil(t, unresTransaction)
	res, err := tmc.GetTransactionInfo(ctx, primaryTablet, unresTransaction.Dtid)
	require.NoError(t, err)
	assert.Equal(t, "PREPARED", res.State)
	assert.Equal(t, "", res.Message)
	assert.Equal(t, []string{"insert into twopc_t1(id, col) values (9, 4)"}, res.Statements)

	// Also try running the RPC from vtctld and verify we see the same values.
	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("DistributedTransaction",
		"Read",
		"--dtid="+unresTransaction.Dtid,
	)
	require.NoError(t, err)
	require.Contains(t, out, "insert into twopc_t1(id, col) values (9, 4)")
	require.Contains(t, out, unresTransaction.Dtid)

	// Read the data from vtadmin API, and verify that too has the same information.
	apiRes := clusterInstance.VtadminProcess.MakeAPICallRetry(t, fmt.Sprintf("/api/transaction/local/%v/info", unresTransaction.Dtid))
	require.Contains(t, apiRes, "insert into twopc_t1(id, col) values (9, 4)")
	require.Contains(t, apiRes, unresTransaction.Dtid)
	require.Contains(t, apiRes, strconv.FormatInt(res.TimeCreated, 10))

	// Wait for the commit to have returned.
	wg.Wait()
}

// TestVindexes tests that different vindexes work well with two-phase commit.
func TestVindexes(t *testing.T) {
	testcases := []struct {
		name        string
		initQueries []string
		testQueries []string
		logExpected map[string][]string
	}{
		{
			name: "Lookup Single Update",
			initQueries: []string{
				"insert into twopc_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"update twopc_lookup set col = 9 where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"insert into lookup(col, id, keyspace_id) values (9, 6, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"insert into lookup(col, id, keyspace_id) values (9, 6, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
				},
				"ks.twopc_lookup:40-80": {
					"update:[INT64(6) INT64(9) INT64(9)]",
				},
				"ks.lookup:80-": {
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"insert:[INT64(9) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Lookup-Unique Single Update",
			initQueries: []string{
				"insert into twopc_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"update twopc_lookup set col_unique = 20 where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup_unique where col_unique = 9 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"insert into lookup_unique(col_unique, keyspace_id) values (20, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup_unique where col_unique = 9 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"insert into lookup_unique(col_unique, keyspace_id) values (20, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
				},
				"ks.twopc_lookup:40-80": {
					"update:[INT64(6) INT64(4) INT64(20)]",
				},
				"ks.lookup_unique:80-": {
					"delete:[INT64(9) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"insert:[INT64(20) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Lookup And Lookup-Unique Single Delete",
			initQueries: []string{
				"insert into twopc_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"delete from twopc_lookup where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from lookup_unique where col_unique = 9 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from lookup_unique where col_unique = 9 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
				},
				"ks.twopc_lookup:40-80": {
					"delete:[INT64(6) INT64(4) INT64(9)]",
				},
				"ks.lookup_unique:80-": {
					"delete:[INT64(9) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.lookup:80-": {
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Lookup And Lookup-Unique Single Insertion",
			initQueries: []string{
				"insert into twopc_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"insert into twopc_lookup(id, col, col_unique) values(20, 4, 22)",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"insert into lookup(col, id, keyspace_id) values (4, 20, _binary'(\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"insert into lookup(col, id, keyspace_id) values (4, 20, _binary'(\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
				},
				"ks.lookup:80-": {
					"insert:[INT64(4) INT64(20) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.lookup_unique:-40": {
					"insert:[INT64(22) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.twopc_lookup:-40": {
					"insert:[INT64(20) INT64(4) INT64(22)]",
				},
			},
		},
		{
			name: "Lookup And Lookup-Unique Mix",
			initQueries: []string{
				"insert into twopc_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"insert into twopc_lookup(id, col, col_unique) values(20, 4, 22)",
				"update twopc_lookup set col = 9 where col_unique = 9",
				"delete from twopc_lookup where id = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"insert into lookup(col, id, keyspace_id) values (4, 20, _binary'(\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(3) BLOB(\"insert into lookup(col, id, keyspace_id) values (9, 6, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(4) BLOB(\"delete from lookup where col = 4 and id = 9 and keyspace_id = _binary'\\x90\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(5) BLOB(\"delete from lookup_unique where col_unique = 4 and keyspace_id = _binary'\\x90\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"insert:[VARCHAR(\"dtid-3\") INT64(6) BLOB(\"delete from twopc_lookup where id = 9 limit 10001 /* INT64 */\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"insert into lookup(col, id, keyspace_id) values (4, 20, _binary'(\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(2) BLOB(\"delete from lookup where col = 4 and id = 6 and keyspace_id = _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(3) BLOB(\"insert into lookup(col, id, keyspace_id) values (9, 6, _binary'`\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0')\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(4) BLOB(\"delete from lookup where col = 4 and id = 9 and keyspace_id = _binary'\\x90\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(5) BLOB(\"delete from lookup_unique where col_unique = 4 and keyspace_id = _binary'\\x90\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0' limit 10001\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(6) BLOB(\"delete from twopc_lookup where id = 9 limit 10001 /* INT64 */\")]",
				},
				"ks.redo_statement:40-80": {
					"insert:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"update twopc_lookup set col = 9 where col_unique = 9 limit 10001 /* INT64 */\")]",
					"delete:[VARCHAR(\"dtid-3\") INT64(1) BLOB(\"update twopc_lookup set col = 9 where col_unique = 9 limit 10001 /* INT64 */\")]",
				},
				"ks.twopc_lookup:-40": {
					"insert:[INT64(20) INT64(4) INT64(22)]",
				},
				"ks.twopc_lookup:40-80": {
					"update:[INT64(6) INT64(9) INT64(9)]",
				},
				"ks.twopc_lookup:80-": {
					"delete:[INT64(9) INT64(4) INT64(4)]",
				},
				"ks.lookup_unique:-40": {
					"insert:[INT64(22) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.lookup_unique:80-": {
					"delete:[INT64(4) VARBINARY(\"\\x90\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.lookup:80-": {
					"insert:[INT64(4) INT64(20) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"insert:[INT64(9) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(4) INT64(9) VARBINARY(\"\\x90\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Consistent Lookup Single Update",
			initQueries: []string{
				"insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"update twopc_consistent_lookup set col = 9 where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.twopc_consistent_lookup:40-80": {
					"update:[INT64(6) INT64(9) INT64(9)]",
				},
				"ks.consistent_lookup:80-": {
					"insert:[INT64(9) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Consistent Lookup-Unique Single Update",
			initQueries: []string{
				"insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"update twopc_consistent_lookup set col_unique = 20 where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.twopc_consistent_lookup:40-80": {
					"update:[INT64(6) INT64(4) INT64(20)]",
				},
				"ks.consistent_lookup_unique:80-": {
					"insert:[INT64(20) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(9) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Consistent Lookup And Consistent Lookup-Unique Single Delete",
			initQueries: []string{
				"insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"delete from twopc_consistent_lookup where col_unique = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.twopc_consistent_lookup:40-80": {
					"delete:[INT64(6) INT64(4) INT64(9)]",
				},
				"ks.consistent_lookup_unique:80-": {
					"delete:[INT64(9) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.consistent_lookup:80-": {
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
		{
			name: "Consistent Lookup And Consistent Lookup-Unique Mix",
			initQueries: []string{
				"insert into twopc_consistent_lookup(id, col, col_unique) values(4, 4, 6)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(6, 4, 9)",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(9, 4, 4)",
			},
			testQueries: []string{
				"begin",
				"insert into twopc_consistent_lookup(id, col, col_unique) values(20, 4, 22)",
				"update twopc_consistent_lookup set col = 9 where col_unique = 9",
				"delete from twopc_consistent_lookup where id = 9",
				"commit",
			},
			logExpected: map[string][]string{
				"ks.redo_statement:80-": {
					"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"delete from twopc_consistent_lookup where id = 9 limit 10001 /* INT64 */\")]",
					"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"delete from twopc_consistent_lookup where id = 9 limit 10001 /* INT64 */\")]",
				},
				"ks.redo_statement:40-80": {
					"insert:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"update twopc_consistent_lookup set col = 9 where col_unique = 9 limit 10001 /* INT64 */\")]",
					"delete:[VARCHAR(\"dtid-1\") INT64(1) BLOB(\"update twopc_consistent_lookup set col = 9 where col_unique = 9 limit 10001 /* INT64 */\")]",
				},
				"ks.twopc_consistent_lookup:-40": {
					"insert:[INT64(20) INT64(4) INT64(22)]",
				},
				"ks.twopc_consistent_lookup:40-80": {
					"update:[INT64(6) INT64(9) INT64(9)]",
				},
				"ks.twopc_consistent_lookup:80-": {
					"delete:[INT64(9) INT64(4) INT64(4)]",
				},
				"ks.consistent_lookup_unique:-40": {
					"insert:[INT64(22) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.consistent_lookup_unique:80-": {
					"delete:[INT64(4) VARBINARY(\"\\x90\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
				"ks.consistent_lookup:80-": {
					"insert:[INT64(4) INT64(20) VARBINARY(\"(\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"insert:[INT64(9) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(4) INT64(6) VARBINARY(\"`\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
					"delete:[INT64(4) INT64(9) VARBINARY(\"\\x90\\x00\\x00\\x00\\x00\\x00\\x00\\x00\")]",
				},
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
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

			// initial insert
			for _, query := range tt.initQueries {
				execute(qCtx, t, conn, query)
			}

			// ignore initial change
			tableMap := make(map[string][]*querypb.Field)
			dtMap := make(map[string]string)
			_ = retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)

			// Insert into multiple shards
			for _, query := range tt.testQueries {
				execute(qCtx, t, conn, query)
			}

			// Below check ensures that the transaction is resolved by the resolver on receiving unresolved transaction signal from MM.
			logTable := retrieveTransitionsWithTimeout(t, ch, tableMap, dtMap, 2*time.Second)
			for key, val := range tt.logExpected {
				assert.EqualValues(t, val, logTable[key], key)
			}
		})
	}
}

func getTablet(tabletGrpcPort int) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}
