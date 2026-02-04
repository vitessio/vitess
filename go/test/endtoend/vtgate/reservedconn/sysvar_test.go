/*
Copyright 2020 The Vitess Authors.

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

package reservedconn

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestSetSysVarSingle(t *testing.T) {
	ctx := t.Context()
	type queriesWithExpectations struct {
		name, expr string
		expected   []string
	}

	queries := []queriesWithExpectations{{
		name:     "default_storage_engine", // ignored
		expr:     "INNODB",
		expected: []string{`[[VARCHAR("InnoDB")]]`},
	}, {
		name:     "character_set_client", // check and ignored
		expr:     "utf8mb4",
		expected: []string{`[[VARCHAR("utf8mb4")]]`, `[[VARCHAR("utf8")]]`},
	}, {
		name:     "character_set_client", // ignored so will keep the actual value
		expr:     "@charvar",
		expected: []string{`[[VARCHAR("utf8mb4")]]`, `[[VARCHAR("utf8")]]`},
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     "''",
		expected: []string{`[[VARCHAR("")]]`},
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     `concat(@@sql_mode,"NO_ZERO_DATE")`,
		expected: []string{`[[VARCHAR("NO_ZERO_DATE")]]`},
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     "@@sql_mode",
		expected: []string{`[[VARCHAR("NO_ZERO_DATE")]]`},
	}, {
		name:     "SQL_SAFE_UPDATES", // use reserved conn
		expr:     "1",
		expected: []string{"[[INT64(1)]]"},
	}, {
		name:     "sql_auto_is_null", // ignored so will keep the actual value
		expr:     "on",
		expected: []string{`[[INT64(0)]]`},
	}, {
		name:     "sql_notes", // use reserved conn
		expr:     "off",
		expected: []string{"[[INT64(0)]]"},
	}}

	conn, err := mysql.Connect(ctx, &vtParams)

	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		query := fmt.Sprintf("set %s = %s", q.name, q.expr)
		t.Run(fmt.Sprintf("%d-%s", i, query), func(t *testing.T) {
			utils.Exec(t, conn, query)
			utils.AssertMatchesAny(t, conn, "select @@"+q.name, q.expected...)
		})
	}
}

func TestSetSystemVariable(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set session sql_mode = 'NO_ZERO_DATE', session default_week_format = 1")
	q := `select str_to_date('00/00/0000', '%m/%d/%Y'), WEEK('2008-02-20')`
	utils.AssertMatches(t, conn, q, `[[NULL INT64(8)]]`)

	utils.AssertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)
	utils.Exec(t, conn, "set @@sql_mode = '', session default_week_format = 0")

	utils.AssertMatches(t, conn, q, `[[DATE("0000-00-00") INT64(7)]]`)

	utils.Exec(t, conn, "SET @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'),  @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483")
	utils.AssertMatches(t, conn, "select @@sql_mode", `[[VARCHAR(",STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO")]]`)
}

func TestSetSystemVarWithTxFailure(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// before changing any settings, let's confirm sql_safe_updates value
	utils.AssertMatches(t, conn, `select @@sql_safe_updates from test where id = 80`, `[[INT64(0)]]`)

	utils.Exec(t, conn, "set sql_safe_updates = 1")
	utils.Exec(t, conn, "begin")

	qr := utils.Exec(t, conn, "select connection_id() from test where id = 80")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet() // 80-
	vttablet1.VttabletProcess.QueryTablet("kill "+qr.Rows[0][0].ToString(), keyspaceName, false)

	// transaction fails on commit - we should no longer be in a transaction
	_, err = conn.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// we still want to have our system setting applied
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestSetSystemVarWithConnectionTimeout(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "delete from test")

	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	utils.Exec(t, conn, "set sql_safe_updates = 1")
	utils.AssertMatches(t, conn, "select @@sql_safe_updates from test where id = 80", "[[INT64(1)]]")

	// Connection timeout.
	time.Sleep(10 * time.Second)

	// connection has timed out, but vtgate will recreate the connection for us
	utils.AssertMatches(t, conn, "select @@sql_safe_updates from test where id = 80", "[[INT64(1)]]")
}

func TestSetSystemVariableAndThenSuccessfulTx(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "delete from test")

	utils.Exec(t, conn, "set sql_safe_updates = 1")
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
	utils.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestSetSystemVariableAndThenSuccessfulAutocommitDML(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, `delete from test`)

	utils.Exec(t, conn, `set sql_safe_updates = 1`)

	utils.Exec(t, conn, `insert into test (id, val1) values (80, null)`)
	utils.AssertMatches(t, conn, `select id, val1 from test`, `[[INT64(80) NULL]]`)
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	utils.Exec(t, conn, `update test set val2 = 2 where val1 is null`)
	utils.AssertMatches(t, conn, `select id, val1, val2 from test`, `[[INT64(80) NULL INT32(2)]]`)
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	utils.Exec(t, conn, `update test set val1 = 'text' where val1 is null`)
	utils.AssertMatches(t, conn, `select id, val1, val2 from test`, `[[INT64(80) VARCHAR("text") INT32(2)]]`)
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	utils.Exec(t, conn, `delete from test where val1 = 'text'`)
	utils.AssertMatches(t, conn, `select id, val1, val2 from test`, `[]`)
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestStartTxAndSetSystemVariableAndThenSuccessfulCommit(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "delete from test")

	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "set sql_safe_updates = 1")
	utils.Exec(t, conn, "insert into test (id, val1) values (54, null)")
	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(54) NULL]]")
	utils.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestSetSystemVarAutocommitWithConnError(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("For partial keyspaces, kill is called on the source keyspace but queries execute on the target, so this test will fail")
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from test")
	utils.Exec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	utils.Exec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	utils.AssertMatches(t, conn, "select id from test order by id", "[[INT64(1)] [INT64(4)]]")
	qr := utils.Exec(t, conn, "select connection_id() from test where id = 1")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet() // -80
	_, err = vttablet1.VttabletProcess.QueryTablet("kill "+qr.Rows[0][0].ToString(), keyspaceName, false)
	require.NoError(t, err)

	// first query to 80- shard should pass
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 4", "[[INT64(4) NULL]]")

	// first query to -80 shard will fail, but vtgate will auto-retry for us
	utils.Exec(t, conn, "insert into test (id, val1) values (2, null)")
	utils.AssertMatches(t, conn, "select id from test where id = 2", "[[INT64(2)]]")
	utils.AssertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 2", "[[INT64(2) INT64(1)]]")
}

func TestSetSystemVarInTxWithConnError(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from test")
	utils.Exec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	utils.Exec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	qr := utils.Exec(t, conn, "select connection_id() from test where id = 4")
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into test (id, val1) values (2, null)")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[1].PrimaryTablet() // 80-
	_, err = vttablet1.VttabletProcess.QueryTablet("kill "+qr.Rows[0][0].ToString(), keyspaceName, false)
	require.NoError(t, err)

	// query to -80 shard should pass and remain in transaction.
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 2", "[[INT64(2) NULL]]")
	utils.Exec(t, conn, "rollback")
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 2", "[]")

	// first query to -80 shard will fail, but vtgate should retry once and succeed the second time
	utils.Exec(t, conn, "select @@sql_safe_updates from test where id = 4")

	// subsequent queries on 80- will pass
	utils.AssertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 4", "[[INT64(4) INT64(1)]]")
}

func BenchmarkReservedConnFieldQuery(b *testing.B) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	defer conn.Close()

	utils.Exec(b, conn, "delete from test")
	utils.Exec(b, conn, "insert into test (id, val1) values (1, 'toto'), (4, 'tata')")

	// set sql_mode to empty to force the use of reserved connection
	utils.Exec(b, conn, "set sql_mode = ''")
	utils.AssertMatches(b, conn, "select 	@@sql_mode", `[[VARCHAR("")]]`)

	for b.Loop() {
		utils.Exec(b, conn, "select id, val1 from test")
	}
}

func TestEnableSystemSettings(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// test set @@enable_system_settings to false and true
	utils.Exec(t, conn, "set enable_system_settings = false")
	utils.AssertMatches(t, conn, `select @@enable_system_settings`, `[[INT64(0)]]`)
	utils.Exec(t, conn, "set enable_system_settings = true")
	utils.AssertMatches(t, conn, `select @@enable_system_settings`, `[[INT64(1)]]`)

	// prepare the @@sql_mode variable
	utils.Exec(t, conn, "set sql_mode = 'NO_ZERO_DATE'")
	utils.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)

	// check disabling @@enable_system_settings
	utils.Exec(t, conn, "set enable_system_settings = false")
	utils.Exec(t, conn, "set sql_mode = ''")                                          // attempting to set @@sql_mode to an empty string
	utils.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`) // @@sql_mode did not change

	// check enabling @@enable_system_settings
	utils.Exec(t, conn, "set enable_system_settings = true")
	utils.Exec(t, conn, "set sql_mode = ''")                              // changing @@sql_mode to empty string
	utils.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("")]]`) // @@sql_mode did change
}

// Tests type consitency through multiple queries
func TestSystemVariableType(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from test")
	utils.Exec(t, conn, "insert into test (id, val1, val2, val3) values (1, null, 0, 0)")

	// regardless of the "from", the select @@autocommit should return the same type
	query1 := "select @@autocommit"
	query2 := "select @@autocommit from test"

	utils.Exec(t, conn, "set autocommit = false")
	assertResponseMatch(t, conn, query1, query2)

	utils.Exec(t, conn, "set autocommit = true")
	assertResponseMatch(t, conn, query1, query2)
}

func TestSysvarSocket(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := utils.Exec(t, conn, "select @@socket")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "mysql.sock")

	_, err = utils.ExecAllowError(t, conn, "set socket = '/any/path'")
	require.Error(t, err)
	sqlErr, ok := err.(*sqlerror.SQLError)
	require.True(t, ok, "not a mysql error: %T", err)
	assert.Equal(t, sqlerror.ERIncorrectGlobalLocalVar, sqlErr.Number())
	assert.Equal(t, sqlerror.SSUnknownSQLState, sqlErr.SQLState())
	assert.Equal(t, "VT03010: variable 'socket' is a read only variable (errno 1238) (sqlstate HY000) during query: set socket = '/any/path'", sqlErr.Error())
}

func TestReservedConnInStreaming(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "delete from test")

	utils.Exec(t, conn, "set workload = olap")
	utils.Exec(t, conn, "set sql_safe_updates = 1")
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
	utils.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestUnifiedOlapAndOltp(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from test")
	checkOltpAndOlapInterchangingTx(t, conn)

	// modify some system settings to active reserved connection use.
	utils.Exec(t, conn, "set sql_safe_updates = 1")

	utils.Exec(t, conn, "delete from test")
	checkOltpAndOlapInterchangingTx(t, conn)
}

func checkOltpAndOlapInterchangingTx(t *testing.T, conn *mysql.Conn) {
	// start transaction in execute
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// move to streaming
	utils.Exec(t, conn, "set workload = olap")

	// checking data in streaming
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// rollback the tx
	utils.Exec(t, conn, "rollback")
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[]")

	// move back to oltp
	utils.Exec(t, conn, "set workload = oltp")
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[]")

	// move to streaming and start transaction
	utils.Exec(t, conn, "set workload = olap")
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// checking data in streaming
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// move back to oltp and commit the tx
	utils.Exec(t, conn, "set workload = oltp")
	utils.Exec(t, conn, "commit")

	// check in oltp
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// check in olap
	utils.Exec(t, conn, "set workload = oltp")
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")
}

func TestSysVarTxIsolation(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// will run every check twice to see that the isolation level is set for all the queries in the session and

	// default from mysql
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("REPEATABLE-READ")]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)
	// second run, ensuring it has the same value.
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)

	// setting to different value.
	utils.Exec(t, conn, "set @@transaction_isolation = 'read-committed'")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)

	// changing setting to different value.
	utils.Exec(t, conn, "set session transaction isolation level read uncommitted")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-UNCOMMITTED")]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-UNCOMMITTED`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-UNCOMMITTED`)

	// changing setting to different value.
	utils.Exec(t, conn, "set transaction isolation level serializable")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("SERIALIZABLE")]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `SERIALIZABLE`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	utils.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `SERIALIZABLE`)
}

// TestSetTxIsolationWithTabletAlias tests that SET TRANSACTION ISOLATION LEVEL
// works correctly when using tablet alias targeting mode.
// This is a regression test for an issue where setting the transaction isolation level
// while targeting a specific tablet would fail with MySQL error 1568:
// "Transaction characteristics can't be changed while a transaction is in progress"
func TestSetTxIsolationWithTabletAlias(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Find a primary tablet alias for the -80 shard
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// Target the specific tablet using tablet alias
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Set autocommit=0 which puts the session in "transaction" mode
	// This is the key condition that triggers the bug: with autocommit=0,
	// Vitess incorrectly uses ReserveBeginExecute which sends BEGIN before
	// the SET statement, causing MySQL error 1568.
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// Check initial state and connection ID
	qr := utils.Exec(t, conn, "SELECT @@transaction_isolation, @@autocommit, connection_id()")
	t.Logf("Initial state: %v", qr.Rows)

	// Test 1: Setting isolation level with autocommit=0 should work
	// This was failing with error 1568 because Vitess was incorrectly starting
	// a transaction before sending the SET statement to MySQL
	utils.Exec(t, conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	// Verify the isolation level was set
	qr = utils.Exec(t, conn, "SELECT @@transaction_isolation, connection_id()")
	t.Logf("After SET READ COMMITTED: %v", qr.Rows)
	utils.AssertMatches(t, conn, "SELECT @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)

	// Test 2: Change isolation level again
	utils.Exec(t, conn, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	qr = utils.Exec(t, conn, "SELECT @@transaction_isolation, connection_id()")
	t.Logf("After SET SERIALIZABLE: %v", qr.Rows)
	utils.AssertMatches(t, conn, "SELECT @@transaction_isolation", `[[VARCHAR("SERIALIZABLE")]]`)

	// Test 3: Use alternative syntax
	utils.Exec(t, conn, "SET @@transaction_isolation = 'READ-UNCOMMITTED'")
	qr = utils.Exec(t, conn, "SELECT @@transaction_isolation, connection_id()")
	t.Logf("After SET READ-UNCOMMITTED: %v", qr.Rows)
	utils.AssertMatches(t, conn, "SELECT @@transaction_isolation", `[[VARCHAR("READ-UNCOMMITTED")]]`)

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}

// TestSysVarInnodbWaitTimeout tests the innodb_lock_wait_timeout system variable
func TestSysVarInnodbWaitTimeout(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// default from mysql
	utils.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[UINT64(20)]]`)
	utils.AssertMatches(t, conn, "select @@global.innodb_lock_wait_timeout", `[[UINT64(20)]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@innodb_lock_wait_timeout", `UINT64(20)`)
	utils.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout", `UINT64(20)`)

	// setting to different value.
	utils.Exec(t, conn, "set @@innodb_lock_wait_timeout = 120")
	utils.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[INT64(120)]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout, connection_id()", `UINT64(20)`)
	utils.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(120)`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	utils.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(120)`)

	// changing setting to different value.
	utils.Exec(t, conn, "set @@innodb_lock_wait_timeout = 240")
	utils.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[INT64(240)]]`)
	// ensuring it goes to mysql
	utils.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout, connection_id()", `UINT64(20)`)
	utils.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(240)`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	utils.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(240)`)
}

// TestMultipleSetsWithAutocommitOff tests that multiple SET statements work correctly
// in sequence when autocommit=0 with tablet alias targeting.
func TestMultipleSetsWithAutocommitOff(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Find a primary tablet alias for the -80 shard
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// Target the specific tablet using tablet alias
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Set autocommit=0
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// Get initial connection ID
	qr := utils.Exec(t, conn, "SELECT connection_id()")
	initialConnID := qr.Rows[0][0].ToString()
	t.Logf("Initial connection ID: %s", initialConnID)

	// Multiple SET statements should work without error
	utils.Exec(t, conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	utils.Exec(t, conn, "SET @@sql_mode = ''")
	utils.Exec(t, conn, "SET @@wait_timeout = 28800")

	// Verify all settings are applied correctly
	utils.AssertMatches(t, conn, "SELECT @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	utils.AssertMatches(t, conn, "SELECT @@sql_mode", `[[VARCHAR("")]]`)
	utils.AssertMatches(t, conn, "SELECT @@wait_timeout", `[[INT64(28800)]]`)

	// Verify connection ID is consistent (same reserved connection)
	qr = utils.Exec(t, conn, "SELECT connection_id()")
	finalConnID := qr.Rows[0][0].ToString()
	assert.Equal(t, initialConnID, finalConnID, "connection ID should remain consistent")

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}

// TestShowStatementsWithAutocommitOff tests that SHOW statements work correctly
// when autocommit=0 with tablet alias targeting.
func TestShowStatementsWithAutocommitOff(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Find a primary tablet alias for the -80 shard
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// Target the specific tablet using tablet alias
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Set autocommit=0
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// SHOW statements should work without transaction errors
	utils.Exec(t, conn, "SHOW TABLES")
	utils.Exec(t, conn, "SHOW VARIABLES LIKE 'tx%'")
	utils.Exec(t, conn, "SHOW STATUS LIKE 'Threads%'")

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}

// TestUseStatementWithAutocommitOff tests that USE statements work correctly
// when autocommit=0.
func TestUseStatementWithAutocommitOff(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Set autocommit=0 first
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// USE statements should work without transaction errors
	utils.Exec(t, conn, fmt.Sprintf("USE `%s`", keyspaceName))
	utils.Exec(t, conn, fmt.Sprintf("USE `%s:-80`", keyspaceName))
	utils.Exec(t, conn, fmt.Sprintf("USE `%s:-80@primary`", keyspaceName))

	// Find a primary tablet alias for testing tablet alias targeting
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// USE with tablet alias should also work
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}

// TestMixedSetSelectWithAutocommitOff tests the interaction between SET and SELECT
// statements when autocommit=0 with tablet alias targeting.
func TestMixedSetSelectWithAutocommitOff(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Find a primary tablet alias for the -80 shard
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// Target the specific tablet using tablet alias
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Set autocommit=0
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// Get initial connection ID
	qr := utils.Exec(t, conn, "SELECT connection_id()")
	initialConnID := qr.Rows[0][0].ToString()
	t.Logf("Initial connection ID: %s", initialConnID)

	// 1. SET statement (no tx started yet)
	utils.Exec(t, conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	// 2. SELECT should start implicit transaction
	qr = utils.Exec(t, conn, "SELECT id FROM test WHERE id = 80")
	t.Logf("SELECT result: %v", qr.Rows)

	// 3. Another SET should work (tx exists now)
	utils.Exec(t, conn, "SET @@wait_timeout = 28800")

	// 4. Another SELECT should use same tx
	utils.Exec(t, conn, "SELECT id FROM test WHERE id = 80")

	// Verify connection ID is consistent
	qr = utils.Exec(t, conn, "SELECT connection_id()")
	finalConnID := qr.Rows[0][0].ToString()
	assert.Equal(t, initialConnID, finalConnID, "connection ID should remain consistent")

	// Verify isolation level is applied
	utils.AssertMatches(t, conn, "SELECT @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)

	// 5. COMMIT
	utils.Exec(t, conn, "COMMIT")

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}

// TestTransactionBoundariesWithAutocommitOff tests that transaction boundaries
// are correct with mixed statement types when autocommit=0.
func TestTransactionBoundariesWithAutocommitOff(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Find a primary tablet alias for the -80 shard
	var primaryAlias string
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspaceName {
			continue
		}
		for _, shard := range ks.Shards {
			if shard.Name == "-80" {
				for _, tablet := range shard.Vttablets {
					if tablet.Type == "primary" {
						primaryAlias = tablet.Alias
						break
					}
				}
			}
		}
	}
	require.NotEmpty(t, primaryAlias, "no PRIMARY tablet found for -80 shard")

	// Target the specific tablet using tablet alias
	useStmt := fmt.Sprintf("USE `%s:-80@primary|%s`", keyspaceName, primaryAlias)
	utils.Exec(t, conn, useStmt)

	// Set autocommit=0
	utils.Exec(t, conn, "SET @@autocommit = 0")

	// 1. SET (no tx started)
	utils.Exec(t, conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	// 2. SHOW (no tx started)
	utils.Exec(t, conn, "SHOW TABLES")

	// 3. SELECT (implicit tx starts)
	qr := utils.Exec(t, conn, "SELECT id FROM test WHERE id = 80")
	t.Logf("First SELECT: %v", qr.Rows)

	// 4. ROLLBACK (tx ends)
	utils.Exec(t, conn, "ROLLBACK")

	// 5. SET (no tx started again)
	utils.Exec(t, conn, "SET @@wait_timeout = 28800")

	// 6. INSERT (implicit tx starts) - use a unique ID to avoid conflicts
	utils.Exec(t, conn, "INSERT INTO test (id, val1) VALUES (9999, 'test_boundary')")

	// 7. Verify the insert is visible in same transaction
	utils.AssertMatches(t, conn, "SELECT val1 FROM test WHERE id = 9999", `[[VARCHAR("test_boundary")]]`)

	// 8. COMMIT (tx ends)
	utils.Exec(t, conn, "COMMIT")

	// Verify data was committed
	utils.AssertMatches(t, conn, "SELECT val1 FROM test WHERE id = 9999", `[[VARCHAR("test_boundary")]]`)

	// Clean up test data
	utils.Exec(t, conn, "DELETE FROM test WHERE id = 9999")
	utils.Exec(t, conn, "COMMIT")

	// Reset autocommit
	utils.Exec(t, conn, "SET @@autocommit = 1")
}
