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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vitesst"
)

func TestSetSysVarSingle(t *testing.T) {
	setup(t)
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
			vitesst.Exec(t, conn, query)
			vitesst.AssertMatchesAny(t, conn, "select @@"+q.name, q.expected...)
		})
	}
}

func TestSetSystemVariable(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set session sql_mode = 'NO_ZERO_DATE', session default_week_format = 1")
	q := `select str_to_date('00/00/0000', '%m/%d/%Y'), WEEK('2008-02-20')`
	vitesst.AssertMatches(t, conn, q, `[[NULL INT64(8)]]`)

	vitesst.AssertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)
	vitesst.Exec(t, conn, "set @@sql_mode = '', session default_week_format = 0")

	vitesst.AssertMatches(t, conn, q, `[[DATE("0000-00-00") INT64(7)]]`)

	vitesst.Exec(t, conn, "SET @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'),  @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483")
	vitesst.AssertMatches(t, conn, "select @@sql_mode", `[[VARCHAR(",STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO")]]`)
}

func TestSetSystemVarWithTxFailure(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// before changing any settings, let's confirm sql_safe_updates value
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates from test where id = 80`, `[[INT64(0)]]`)

	vitesst.Exec(t, conn, "set sql_safe_updates = 1")
	vitesst.Exec(t, conn, "begin")

	qr := vitesst.Exec(t, conn, "select connection_id() from test where id = 80")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspace(keyspaceName).Shard("-80").Primary() // 80-
	vttablet1.QueryTabletWithDB(t.Context(), "kill "+qr.Rows[0][0].ToString(), "")

	// transaction fails on commit - we should no longer be in a transaction
	_, err = conn.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// we still want to have our system setting applied
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestSetSystemVarWithConnectionTimeout(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	vitesst.Exec(t, conn, "delete from test")

	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	vitesst.Exec(t, conn, "set sql_safe_updates = 1")
	vitesst.AssertMatches(t, conn, "select @@sql_safe_updates from test where id = 80", "[[INT64(1)]]")

	// Connection timeout.
	time.Sleep(10 * time.Second)

	// connection has timed out, but vtgate will recreate the connection for us
	vitesst.AssertMatches(t, conn, "select @@sql_safe_updates from test where id = 80", "[[INT64(1)]]")
}

func TestSetSystemVariableAndThenSuccessfulTx(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	vitesst.Exec(t, conn, "delete from test")

	vitesst.Exec(t, conn, "set sql_safe_updates = 1")
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
	vitesst.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestSetSystemVariableAndThenSuccessfulAutocommitDML(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	vitesst.Exec(t, conn, `delete from test`)

	vitesst.Exec(t, conn, `set sql_safe_updates = 1`)

	vitesst.Exec(t, conn, `insert into test (id, val1) values (80, null)`)
	vitesst.AssertMatches(t, conn, `select id, val1 from test`, `[[INT64(80) NULL]]`)
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	vitesst.Exec(t, conn, `update test set val2 = 2 where val1 is null`)
	vitesst.AssertMatches(t, conn, `select id, val1, val2 from test`, `[[INT64(80) NULL INT32(2)]]`)
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	vitesst.Exec(t, conn, `update test set val1 = 'text' where val1 is null`)
	vitesst.AssertMatches(t, conn, `select id, val1, val2 from test`, `[[INT64(80) VARCHAR("text") INT32(2)]]`)
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)

	vitesst.Exec(t, conn, `delete from test where val1 = 'text'`)
	vitesst.AssertMatches(t, conn, `select id, val1, val2 from test`, `[]`)
	vitesst.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

// This test ensures that when autocommit is disabled, `SET` commands do not
// cause an implicit transaction to be started.
//
// We test this via `set session transaction isolation level` because
// changing the session transaction isolation level affects only the next
// transaction that's started.
func TestSetSystemVariableWithAutocommitDisabled(t *testing.T) {
	setup(t)
	conn1, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	vitesst.Exec(t, conn1, "delete from test")
	vitesst.Exec(t, conn1, "delete from test_vdx")

	vitesst.Exec(t, conn1, "set autocommit = 0")

	vitesst.Exec(t, conn1, "set session transaction isolation level read uncommitted")
	vitesst.AssertMatches(t, conn1, "select @@transaction_isolation", `[[VARCHAR("READ-UNCOMMITTED")]]`)

	vitesst.Exec(t, conn2, "begin")
	vitesst.Exec(t, conn2, "insert into test (id, val1) values (80, null)")

	vitesst.AssertMatches(t, conn1, "select id from test where id = 80", `[[INT64(80)]]`)
}

func TestStartTxAndSetSystemVariableAndThenSuccessfulCommit(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	vitesst.Exec(t, conn, "delete from test")

	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "set sql_safe_updates = 1")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (54, null)")
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(54) NULL]]")
	vitesst.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestSetSystemVarAutocommitWithConnError(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "delete from test")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	vitesst.Exec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	vitesst.AssertMatches(t, conn, "select id from test order by id", "[[INT64(1)] [INT64(4)]]")
	qr := vitesst.Exec(t, conn, "select connection_id() from test where id = 1")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspace(keyspaceName).Shard("-80").Primary() // -80
	_, err = vttablet1.QueryTabletWithDB(t.Context(), "kill "+qr.Rows[0][0].ToString(), "")
	require.NoError(t, err)

	// first query to 80- shard should pass
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 4", "[[INT64(4) NULL]]")

	// first query to -80 shard will fail, but vtgate will auto-retry for us
	vitesst.Exec(t, conn, "insert into test (id, val1) values (2, null)")
	vitesst.AssertMatches(t, conn, "select id from test where id = 2", "[[INT64(2)]]")
	vitesst.AssertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 2", "[[INT64(2) INT64(1)]]")
}

func TestSetSystemVarInTxWithConnError(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "delete from test")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	vitesst.Exec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	qr := vitesst.Exec(t, conn, "select connection_id() from test where id = 4")
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (2, null)")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspace(keyspaceName).Shard("80-").Primary() // 80-
	_, err = vttablet1.QueryTabletWithDB(t.Context(), "kill "+qr.Rows[0][0].ToString(), "")
	require.NoError(t, err)

	// query to -80 shard should pass and remain in transaction.
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 2", "[[INT64(2) NULL]]")
	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 2", "[]")

	// first query to -80 shard will fail, but vtgate should retry once and succeed the second time
	vitesst.Exec(t, conn, "select @@sql_safe_updates from test where id = 4")

	// subsequent queries on 80- will pass
	vitesst.AssertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 4", "[[INT64(4) INT64(1)]]")
}

func BenchmarkReservedConnFieldQuery(b *testing.B) {
	setup(b)
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	defer conn.Close()

	vitesst.Exec(b, conn, "delete from test")
	vitesst.Exec(b, conn, "insert into test (id, val1) values (1, 'toto'), (4, 'tata')")

	// set sql_mode to empty to force the use of reserved connection
	vitesst.Exec(b, conn, "set sql_mode = ''")
	vitesst.AssertMatches(b, conn, "select 	@@sql_mode", `[[VARCHAR("")]]`)

	for b.Loop() {
		vitesst.Exec(b, conn, "select id, val1 from test")
	}
}

func TestEnableSystemSettings(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// test set @@enable_system_settings to false and true
	vitesst.Exec(t, conn, "set enable_system_settings = false")
	vitesst.AssertMatches(t, conn, `select @@enable_system_settings`, `[[INT64(0)]]`)
	vitesst.Exec(t, conn, "set enable_system_settings = true")
	vitesst.AssertMatches(t, conn, `select @@enable_system_settings`, `[[INT64(1)]]`)

	// prepare the @@sql_mode variable
	vitesst.Exec(t, conn, "set sql_mode = 'NO_ZERO_DATE'")
	vitesst.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)

	// check disabling @@enable_system_settings
	vitesst.Exec(t, conn, "set enable_system_settings = false")
	vitesst.Exec(t, conn, "set sql_mode = ''")                                          // attempting to set @@sql_mode to an empty string
	vitesst.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`) // @@sql_mode did not change

	// check enabling @@enable_system_settings
	vitesst.Exec(t, conn, "set enable_system_settings = true")
	vitesst.Exec(t, conn, "set sql_mode = ''")                              // changing @@sql_mode to empty string
	vitesst.AssertMatches(t, conn, "select 	@@sql_mode", `[[VARCHAR("")]]`) // @@sql_mode did change
}

// Tests type consitency through multiple queries
func TestSystemVariableType(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "delete from test")
	vitesst.Exec(t, conn, "insert into test (id, val1, val2, val3) values (1, null, 0, 0)")

	// regardless of the "from", the select @@autocommit should return the same type
	query1 := "select @@autocommit"
	query2 := "select @@autocommit from test"

	vitesst.Exec(t, conn, "set autocommit = false")
	assertResponseMatch(t, conn, query1, query2)

	vitesst.Exec(t, conn, "set autocommit = true")
	assertResponseMatch(t, conn, query1, query2)
}

func TestSysvarSocket(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := vitesst.Exec(t, conn, "select @@socket")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "mysql.sock")

	_, err = vitesst.ExecAllowError(t, conn, "set socket = '/any/path'")
	require.Error(t, err)
	sqlErr, ok := err.(*sqlerror.SQLError)
	require.True(t, ok, "not a mysql error: %T", err)
	assert.Equal(t, sqlerror.ERIncorrectGlobalLocalVar, sqlErr.Number())
	assert.Equal(t, sqlerror.SSUnknownSQLState, sqlErr.SQLState())
	assert.Equal(t, "VT03010: variable 'socket' is a read only variable (errno 1238) (sqlstate HY000) during query: set socket = '/any/path'", sqlErr.Error())
}

func TestReservedConnInStreaming(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	vitesst.Exec(t, conn, "delete from test")

	vitesst.Exec(t, conn, "set workload = olap")
	vitesst.Exec(t, conn, "set sql_safe_updates = 1")
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
	vitesst.AssertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestUnifiedOlapAndOltp(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "delete from test")
	checkOltpAndOlapInterchangingTx(t, conn)

	// modify some system settings to active reserved connection use.
	vitesst.Exec(t, conn, "set sql_safe_updates = 1")

	vitesst.Exec(t, conn, "delete from test")
	checkOltpAndOlapInterchangingTx(t, conn)
}

func checkOltpAndOlapInterchangingTx(t *testing.T, conn *mysql.Conn) {
	// start transaction in execute
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// move to streaming
	vitesst.Exec(t, conn, "set workload = olap")

	// checking data in streaming
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// rollback the tx
	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[]")

	// move back to oltp
	vitesst.Exec(t, conn, "set workload = oltp")
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[]")

	// move to streaming and start transaction
	vitesst.Exec(t, conn, "set workload = olap")
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into test (id, val1) values (80, null)")

	// checking data in streaming
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// move back to oltp and commit the tx
	vitesst.Exec(t, conn, "set workload = oltp")
	vitesst.Exec(t, conn, "commit")

	// check in oltp
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")

	// check in olap
	vitesst.Exec(t, conn, "set workload = oltp")
	vitesst.AssertMatches(t, conn, "select id, val1 from test where id = 80", "[[INT64(80) NULL]]")
}

func TestSysVarTxIsolation(t *testing.T) {
	setup(t)
	t.Run("returns the default isolation level if unchanged", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		defer conn.Close()

		vitesst.Exec(t, conn, "delete from test")
		vitesst.Exec(t, conn, "delete from test_vdx")

		// default from mysql
		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("REPEATABLE-READ")]]`)
		// ensuring it goes to mysql
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)
		// second run, ensuring it has the same value.
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)

		// Switch to shard targeting
		vitesst.Exec(t, conn, "use `"+keyspaceName+":-80`")

		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("REPEATABLE-READ")]]`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `REPEATABLE-READ`)
	})

	t.Run("allows changing the isolation level via special syntax", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		defer conn.Close()

		vitesst.Exec(t, conn, "delete from test")
		vitesst.Exec(t, conn, "delete from test_vdx")

		// setting to different value.
		vitesst.Exec(t, conn, "set session transaction isolation level read committed")

		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)

		// Switch to shard targeting
		vitesst.Exec(t, conn, "use `"+keyspaceName+":-80`")
		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)

		vitesst.Exec(t, conn, "set session transaction isolation level read uncommitted")
		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-UNCOMMITTED")]]`)
	})

	t.Run("allows changing the isolation level via session variable", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		defer conn.Close()

		vitesst.Exec(t, conn, "delete from test")
		vitesst.Exec(t, conn, "delete from test_vdx")

		// setting to different value.
		vitesst.Exec(t, conn, "set @@session.transaction_isolation = 'read-committed'")

		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)
		vitesst.AssertContains(t, conn, "select @@transaction_isolation, connection_id()", `READ-COMMITTED`)

		// Switch to shard targeting
		vitesst.Exec(t, conn, "use `"+keyspaceName+":-80`")
		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)

		vitesst.Exec(t, conn, "set @@session.transaction_isolation = 'read-uncommitted'")
		vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-UNCOMMITTED")]]`)
	})
}

// TestSysVarInnodbWaitTimeout tests the innodb_lock_wait_timeout system variable
func TestSysVarInnodbWaitTimeout(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// default from mysql
	vitesst.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[UINT64(20)]]`)
	vitesst.AssertMatches(t, conn, "select @@global.innodb_lock_wait_timeout", `[[UINT64(20)]]`)
	// ensuring it goes to mysql
	vitesst.AssertContains(t, conn, "select @@innodb_lock_wait_timeout", `UINT64(20)`)
	vitesst.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout", `UINT64(20)`)

	// setting to different value.
	vitesst.Exec(t, conn, "set @@innodb_lock_wait_timeout = 120")
	vitesst.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[INT64(120)]]`)
	// ensuring it goes to mysql
	vitesst.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout, connection_id()", `UINT64(20)`)
	vitesst.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(120)`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	vitesst.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(120)`)

	// changing setting to different value.
	vitesst.Exec(t, conn, "set @@innodb_lock_wait_timeout = 240")
	vitesst.AssertMatches(t, conn, "select @@innodb_lock_wait_timeout", `[[INT64(240)]]`)
	// ensuring it goes to mysql
	vitesst.AssertContains(t, conn, "select @@global.innodb_lock_wait_timeout, connection_id()", `UINT64(20)`)
	vitesst.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(240)`)
	// second run, to ensuring the setting is applied on the session and not just on next query after settings.
	vitesst.AssertContains(t, conn, "select @@innodb_lock_wait_timeout, connection_id()", `INT64(240)`)
}

// TestImplicitTxOnAutocommitOff verifies that vtgate only starts implicit
// transactions for statements that access real table data when autocommit=0,
// matching MySQL's behavior.
func TestImplicitTxOnAutocommitOff(t *testing.T) {
	setup(t)
	tests := []struct {
		name     string
		query    string
		startsTx bool
	}{
		{
			name:     "SELECT from real table starts tx",
			query:    "select id from test where id = 1",
			startsTx: true,
		},
		{
			name:     "SELECT @@variable does not start tx",
			query:    "select @@autocommit",
			startsTx: false,
		},
		{
			name:     "SELECT 1 does not start tx",
			query:    "select 1",
			startsTx: false,
		},
		{
			name:     "SELECT from dual does not start tx",
			query:    "select 1 from dual",
			startsTx: false,
		},
		{
			name:     "INSERT starts tx",
			query:    "insert into test (id, val1) values (999, null)",
			startsTx: true,
		},
		{
			name:     "UPDATE starts tx",
			query:    "update test set val1 = 'x' where id = 999",
			startsTx: true,
		},
		{
			name:     "DELETE starts tx",
			query:    "delete from test where id = 999",
			startsTx: true,
		},
		{
			name:     "SET variable does not start tx",
			query:    "set sql_safe_updates = 1",
			startsTx: false,
		},
		{
			name:     "COMMIT does not start tx",
			query:    "commit",
			startsTx: false,
		},
		{
			name:     "ROLLBACK does not start tx",
			query:    "rollback",
			startsTx: false,
		},
		// SHOW commands that start implicit transactions (access information_schema / data dictionaries):
		{
			name:     "SHOW TABLES starts tx",
			query:    "show tables",
			startsTx: true,
		},
		{
			name:     "SHOW DATABASES starts tx",
			query:    "show databases",
			startsTx: true,
		},
		{
			name:     "SHOW COLUMNS starts tx",
			query:    "show columns from test",
			startsTx: true,
		},
		{
			name:     "SHOW INDEX starts tx",
			query:    "show index from test",
			startsTx: true,
		},
		{
			name:     "SHOW TABLE STATUS starts tx",
			query:    "show table status",
			startsTx: true,
		},
		{
			name:     "SHOW TRIGGERS starts tx",
			query:    "show triggers",
			startsTx: true,
		},
		{
			name:     "SHOW CHARSET starts tx",
			query:    "show charset",
			startsTx: true,
		},
		{
			name:     "SHOW COLLATION starts tx",
			query:    "show collation",
			startsTx: true,
		},
		{
			name:     "SHOW FUNCTION STATUS starts tx",
			query:    "show function status",
			startsTx: true,
		},
		{
			name:     "SHOW PROCEDURE STATUS starts tx",
			query:    "show procedure status",
			startsTx: true,
		},
		// SHOW commands that do NOT start implicit transactions (read server state only):
		{
			name:     "SHOW VARIABLES does not start tx",
			query:    "show variables like 'version'",
			startsTx: false,
		},
		{
			name:     "SHOW SESSION VARIABLES does not start tx",
			query:    "show session variables like 'version'",
			startsTx: false,
		},
		{
			name:     "SHOW GLOBAL VARIABLES does not start tx",
			query:    "show global variables like 'version'",
			startsTx: false,
		},
		{
			name:     "SHOW STATUS does not start tx",
			query:    "show status like 'Uptime'",
			startsTx: false,
		},
		{
			name:     "SHOW GLOBAL STATUS does not start tx",
			query:    "show global status like 'Uptime'",
			startsTx: false,
		},
		{
			name:     "SHOW WARNINGS does not start tx",
			query:    "show warnings",
			startsTx: false,
		},
		{
			name:     "SHOW ENGINES does not start tx",
			query:    "show engines",
			startsTx: false,
		},
		{
			name:     "SHOW PLUGINS does not start tx",
			query:    "show plugins",
			startsTx: false,
		},
		{
			name:     "SHOW PRIVILEGES does not start tx",
			query:    "show privileges",
			startsTx: false,
		},
		{
			name:     "SHOW OPEN TABLES does not start tx",
			query:    "show open tables",
			startsTx: false,
		},
		// ShowCreate commands do not start implicit transactions.
		{
			name:     "SHOW CREATE TABLE does not start tx",
			query:    "show create table test",
			startsTx: false,
		},
		{
			name:     "SHOW CREATE DATABASE does not start tx",
			query:    "show create database " + keyspaceName,
			startsTx: false,
		},
		// Server-state SHOW commands are sent to MySQL as-is and do not start implicit transactions.
		{
			name:     "SHOW PROCESSLIST does not start tx",
			query:    "show processlist",
			startsTx: false,
		},
		{
			name:     "SHOW BINARY LOGS does not start tx",
			query:    "show binary logs",
			startsTx: false,
		},
		{
			name:     "SHOW GRANTS does not start tx",
			query:    "show grants",
			startsTx: false,
		},
		{
			name:     "SHOW ERRORS does not start tx",
			query:    "show errors",
			startsTx: false,
		},
		{
			name:     "SHOW EVENTS does not start tx",
			query:    "show events",
			startsTx: false,
		},
		{
			name:     "SHOW PROFILES does not start tx",
			query:    "show profiles",
			startsTx: false,
		},
		{
			name:     "SHOW REPLICA STATUS does not start tx",
			query:    "show replica status",
			startsTx: false,
		},
		{
			name:     "SHOW ENGINE INNODB STATUS does not start tx",
			query:    "show engine innodb status",
			startsTx: false,
		},
		// Vitess-specific SHOW commands are handled internally by vtgate
		// and should not start implicit transactions.
		{
			name:     "SHOW VITESS_TABLETS does not start tx",
			query:    "show vitess_tablets",
			startsTx: false,
		},
		{
			name:     "SHOW VITESS_SHARDS does not start tx",
			query:    "show vitess_shards",
			startsTx: false,
		},
		{
			name:     "SHOW VITESS_TARGET does not start tx",
			query:    "show vitess_target",
			startsTx: false,
		},
		{
			name:     "SHOW VSCHEMA TABLES does not start tx",
			query:    "show vschema tables",
			startsTx: false,
		},
		{
			name:     "SHOW VSCHEMA KEYSPACES does not start tx",
			query:    "show vschema keyspaces",
			startsTx: false,
		},
		{
			name:     "SHOW VSCHEMA VINDEXES does not start tx",
			query:    "show vschema vindexes",
			startsTx: false,
		},
		{
			name:     "SHOW KEYSPACES does not start tx",
			query:    "show keyspaces",
			startsTx: false,
		},
		{
			name:     "SHOW VITESS_MIGRATIONS does not start tx",
			query:    "show vitess_migrations",
			startsTx: false,
		},
		{
			name:     "SHOW VITESS_REPLICATION_STATUS does not start tx",
			query:    "show vitess_replication_status",
			startsTx: false,
		},
		{
			name:     "SHOW GLOBAL GTID_EXECUTED does not start tx",
			query:    "show global gtid_executed",
			startsTx: false,
		},
		{
			name:     "SHOW GLOBAL VGTID_EXECUTED does not start tx",
			query:    "show global vgtid_executed",
			startsTx: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := mysql.Connect(t.Context(), &vtParams)
			require.NoError(t, err)
			defer conn.Close()

			vitesst.Exec(t, conn, "delete from test")
			vitesst.Exec(t, conn, "delete from test_vdx")

			vitesst.Exec(t, conn, "set autocommit = 0")

			result := vitesst.Exec(t, conn, tc.query)

			inTx := result.StatusFlags&mysql.ServerStatusInTrans != 0
			if tc.startsTx {
				assert.True(t, inTx, "expected %q to start an implicit transaction", tc.query)
			} else {
				assert.False(t, inTx, "expected %q to NOT start an implicit transaction", tc.query)
			}
		})
	}

	t.Run("ROLLBACK TO SAVEPOINT returns an error when no transaction has been started", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		defer conn.Close()

		vitesst.Exec(t, conn, "set autocommit = 0")

		_, err = vitesst.ExecAllowError(t, conn, "ROLLBACK TO SAVEPOINT sp1")
		require.Error(t, err)
		sqlErr, ok := err.(*sqlerror.SQLError)
		require.True(t, ok, "not a mysql error: %T", err)
		assert.Equal(t, sqlerror.ERSPDoesNotExist, sqlErr.Number())
		assert.Equal(t, sqlerror.SSClientError, sqlErr.SQLState())
		assert.Contains(t, sqlErr.Error(), "SAVEPOINT does not exist: ROLLBACK TO SAVEPOINT sp1 (errno 1305) (sqlstate 42000)")

		result := vitesst.Exec(t, conn, "select 1")
		inTx := result.StatusFlags&mysql.ServerStatusInTrans != 0
		assert.False(t, inTx, "expected ROLLBACK TO SAVEPOINT to not start a transaction")
	})

	t.Run("RELEASE SAVEPOINT returns an error when no transaction has been started", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		defer conn.Close()

		vitesst.Exec(t, conn, "set autocommit = 0")

		_, err = vitesst.ExecAllowError(t, conn, "RELEASE SAVEPOINT sp1")
		require.Error(t, err)
		sqlErr, ok := err.(*sqlerror.SQLError)
		require.True(t, ok, "not a mysql error: %T", err)
		assert.Equal(t, sqlerror.ERSPDoesNotExist, sqlErr.Number())
		assert.Equal(t, sqlerror.SSClientError, sqlErr.SQLState())
		assert.Contains(t, sqlErr.Error(), "SAVEPOINT does not exist: RELEASE SAVEPOINT sp1 (errno 1305) (sqlstate 42000)")

		result := vitesst.Exec(t, conn, "select 1")
		inTx := result.StatusFlags&mysql.ServerStatusInTrans != 0
		assert.False(t, inTx, "expected RELEASE SAVEPOINT to not start a transaction")
	})
}
