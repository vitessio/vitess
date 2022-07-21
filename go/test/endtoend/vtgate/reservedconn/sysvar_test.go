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

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSetSysVarSingle(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
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
			utils.AssertMatchesAny(t, conn, fmt.Sprintf("select @@%s", q.name), q.expected...)
		})
	}
}

func TestSetSystemVariable(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)

	// transaction fails on commit - we should no longer be in a transaction
	_, err = conn.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// we still want to have our system setting applied
	utils.AssertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestSetSystemVarWithConnectionTimeout(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
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
	_, err = vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)
	require.NoError(t, err)

	// first query to 80- shard should pass
	utils.AssertMatches(t, conn, "select id, val1 from test where id = 4", "[[INT64(4) NULL]]")

	// first query to -80 shard will fail, but vtgate will auto-retry for us
	utils.Exec(t, conn, "insert into test (id, val1) values (2, null)")
	utils.AssertMatches(t, conn, "select id from test where id = 2", "[[INT64(2)]]")
	utils.AssertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 2", "[[INT64(2) INT64(1)]]")
}

func TestSetSystemVarInTxWithConnError(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	_, err = vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)
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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	defer conn.Close()

	utils.Exec(b, conn, "delete from test")
	utils.Exec(b, conn, "insert into test (id, val1) values (1, 'toto'), (4, 'tata')")

	// set sql_mode to empty to force the use of reserved connection
	utils.Exec(b, conn, "set sql_mode = ''")
	utils.AssertMatches(b, conn, "select 	@@sql_mode", `[[VARCHAR("")]]`)

	for i := 0; i < b.N; i++ {
		utils.Exec(b, conn, "select id, val1 from test")
	}
}

func TestEnableSystemSettings(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := utils.Exec(t, conn, "select @@socket")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "mysql.sock")

	_, err = utils.ExecAllowError(t, conn, "set socket = '/any/path'")
	require.Error(t, err)
	sqlErr, ok := err.(*mysql.SQLError)
	require.True(t, ok, "not a mysql error: %T", err)
	assert.Equal(t, mysql.ERIncorrectGlobalLocalVar, sqlErr.Number())
	assert.Equal(t, mysql.SSUnknownSQLState, sqlErr.SQLState())
	assert.Equal(t, "variable 'socket' is a read only variable (errno 1238) (sqlstate HY000) during query: set socket = '/any/path'", sqlErr.Error())
}

func TestReservedConnInStreaming(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

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
