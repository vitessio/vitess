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

package setstatement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestCharsetIntro(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	checkedExec(t, conn, "delete from test")
	checkedExec(t, conn, "insert into test (id,val1) values (666, _binary'abc')")
	checkedExec(t, conn, "update test set val1 = _latin1'xyz' where id = 666")
	checkedExec(t, conn, "delete from test where val1 = _utf8'xyz'")
	qr := checkedExec(t, conn, "select id from test where val1 = _utf8mb4'xyz'")
	require.EqualValues(t, 0, qr.RowsAffected)
}

func TestSetSysVarSingle(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	type queriesWithExpectations struct {
		name, expr, expected string
	}

	queries := []queriesWithExpectations{{
		name:     "default_storage_engine", // ignored
		expr:     "INNODB",
		expected: `[[VARCHAR("InnoDB")]]`,
	}, {
		name:     "character_set_client", // check and ignored
		expr:     "utf8",
		expected: `[[VARCHAR("utf8")]]`,
	}, {
		name:     "character_set_client", // ignored so will keep the actual value
		expr:     "@charvar",
		expected: `[[VARCHAR("utf8")]]`,
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     "''",
		expected: `[[VARCHAR("")]]`,
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     `concat(@@sql_mode,"NO_ZERO_DATE")`,
		expected: `[[VARCHAR("NO_ZERO_DATE")]]`,
	}, {
		name:     "sql_mode", // use reserved conn
		expr:     "@@sql_mode",
		expected: `[[VARCHAR("NO_ZERO_DATE")]]`,
	}, {
		name:     "SQL_SAFE_UPDATES", // use reserved conn
		expr:     "1",
		expected: "[[INT64(1)]]",
	}, {
		name:     "sql_auto_is_null", // ignored so will keep the actual value
		expr:     "on",
		expected: `[[INT64(0)]]`,
	}, {
		name:     "sql_notes", // use reserved conn
		expr:     "off",
		expected: "[[INT64(0)]]",
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		query := fmt.Sprintf("set %s = %s", q.name, q.expr)
		t.Run(fmt.Sprintf("%d-%s", i, query), func(t *testing.T) {
			_, err := exec(t, conn, query)
			require.NoError(t, err)
			assertMatches(t, conn, fmt.Sprintf("select @@%s", q.name), q.expected)
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

	checkedExec(t, conn, "set session sql_mode = 'NO_ZERO_DATE', session default_week_format = 1")
	q := `select str_to_date('00/00/0000', '%m/%d/%Y'), WEEK('2008-02-20')`
	assertMatches(t, conn, q, `[[NULL INT64(8)]]`)

	assertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)
	checkedExec(t, conn, "set @@sql_mode = '', session default_week_format = 0")

	assertMatches(t, conn, q, `[[DATE("0000-00-00") INT64(7)]]`)

	checkedExec(t, conn, "SET @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'),  @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483")
	assertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES")]]`)
}

func TestSetSystemVarWithTxFailure(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	checkedExec(t, conn, "insert into test (id, val1) values (80, null)")

	// before changing any settings, let's confirm sql_safe_updates value
	assertMatches(t, conn, `select @@sql_safe_updates from test where id = 80`, `[[INT64(0)]]`)

	checkedExec(t, conn, "set sql_safe_updates = 1")
	checkedExec(t, conn, "begin")

	qr := checkedExec(t, conn, "select connection_id() from test where id = 80")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].MasterTablet() // 80-
	vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)

	// transaction fails on commit - we should no longer be in a transaction
	_, err = conn.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// we still want to have our system setting applied
	assertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestSetSystemVarWithConnectionFailure(t *testing.T) {
	t.Skip("failing at the moment")
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	checkedExec(t, conn, "delete from test")

	checkedExec(t, conn, "insert into test (id, val1) values (80, null)")
	checkedExec(t, conn, "set sql_safe_updates = 1")
	qr := checkedExec(t, conn, "select connection_id() from test where id = 80")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].MasterTablet() // 80-
	vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)

	// we still want to have our system setting applied
	_, err = exec(t, conn, `select @@sql_safe_updates from test where id = 80`)
	require.NoError(t, err)
}

func TestSetSystemVariableAndThenSuccessfulTx(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	checkedExec(t, conn, "delete from test")

	checkedExec(t, conn, "set sql_safe_updates = 1")
	checkedExec(t, conn, "begin")
	checkedExec(t, conn, "insert into test (id, val1) values (80, null)")
	checkedExec(t, conn, "commit")
	assertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
	assertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestStartTxAndSetSystemVariableAndThenSuccessfulCommit(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	checkedExec(t, conn, "delete from test")

	checkedExec(t, conn, "begin")
	checkedExec(t, conn, "set sql_safe_updates = 1")
	checkedExec(t, conn, "insert into test (id, val1) values (54, null)")
	checkedExec(t, conn, "commit")
	assertMatches(t, conn, "select id, val1 from test", "[[INT64(54) NULL]]")
	assertMatches(t, conn, "select @@sql_safe_updates", "[[INT64(1)]]")
}

func TestSetSystemVarAutocommitWithConnError(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	checkedExec(t, conn, "delete from test")
	checkedExec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	checkedExec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	assertMatches(t, conn, "select id from test order by id", "[[INT64(1)] [INT64(4)]]")
	qr := checkedExec(t, conn, "select connection_id() from test where id = 1")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].MasterTablet() // -80
	_, err = vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)
	require.NoError(t, err)

	// first query to 80- shard should pass
	assertMatches(t, conn, "select id, val1 from test where id = 4", "[[INT64(4) NULL]]")

	// first query to -80 shard will fail
	_, err = exec(t, conn, "insert into test (id, val1) values (2, null)")
	require.Error(t, err)

	// subsequent queries on -80 will pass
	assertMatches(t, conn, "select id from test where id = 2", "[]")
	assertMatches(t, conn, "insert into test (id, val1) values (2, null)", "[]")
	assertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 2", "[[INT64(2) INT64(1)]]")
}

func TestSetSystemVarInTxWithConnError(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	checkedExec(t, conn, "delete from test")
	checkedExec(t, conn, "insert into test (id, val1) values (1, null), (4, null)")

	checkedExec(t, conn, "set sql_safe_updates = 1") // this should force us into a reserved connection
	qr := checkedExec(t, conn, "select connection_id() from test where id = 4")
	checkedExec(t, conn, "begin")
	checkedExec(t, conn, "insert into test (id, val1) values (2, null)")

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[1].MasterTablet() // 80-
	_, err = vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)
	require.NoError(t, err)

	// query to -80 shard should pass and remain in transaction.
	assertMatches(t, conn, "select id, val1 from test where id = 2", "[[INT64(2) NULL]]")
	checkedExec(t, conn, "rollback")
	assertMatches(t, conn, "select id, val1 from test where id = 2", "[]")

	// first query to 80- shard will fail
	_, err = exec(t, conn, "select @@sql_safe_updates from test where id = 4")
	require.Error(t, err)

	// subsequent queries on 80- will pass
	assertMatches(t, conn, "select id, @@sql_safe_updates from test where id = 4", "[[INT64(4) INT64(1)]]")
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr, err := exec(t, conn, query)
	require.NoError(t, err)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
