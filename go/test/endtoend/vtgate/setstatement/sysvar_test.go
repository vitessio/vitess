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

	_, err = exec(t, conn, "delete from test")
	require.NoError(t, err)
	_, err = exec(t, conn, "insert into test (id,val1) values (666, _binary'abc')")
	require.NoError(t, err)
	_, err = exec(t, conn, "update test set val1 = _latin1'xyz' where id = 666")
	require.NoError(t, err)
	_, err = exec(t, conn, "delete from test where val1 = _utf8'xyz'")
	require.NoError(t, err)
	qr, err := exec(t, conn, "select id from test where val1 = _utf8mb4'xyz'")
	require.NoError(t, err)
	require.EqualValues(t, 0, qr.RowsAffected)
}

func TestSetSysVar(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	type queriesWithExpectations struct {
		query           string
		expectedRows    string
		rowsAffected    int
		errMsg          string
		expectedWarning string
	}

	queries := []queriesWithExpectations{{
		query:        `set @@default_storage_engine = INNODB`,
		expectedRows: ``, rowsAffected: 0,
		expectedWarning: "[[VARCHAR(\"Warning\") UINT16(1235) VARCHAR(\"Ignored inapplicable SET default_storage_engine = INNODB\")]]",
	}, {
		query:        `set @@sql_mode = @@sql_mode`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        `set @@sql_mode = concat(@@sql_mode,"")`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        `set @@SQL_SAFE_UPDATES = 1`,
		expectedRows: ``, rowsAffected: 0,
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q.query), func(t *testing.T) {
			qr, err := exec(t, conn, q.query)
			if q.errMsg != "" {
				require.Contains(t, err.Error(), q.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint64(q.rowsAffected), qr.RowsAffected, "rows affected wrong for query: %s", q.query)
				if q.expectedRows != "" {
					result := fmt.Sprintf("%v", qr.Rows)
					if diff := cmp.Diff(q.expectedRows, result); diff != "" {
						t.Errorf("%s\nfor query: %s", diff, q.query)
					}
				}
				if q.expectedWarning != "" {
					qr, err := exec(t, conn, "show warnings")
					require.NoError(t, err)
					if got, want := fmt.Sprintf("%v", qr.Rows), q.expectedWarning; got != want {
						t.Errorf("select:\n%v want\n%v", got, want)
					}
				}
			}
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

	exec(t, conn, "set @@sql_mode = 'NO_ZERO_DATE'")
	q := `select str_to_date('00/00/0000', '%m/%d/%Y')`
	assertMatches(t, conn, q, `[[NULL]]`)

	assertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)
	exec(t, conn, "set @@sql_mode = ''")

	assertMatches(t, conn, q, `[[DATE("0000-00-00")]]`)
}

func TestSetSystemVarWithTxFailure(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "insert into test (id, val1) values (80, null)")

	// before changing any settings, let's confirm sql_safe_updates value
	assertMatches(t, conn, `select @@sql_safe_updates from test where id = 80`, `[[INT64(0)]]`)

	exec(t, conn, "set sql_safe_updates = 1")
	exec(t, conn, "begin")

	qr, err := exec(t, conn, "select connection_id() from test where id = 80")
	require.NoError(t, err)

	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[0].MasterTablet() // 80-
	vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)

	// transaction fails on commit - we should no longer be in a transaction
	_, err = conn.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// we still want to have our system setting applied
	assertMatches(t, conn, `select @@sql_safe_updates`, `[[INT64(1)]]`)
}

func TestSetSystemVariableAndThenSuccessfulTx(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "set sql_safe_updates = 1")

	exec(t, conn, "begin")
	exec(t, conn, "insert into test (id, val1) values (80, null)")
	exec(t, conn, "commit")
	assertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
}

func TestStartTxAndSetSystemVariableAndThenSuccessfulCommit(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "begin")
	exec(t, conn, "set sql_safe_updates = 1")
	exec(t, conn, "insert into test (id, val1) values (80, null)")
	exec(t, conn, "commit")
	assertMatches(t, conn, "select id, val1 from test", "[[INT64(80) NULL]]")
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
