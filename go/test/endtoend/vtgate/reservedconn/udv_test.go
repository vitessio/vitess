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

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSetUDV(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	type queriesWithExpectations struct {
		query        string
		expectedRows string
		rowsAffected int
		rowsReturned int
	}

	queries := []queriesWithExpectations{{
		query:        "select @foo",
		expectedRows: "[[NULL]]", rowsReturned: 1,
	}, {
		query: "set @foo = 'abc', @bar = 42, @baz = 30.5, @tablet = concat('foo','bar')",
	}, {
		query: "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */",
	}, { // This is handled at vtgate.
		query:        "select @foo, @bar, @baz, @tablet",
		expectedRows: `[[VARBINARY("abc") INT64(42) FLOAT64(30.5) VARBINARY("foobar")]]`, rowsReturned: 1,
	}, { // Cannot really check a specific value for sql_mode as it will differ based on database selected to run these tests.
		query:        "select @OLD_SQL_MODE = @@SQL_MODE",
		expectedRows: `[[INT64(1)]]`, rowsReturned: 1,
	}, { // This one is sent to tablet.
		query:        "select @foo, @bar, @baz, @tablet, @OLD_SQL_MODE = @@SQL_MODE",
		expectedRows: `[[VARCHAR("abc") INT64(42) DECIMAL(30.5) VARCHAR("foobar") INT64(1)]]`, rowsReturned: 1,
	}, {
		query:        "insert into test(id, val1, val2, val3) values(1, @foo, null, null), (2, null, @bar, null), (3, null, null, @baz)",
		expectedRows: ``, rowsAffected: 3,
	}, {
		query:        "select id, val1, val2, val3 from test order by id",
		expectedRows: `[[INT64(1) VARCHAR("abc") NULL NULL] [INT64(2) NULL INT32(42) NULL] [INT64(3) NULL NULL FLOAT32(30.5)]]`, rowsReturned: 3,
	}, {
		query:        "select id, val1 from test where val1=@foo",
		expectedRows: `[[INT64(1) VARCHAR("abc")]]`, rowsReturned: 1,
	}, {
		query:        "select id, val2 from test where val2=@bar",
		expectedRows: `[[INT64(2) INT32(42)]]`, rowsReturned: 1,
	}, {
		query:        "select id, val3 from test where val3=@baz",
		expectedRows: `[[INT64(3) FLOAT32(30.5)]]`, rowsReturned: 1,
	}, {
		query:        "delete from test where val2 = @bar",
		expectedRows: ``, rowsAffected: 1,
	}, {
		query:        "select id, val2 from test where val2=@bar",
		expectedRows: ``,
	}, {
		query:        "update test set val2 = @bar where val1 = @foo",
		expectedRows: ``, rowsAffected: 1,
	}, {
		query:        "select id, val1, val2 from test where val1=@foo",
		expectedRows: `[[INT64(1) VARCHAR("abc") INT32(42)]]`, rowsReturned: 1,
	}, {
		query:        "insert into test(id, val1, val2, val3) values (42, @tablet, null, null)",
		expectedRows: ``, rowsAffected: 1,
	}, {
		query:        "select id, val1 from test where val1 = @tablet",
		expectedRows: `[[INT64(42) VARCHAR("foobar")]]`, rowsReturned: 1,
	}, {
		query:        "set @foo = now(), @bar = now(), @dd = date('2020-10-20'), @tt = time('10:15')",
		expectedRows: `[]`,
	}, {
		query:        "select @foo = @bar, @dd, @tt",
		expectedRows: `[[INT64(1) VARCHAR("2020-10-20") VARCHAR("10:15:00")]]`, rowsReturned: 1,
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	_, err = exec(t, conn, "delete from test")
	require.NoError(t, err)

	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q.query), func(t *testing.T) {
			qr, err := exec(t, conn, q.query)
			require.NoError(t, err)
			assert.EqualValues(t, q.rowsAffected, qr.RowsAffected, "rows affected wrong for query: %s", q.query)
			assert.EqualValues(t, q.rowsReturned, len(qr.Rows), "rows returned wrong for query: %s", q.query)
			if q.expectedRows != "" {
				result := fmt.Sprintf("%v", qr.Rows)
				if diff := cmp.Diff(q.expectedRows, result); diff != "" {
					t.Errorf("%s\nfor query: %s", diff, q.query)
				}
			}
		})
	}
}

func TestMysqlDumpInitialLog(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	queries := []string{
		"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;",
		"/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;",
		"/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;",
		"/*!50503 SET NAMES utf8mb4 */;",
		"/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;",
		"/*!40103 SET TIME_ZONE='+00:00' */;",
		"/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;",
		"/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;",
		"/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;",
		"/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;",
		"/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;",
		"/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;",
		"/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;",
		"/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;",
		"/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;",
		"/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;",
		"/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, more, err := conn.ExecuteFetchMulti(query, 1000, true)
			require.NoError(t, err)
			require.False(t, more)
		})
	}
}

func TestUserDefinedVariableResolvedAtTablet(t *testing.T) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// this should set the UDV foo to a value that has to be evaluated by mysqld
	exec(t, conn, "set @foo = CONCAT('Any','Expression','Is','Valid')")

	// now getting that value should return the value from the tablet
	qr, err := exec(t, conn, "select @foo")
	require.NoError(t, err)
	got := fmt.Sprintf("%v", qr.Rows)
	utils.MustMatch(t, `[[VARBINARY("AnyExpressionIsValid")]]`, got, "didnt match")
}
