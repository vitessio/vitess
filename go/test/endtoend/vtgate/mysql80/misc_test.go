/*
Copyright 2021 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestFunctionInDefault(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set the sql mode ALLOW_INVALID_DATES
	exec(t, conn, `SET sql_mode = 'ALLOW_INVALID_DATES'`)

	exec(t, conn, `create table function_default (x varchar(25) DEFAULT (TRIM(" check ")))`)
	exec(t, conn, "drop table function_default")

	exec(t, conn, `create table function_default (
ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
dt2 DATETIME DEFAULT CURRENT_TIMESTAMP,
ts3 TIMESTAMP DEFAULT 0,
dt3 DATETIME DEFAULT 0,
ts4 TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
dt4 DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
ts5 TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts6 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
dt5 DATETIME ON UPDATE CURRENT_TIMESTAMP,
dt6 DATETIME NOT NULL ON UPDATE CURRENT_TIMESTAMP,
ts7 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
ts8 TIMESTAMP DEFAULT NOW(),
ts9 TIMESTAMP DEFAULT LOCALTIMESTAMP,
ts10 TIMESTAMP DEFAULT LOCALTIME,
ts11 TIMESTAMP DEFAULT LOCALTIMESTAMP(),
ts12 TIMESTAMP DEFAULT LOCALTIME()
)`)
	exec(t, conn, "drop table function_default")

	// this query works because utc_timestamp will get parenthesised before reaching MySQL. However, this syntax is not supported in MySQL 8.0
	exec(t, conn, `create table function_default (ts TIMESTAMP DEFAULT UTC_TIMESTAMP)`)
	exec(t, conn, "drop table function_default")

	exec(t, conn, `create table function_default (x varchar(25) DEFAULT "check")`)
	exec(t, conn, "drop table function_default")
}

// TestCheckConstraint test check constraints on CREATE TABLE
// This feature is supported from MySQL 8.0.16 and MariaDB 10.2.1.
func TestCheckConstraint(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := `CREATE TABLE t7 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0), c3 INT CHECK (c3 < 100), CONSTRAINT c1_nonzero CHECK (c1 <> 0), CHECK (c1 > c3));`
	exec(t, conn, query)

	checkQuery := `SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 't7' order by CONSTRAINT_NAME;`
	expected := `[[VARCHAR("c1_nonzero")] [VARCHAR("c2_positive")] [VARCHAR("t7_chk_1")] [VARCHAR("t7_chk_2")] [VARCHAR("t7_chk_3")] [VARCHAR("t7_chk_4")]]`

	assertMatches(t, conn, checkQuery, expected)

	cleanup := `DROP TABLE t7`
	exec(t, conn, cleanup)
}

func TestVersionCommentWorks(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	exec(t, conn, "/*!80000 SET SESSION information_schema_stats_expiry=0 */")
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
