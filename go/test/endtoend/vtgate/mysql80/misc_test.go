/*
Copyright 2019 The Vitess Authors.

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
	"testing"

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

	// store the original sql mode
	res := exec(t, conn, `SELECT @@GLOBAL.sql_mode`)
	originalSQLMode := string(res.Rows[0][0].Raw())
	// set the sql mode ALLOW_INVALID_DATES
	_, err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.QueryTablet(`SET GLOBAL sql_mode = 'ALLOW_INVALID_DATES'`, "", false)
	require.NoError(t, err)
	// restore the sql mode
	defer func() {
		_, err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.QueryTablet(`SET GLOBAL sql_mode = '`+originalSQLMode+`'`, "", false)
		require.NoError(t, err)
	}()

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

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
