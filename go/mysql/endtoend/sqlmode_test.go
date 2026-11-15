/*
Copyright 2026 The Vitess Authors.

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

package endtoend

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
)

// SessionSetupQuery — the statement the dbconfigs Connector runs on every connection
// Vitess creates (pinned in the dbconfigs tests) — neutralizes the session sql_mode
// from the session's own current value, not from the global: the server's connection
// initialization (init_connect) runs before it, and the runtime modes it applies must
// survive. Only the lexer modes are stripped. The same statement restores the
// character set the connection negotiated, which init_connect can also change.
func TestNeutralizeSessionQueryPreservesInitConnect(t *testing.T) {
	ctx := t.Context()
	admin, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	qr, err := admin.ExecuteFetch("select @@global.init_connect", 1, false)
	require.NoError(t, err)
	prevInitConnect := qr.Rows[0][0].ToString()
	t.Cleanup(func() {
		_, err := admin.ExecuteFetch(fmt.Sprintf("set global init_connect = '%s'", prevInitConnect), 0, false)
		assert.NoError(t, err)
		_, err = admin.ExecuteFetch("drop user if exists 'sqlmode_init'@'localhost'", 0, false)
		assert.NoError(t, err)
	})

	// ALLOW_INVALID_DATES is a runtime mode absent from the test server's global
	// value; ANSI_QUOTES is a lexer mode. init_connect applies both to the session.
	// It also switches the character set, which SessionSetupQuery restores.
	_, err = admin.ExecuteFetch(`set global init_connect = "SET SESSION sql_mode = CONCAT(@@sql_mode, ',ALLOW_INVALID_DATES,ANSI_QUOTES'); SET NAMES latin1"`, 0, false)
	require.NoError(t, err)

	// init_connect only runs for users without connection-admin privileges
	_, err = admin.ExecuteFetch("create user 'sqlmode_init'@'localhost' identified by 'sqlmode_init_pw'", 0, false)
	require.NoError(t, err)
	_, err = admin.ExecuteFetch("grant select on *.* to 'sqlmode_init'@'localhost'", 0, false)
	require.NoError(t, err)

	params := connParams
	params.Uname = "sqlmode_init"
	params.Pass = "sqlmode_init_pw"
	conn, err := mysql.Connect(ctx, &params)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	// the connection carries init_connect's modes, the lexer mode included —
	// pinning that the test exercises the intended scenario
	qr, err = conn.ExecuteFetch("select @@session.sql_mode", 1, false)
	require.NoError(t, err)
	rawMode := qr.Rows[0][0].ToString()
	require.Contains(t, rawMode, "ALLOW_INVALID_DATES")
	require.Contains(t, rawMode, "ANSI_QUOTES")
	qr, err = conn.ExecuteFetch("select @@session.character_set_client", 1, false)
	require.NoError(t, err)
	require.Equal(t, "latin1", qr.Rows[0][0].ToString())

	// utf8mb4_general_ci is a collation that MySQL 5.7, 8.x and MariaDB all have
	_, err = conn.ExecuteFetch(sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_general_ci"), 0, false)
	require.NoError(t, err)

	// the character set the connection negotiated is restored in the same statement
	qr, err = conn.ExecuteFetch("select @@session.character_set_client, @@session.character_set_connection, @@session.character_set_results, @@session.collation_connection", 1, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"utf8mb4", "utf8mb4", "utf8mb4", "utf8mb4_general_ci"},
		[]string{qr.Rows[0][0].ToString(), qr.Rows[0][1].ToString(), qr.Rows[0][2].ToString(), qr.Rows[0][3].ToString()})

	qr, err = conn.ExecuteFetch("select @@session.sql_mode", 1, false)
	require.NoError(t, err)
	neutralized := qr.Rows[0][0].ToString()
	assert.Contains(t, neutralized, "ALLOW_INVALID_DATES", "init_connect's runtime mode must survive the neutralization")
	assert.NotContains(t, neutralized, "ANSI_QUOTES", "lexer modes must be stripped")
	assert.Equal(t, strings.Contains(rawMode, "STRICT_TRANS_TABLES"), strings.Contains(neutralized, "STRICT_TRANS_TABLES"),
		"neutralization must not change unrelated runtime modes")
}

// TestSessionSetupQueryWithRawResults runs the raw variant of the session setup
// on a connection that was switched to latin1. The character set the
// connection negotiated is restored and character_set_results is NULL, so
// MySQL sends a latin1_german2_ci VARCHAR(10) with 'Müller' as 4D FC 6C 6C 65
// 72 with the collation 31 and the length 10, and the plain setup gets it as
// 4D C3 BC 6C 6C 65 72 with the collation 255 and the length 40.
func TestSessionSetupQueryWithRawResults(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	_, err = conn.ExecuteFetch("set names latin1", 0, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("create table if not exists raw_results (v varchar(10) character set latin1 collate latin1_german2_ci)", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.ExecuteFetch("drop table if exists raw_results", 0, false)
		assert.NoError(t, err)
	})

	read := func(setup string) (*sqltypes.Result, []string) {
		_, err := conn.ExecuteFetch(setup, 0, false)
		require.NoError(t, err)
		qr, err := conn.ExecuteFetch("select @@character_set_client, @@character_set_connection, @@collation_connection, ifnull(@@character_set_results, 'NULL')", 1, false)
		require.NoError(t, err)
		var vars []string
		for _, v := range qr.Rows[0] {
			vars = append(vars, v.ToString())
		}
		_, err = conn.ExecuteFetch("delete from raw_results", 0, false)
		require.NoError(t, err)
		_, err = conn.ExecuteFetch("insert into raw_results values (_utf8mb4 x'4DC3BC6C6C6572')", 0, false)
		require.NoError(t, err)
		qr, err = conn.ExecuteFetch("select v from raw_results", 1, true)
		require.NoError(t, err)
		return qr, vars
	}

	qr, vars := read(sqlmode.SessionSetupQueryWithRawResults("utf8mb4", "utf8mb4_0900_ai_ci"))
	assert.Equal(t, []string{"utf8mb4", "utf8mb4", "utf8mb4_0900_ai_ci", "NULL"}, vars)
	assert.Equal(t, "M\xFCller", qr.Rows[0][0].ToString())
	assert.Equal(t, uint32(31), qr.Fields[0].Charset)
	assert.Equal(t, uint32(10), qr.Fields[0].ColumnLength)

	qr, vars = read(sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_0900_ai_ci"))
	assert.Equal(t, []string{"utf8mb4", "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4"}, vars)
	assert.Equal(t, "M\xC3\xBCller", qr.Rows[0][0].ToString())
	assert.Equal(t, uint32(255), qr.Fields[0].Charset)
	assert.Equal(t, uint32(40), qr.Fields[0].ColumnLength)
}
