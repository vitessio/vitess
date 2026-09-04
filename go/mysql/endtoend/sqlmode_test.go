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
)

// NeutralizeSessionQuery — the statement the dbconfigs Connector runs on every
// connection Vitess creates (pinned in the dbconfigs tests) — neutralizes the session
// sql_mode from the session's own current value, not from the global: the server's
// connection initialization (init_connect) runs before it, and the runtime modes it
// applies must survive. Only the lexer modes are stripped.
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
	_, err = admin.ExecuteFetch(`set global init_connect = "SET SESSION sql_mode = CONCAT(@@sql_mode, ',ALLOW_INVALID_DATES,ANSI_QUOTES')"`, 0, false)
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

	_, err = conn.ExecuteFetch(sqlmode.NeutralizeSessionQuery, 0, false)
	require.NoError(t, err)

	qr, err = conn.ExecuteFetch("select @@session.sql_mode", 1, false)
	require.NoError(t, err)
	neutralized := qr.Rows[0][0].ToString()
	assert.Contains(t, neutralized, "ALLOW_INVALID_DATES", "init_connect's runtime mode must survive the neutralization")
	assert.NotContains(t, neutralized, "ANSI_QUOTES", "lexer modes must be stripped")
	assert.Equal(t, strings.Contains(rawMode, "STRICT_TRANS_TABLES"), strings.Contains(neutralized, "STRICT_TRANS_TABLES"),
		"neutralization must not change unrelated runtime modes")
}
