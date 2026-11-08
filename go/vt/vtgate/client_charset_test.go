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

package vtgate

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sysvars"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

const (
	// latin1Only parses only in latin1, where 0xA0 is a space. In UTF-8 the
	// byte is part of the name "select\xA01", which cannot start a statement.
	latin1Only = "select\xA01 from dual"
	// utf8Only parses only in UTF-8, where it names the column é. In latin1
	// the second byte, ©, is not a letter.
	utf8Only = "select 1 as \xC3\xA9 from dual"
)

// sessionWithCharset returns a session whose client uses the character set
// name, as the handshake or a SET statement leaves it.
func sessionWithCharset(name string) *econtext.SafeSession {
	return econtext.NewAutocommitSession(&vtgatepb.Session{
		TargetString:    "@primary",
		SystemVariables: map[string]string{sysvars.CharacterSetClient.Name: "'" + name + "'"},
	})
}

// assertReadsAs checks that the session's client has the character set name,
// that VTGate answers @@character_set_client with it, and that the executor
// reads the session's queries by the rule of that character set: latin1, or
// UTF-8 for every other one. It accepts the query that only that rule accepts,
// and rejects the one that only the other rule accepts.
func assertReadsAs(t *testing.T, executor *Executor, session *econtext.SafeSession, name string) {
	t.Helper()
	assert.Equal(t, "'"+name+"'", session.SystemVariables[sysvars.CharacterSetClient.Name])
	qr, err := executorExecSession(t.Context(), executor, session, "select @@character_set_client from dual", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, name, qr.Rows[0][0].ToString())

	accepted, rejected := utf8Only, latin1Only
	if name == "latin1" {
		accepted, rejected = latin1Only, utf8Only
	}
	_, err = executorExecSession(t.Context(), executor, session, accepted, nil)
	require.NoError(t, err, "%q must parse in %s", accepted, name)
	_, err = executorExecSession(t.Context(), executor, session, rejected, nil)
	assert.ErrorContains(t, err, "syntax error", "%q must not parse in %s", rejected, name)
}

// TestHandshakeClientCharset tests that VTGate accepts a handshake only in a
// character set that it has a rule for, that the character set of the
// handshake sets character_set_client, and that it selects the rule by which
// VTGate reads the client's names.
func TestHandshakeClientCharset(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	testcases := []struct {
		name     string
		id       collations.ID
		expected string // empty when VTGate refuses the handshake
	}{
		{"latin1_swedish_ci", 8, "latin1"},
		{"latin1_bin", 47, "latin1"},
		{"utf8mb4_0900_ai_ci", collations.CollationUtf8mb4ID, "utf8mb4"},
		{"utf8mb4_general_ci", 45, "utf8mb4"},
		{"utf8mb3_general_ci", 33, "utf8mb3"},
		{"no collation", collations.Unknown, "utf8mb4"},
		// koi8r is safe to tokenize, but VTGate would read its names by
		// another rule than MySQL does.
		{"koi8r_general_ci", 7, ""},
		{"cp1251_general_ci", 51, ""},
		{"binary", collations.CollationBinaryID, ""},
		// sjis cannot be tokenized safely at all.
		{"sjis_japanese_ci", 13, ""},
		{"an ID that MySQL does not define", 1000, ""},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			vh := &vtgateHandler{}
			conn := &mysql.Conn{CharacterSet: tc.id}
			err := vh.CheckHandshakeCharset(conn)
			if tc.expected == "" {
				var sqlErr *sqlerror.SQLError
				require.ErrorAs(t, err, &sqlErr)
				assert.Equal(t, sqlerror.ERUnknownCharacterSet, sqlErr.Number())
				return
			}
			require.NoError(t, err)
			assertReadsAs(t, executor, econtext.NewAutocommitSession(vh.session(conn)), tc.expected)
		})
	}
}

// TestHandshakeClientCharsetOverTheWire tests that a client that connects in a
// character set that VTGate has no rule for is refused before it is
// authenticated, and that one that connects in latin1 has its names read in
// latin1.
func TestHandshakeClientCharsetOverTheWire(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)
	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	connect := func(id collations.ID) (*mysql.Conn, error) {
		return mysql.Connect(t.Context(), &mysql.ConnParams{Host: addr.IP.String(), Port: addr.Port, Uname: "user1", Charset: id})
	}

	_, err = connect(7) // koi8r_general_ci
	var sqlErr *sqlerror.SQLError
	require.ErrorAs(t, err, &sqlErr)
	assert.Equal(t, sqlerror.ERUnknownCharacterSet, sqlErr.Number())
	assert.Contains(t, sqlErr.Error(), "VTGate does not support the character set koi8r for a connection")

	conn, err := connect(8) // latin1_swedish_ci
	require.NoError(t, err)
	defer conn.Close()
	qr, err := conn.ExecuteFetch("select @@character_set_client from dual", 1, false)
	require.NoError(t, err)
	assert.Equal(t, "latin1", qr.Rows[0][0].ToString())
	_, err = conn.ExecuteFetch(latin1Only, 1, false)
	require.NoError(t, err)
}

// TestNoClientCharset tests that a session without character_set_client, such
// as one of a gRPC client, reads names as UTF-8 and reports utf8mb4.
func TestNoClientCharset(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := econtext.NewAutocommitSession(&vtgatepb.Session{TargetString: "@primary"})
	qr, err := executorExecSession(ctx, executor, session, "select @@session.character_set_client from dual", nil)
	require.NoError(t, err)
	assert.Equal(t, "utf8mb4", qr.Rows[0][0].ToString())
	_, err = executorExecSession(ctx, executor, session, utf8Only, nil)
	require.NoError(t, err)
	_, err = executorExecSession(ctx, executor, session, latin1Only, nil)
	assert.ErrorContains(t, err, "syntax error")
}

// TestSetClientCharset tests that SET NAMES, SET CHARACTER SET and SET
// character_set_client change the character set that VTGate reads the
// client's names in, for the values that VTGate accepts.
func TestSetClientCharset(t *testing.T) {
	testcases := []struct {
		set      string
		expected string
	}{
		{"set names latin1", "latin1"},
		{"set names 'latin1' collate latin1_bin", "latin1"},
		{"set character set latin1", "latin1"},
		{"set charset 'LATIN1'", "latin1"},
		{"set character_set_client = latin1", "latin1"},
		{"set @@session.character_set_client = 'latin1'", "latin1"},
		// MySQL also accepts a collation ID: 8 is latin1_swedish_ci.
		{"set character_set_client = 8", "latin1"},
		{"set names utf8mb4", "utf8mb4"},
		{"set names utf8", "utf8mb3"},
		{"set names default", "utf8mb4"},
		{"set character set default", "utf8mb4"},
		{"set character_set_client = default", "utf8mb4"},
		{"set character_set_client = utf8mb3", "utf8mb3"},
		{"set character_set_client = 'koi8r'", "koi8r"},
	}
	for _, tc := range testcases {
		t.Run(tc.set, func(t *testing.T) {
			executor, _, _, _, ctx := createExecutorEnv(t)
			// Start from the other rule, so that the statement must change it.
			start := "latin1"
			if tc.expected == "latin1" {
				start = "utf8mb4"
			}
			session := sessionWithCharset(start)
			_, err := executorExecSession(ctx, executor, session, tc.set, nil)
			require.NoError(t, err)
			assertReadsAs(t, executor, session, tc.expected)
		})
	}
}

// TestSetClientCharsetRejected tests that a character set that VTGate rejects
// leaves the client's character set as it was.
func TestSetClientCharsetRejected(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	for _, set := range []string{"set names koi8r", "set character_set_client = 'sjis'", "set character_set_client = 28", "set character_set_client = 'nope'"} {
		session := sessionWithCharset("latin1")
		_, err := executorExecSession(ctx, executor, session, set, nil)
		require.Error(t, err, set)
		assertReadsAs(t, executor, session, "latin1")
	}
}

// TestClientCharsetStaysInVTGate tests that VTGate keeps character_set_client
// to itself: setting it sends nothing to MySQL, and it neither reserves a
// connection nor adds a SET_VAR hint to the queries that follow, even with
// system settings enabled.
func TestClientCharsetStaysInVTGate(t *testing.T) {
	for _, set := range []string{"set character_set_client = latin1", "set names latin1"} {
		t.Run(set, func(t *testing.T) {
			executor, _, _, sbc, ctx := createCustomExecutor(t, "{}", "8.0.0")
			executor.config.Normalize = true
			session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})

			_, err := executorExecSession(ctx, executor, session, set, map[string]*querypb.BindVariable{})
			require.NoError(t, err)
			assert.Empty(t, sbc.Queries, "setting the character set must send nothing to MySQL")
			assert.False(t, session.InReservedConn())
			vcursor, err := executor.newVCursor(session, makeComments(""), nil)
			require.NoError(t, err)
			assert.Empty(t, vcursor.PrepareSetVarComment())
			assert.Empty(t, session.SetPreQueries())

			for _, sql := range []string{"select 1 from user", "update user set col = 2", "create table foo(bar bigint)"} {
				sbc.Queries = nil
				_, err = executorExecSession(ctx, executor, session, sql, map[string]*querypb.BindVariable{})
				require.NoError(t, err, sql)
				assert.False(t, session.InReservedConn(), sql)
				for _, query := range sbc.Queries {
					assert.NotContains(t, query.Sql, "SET_VAR", sql)
					assert.NotContains(t, strings.ToLower(query.Sql), "character_set_client", sql)
				}
			}
		})
	}
}

// TestInvalidClientCharset tests that VTGate rejects the queries of a session
// whose character_set_client it did not write, which a gRPC client can send,
// rather than read them by a rule the client did not ask for.
func TestInvalidClientCharset(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := econtext.NewAutocommitSession(&vtgatepb.Session{SystemVariables: map[string]string{sysvars.CharacterSetClient.Name: "latin1"}})
	_, err := executorExecSession(ctx, executor, session, "select 1 from dual", nil)
	assert.ErrorContains(t, err, "invalid value for character_set_client in the session: latin1")
}

// TestPlanCacheClientCharset tests that a plan is not reused for a session
// whose character set reads the same text differently, on the text protocol
// and on the binary protocol, which looks the plan up by the text before it
// parses it.
func TestPlanCacheClientCharset(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared %v", prepared), func(t *testing.T) {
			executor, _, _, _, ctx := createExecutorEnv(t)
			_, err := executor.Execute(ctx, nil, "TestExecute", sessionWithCharset("latin1"), latin1Only, nil, prepared)
			require.NoError(t, err)
			assertCacheSize(t, executor.plans, 1)

			_, err = executor.Execute(ctx, nil, "TestExecute", sessionWithCharset("utf8mb4"), latin1Only, nil, prepared)
			assert.ErrorContains(t, err, "syntax error", "the UTF-8 session must not use the latin1 plan")
		})
	}
}

// TestShowVariablesClientCharset tests that SHOW VARIABLES reports the
// session's character_set_client rather than the one of VTGate's connection
// to MySQL.
func TestShowVariablesClientCharset(t *testing.T) {
	executor, _, _, sbc, ctx := createCustomExecutor(t, "{}", "8.0.0")
	executor.config.Normalize = true
	session := econtext.NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	_, err := executorExecSession(ctx, executor, session, "set names latin1", nil)
	require.NoError(t, err)

	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Variable_name|Value", "varchar|varchar"),
		"character_set_client|utf8mb4")})
	qr, err := executorExecSession(ctx, executor, session, "show variables like 'character_set_client'", nil)
	require.NoError(t, err)
	assert.Equal(t, `[[VARCHAR("character_set_client") VARCHAR("latin1")]]`, fmt.Sprintf("%v", qr.Rows))
}
