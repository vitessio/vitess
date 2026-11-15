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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// The expected values in these tests were measured on MySQL 8.0.46 and
// 8.4.13, which agree on all of them.

// setConvertResultCharset sets --convert-result-charset for the test. The
// executor reads the flag when it is created.
func setConvertResultCharset(t *testing.T, on bool) {
	old := convertResultCharset
	convertResultCharset = on
	t.Cleanup(func() { convertResultCharset = old })
}

func resultCharsetSession() *vtgatepb.Session {
	return &vtgatepb.Session{
		Autocommit:   true,
		TargetString: KsTestUnsharded + "@primary",
		Options:      &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
	}
}

// errorSession is a session for statements that fail. Its keyspace is
// another one than that of resultCharsetSession, because TestVTGateExecuteError
// counts the errors of statements in TestUnsharded.
func errorSession() *vtgatepb.Session {
	session := resultCharsetSession()
	session.TargetString = KsTestSharded + "@primary"
	return session
}

// textResult is a utf8mb4 VARCHAR(10) column with the name é and the value
// é€😀, as a tablet returns it to vtgate.
func textResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "é", OrgName: "u4", Table: "t", OrgTable: "t", Database: "db", Type: sqltypes.VarChar, Charset: uint32(collations.CollationUtf8mb4ID), ColumnLength: 40},
			{Name: "i", Type: sqltypes.Int32, Charset: uint32(collations.CollationBinaryID), ColumnLength: 11},
		},
		Rows: []sqltypes.Row{{sqltypes.NewVarChar("é€😀"), sqltypes.NewInt32(42)}},
	}
}

// TestSetResultCharset covers how SET NAMES, SET CHARACTER SET and SET
// character_set_results set the character_set_results of the session with
// --convert-result-charset: the collation that result fields then report.
func TestSetResultCharset(t *testing.T) {
	setConvertResultCharset(t, true)
	vtg, _, ctx := createVtgateEnv(t)

	tests := []struct {
		set       string
		collation collations.ID
		null      bool
	}{
		{set: "set names utf8mb4", collation: 255},
		{set: "set names utf8mb4 collate utf8mb4_bin", collation: 46},
		{set: "set names 'utf8mb3' collate utf8mb3_bin", collation: 83},
		{set: "set names utf8", collation: 33},
		{set: "set names utf8mb3", collation: 33},
		{set: "set names utf8 collate utf8_bin", collation: 83},
		{set: "set names utf8mb4 collate 'utf8mb4_0900_bin'", collation: 309},
		{set: "set names default", collation: 0},
		{set: "set character set utf8", collation: 33},
		{set: "set charset default", collation: 0},
		{set: "set character_set_results = gb2312", collation: 24},
		{set: "set character_set_results = 'sjis'", collation: 13},
		{set: "set character_set_results = ucs2", collation: 35},
		{set: "set character_set_results = utf8", collation: 33},
		{set: "set character_set_results = binary", collation: 63},
		{set: "set character_set_results = 46", collation: 46},
		{set: "set character_set_results = 8", collation: 8},
		{set: "set character_set_results = concat('lat', 'in1')", collation: 8},
		{set: "set character_set_results = null", null: true},
		{set: "set character_set_results = default", collation: 0},
		{set: "set @@session.character_set_results = gb2312", collation: 24},
		{set: "set session character_set_results = latin1", collation: 8},
		{set: "set names utf8mb4 collate utf8mb4_bin, character_set_results = gb2312", collation: 24},
		{set: "set character_set_results = gb2312, names utf8mb3", collation: 33},
	}
	for _, tc := range tests {
		t.Run(tc.set, func(t *testing.T) {
			// Start from another value, so that the test sees the change.
			session := resultCharsetSession()
			session.CharacterSetResults = 11
			session, _, err := vtg.Execute(ctx, nil, session, tc.set, nil, false)
			require.NoError(t, err)
			assert.Equal(t, uint32(tc.collation), session.CharacterSetResults)
			assert.Equal(t, tc.null, session.CharacterSetResultsNull)
		})
	}
}

// TestSetResultCharsetErrors covers the values that MySQL refuses, with the
// error number it refuses them with, and the character sets that SET NAMES
// and SET CHARACTER SET refuse in vtgate: all but utf8mb4 and utf8mb3.
func TestSetResultCharsetErrors(t *testing.T) {
	setConvertResultCharset(t, true)
	vtg, _, ctx := createVtgateEnv(t)

	tests := []struct {
		set     string
		num     sqlerror.ErrorCode
		message string
	}{
		{"set names utf8 collate utf8mb4_bin", sqlerror.ERCollationCharsetMismatch, "COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8mb3'"},
		{"set names utf8mb4 collate binary", sqlerror.ERCollationCharsetMismatch, "COLLATION 'binary' is not valid for CHARACTER SET 'utf8mb4'"},
		{"set names utf8mb4 collate nosuch", sqlerror.ERUnknownCollation, "Unknown collation: 'nosuch'"},
		{"set character_set_results = nosuch", sqlerror.ERUnknownCharacterSet, "Unknown character set: 'nosuch'"},
		{"set character_set_results = utf8mb4_bin", sqlerror.ERUnknownCharacterSet, "Unknown character set: 'utf8mb4_bin'"},
		{"set character_set_results = ''", sqlerror.ERUnknownCharacterSet, "Unknown character set: ''"},
		{"set character_set_results = 'NULL'", sqlerror.ERUnknownCharacterSet, "Unknown character set: 'NULL'"},
		{"set character_set_results = 0", sqlerror.ERUnknownCharacterSet, "Unknown character set: '0'"},
		{"set character_set_results = 9999", sqlerror.ERUnknownCharacterSet, "Unknown character set: '9999'"},
		{"set character_set_results = 1.5", sqlerror.ERWrongTypeForVar, "Incorrect argument type to variable 'character_set_results'"},
		// vtgate reads every query as utf8mb4, and SET NAMES refuses every
		// other character set.
		{"set names gb2312", sqlerror.ERNotSupportedYet, "charset/name gb2312 is not supported"},
		{"set charset ascii", sqlerror.ERNotSupportedYet, "charset/name ascii is not supported"},
		{"set names latin1", sqlerror.ERNotSupportedYet, "charset/name latin1 is not supported: vtgate with --convert-result-charset accepts SET NAMES and SET CHARACTER SET with utf8mb4 and utf8mb3, and SET character_set_results gives results in another character set"},
		{"set names 'latin1' collate latin1_german1_ci", sqlerror.ERNotSupportedYet, "charset/name latin1 is not supported"},
		{"set names latin1 collate utf8mb4_bin", sqlerror.ERNotSupportedYet, "charset/name latin1 is not supported"},
		{"set names binary", sqlerror.ERNotSupportedYet, "charset/name binary is not supported"},
		{"set names ucs2", sqlerror.ERNotSupportedYet, "charset/name ucs2 is not supported"},
		{"set character set latin1", sqlerror.ERNotSupportedYet, "charset/name latin1 is not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.set, func(t *testing.T) {
			session := errorSession()
			session.CharacterSetResults = 11
			session, _, err := vtg.Execute(ctx, nil, session, tc.set, nil, false)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.message)
			sqlErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			assert.Equal(t, tc.num, sqlErr.Num)
			// The session is as it was.
			assert.Equal(t, uint32(11), session.CharacterSetResults)
		})
	}
}

// TestReadResultCharset covers SELECT @@character_set_results and SHOW
// VARIABLES with --convert-result-charset: vtgate answers with the value of
// the session, which MySQL behind it does not have.
func TestReadResultCharset(t *testing.T) {
	setConvertResultCharset(t, true)
	vtg, sbc, ctx := createVtgateEnv(t)

	tests := []struct {
		set  string
		want sqltypes.Value
	}{
		{"set names utf8", sqltypes.NewVarChar("utf8mb3")},
		{"set character_set_results = 5", sqltypes.NewVarChar("latin1")},
		{"set character_set_results = 46", sqltypes.NewVarChar("utf8mb4")},
		{"set character_set_results = binary", sqltypes.NewVarChar("binary")},
		{"set character_set_results = gb2312", sqltypes.NewVarChar("gb2312")},
		{"set character_set_results = default", sqltypes.NewVarChar("utf8mb4")},
		{"set character_set_results = null", sqltypes.NULL},
	}
	for _, tc := range tests {
		t.Run(tc.set, func(t *testing.T) {
			session, _, err := vtg.Execute(ctx, nil, resultCharsetSession(), tc.set, nil, false)
			require.NoError(t, err)

			for _, query := range []string{"select @@character_set_results", "select @@session.character_set_results"} {
				_, qr, err := vtg.Execute(ctx, nil, session, query, nil, false)
				require.NoError(t, err)
				require.Len(t, qr.Rows, 1, query)
				assert.Equal(t, tc.want.ToString(), qr.Rows[0][0].ToString(), query)
				assert.Equal(t, tc.want.IsNull(), qr.Rows[0][0].IsNull(), query)
			}

			// MySQL shows NULL as the empty string.
			sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Variable_name|Value", "varchar|varchar"),
				"character_set_results|utf8mb4",
			)})
			_, qr, err := vtg.Execute(ctx, nil, session, "show variables like 'character_set_results'", nil, false)
			require.NoError(t, err)
			require.Len(t, qr.Rows, 1)
			assert.Equal(t, tc.want.ToString(), qr.Rows[0][1].ToString())
		})
	}
}

// TestConvertResultCharset converts the results of Execute, StreamExecute
// and Prepare to the character_set_results of the session, once: with gb2312,
// é€😀 is A8 A6 3F 3F, which a second conversion would turn into ????.
func TestConvertResultCharset(t *testing.T) {
	setConvertResultCharset(t, true)
	vtg, sbc, ctx := createVtgateEnv(t)

	check := func(t *testing.T, fields []*querypb.Field) {
		require.Len(t, fields, 2)
		assert.Equal(t, "\xA8\xA6", fields[0].Name)
		assert.Equal(t, "u4", fields[0].OrgName)
		assert.Equal(t, uint32(24), fields[0].Charset)
		assert.Equal(t, uint32(20), fields[0].ColumnLength)
		assert.Equal(t, uint32(collations.CollationBinaryID), fields[1].Charset)
		assert.Equal(t, uint32(11), fields[1].ColumnLength)
	}
	session, _, err := vtg.Execute(ctx, nil, resultCharsetSession(), "set character_set_results = gb2312", nil, false)
	require.NoError(t, err)

	t.Run("Execute", func(t *testing.T) {
		sbc.SetResults([]*sqltypes.Result{textResult()})
		_, qr, err := vtg.Execute(ctx, nil, session, "select u4, i from t1", nil, false)
		require.NoError(t, err)
		check(t, qr.Fields)
		assert.Equal(t, "\xA8\xA6??", qr.Rows[0][0].ToString())
		assert.Equal(t, "42", qr.Rows[0][1].ToString())
	})

	t.Run("ExecuteMulti", func(t *testing.T) {
		sbc.SetResults([]*sqltypes.Result{textResult()})
		_, qrs, err := vtg.ExecuteMulti(ctx, nil, session, "select u4, i from t1")
		require.NoError(t, err)
		require.Len(t, qrs, 1)
		check(t, qrs[0].Fields)
		assert.Equal(t, "\xA8\xA6??", qrs[0].Rows[0][0].ToString())
	})

	t.Run("StreamExecute", func(t *testing.T) {
		sbc.SetResults([]*sqltypes.Result{textResult()})
		var fields []*querypb.Field
		var rows []sqltypes.Row
		_, err := vtg.StreamExecute(ctx, nil, session, "select u4, i from t1", nil, false, func(qr *sqltypes.Result) error {
			if qr.Fields != nil {
				fields = qr.Fields
			}
			rows = append(rows, qr.Rows...)
			return nil
		})
		require.NoError(t, err)
		check(t, fields)
		require.Len(t, rows, 1)
		assert.Equal(t, "\xA8\xA6??", rows[0][0].ToString())
	})

	t.Run("Prepare", func(t *testing.T) {
		sbc.SetResults([]*sqltypes.Result{textResult()})
		_, fields, _, err := vtg.Prepare(ctx, session, "select u4, i from t1 where i = ?")
		require.NoError(t, err)
		check(t, fields)
	})

	t.Run("NULL", func(t *testing.T) {
		// A column of latin1_german1_ci arrives from a tablet with
		// --queryserver-config-raw-result-charset as MySQL sends it with
		// character_set_results = NULL. vtgate holds its text in utf8mb4
		// and sends it out in latin1 again, with the length of the column.
		session, _, err := vtg.Execute(ctx, nil, resultCharsetSession(), "set character_set_results = null", nil, false)
		require.NoError(t, err)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{{Name: "Ä", Type: sqltypes.VarChar, Charset: 5, ColumnLength: 10}},
			Rows:   []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller"))}},
		}})
		_, qr, err := vtg.Execute(ctx, nil, session, "select l from t1", nil, false)
		require.NoError(t, err)
		assert.Equal(t, "Ä", qr.Fields[0].Name)
		assert.Equal(t, uint32(5), qr.Fields[0].Charset)
		assert.Equal(t, uint32(10), qr.Fields[0].ColumnLength)
		assert.Equal(t, "M\xFCller", qr.Rows[0][0].ToString())
	})
}

// TestResultCharsetWithoutFlag shows that without --convert-result-charset
// vtgate behaves as it did: results stay in utf8mb4 whatever the session has,
// SET NAMES with a COLLATE changes nothing, and character_set_results is
// checked against MySQL and refused for a character set that Vitess does not
// parse.
func TestResultCharsetWithoutFlag(t *testing.T) {
	setConvertResultCharset(t, false)
	vtg, sbc, ctx := createVtgateEnv(t)

	session := resultCharsetSession()
	session, _, err := vtg.Execute(ctx, nil, session, "set names latin1 collate utf8mb4_bin", nil, false)
	require.NoError(t, err)
	assert.Zero(t, session.CharacterSetResults)

	_, _, err = vtg.Execute(ctx, nil, errorSession(), "set character_set_results = gbk", nil, false)
	require.ErrorContains(t, err, "unsupported connection character set gbk for character_set_results: use utf8mb4")

	// A gRPC client can put any value in the session, and it is ignored.
	session.CharacterSetResults = 24
	sbc.SetResults([]*sqltypes.Result{textResult()})
	_, qr, err := vtg.Execute(ctx, nil, session, "select u4, i from t1", nil, false)
	require.NoError(t, err)
	assert.Equal(t, textResult().Fields, qr.Fields)
	assert.Equal(t, "é€😀", qr.Rows[0][0].ToString())

	sbc.SetResults([]*sqltypes.Result{sandboxconn.SingleRowResult})
	_, qr, err = vtg.Execute(ctx, nil, session, "select @@character_set_results from t1", nil, false)
	require.NoError(t, err)
	// The query goes to MySQL.
	assert.Contains(t, sbc.Queries[len(sbc.Queries)-1].Sql, "@@character_set_results")
	assert.Equal(t, sandboxconn.SingleRowResult.Rows, qr.Rows)
}

// TestHandshakeResultCharset covers the character_set_results that a MySQL
// protocol client gets with the collation of its handshake. MySQL takes the
// collation itself, and its default for 0 and for an ID it does not have.
// After COM_RESET_CONNECTION, MySQL has its default, not the collation of the
// handshake.
func TestHandshakeResultCharset(t *testing.T) {
	setConvertResultCharset(t, true)
	vtg, _, _ := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)

	tests := []struct {
		handshake, want collations.ID
	}{
		{46, 46},   // utf8mb4_bin
		{255, 255}, // utf8mb4_0900_ai_ci
		{33, 33},   // utf8mb3_general_ci
		{83, 83},   // utf8mb3_bin
		{0, 0},
		{100, 0}, // no collation has the ID 100
	}
	for _, tc := range tests {
		c := &mysql.Conn{CharacterSet: tc.handshake}
		require.NoError(t, vh.CheckHandshake(c), "handshake %d", tc.handshake)
		assert.Equal(t, uint32(tc.want), vh.session(c).CharacterSetResults, "handshake %d", tc.handshake)
	}

	c := &mysql.Conn{CharacterSet: 33}
	require.Equal(t, uint32(33), vh.session(c).CharacterSetResults)
	vh.ComResetConnection(c)
	assert.Zero(t, vh.session(c).CharacterSetResults)
}

// TestHandshakeRefused covers the clients that vtgate refuses with
// --convert-result-charset: those of a character set other than utf8mb4
// and utf8mb3, with the error that MySQL refuses a ucs2 client with. Without
// the flag, vtgate accepts them all.
func TestHandshakeRefused(t *testing.T) {
	tests := []struct {
		handshake collations.ID
		charset   string
	}{
		{8, "latin1"},
		{5, "latin1"},  // latin1_german1_ci
		{31, "latin1"}, // latin1_german2_ci, which Vitess does not implement
		{24, "gb2312"},
		{28, "gbk"}, // which Vitess does not implement
		{63, "binary"},
		{35, "ucs2"},
		{54, "utf16"},
		{56, "utf16le"},
		{60, "utf32"},
		{248, "gb18030"},
	}

	t.Run("on", func(t *testing.T) {
		setConvertResultCharset(t, true)
		vtg, _, _ := createVtgateEnv(t)
		vh := newVtgateHandler(vtg)
		for _, tc := range tests {
			err := vh.CheckHandshake(&mysql.Conn{CharacterSet: tc.handshake})
			require.Error(t, err, "handshake %d", tc.handshake)
			sqlErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			assert.Equal(t, sqlerror.ERWrongValueForVar, sqlErr.Num, "handshake %d", tc.handshake)
			assert.Equal(t, sqlerror.SSClientError, sqlErr.State, "handshake %d", tc.handshake)
			assert.Equal(t, "Variable 'character_set_client' can't be set to the value of '"+tc.charset+"': vtgate with --convert-result-charset accepts utf8mb4 and utf8mb3 clients, and SET character_set_results gives results in another character set", sqlErr.Message, "handshake %d", tc.handshake)
		}
	})

	t.Run("off", func(t *testing.T) {
		setConvertResultCharset(t, false)
		vtg, _, _ := createVtgateEnv(t)
		vh := newVtgateHandler(vtg)
		for _, tc := range tests {
			c := &mysql.Conn{CharacterSet: tc.handshake}
			require.NoError(t, vh.CheckHandshake(c), "handshake %d", tc.handshake)
			assert.Zero(t, vh.session(c).CharacterSetResults, "handshake %d", tc.handshake)
		}
	})
}

// TestErrorMessageResultCharset covers the error messages that a MySQL
// protocol client gets: with gb2312 results, "Duplicate entry 'Müller'" has
// 4D A8 B9 6C 6C 65 72, and with NULL it is as it was.
func TestErrorMessageResultCharset(t *testing.T) {
	const message = "Duplicate entry 'Müller' for key 'gate_m.uk_l'"

	t.Run("on", func(t *testing.T) {
		setConvertResultCharset(t, true)
		vtg, _, _ := createVtgateEnv(t)
		vh := newVtgateHandler(vtg)
		c := &mysql.Conn{}
		vh.NewConnection(c)
		require.NotNil(t, c.ErrorMessage)
		// Before the first query, the connection has no session.
		assert.Equal(t, message, c.ErrorMessage(message))

		session := vh.session(c)
		session.CharacterSetResults = 24
		assert.Equal(t, "Duplicate entry 'M\xA8\xB9ller' for key 'gate_m.uk_l'", c.ErrorMessage(message))
		session.CharacterSetResultsNull = true
		assert.Equal(t, message, c.ErrorMessage(message))
	})

	t.Run("off", func(t *testing.T) {
		setConvertResultCharset(t, false)
		vtg, _, _ := createVtgateEnv(t)
		vh := newVtgateHandler(vtg)
		c := &mysql.Conn{}
		vh.NewConnection(c)
		assert.Nil(t, c.ErrorMessage)
	})
}
