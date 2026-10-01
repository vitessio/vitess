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

package endtoend

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/colldata"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	charsetName = "utf8mb4"
)

func columnSize(cs collations.ID, size uint32) uint32 {
	// utf8_general_ci results in smaller max column sizes because MySQL 5.7 is silly
	if colldata.Lookup(cs).Charset().Name() == "utf8mb3" {
		return size * 3 / 4
	}
	return size
}

// Test the SQL query part of the API.
func TestQueries(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)

	// Try a simple error case.
	_, err = conn.ExecuteFetch("select * from aa", 1000, true)
	require.ErrorContains(t, err, "Table 'vttest.aa' doesn't exist")

	// Try a simple DDL.
	result, err := conn.ExecuteFetch("create table a(id int, name varchar(128), primary key(id))", 0, false)
	require.NoError(t, err, "create table failed: %v", err)
	assert.Equal(t, uint64(0), result.RowsAffected, "create table returned RowsAffected %v, was expecting 0", result.RowsAffected)

	// Try a simple insert.
	result, err = conn.ExecuteFetch("insert into a(id, name) values(10, 'nice name')", 1000, true)
	require.NoError(t, err, "insert failed: %v", err)

	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		assert.Failf(t, "unexpected insert result", "unexpected result for insert: %v", result)
	}

	// And re-read what we inserted.
	result, err = conn.ExecuteFetch("select * from a", 1000, true)
	require.NoError(t, err, "insert failed: %v", err)

	collID := getDefaultCollationID()
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         querypb.Type_INT32,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      collations.CollationBinaryID,
				Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
					querypb.MySqlFlag_PRI_KEY_FLAG |
					querypb.MySqlFlag_PART_KEY_FLAG |
					querypb.MySqlFlag_NUM_FLAG),
			},
			{
				Name:         "name",
				Type:         querypb.Type_VARCHAR,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "name",
				ColumnLength: columnSize(collID, 512),
				Charset:      uint32(collID),
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
		},
	}
	if !result.Equal(expectedResult) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedResult.Fields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		assert.True(t, result.Equal(expectedResult), "unexpected result for select, got:\n%v\nexpected:\n%v\n", result, expectedResult)
	}

	// Insert a few rows.
	for i := range 100 {
		result, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		require.NoError(t, err, "ExecuteFetch(%v) failed: %v", i, err)
		assert.Equal(t, uint64(1), result.RowsAffected, "insert into returned RowsAffected %v, was expecting 1", result.RowsAffected)
	}

	// And use a streaming query to read them back.
	// Do it twice to make sure state is reset properly.
	readRowsUsingStream(t, conn, 101)
	readRowsUsingStream(t, conn, 101)

	// And drop the table.
	result, err = conn.ExecuteFetch("drop table a", 0, false)
	require.NoError(t, err, "drop table failed: %v", err)
	assert.Equal(t, uint64(0), result.RowsAffected, "insert into returned RowsAffected %v, was expecting 0", result.RowsAffected)
}

func TestBinaryBindConnectionCharset(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse("select :v, charset(:v), collation(:v), coercibility(:v), :v + 0")
	require.NoError(t, err)
	query := sqlparser.NewParsedQuery(stmt)

	for name, charset := range map[string]collations.ID{"utf8mb4": 45, "sjis": 13, "cp932": 95, "gbk": 28, "big5": 1, "gb18030": 248} {
		t.Run(name, func(t *testing.T) {
			params := connParams
			params.Charset = charset
			conn, err := mysql.Connect(t.Context(), &params)
			require.NoError(t, err)
			t.Cleanup(conn.Close)

			for _, tc := range []struct {
				name   string
				value  string
				number string
			}{
				{name: "empty", number: "0"},
				{name: "sjis quote", value: "\x81'", number: "0"},
				{name: "sjis backslash", value: "\x81\\", number: "0"},
				{name: "controls and wildcards", value: "\xff\x00'\\%\\_", number: "0"},
				{name: "ASCII numeric string", value: "123", number: "123"},
				{name: "non-ASCII numeric string", value: "123\x81\x40", number: "123"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					sql, err := query.GenerateQuery(map[string]*querypb.BindVariable{
						"v": sqltypes.BytesBindVariable([]byte(tc.value)),
					}, nil)
					require.NoError(t, err)
					result, err := conn.ExecuteFetch(sql, 1, true)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					require.Len(t, result.Rows[0], 5)
					assert.Equal(t, tc.value, result.Rows[0][0].ToString())
					assert.Equal(t, "binary", result.Rows[0][1].ToString())
					assert.Equal(t, "binary", result.Rows[0][2].ToString())
					assert.Equal(t, "4", result.Rows[0][3].ToString())
					assert.Equal(t, tc.number, result.Rows[0][4].ToString())
				})
			}
		})
	}
}

// TestTextBindConnectionCharset checks text semantics as well as escape safety.
// The injection payloads are well-formed in their connection charset, so an
// invalid-character error cannot mask a misplaced closing quote. CAST catches
// lost repertoire metadata even when the decoded bytes still match.
func TestTextBindConnectionCharset(t *testing.T) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("select :v, charset(:v), coercibility(:v), collation(:v) = @@collation_connection")
	require.NoError(t, err)
	query := sqlparser.NewParsedQuery(stmt)
	stmt, err = parser.Parse("select hex(cast(:v as char character set latin1)), hex(cast(_latin1 :v as char character set latin1))")
	require.NoError(t, err)
	conversionQuery := sqlparser.NewParsedQuery(stmt)

	t.Run("formatted literals", func(t *testing.T) {
		params := connParams
		params.Charset = 45
		conn, err := mysql.Connect(t.Context(), &params)
		require.NoError(t, err)
		t.Cleanup(conn.Close)
		for _, query := range []string{
			"create temporary table literal_encoding (v enum('é\\n'), n varchar(10) character set latin1 default 'é\\n' comment 'é\\n', key k(n) comment 'é\\n') comment='é\\n'",
			"insert into literal_encoding () values ()",
			"select hex(cast(n as char character set latin1)) from literal_encoding",
			"show tables like 'é\\n'",
			"prepare encoding_stmt from 'select 1 -- é\\n'",
			"deallocate prepare encoding_stmt",
			"select hex(cast(N'é\\n' as char character set latin1))",
		} {
			stmt, err := parser.ParseStrictDDL(query)
			require.NoError(t, err)
			result, err := conn.ExecuteFetch(sqlparser.String(stmt), 10, true)
			require.NoError(t, err, query)
			if strings.HasPrefix(query, "select") {
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 1)
				assert.Equal(t, "E90A", result.Rows[0][0].ToString())
			}
		}
	})

	t.Run("different client and connection charsets", func(t *testing.T) {
		params := connParams
		params.Charset = 45
		conn, err := mysql.Connect(t.Context(), &params)
		require.NoError(t, err)
		t.Cleanup(conn.Close)
		_, err = conn.ExecuteFetch("set character_set_connection = latin1", 0, false)
		require.NoError(t, err)
		sql, err := conversionQuery.GenerateQuery(map[string]*querypb.BindVariable{
			"v": sqltypes.StringBindVariable("p'é\n"),
		}, nil)
		require.NoError(t, err)
		result, err := conn.ExecuteFetch(sql, 1, true)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Len(t, result.Rows[0], 2)
		assert.Equal(t, "7027E90A", result.Rows[0][0].ToString())
		assert.Equal(t, "7027C3A90A", result.Rows[0][1].ToString())

		stmt, err := parser.Parse("select hex(cast(N'é\\n' as char character set latin1))")
		require.NoError(t, err)
		result, err = conn.ExecuteFetch(sqlparser.String(stmt), 1, true)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Len(t, result.Rows[0], 1)
		assert.Equal(t, "E90A", result.Rows[0][0].ToString())
	})

	type hazard struct {
		name  string
		value string
	}
	for _, cs := range []struct {
		name    string
		charset collations.ID
		cases   []hazard
	}{
		{name: "utf8mb4", charset: 45, cases: []hazard{
			// `é` is c3 a9, so the quote and the backslash each land right
			// after a high byte.
			{name: "high run then quote", value: "é', 'injected"},
			{name: "high run then backslash", value: "é\\', 'injected"},
			{name: "high run then NUL", value: "é\x00"},
			{name: "high run then backspace", value: "é\b"},
			{name: "high run then newline", value: "é\n"},
			{name: "high run then carriage return", value: "é\r"},
			{name: "high run then tab", value: "é\t"},
			{name: "high run then ctrl-Z", value: "é\x1a"},
			{name: "high run then wildcard", value: "é\\%"},
			{name: "later high run", value: "x' é\n"},
			{name: "multiple high runs", value: "é\né\\"},
			{name: "ASCII quote", value: "it's fine"},
		}},
		{name: "sjis", charset: 13, cases: []hazard{
			// 81 40 and 81 5c are both real sjis characters -- 0x5c is a
			// valid trail byte, which is the whole problem.
			{name: "lead byte then quote", value: "\x81\x40', 'injected"},
			{name: "lead byte then backslash", value: "\x81\x5c', 'injected"},
			{name: "ASCII quote", value: "it's fine"},
		}},
		{name: "cp932", charset: 95, cases: []hazard{
			{name: "lead byte then quote", value: "\x81\x40', 'injected"},
			{name: "lead byte then backslash", value: "\x81\x5c', 'injected"},
		}},
		{name: "gbk", charset: 28, cases: []hazard{
			{name: "lead byte then quote", value: "\x81\x40', 'injected"},
			{name: "lead byte then backslash", value: "\x81\x5c', 'injected"},
		}},
		{name: "big5", charset: 1, cases: []hazard{
			{name: "lead byte then quote", value: "\xa1\x40', 'injected"},
			{name: "lead byte then backslash", value: "\xa1\x5c', 'injected"},
		}},
		{name: "gb18030", charset: 248, cases: []hazard{
			{name: "lead byte then quote", value: "\x81\x40', 'injected"},
			{name: "lead byte then backslash", value: "\x81\x5c', 'injected"},
			{name: "four-byte character", value: "\x81\x30\x81\x30\\', 'injected"},
		}},
	} {
		t.Run(cs.name, func(t *testing.T) {
			params := connParams
			params.Charset = cs.charset
			conn, err := mysql.Connect(t.Context(), &params)
			require.NoError(t, err)
			t.Cleanup(conn.Close)

			for _, tc := range cs.cases {
				t.Run(tc.name, func(t *testing.T) {
					sql, err := query.GenerateQuery(map[string]*querypb.BindVariable{
						"v": sqltypes.StringBindVariable(tc.value),
					}, nil)
					require.NoError(t, err)
					result, err := conn.ExecuteFetch(sql, 1, true)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					require.Len(t, result.Rows[0], 4, "literal did not hold the whole value")
					assert.Equal(t, tc.value, result.Rows[0][0].ToString())
					assert.Equal(t, cs.name, result.Rows[0][1].ToString())
					assert.Equal(t, "4", result.Rows[0][2].ToString())
					assert.Equal(t, "1", result.Rows[0][3].ToString())

					if cs.name == "utf8mb4" {
						sql, err = conversionQuery.GenerateQuery(map[string]*querypb.BindVariable{
							"v": sqltypes.StringBindVariable(tc.value),
						}, nil)
						require.NoError(t, err)
						result, err = conn.ExecuteFetch(sql, 1, true)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1)
						require.Len(t, result.Rows[0], 2)
						want := fmt.Sprintf("%X", strings.ReplaceAll(tc.value, "é", "\xe9"))
						assert.Equal(t, want, result.Rows[0][0].ToString())
						assert.Equal(t, fmt.Sprintf("%X", tc.value), result.Rows[0][1].ToString())
					}
				})
			}
		})
	}
}

func TestLargeQueries(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)

	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randString := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.IntN(len(letterBytes))]
		}
		return string(b)
	}

	for i := range 2 {
		for j := -2; j < 2; j++ {
			expectedString := randString((i+1)*mysql.MaxPacketSize + j)

			result, err := conn.ExecuteFetch(fmt.Sprintf("select \"%s\"", expectedString), -1, true)
			require.NoError(t, err, "ExecuteFetch failed: %v", err)

			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0].IsNull() {
				require.Fail(t, "ExecuteFetch on large query returned poorly-formed result. "+
					"Expected single row single column string.")
			}
			require.Equal(t, expectedString, result.Rows[0][0].ToString(), "Result row was incorrect. Suppressing large string")
		}
	}
}

func readRowsUsingStream(t *testing.T, conn *mysql.Conn, expectedCount int) {
	// Start the streaming query.
	if err := conn.ExecuteStreamFetch("select * from a"); err != nil {
		require.NoError(t, err)
	}

	// Check the fields.
	collID := getDefaultCollationID()
	expectedFields := []*querypb.Field{
		{
			Name:         "id",
			Type:         querypb.Type_INT32,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "id",
			ColumnLength: 11,
			Charset:      collations.CollationBinaryID,
			Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
				querypb.MySqlFlag_PRI_KEY_FLAG |
				querypb.MySqlFlag_PART_KEY_FLAG |
				querypb.MySqlFlag_NUM_FLAG),
		},
		{
			Name:         "name",
			Type:         querypb.Type_VARCHAR,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "name",
			ColumnLength: columnSize(collID, 512),
			Charset:      uint32(collID),
		},
	}
	fields, err := conn.Fields()
	require.NoError(t, err, "Fields failed: %v", err)

	if !sqltypes.FieldsEqual(fields, expectedFields) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedFields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		require.True(t, sqltypes.FieldsEqual(fields, expectedFields), "fields are not right, got:\n%v\nexpected:\n%v", fields, expectedFields)
	}

	// Read the rows.
	count := 0
	for {
		row, err := conn.FetchNext(nil)
		require.NoError(t, err, "FetchNext failed: %v", err)

		if row == nil {
			// We're done.
			break
		}
		require.Len(t, row, 2, "Unexpected row found: %v", row)

		count++
	}
	assert.Equal(t, expectedCount, count, "Got unexpected count %v for query, was expecting %v", count, expectedCount)

	conn.CloseResult()
}

func doTestWarnings(t *testing.T, disableClientDeprecateEOF bool) {
	ctx := t.Context()

	connParams.DisableClientDeprecateEOF = disableClientDeprecateEOF

	conn, err := mysql.Connect(ctx, &connParams)
	expectNoError(t, err)
	defer conn.Close()

	result, err := conn.ExecuteFetch("create table a(id int, val int not null, primary key(id))", 0, false)
	require.NoError(t, err, "create table failed: %v", err)
	assert.Equal(t, uint64(0), result.RowsAffected, "create table returned RowsAffected %v, was expecting 0", result.RowsAffected)

	// Disable strict mode
	_, err = conn.ExecuteFetch("set session sql_mode=''", 0, false)
	require.NoError(t, err, "disable strict mode failed: %v", err)

	// Try a simple insert with a null value
	result, warnings, err := conn.ExecuteFetchWithWarningCount("insert into a(id) values(10)", 1000, true)
	require.NoError(t, err, "insert failed: %v", err)

	assert.Equal(t, uint64(1), result.RowsAffected, "unexpected rows affected by insert; result: %v", result)
	assert.Empty(t, result.Rows, "unexpected row count in result for insert: %v", result)
	assert.Equal(t, uint16(1), warnings, "unexpected result for warnings: %v", warnings)

	_, err = conn.ExecuteFetch("drop table a", 0, false)
	require.NoError(t, err, "create table failed: %v", err)
}

func TestWarningsDeprecateEOF(t *testing.T) {
	doTestWarnings(t, false)
}

func TestWarningsNoDeprecateEOF(t *testing.T) {
	doTestWarnings(t, true)
}

func TestSysInfo(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("drop table if exists `a`", 1000, true)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("CREATE TABLE `a` (`one` int NOT NULL,`two` int NOT NULL,PRIMARY KEY (`one`,`two`)) ENGINE=InnoDB DEFAULT CHARSET="+charsetName, 1000, true)
	require.NoError(t, err)
	defer conn.ExecuteFetch("drop table `a`", 1000, true)

	qr, err := conn.ExecuteFetch(`SELECT
		column_name column_name,
		data_type data_type,
		column_type full_data_type,
		character_maximum_length character_maximum_length,
		numeric_precision numeric_precision,
		numeric_scale numeric_scale,
		datetime_precision datetime_precision,
		column_default column_default,
		is_nullable is_nullable,
		extra extra,
		table_name table_name
	FROM information_schema.columns
	WHERE table_schema = 'vttest' and table_name = 'a'
	ORDER BY ordinal_position`, 1000, true)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 2)

	// is_nullable
	assert.Equal(t, `VARCHAR("NO")`, qr.Rows[0][8].String())
	assert.Equal(t, `VARCHAR("NO")`, qr.Rows[1][8].String())

	// table_name
	// This can be either a VARCHAR or a VARBINARY. On Linux and MySQL 8, the
	// string is tagged with a binary encoding, so it is VARBINARY.
	// On case-insensitive filesystems, it's a VARCHAR.
	assert.Contains(t, []string{`VARBINARY("a")`, `VARCHAR("a")`}, qr.Rows[0][10].String())
	assert.Contains(t, []string{`VARBINARY("a")`, `VARCHAR("a")`}, qr.Rows[1][10].String())

	assert.Equal(t, sqltypes.Uint64, qr.Fields[4].Type)
	assert.Equal(t, querypb.Type_UINT64, qr.Rows[0][4].Type())
}

func getDefaultCollationID() collations.ID {
	collationHandler := collations.MySQL8()
	return collationHandler.DefaultCollationForCharset(charsetName)
}
