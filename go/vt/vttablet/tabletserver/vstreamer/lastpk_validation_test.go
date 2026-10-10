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

package vstreamer

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// field is a shorthand for a table column of a given type.
func field(name string, typ querypb.Type) *querypb.Field {
	return &querypb.Field{Name: name, Type: typ}
}

// clientValue builds a value the way a client-supplied lastpk arrives: the type
// comes from the request's own Fields and the bytes are taken verbatim, exactly
// what sqltypes.MakeRowTrusted does. No validation happens on the way in.
func clientValue(typ querypb.Type, val string) sqltypes.Value {
	return sqltypes.MakeTrusted(typ, []byte(val))
}

// TestGetLastPKFromQR checks that getLastPKFromQR decodes a well-formed lastpk
// QueryResult, and rejects a malformed one with INVALID_ARGUMENT instead of
// panicking in sqltypes.MakeRowTrusted, which trusts the row's shape.
func TestGetLastPKFromQR(t *testing.T) {
	idField := field("id", querypb.Type_INT64)
	nameField := field("name", querypb.Type_VARCHAR)
	testcases := []struct {
		name    string
		qr      *querypb.QueryResult
		want    []sqltypes.Value
		wantErr string
	}{
		{
			name: "no lastpk",
		},
		{
			name: "one value",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows:   []*querypb.Row{{Lengths: []int64{1}, Values: []byte("5")}},
			},
			want: []sqltypes.Value{sqltypes.NewInt64(5)},
		},
		{
			name: "a NULL value and a string",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField, nameField},
				Rows:   []*querypb.Row{{Lengths: []int64{-1, 3}, Values: []byte("abc")}},
			},
			want: []sqltypes.Value{sqltypes.NULL, sqltypes.NewVarChar("abc")},
		},
		{
			name: "no rows",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
			},
			wantErr: "lastpk has 0 rows, expected 1",
		},
		{
			name: "two rows",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows: []*querypb.Row{
					{Lengths: []int64{1}, Values: []byte("1")},
					{Lengths: []int64{1}, Values: []byte("2")},
				},
			},
			wantErr: "lastpk has 2 rows, expected 1",
		},
		{
			name: "nil row",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows:   []*querypb.Row{nil},
			},
			wantErr: "lastpk row is nil",
		},
		{
			name: "more lengths than fields",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows:   []*querypb.Row{{Lengths: []int64{1, 1}, Values: []byte("1")}},
			},
			wantErr: "lastpk row has 2 values, but there are 1 fields",
		},
		{
			name: "fewer lengths than fields",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField, nameField},
				Rows:   []*querypb.Row{{Lengths: []int64{1}, Values: []byte("1")}},
			},
			wantErr: "lastpk row has 1 values, but there are 2 fields",
		},
		{
			name: "nil field",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{nil},
				Rows:   []*querypb.Row{{Lengths: []int64{1}, Values: []byte("1")}},
			},
			wantErr: "lastpk field 0 is nil",
		},
		{
			name: "negative length other than NULL",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows:   []*querypb.Row{{Lengths: []int64{-2}}},
			},
			wantErr: "lastpk value 0 has an invalid length -2",
		},
		{
			name: "length past the values",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField},
				Rows:   []*querypb.Row{{Lengths: []int64{10}, Values: []byte("1")}},
			},
			wantErr: "lastpk value 0 has length 10, but only 1 bytes of values remain",
		},
		{
			name: "second length past the values",
			qr: &querypb.QueryResult{
				Fields: []*querypb.Field{idField, nameField},
				Rows:   []*querypb.Row{{Lengths: []int64{1, 3}, Values: []byte("1ab")}},
			},
			wantErr: "lastpk value 1 has length 3, but only 2 bytes of values remain",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getLastPKFromQR(tc.qr)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestValidateLastPKRejectsInjection covers a client-supplied lastpk being
// spliced into the copy-phase WHERE clause.
//
// Value.EncodeSQL quotes and escapes only the Null, binary, quoted and Bit
// types. For a numeric type it writes the bytes verbatim, and the type comes
// from the client's own request rather than from the table. A client can
// therefore declare INT64 and send arbitrary SQL, which buildSelect writes
// straight into the snapshot query.
func TestValidateLastPKRejectsInjection(t *testing.T) {
	testcases := []struct {
		name   string
		fields []*querypb.Field
		lastpk []sqltypes.Value
	}{
		{
			name:   "the reported vector",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{
				clientValue(querypb.Type_INT64, "0) union select user, authentication_string from mysql.user #"),
			},
		},
		{
			name:   "int64 that is not a number",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, "abc")},
		},
		{
			name:   "int64 with a trailing statement",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, "1; drop table t")},
		},
		{
			name:   "int64 with surrounding space",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, " 42 ")},
		},
		{
			// decimal.NewFromMySQL stops scanning once the integral part exceeds
			// MySQL's precision, so a tail after it is not rejected by parsing
			// alone.
			name:   "decimal long integral with a tail",
			fields: []*querypb.Field{field("d", querypb.Type_DECIMAL)},
			lastpk: []sqltypes.Value{
				clientValue(querypb.Type_DECIMAL, "1"+strings.Repeat("9", 79)+") union select 1 #"),
			},
		},
		{
			// fastparse accepts Go's float words; MySQL has no such literals.
			name:   "float spelled NaN",
			fields: []*querypb.Field{field("f", querypb.Type_FLOAT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_FLOAT64, "NaN")},
		},
		{
			// The client declares a type the column does not have. The value has
			// to be checked against the table, not against the request.
			name:   "client declares int64 for a varchar column",
			fields: []*querypb.Field{field("name", querypb.Type_VARCHAR)},
			lastpk: []sqltypes.Value{
				clientValue(querypb.Type_INT64, "0) union select 1 #"),
			},
		},
		{
			// A quoted type declared for another quoted type would be escaped
			// either way, but a lastpk is a token Vitess hands out with the
			// column's own type, so any other type is a bug or an attack.
			name:   "client declares varbinary for a varchar column",
			fields: []*querypb.Field{field("name", querypb.Type_VARCHAR)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_VARBINARY, "abc")},
		},
		{
			// A number declared for a numeric column is parsed again as the
			// column's type, so a decimal declared for an integer column must
			// still be an integer.
			name:   "client declares a decimal for an int32 column",
			fields: []*querypb.Field{field("id", querypb.Type_INT32)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_DECIMAL, "1.5")},
		},
		{
			name: "injection in the second pk column",
			fields: []*querypb.Field{
				field("a", querypb.Type_INT64),
				field("b", querypb.Type_INT64),
			},
			lastpk: []sqltypes.Value{
				clientValue(querypb.Type_INT64, "1"),
				clientValue(querypb.Type_INT64, "2) union select 1 #"),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			pkColumns := make([]int, len(tc.fields))
			for i := range tc.fields {
				pkColumns[i] = i
			}

			_, err := validateLastPK(tc.lastpk, tc.fields, pkColumns)
			assert.Error(t, err, "lastpk %v was accepted for fields %v", tc.lastpk, tc.fields)
		})
	}
}

// TestValidateLastPKAcceptsRealValues pins the values a genuine copy phase
// resumes with, so the check cannot break ordinary streaming.
func TestValidateLastPKAcceptsRealValues(t *testing.T) {
	testcases := []struct {
		name   string
		fields []*querypb.Field
		lastpk []sqltypes.Value
	}{
		{
			name:   "int64",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, "12345")},
		},
		{
			name:   "negative int64",
			fields: []*querypb.Field{field("id", querypb.Type_INT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, "-9223372036854775808")},
		},
		{
			name:   "uint64 at the limit",
			fields: []*querypb.Field{field("id", querypb.Type_UINT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_UINT64, "18446744073709551615")},
		},
		{
			name:   "decimal",
			fields: []*querypb.Field{field("d", querypb.Type_DECIMAL)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_DECIMAL, "1.5")},
		},
		{
			name:   "float",
			fields: []*querypb.Field{field("f", querypb.Type_FLOAT64)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_FLOAT64, "-1.5e-30")},
		},
		{
			// A quoted type is escaped by EncodeSQL, so a quote in the value is
			// data and must keep working.
			name:   "varchar holding a quote",
			fields: []*querypb.Field{field("name", querypb.Type_VARCHAR)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_VARCHAR, "O'Brien")},
		},
		{
			name:   "varbinary holding arbitrary bytes",
			fields: []*querypb.Field{field("b", querypb.Type_VARBINARY)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_VARBINARY, "\x00\xff') union select 1 #")},
		},
		{
			name:   "composite pk",
			fields: []*querypb.Field{field("a", querypb.Type_INT64), field("b", querypb.Type_VARCHAR)},
			lastpk: []sqltypes.Value{
				clientValue(querypb.Type_INT64, "7"),
				clientValue(querypb.Type_VARCHAR, "abc"),
			},
		},
		{
			// A client building its own lastpk may use a 64-bit integer for any
			// integer column.
			name:   "client declares int64 for an int32 column",
			fields: []*querypb.Field{field("id", querypb.Type_INT32)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_INT64, "7")},
		},
		{
			name:   "timestamp",
			fields: []*querypb.Field{field("t", querypb.Type_TIMESTAMP)},
			lastpk: []sqltypes.Value{clientValue(querypb.Type_TIMESTAMP, "2026-08-04 12:00:00")},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			pkColumns := make([]int, len(tc.fields))
			for i := range tc.fields {
				pkColumns[i] = i
			}

			values, err := validateLastPK(tc.lastpk, tc.fields, pkColumns)
			require.NoError(t, err)
			for i, value := range values {
				assert.Equal(t, tc.fields[i].Type, value.Type())
				assert.Equal(t, tc.lastpk[i].Raw(), value.Raw())
			}
		})
	}
}

// TestBuildSelectRejectsInjectedLastPK proves the check is wired into the query
// builder, so the validator cannot become dead code while the sink stays open.
func TestBuildSelectRejectsInjectedLastPK(t *testing.T) {
	fields := []*querypb.Field{field("id", querypb.Type_INT64), field("val", querypb.Type_VARCHAR)}
	table := &binlogdatapb.MinimalTable{Name: "t1", Fields: fields, PKColumns: []int64{0}}

	newStreamer := func(lastpk []sqltypes.Value) *rowStreamer {
		return &rowStreamer{
			lastpk:    lastpk,
			pkColumns: []int{0},
			// NoTimeouts keeps buildSelect from reaching for rs.config, which a
			// real streamer receives from the engine.
			options: &binlogdatapb.VStreamOptions{NoTimeouts: true},
			plan: &Plan{
				Table: &Table{Name: "t1", Fields: fields},
			},
		}
	}

	t.Run("injected value is rejected", func(t *testing.T) {
		rs := newStreamer([]sqltypes.Value{
			clientValue(querypb.Type_INT64, "0) union select user, authentication_string from mysql.user #"),
		})
		query, err := rs.buildSelect(table)
		require.Error(t, err, "buildSelect produced %q", query)
		assert.NotContains(t, query, "mysql.user")
	})

	t.Run("wrong number of values is an invalid argument", func(t *testing.T) {
		rs := newStreamer([]sqltypes.Value{
			clientValue(querypb.Type_INT64, "1"),
			clientValue(querypb.Type_INT64, "2"),
		})
		_, err := rs.buildSelect(table)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
	})

	t.Run("real value still builds", func(t *testing.T) {
		rs := newStreamer([]sqltypes.Value{clientValue(querypb.Type_INT64, "42")})
		query, err := rs.buildSelect(table)
		require.NoError(t, err)
		assert.Contains(t, query, "id > 42")
	})
}

// outsideStringLiterals returns the parts of q that lie outside string literals
// when doubled quotes are the only escape, which is how mysqld reads a statement
// under NO_BACKSLASH_ESCAPES. VerifyMode only requires that a strict mode be
// present, so a tablet may well be running with that flag set.
func outsideStringLiterals(q string) string {
	var out strings.Builder
	for i := 0; i < len(q); {
		if q[i] != '\'' {
			out.WriteByte(q[i])
			i++
			continue
		}
		i++ // opening quote
		for i < len(q) {
			if q[i] == '\'' {
				if i+1 < len(q) && q[i+1] == '\'' {
					i += 2
					continue
				}
				i++
				break
			}
			i++
		}
		out.WriteByte(' ')
	}
	return out.String()
}

// TestLastPKValuesSurviveNoBackslashEscapes covers a textual primary key.
// EncodeSQL escapes those with backslashes, which stop being escapes when the
// tablet's sql_mode includes NO_BACKSLASH_ESCAPES; the literal then ends early
// and the rest of the value becomes SQL. A UNION is enough on its own, so this
// does not even need CLIENT_MULTI_STATEMENTS.
func TestLastPKValuesSurviveNoBackslashEscapes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     querypb.Type
		payload string
	}{
		{"varchar breakout", querypb.Type_VARCHAR, `') union select 1 #`},
		{"varchar with a backslash", querypb.Type_VARCHAR, `a\') union select 1 #`},
		{"varbinary breakout", querypb.Type_VARBINARY, `') union select 1 #`},
		{"char breakout", querypb.Type_CHAR, `') union select 1 #`},
		{"timestamp breakout", querypb.Type_TIMESTAMP, `') union select 1 #`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The column has the declared type, so the value reaches the writer.
			fields := []*querypb.Field{field("name", tc.typ), field("v", querypb.Type_VARCHAR)}
			table := &binlogdatapb.MinimalTable{Name: "t1", Fields: fields, PKColumns: []int64{0}}
			rs := &rowStreamer{
				lastpk:    []sqltypes.Value{clientValue(tc.typ, tc.payload)},
				pkColumns: []int{0},
				options:   &binlogdatapb.VStreamOptions{NoTimeouts: true},
				plan:      &Plan{Table: &Table{Name: "t1", Fields: fields}},
			}
			query, err := rs.buildSelect(table)
			require.NoError(t, err)
			outside := strings.ToLower(outsideStringLiterals(query))
			assert.NotContains(t, outside, "union",
				"under NO_BACKSLASH_ESCAPES the literal ends early; built %q", query)
			assert.NotContains(t, outside, "#",
				"under NO_BACKSLASH_ESCAPES a comment escapes the literal; built %q", query)
		})
	}
}

// TestLastPKValuesRoundTrip pins what buildSelect writes for each kind of lastpk
// value, and checks with mysqld that the literal reads back as the value's bytes,
// so the encoding cannot corrupt a real resume bound.
func TestLastPKValuesRoundTrip(t *testing.T) {
	everyByte := make([]byte, 256)
	for i := range everyByte {
		everyByte[i] = byte(i)
	}
	for _, tc := range []struct {
		name    string
		colType querypb.Type
		value   sqltypes.Value
		// literal is what buildSelect writes for the value, when the case pins it.
		literal string
		// readBack is the value's bytes as mysqld reads the literal.
		readBack string
	}{
		{"number", querypb.Type_INT64, clientValue(querypb.Type_INT64, "42"), "42", "42"},
		{"number declared as another numeric type", querypb.Type_INT32, clientValue(querypb.Type_INT64, "7"), "7", "7"},
		{"null", querypb.Type_VARCHAR, sqltypes.NULL, "null", ""},
		{"bit", querypb.Type_BIT, clientValue(querypb.Type_BIT, "\x05"), "b'00000101'", "\x05"},
		{"varbinary keeps its introducer", querypb.Type_VARBINARY, clientValue(querypb.Type_VARBINARY, "a'b"), "_binary'a''b'", "a'b"},
		{"varchar", querypb.Type_VARCHAR, clientValue(querypb.Type_VARCHAR, "O'Brien"), "'O''Brien'", "O'Brien"},
		{"varchar with a backslash", querypb.Type_VARCHAR, clientValue(querypb.Type_VARCHAR, `quote'and\backslash`), `'quote''and\\backslash'`, `quote'and\backslash`},
		{"empty varchar", querypb.Type_VARCHAR, clientValue(querypb.Type_VARCHAR, ""), "''", ""},
		{"timestamp", querypb.Type_TIMESTAMP, clientValue(querypb.Type_TIMESTAMP, "2026-08-04 12:00:00"), "'2026-08-04 12:00:00'", "2026-08-04 12:00:00"},
		// NUL, newline and Ctrl-Z are written as they are rather than escaped.
		{"varchar with control bytes", querypb.Type_VARCHAR, clientValue(querypb.Type_VARCHAR, "a\x00b\nc\x1ad"), "", "a\x00b\nc\x1ad"},
		{"varbinary with every byte", querypb.Type_VARBINARY, clientValue(querypb.Type_VARBINARY, string(everyByte)), "", string(everyByte)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := []*querypb.Field{field("pk", tc.colType)}
			table := &binlogdatapb.MinimalTable{Name: "t1", Fields: fields, PKColumns: []int64{0}}
			rs := &rowStreamer{
				lastpk:    []sqltypes.Value{tc.value},
				pkColumns: []int{0},
				options:   &binlogdatapb.VStreamOptions{NoTimeouts: true},
				plan:      &Plan{Table: &Table{Name: "t1", Fields: fields}},
			}
			query, err := rs.buildSelect(table)
			require.NoError(t, err)

			const before, after = "where (pk > ", ") order by "
			start := strings.Index(query, before)
			end := strings.LastIndex(query, after)
			require.True(t, start >= 0 && end > start, "no lastpk bound in %q", query)
			literal := query[start+len(before) : end]
			if tc.literal != "" {
				assert.Equal(t, tc.literal, literal)
			}

			qr, err := env.Mysqld.FetchSuperQuery(t.Context(), "select hex(cast("+literal+" as binary))")
			require.NoError(t, err)
			require.Len(t, qr.Rows, 1)
			if tc.value.IsNull() {
				assert.True(t, qr.Rows[0][0].IsNull(), "literal was %s", literal)
				return
			}
			assert.Equal(t, strings.ToUpper(hex.EncodeToString([]byte(tc.readBack))), qr.Rows[0][0].ToString(), "literal was %s", literal)
		})
	}
}
