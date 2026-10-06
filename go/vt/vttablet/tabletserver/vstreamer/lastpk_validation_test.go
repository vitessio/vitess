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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

// TestLastPKTextualValuesRoundTrip pins that a textual value still means itself,
// so the encoding change cannot corrupt a real resume bound.
func TestLastPKTextualValuesRoundTrip(t *testing.T) {
	fields := []*querypb.Field{field("name", querypb.Type_VARCHAR)}
	table := &binlogdatapb.MinimalTable{Name: "t1", Fields: fields, PKColumns: []int64{0}}

	for _, payload := range []string{"abc", "O'Brien", `back\slash`, "quote'and\\backslash", ""} {
		t.Run(payload, func(t *testing.T) {
			rs := &rowStreamer{
				lastpk:    []sqltypes.Value{clientValue(querypb.Type_VARCHAR, payload)},
				pkColumns: []int{0},
				options:   &binlogdatapb.VStreamOptions{NoTimeouts: true},
				plan:      &Plan{Table: &Table{Name: "t1", Fields: fields}},
			}
			query, err := rs.buildSelect(table)
			require.NoError(t, err)

			// Recover the literal and check it decodes back to the value under
			// ordinary sql_mode, where both '' and \' are escapes.
			start := strings.Index(query, "> '") + 2
			require.Greater(t, start, 2, "no literal in %q", query)
			literal := query[start:strings.LastIndex(query, ")")]
			assert.Equal(t, payload, decodeLiteral(literal), "literal was %s", literal)
		})
	}
}

// decodeLiteral undoes both doubled quotes and backslash escaping.
func decodeLiteral(lit string) string {
	lit = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(lit), "'"), "'")
	var out strings.Builder
	for i := 0; i < len(lit); i++ {
		switch {
		case lit[i] == '\'' && i+1 < len(lit) && lit[i+1] == '\'':
			out.WriteByte('\'')
			i++
		case lit[i] == '\\' && i+1 < len(lit):
			out.WriteByte(lit[i+1])
			i++
		default:
			out.WriteByte(lit[i])
		}
	}
	return out.String()
}
