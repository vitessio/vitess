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

package sqlparser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestNewParsedQuery(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("select * from a where id =:id")
	require.NoError(t, err)
	pq := NewParsedQuery(stmt)
	want := &ParsedQuery{
		Query:         "select * from a where id = :id",
		bindLocations: []BindLocation{{Offset: 27, Length: 3, isExpression: true}},
	}
	assert.Equalf(t, want, pq, "GenerateParsedQuery")
}

func TestGenerateQuery(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars map[string]*querypb.BindVariable
		extras   map[string]Encodable
		output   string
	}{
		{
			desc:  "no substitutions",
			query: "select * from a where id = 2",
			bindVars: map[string]*querypb.BindVariable{
				"id": sqltypes.Int64BindVariable(1),
			},
			output: "select * from a where id = 2",
		}, {
			desc:  "missing bind var",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVariable(1),
			},
			output: "missing bind var id2",
		}, {
			desc:  "simple bindvar substitution",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVariable(1),
				"id2": sqltypes.NullBindVariable,
			},
			output: "select * from a where id1 = 1 and id2 = null",
		}, {
			desc:  "non-ASCII bind vars",
			query: "insert into t values (:a, :b, :c, :d)",
			bindVars: map[string]*querypb.BindVariable{
				"a": sqltypes.BytesBindVariable([]byte{0x81, '\''}),
				"b": sqltypes.BytesBindVariable([]byte{0x81, '\\'}),
				"c": sqltypes.StringBindVariable("é"),
				// A text bind var is not validated against the connection
				// charset either, so it gets the same treatment.
				"d": sqltypes.StringBindVariable("\x81'"),
			},
			output: "insert into t values (" +
				"_binary'\x81''', " + // quote doubled, so the lead byte stays raw
				"_binary'" + "\\\x81" + "\\\\" + "', " + // `\` has no doubled form, so the run is escaped
				"'é', " +
				"'\x81''')",
		}, {
			desc:  "tuple *querypb.BindVariable",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]any{1, "aa"}),
			},
			output: "select * from a where id in (1, 'aa')",
		}, {
			desc:  "json bindvar and raw bindvar",
			query: "insert into t values (:v1, :v2)",
			bindVars: map[string]*querypb.BindVariable{
				"v1": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(querypb.Type_JSON, []byte(`{"key": "value"}`))),
				"v2": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(querypb.Type_RAW, []byte(`json_object("k", "v")`))),
			},
			output: `insert into t values ('{"key": "value"}', json_object("k", "v"))`,
		}, {
			desc:  "list bind vars 0 arguments",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]any{}),
			},
			output: "empty list supplied for vals",
		}, {
			desc:  "non-list bind var supplied",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.Int64BindVariable(1),
			},
			output: "unexpected list arg type (INT64) for key vals",
		}, {
			desc:  "row tuple",
			query: "select 1 from (values ::a) dt",
			bindVars: map[string]*querypb.BindVariable{
				"a": createRowTupleBV(),
			},
			output: "select 1 from (values row('a', 1), row('b', 2)) as dt",
		}, {
			desc:  "list bind var for non-list",
			query: "select * from a where id = :vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]any{1}),
			},
			output: "unexpected arg type (TUPLE) for non-list key vals",
		}, {
			desc:  "single column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []IdentifierCI{NewIdentifierCI("pk")},
					Rows: [][]sqltypes.Value{
						{sqltypes.NewInt64(1)},
						{sqltypes.NewVarChar("aa")},
					},
				},
			},
			output: "select * from a where b = pk in (1, 'aa')",
		}, {
			desc:  "multi column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []IdentifierCI{NewIdentifierCI("pk1"), NewIdentifierCI("pk2")},
					Rows: [][]sqltypes.Value{
						{
							sqltypes.NewInt64(1),
							sqltypes.NewVarChar("aa"),
						},
						{
							sqltypes.NewInt64(2),
							sqltypes.NewVarChar("bb"),
						},
					},
				},
			},
			output: "select * from a where b = (pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
		},
	}

	parser := NewTestParser()
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			tree, err := parser.Parse(tcase.query)
			require.NoError(t, err)
			buf := NewTrackedBuffer(nil)
			buf.Myprintf("%v", tree)
			pq := buf.ParsedQuery()
			bytes, err := pq.GenerateQuery(tcase.bindVars, tcase.extras)
			if err != nil {
				assert.Equal(t, tcase.output, err.Error())
			} else {
				assert.Equal(t, tcase.output, bytes)
			}
		})
	}
}

func TestParseAndBind(t *testing.T) {
	testcases := []struct {
		in    string
		binds []*querypb.BindVariable
		out   string
	}{
		{
			in:  "select * from tbl",
			out: "select * from tbl",
		}, {
			in:  "select * from tbl where b=4 or a=3",
			out: "select * from tbl where b=4 or a=3",
		}, {
			in:  "select * from tbl where b = 4 or a = 3",
			out: "select * from tbl where b = 4 or a = 3",
		}, {
			in:    "select * from tbl where name=%a",
			binds: []*querypb.BindVariable{sqltypes.StringBindVariable("xyz")},
			out:   "select * from tbl where name='xyz'",
		}, {
			in:    "select * from tbl where c=%a",
			binds: []*querypb.BindVariable{sqltypes.Int64BindVariable(17)},
			out:   "select * from tbl where c=17",
		}, {
			in:    "select * from tbl where name=%a and c=%a",
			binds: []*querypb.BindVariable{sqltypes.StringBindVariable("xyz"), sqltypes.Int64BindVariable(17)},
			out:   "select * from tbl where name='xyz' and c=17",
		}, {
			in:    "select * from tbl where name=%a",
			binds: []*querypb.BindVariable{sqltypes.StringBindVariable("it's")},
			out:   "select * from tbl where name='it\\'s'",
		}, {
			in:    "where name=%a",
			binds: []*querypb.BindVariable{sqltypes.StringBindVariable("xyz")},
			out:   "where name='xyz'",
		}, {
			in:    "name=%a",
			binds: []*querypb.BindVariable{sqltypes.StringBindVariable("xyz")},
			out:   "name='xyz'",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			query, err := ParseAndBind(tc.in, tc.binds...)
			require.NoError(t, err)
			assert.Equal(t, tc.out, query)
		})
	}
}

func TestCastBindVars(t *testing.T) {
	testcases := []struct {
		typ   sqltypes.Type
		size  int
		binds map[string]*querypb.BindVariable
		out   string
	}{
		{
			typ:   sqltypes.Decimal,
			binds: map[string]*querypb.BindVariable{"arg": sqltypes.DecimalBindVariable("50")},
			out:   "select CAST(50 AS DECIMAL(0, 0)) from dual",
		},
		{
			typ:   sqltypes.Uint32,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Uint32, Value: sqltypes.NewUint32(42).Raw()}},
			out:   "select CAST(42 AS UNSIGNED) from dual",
		},
		{
			typ:   sqltypes.Float64,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Float64, Value: sqltypes.NewFloat64(42.42).Raw()}},
			out:   "select CAST(42.42 AS DOUBLE) from dual",
		},
		{
			typ:   sqltypes.Float32,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Float32, Value: sqltypes.NewFloat32(42).Raw()}},
			out:   "select CAST(42 AS FLOAT) from dual",
		},
		{
			typ:   sqltypes.Date,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Date, Value: sqltypes.NewDate("2021-10-30").Raw()}},
			out:   "select CAST('2021-10-30' AS DATE) from dual",
		},
		{
			typ:   sqltypes.Time,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Time, Value: sqltypes.NewTime("12:00:00").Raw()}},
			out:   "select CAST('12:00:00' AS TIME) from dual",
		},
		{
			typ:   sqltypes.Time,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Time, Value: sqltypes.NewTime("12:00:00").Raw()}},
			out:   "select CAST('12:00:00' AS TIME(6)) from dual",
		},
		{
			typ:   sqltypes.Timestamp,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Timestamp, Value: sqltypes.NewTimestamp("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME) from dual",
		},
		{
			typ:   sqltypes.Timestamp,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Timestamp, Value: sqltypes.NewTimestamp("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME(6)) from dual",
		},
		{
			typ:   sqltypes.Datetime,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Datetime, Value: sqltypes.NewDatetime("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME) from dual",
		},
		{
			typ:   sqltypes.Datetime,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Datetime, Value: sqltypes.NewDatetime("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME(6)) from dual",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.out, func(t *testing.T) {
			argument := NewTypedArgument("arg", testcase.typ)
			if testcase.size > 0 {
				argument.Size = int32(testcase.size)
			}

			s := &Select{
				SelectExprs: &SelectExprs{
					Exprs: []SelectExpr{NewAliasedExpr(argument, "")},
				},
			}

			pq := NewParsedQuery(s)
			out, err := pq.GenerateQuery(testcase.binds, nil)

			require.NoError(t, err)
			require.Equal(t, testcase.out, out)
		})
	}
}

func TestGenerateQueryTextExpressions(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("select :v, _latin1 :v, :v from t where c in ::vals")
	require.NoError(t, err)
	binds := map[string]*querypb.BindVariable{
		"v": sqltypes.StringBindVariable("é\n"),
		"vals": {
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{
				sqltypes.ValueToProto(sqltypes.NewVarChar("é\n")),
				sqltypes.ValueToProto(sqltypes.NewVarBinary("\x81\\")),
			},
		},
	}
	literal := sqltypes.EncodeStringSQL("é\n")
	want := "select '' " + literal + ", _latin1 " + literal + ", '' " + literal +
		" from t where c in ('' " + literal + ", _binary'\\\x81\\\\')"
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprintf("fast=%v", fast), func(t *testing.T) {
			buf := NewTrackedBuffer(nil)
			buf.fast = fast
			buf.WriteNode(stmt)
			query, err := buf.ParsedQuery().GenerateQuery(binds, nil)
			require.NoError(t, err)
			assert.Equal(t, want, query)
			_, err = parser.Parse(query)
			require.NoError(t, err)
		})
	}

	t.Run("national bind", func(t *testing.T) {
		pq := NewParsedQuery(&UnaryExpr{Operator: NStringOp, Expr: NewArgument("v")})
		query, err := pq.GenerateQuery(binds, nil)
		require.NoError(t, err)
		assert.Equal(t, "_utf8mb3 "+literal, query)
	})

	t.Run("row tuple", func(t *testing.T) {
		stmt, err := parser.Parse("select 1 from (values ::rows) dt")
		require.NoError(t, err)
		query, err := NewParsedQuery(stmt).GenerateQuery(map[string]*querypb.BindVariable{
			"rows": {
				Type:   querypb.Type_ROW_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewVarChar("é\n"), sqltypes.NewInt64(1)))},
			},
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, "select 1 from (values row('' "+literal+", 1)) as dt", query)
	})

	t.Run("raw template expression contexts", func(t *testing.T) {
		pq := BuildParsedQuery("select %e, _latin1 %a, %e from t where c in %e", ":v", ":v", ":v", "::vals")
		query, err := pq.GenerateQuery(binds, nil)
		require.NoError(t, err)
		assert.Equal(t, want, query)
		_, err = parser.Parse(query)
		require.NoError(t, err)
	})

	t.Run("raw expression binds", func(t *testing.T) {
		query, err := ParseAndBind("select convert(%e using latin1) from t where c in %e", binds["v"], binds["vals"])
		require.NoError(t, err)
		assert.Equal(t, "select convert('' "+literal+" using latin1) from t where c in ('' "+literal+", _binary'\\\x81\\\\')", query)
		_, err = parser.Parse(query)
		require.NoError(t, err)
	})

	t.Run("raw template remains single-token", func(t *testing.T) {
		query, err := ParseAndBind("show tables like %a", binds["v"])
		require.NoError(t, err)
		assert.Equal(t, "show tables like "+literal, query)
		_, err = parser.Parse(query)
		require.NoError(t, err)
	})
}

func createRowTupleBV() *querypb.BindVariable {
	v1 := sqltypes.TestTuple(sqltypes.NewVarChar("a"), sqltypes.NewInt64(1))
	v2 := sqltypes.TestTuple(sqltypes.NewVarChar("b"), sqltypes.NewInt64(2))
	return &querypb.BindVariable{
		Type:   querypb.Type_ROW_TUPLE,
		Values: append([]*querypb.Value{sqltypes.ValueToProto(v1)}, sqltypes.ValueToProto(v2)),
	}
}

// BenchmarkGenerateQueryStringBinds measures bind substitution with eight
// string and binary values, so encoding dominates the query traversal.
func BenchmarkGenerateQueryStringBinds(b *testing.B) {
	parser := NewTestParser()
	stmt, err := parser.Parse("insert into t(a, b, c, d, e, f, g, h) values (:a, :b, :c, :d, :e, :f, :g, :h)")
	require.NoError(b, err)
	pq := NewParsedQuery(stmt)

	// Every bind exercises both clean-run copying and escaping. The non-ASCII
	// shape additionally reaches the high-byte run handling, which is the
	// branch that has to escape byte by byte.
	for _, text := range []struct {
		name string
		body string
	}{
		{name: "ascii", body: "It's the quick brown fox that jumps over the lazy dog; again. "},
		{name: "highbytes", body: "L'été où le renard brun sauta par-dessus le chien paresseux; à nouveau. "},
	} {
		for _, size := range []int{64, 1024} {
			payload := strings.Repeat(text.body, size/len(text.body)+1)[:size]
			bindVars := map[string]*querypb.BindVariable{
				"a": sqltypes.StringBindVariable(payload),
				"b": sqltypes.StringBindVariable(payload),
				"c": sqltypes.StringBindVariable(payload),
				"d": sqltypes.StringBindVariable(payload),
				"e": sqltypes.BytesBindVariable([]byte(payload)),
				"f": sqltypes.BytesBindVariable([]byte(payload)),
				"g": sqltypes.BytesBindVariable([]byte(payload)),
				"h": sqltypes.BytesBindVariable([]byte(payload)),
			}
			b.Run(fmt.Sprintf("%s/%dB", text.name, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(8 * size))
				for b.Loop() {
					if _, err := pq.GenerateQuery(bindVars, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
