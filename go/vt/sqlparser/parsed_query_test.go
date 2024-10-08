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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestNewParsedQuery(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("select * from a where id =:id")
	if err != nil {
		t.Error(err)
		return
	}
	pq := NewParsedQuery(stmt)
	want := &ParsedQuery{
		Query:         "select * from a where id = :id",
		bindLocations: []BindLocation{{Offset: 27, Length: 3}},
	}
	if !reflect.DeepEqual(pq, want) {
		t.Errorf("GenerateParsedQuery: %+v, want %+v", pq, want)
	}
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
						{sqltypes.NewVarBinary("aa")},
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
							sqltypes.NewVarBinary("aa"),
						},
						{
							sqltypes.NewInt64(2),
							sqltypes.NewVarBinary("bb"),
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
			assert.NoError(t, err)
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
			out:   "select CAST(50 AS DECIMAL(0, 0)) from ",
		},
		{
			typ:   sqltypes.Uint32,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Uint32, Value: sqltypes.NewUint32(42).Raw()}},
			out:   "select CAST(42 AS UNSIGNED) from ",
		},
		{
			typ:   sqltypes.Float64,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Float64, Value: sqltypes.NewFloat64(42.42).Raw()}},
			out:   "select CAST(42.42 AS DOUBLE) from ",
		},
		{
			typ:   sqltypes.Float32,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Float32, Value: sqltypes.NewFloat32(42).Raw()}},
			out:   "select CAST(42 AS FLOAT) from ",
		},
		{
			typ:   sqltypes.Date,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Date, Value: sqltypes.NewDate("2021-10-30").Raw()}},
			out:   "select CAST('2021-10-30' AS DATE) from ",
		},
		{
			typ:   sqltypes.Time,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Time, Value: sqltypes.NewTime("12:00:00").Raw()}},
			out:   "select CAST('12:00:00' AS TIME) from ",
		},
		{
			typ:   sqltypes.Time,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Time, Value: sqltypes.NewTime("12:00:00").Raw()}},
			out:   "select CAST('12:00:00' AS TIME(6)) from ",
		},
		{
			typ:   sqltypes.Timestamp,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Timestamp, Value: sqltypes.NewTimestamp("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME) from ",
		},
		{
			typ:   sqltypes.Timestamp,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Timestamp, Value: sqltypes.NewTimestamp("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME(6)) from ",
		},
		{
			typ:   sqltypes.Datetime,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Datetime, Value: sqltypes.NewDatetime("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME) from ",
		},
		{
			typ:   sqltypes.Datetime,
			size:  6,
			binds: map[string]*querypb.BindVariable{"arg": {Type: sqltypes.Datetime, Value: sqltypes.NewDatetime("2021-10-22 12:00:00").Raw()}},
			out:   "select CAST('2021-10-22 12:00:00' AS DATETIME(6)) from ",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.out, func(t *testing.T) {
			argument := NewTypedArgument("arg", testcase.typ)
			if testcase.size > 0 {
				argument.Size = int32(testcase.size)
			}

			s := &Select{
				SelectExprs: SelectExprs{
					NewAliasedExpr(argument, ""),
				},
			}

			pq := NewParsedQuery(s)
			out, err := pq.GenerateQuery(testcase.binds, nil)

			require.NoError(t, err)
			require.Equal(t, testcase.out, out)
		})
	}
}
