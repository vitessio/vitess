/*
Copyright 2017 Google Inc.

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

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestParsedQuery(t *testing.T) {
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
				"id": sqltypes.Int64BindVar(1),
			},
			output: "select * from a where id = 2",
		}, {
			desc:  "missing bind var",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVar(1),
			},
			output: "missing bind var id2",
		}, {
			desc:  "simple bindvar substitution",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVar(1),
				"id2": sqltypes.NullBV,
			},
			output: "select * from a where id1 = 1 and id2 = null",
		}, {
			desc:  "tuple *querypb.BindVariable",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.MakeTestBindVar([]interface{}{1, "aa"}),
			},
			output: "select * from a where id in (1, 'aa')",
		}, {
			desc:  "list bind vars 0 arguments",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.MakeTestBindVar([]interface{}{}),
			},
			output: "empty list supplied for vals",
		}, {
			desc:  "non-list bind var supplied",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.Int64BindVar(1),
			},
			output: "unexpected list arg type (INT64) for key vals",
		}, {
			desc:  "list bind var for non-list",
			query: "select * from a where id = :vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.MakeTestBindVar([]interface{}{1}),
			},
			output: "unexpected arg type (TUPLE) for non-list key vals",
		}, {
			desc:  "single column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []ColIdent{NewColIdent("pk")},
					Rows: [][]sqltypes.Value{
						{sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))},
						{sqltypes.MakeString([]byte("aa"))},
					},
				},
			},
			output: "select * from a where b = pk in (1, 'aa')",
		}, {
			desc:  "multi column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []ColIdent{NewColIdent("pk1"), NewColIdent("pk2")},
					Rows: [][]sqltypes.Value{
						{
							sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
							sqltypes.MakeString([]byte("aa")),
						},
						{
							sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
							sqltypes.MakeString([]byte("bb")),
						},
					},
				},
			},
			output: "select * from a where b = (pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
		},
	}

	for _, tcase := range tcases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("parse failed for %s: %v", tcase.desc, err)
			continue
		}
		buf := NewTrackedBuffer(nil)
		buf.Myprintf("%v", tree)
		pq := buf.ParsedQuery()
		bytes, err := pq.GenerateQuery(tcase.bindVars, tcase.extras)
		var got string
		if err != nil {
			got = err.Error()
		} else {
			got = string(bytes)
		}
		if got != tcase.output {
			t.Errorf("for test case: %s, got: '%s', want '%s'", tcase.desc, got, tcase.output)
		}
	}
}

func TestGenerateParsedQuery(t *testing.T) {
	stmt, err := Parse("select * from a where id =:id")
	if err != nil {
		t.Error(err)
		return
	}
	pq := GenerateParsedQuery(stmt)
	want := &ParsedQuery{
		Query:         "select * from a where id = :id",
		bindLocations: []bindLocation{{offset: 27, length: 3}},
	}
	if !reflect.DeepEqual(pq, want) {
		t.Errorf("GenerateParsedQuery: %+v, want %+v", pq, want)
	}
}

// TestUnorthodox is for testing syntactically invalid constructs
// that we use internally for efficient SQL generation.
func TestUnorthodox(t *testing.T) {
	query := "insert into `%s` values %a"
	extras := map[string]Encodable{
		"vals": InsertValues{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
			sqltypes.MakeString([]byte("foo('a')")),
		}, {
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
			sqltypes.MakeString([]byte("bar(`b`)")),
		}},
	}
	buf := NewTrackedBuffer(nil)
	buf.Myprintf(query, "t", ":vals")
	pq := buf.ParsedQuery()
	bytes, err := pq.GenerateQuery(nil, extras)
	if err != nil {
		t.Error(err)
	}
	got := string(bytes)
	want := "insert into `t` values (1, 'foo(\\'a\\')'), (2, 'bar(`b`)')"
	if got != want {
		t.Errorf("GenerateQuery: %s, want %s", got, want)
	}
}
