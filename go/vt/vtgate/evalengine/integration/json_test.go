/*
Copyright 2023 The Vitess Authors.

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

package integration

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func mustJSON(j string) sqltypes.Value {
	v, err := sqltypes.NewJSON(j)
	if err != nil {
		panic(err)
	}
	return v
}

func TestJSONExtract(t *testing.T) {
	var cases = []struct {
		Operator string
		Path     string
	}{
		{Operator: `->>`, Path: "$**.b"},
		{Operator: `->>`, Path: "$.c"},
		{Operator: `->>`, Path: "$.b[1].c"},
		{Operator: `->`, Path: "$.b[1].c"},
		{Operator: `->>`, Path: "$.b[1]"},
		{Operator: `->>`, Path: "$[0][0]"},
		{Operator: `->>`, Path: "$**[0]"},
		{Operator: `->>`, Path: "$.a[0]"},
		{Operator: `->>`, Path: "$[0].a[0]"},
		{Operator: `->>`, Path: "$**.a"},
		{Operator: `->>`, Path: "$[0][0][0].a"},
		{Operator: `->>`, Path: "$[*].b"},
		{Operator: `->>`, Path: "$[*].a"},
	}

	var rows = []sqltypes.Value{
		mustJSON(`[ { "a": 1 }, { "a": 2 } ]`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`),
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	var env evalengine.ExpressionEnv
	env.DefaultCollation = collations.CollationUtf8mb4ID
	env.Fields = []*querypb.Field{
		{
			Name:       "column0",
			Type:       sqltypes.TypeJSON,
			ColumnType: "JSON",
		},
	}

	for _, tc := range cases {
		expr0 := fmt.Sprintf("column0%s'%s'", tc.Operator, tc.Path)
		expr1 := fmt.Sprintf("cast(json_unquote(json_extract(column0, '%s')) as char)", tc.Path)
		expr2 := fmt.Sprintf("cast(%s as char) <=> %s", expr0, expr1)

		for _, row := range rows {
			env.Row = []sqltypes.Value{row}
			compareRemoteExprEnv(t, &env, conn, expr0)
			compareRemoteExprEnv(t, &env, conn, expr1)
			compareRemoteExprEnv(t, &env, conn, expr2)
		}
	}
}

var jsonInputs = []string{
	`true`, `false`, `"true"`, `'false'`,
	`1`, `1.0`, `'1'`, `'1.0'`, `NULL`, `'NULL'`,
	`'foobar'`, `'foo\nbar'`, `'a'`, `JSON_OBJECT()`,
}

func TestJSONObject(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, a := range jsonInputs {
		for _, b := range jsonInputs {
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_OBJECT(%s, %s)", a, b))
		}
	}
	compareRemoteExpr(t, conn, "JSON_OBJECT()")
}

func TestJSONArray(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, a := range jsonInputs {
		compareRemoteExpr(t, conn, fmt.Sprintf("JSON_ARRAY(%s)", a))
		for _, b := range jsonInputs {
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_ARRAY(%s, %s)", a, b))
		}
	}
	compareRemoteExpr(t, conn, "JSON_ARRAY()")
}

var inputJSONObjects = []string{
	`[ { "a": 1 }, { "a": 2 } ]`,
	`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`,
	`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`,
	`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`,
	`{"a": 1, "b": 2, "c": {"d": 4}}`,
	`["a", {"b": [true, false]}, [10, 20]]`,
	`[10, 20, [30, 40]]`,
}

var inputJSONPaths = []string{
	"$**.b", "$.c", "$.b[1].c", "$.b[1].c", "$.b[1]", "$[0][0]", "$**[0]", "$.a[0]",
	"$[0].a[0]", "$**.a", "$[0][0][0].a", "$[*].b", "$[*].a", `$[1].b[0]`, `$[2][2]`,
	`$.a`, `$.e`, `$.b`, `$.c.d`, `$.a.d`, `$[0]`, `$[1]`, `$[2][*]`, `$`,
}

func TestJSONPathOperations(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, obj := range inputJSONObjects {
		compareRemoteExpr(t, conn, fmt.Sprintf("JSON_KEYS('%s')", obj))

		for _, path1 := range inputJSONPaths {
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_EXTRACT('%s', '%s')", obj, path1))
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'one', '%s')", obj, path1))
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'all', '%s')", obj, path1))
			compareRemoteExpr(t, conn, fmt.Sprintf("JSON_KEYS('%s', '%s')", obj, path1))

			for _, path2 := range inputJSONPaths {
				compareRemoteExpr(t, conn, fmt.Sprintf("JSON_EXTRACT('%s', '%s', '%s')", obj, path1, path2))
				compareRemoteExpr(t, conn, fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'one', '%s', '%s')", obj, path1, path2))
				compareRemoteExpr(t, conn, fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'all', '%s', '%s')", obj, path1, path2))
			}
		}
	}
}
