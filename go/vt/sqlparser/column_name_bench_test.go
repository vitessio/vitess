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

package sqlparser

import (
	"strings"
	"testing"
)

func BenchmarkMySQLColumnName(b *testing.B) {
	cases := map[string]string{
		"column":    "select col1",
		"qualified": "select t.col1",
		"int":       "select 42",
		"string":    "select 'hello'",
		"count":     "select count(*)",
		"arith":     "select 1+1",
		"func":      "select concat(a, 'x', b)",
		"aliased":   "select a+1 as x",
		"long":      "select '" + strings.Repeat("abcdefgh", 40) + "'",
		"multibyte": "select 'héllo wörld 日本語 😀'",
		"versioned": "select /*!80000 1 + */ 2",
		"float":     "select 1.5e3",
	}
	parser := NewTestParser()
	env := ColumnNameEnv{}
	for name, q := range cases {
		stmt, err := parser.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		ae := stmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ae.MySQLColumnName(env)
			}
		})
	}
}

// BenchmarkParseColumnNameInput measures parsing selects that exercise the
// recording of column-name inputs.
func BenchmarkParseColumnNameInput(b *testing.B) {
	cases := map[string]string{
		"cols":      "select a, b, c, d, e, f, g, h from t",
		"literals":  "select 1, 'a', 2.5, null, 0x10, N'x', _utf8mb4'y', 1e3 from t",
		"aliased":   "select a as x, b as y, c+1 as z from t",
		"versioned": "select /*!80000 a, */ b, /*!99999 c, */ d from t",
	}
	parser := NewTestParser()
	for name, q := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parser.Parse(q); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
