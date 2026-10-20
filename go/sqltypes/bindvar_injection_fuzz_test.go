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

package sqltypes_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// bindVarFuzzTypes are the wire types a caller can name in a bind variable
// that reach the encoders' raw write, plus a quoted control.
var bindVarFuzzTypes = []querypb.Type{
	querypb.Type_INT64, querypb.Type_UINT64, querypb.Type_FLOAT64, querypb.Type_DECIMAL,
	querypb.Type_HEXNUM, querypb.Type_HEXVAL, querypb.Type_BITNUM, querypb.Type_VARCHAR,
}

// FuzzBindVarNoInjection pins the end-to-end property both gates (vtgate's
// ValidateBindVariables and the tablet's ValidateNestedBindVariables) rely on:
// any bind variable that passes the tablet's validator, once encoded into a
// statement by the tablet's encoder, yields exactly one statement whose first
// select expression is one literal and nothing else. Whatever the payload, it
// can only ever be a value.
//
// The statement carries a trailing sentinel column, ", 1", so that a payload
// which is a literal followed by SQL text is observable: a trailing comment
// swallows the sentinel (one expression instead of two), a tail clause such as
// "from t", "limit 1" or "for update" makes the sentinel a syntax error or
// pushes it into another clause, and the round trip through sqlparser.String
// rejects any alias, comment or clause the parser did accept. The seed corpus
// runs under plain `go test` as a regression test.
func FuzzBindVarNoInjection(f *testing.F) {
	for _, seed := range []string{
		"1", "-1", " 42", "1.5", "1e5", "NaN", "Infinity",
		"0xAB", "x'41'", "0b101",
		"1+1", "1; drop table x #", "1; do sleep(4) #", "(select user())",
		"x'41' or 'a'='a", strings.Repeat("9", 80) + "; drop table x #",
		"1." + strings.Repeat("9", 80) + "; drop table x #",
		"' or 1=1 #", "\\' or 1=1 #",
		"1 -- drop table x", "1 #", "1 from mysql.user", "1 limit 1",
		"1 into outfile '/tmp/x'", "1 for update",
		// Caught only by the round-trip check: an alias and a leading comment.
		"1 a", "/*vt+ x */ 1",
	} {
		for i := range bindVarFuzzTypes {
			f.Add(i, seed)
		}
	}
	parser := sqlparser.NewTestParser()
	f.Fuzz(func(t *testing.T, typeIdx int, payload string) {
		if typeIdx < 0 || typeIdx >= len(bindVarFuzzTypes) {
			return
		}
		bv := &querypb.BindVariable{Type: bindVarFuzzTypes[typeIdx], Value: []byte(payload)}
		if err := sqltypes.ValidateNestedBindVariables(map[string]*querypb.BindVariable{"v": bv}); err != nil {
			return // rejected at the boundary: nothing to encode
		}
		var buf strings.Builder
		buf.WriteString("select ")
		sqlparser.EncodeValue(&buf, bv)
		encoded := buf.String()[len("select "):]
		sql := buf.String() + ", 1"

		pieces, err := parser.SplitStatementToPieces(sql)
		require.NoError(t, err, "encoded: %q", sql)
		require.Len(t, pieces, 1, "encoded payload produced more than one statement: %q", sql)

		stmt, err := parser.Parse(sql)
		require.NoError(t, err, "encoded: %q", sql)
		sel, ok := stmt.(*sqlparser.Select)
		require.True(t, ok, "encoded: %q", sql)
		require.Len(t, sel.SelectExprs.Exprs, 2, "encoded: %q", sql)
		aliased, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
		require.True(t, ok, "encoded: %q", sql)

		expr := aliased.Expr
		if unary, ok := expr.(*sqlparser.UnaryExpr); ok && unary.Operator == sqlparser.UMinusOp {
			expr = unary.Expr // the parser keeps a negative numeric literal's sign as a unary minus
		}
		switch expr.(type) {
		case *sqlparser.Literal, *sqlparser.NullVal:
		default:
			require.Failf(t, "encoded bind variable is not a single literal", "type %s payload %q encoded as %q parses to %T", bv.Type, payload, encoded, expr)
		}
		require.Equal(t, "select "+sqlparser.String(aliased.Expr)+", 1 from dual", sqlparser.String(stmt), "encoded: %q", sql)
	})
}
