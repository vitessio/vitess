/*
Copyright 2022 The Vitess Authors.

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

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestMultiply(t *testing.T) {
	expr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     &sqlparser.Offset{V: 0},
		Right:    &sqlparser.Offset{V: 1},
	}
	evalExpr, err := evalengine.Translate(expr, &evalengine.Config{
		Environment: vtenv.NewTestEnv(),
		Collation:   collations.MySQL8().DefaultConnectionCharset(),
	})
	require.NoError(t, err)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("a|b", "uint64|uint64"),
			"3|2",
			"1|0",
			"1|2",
		)},
	}
	proj := &Projection{
		Cols:       []string{"apa"},
		Exprs:      []evalengine.Expr{evalExpr},
		Input:      fp,
		noTxNeeded: noTxNeeded{},
	}
	qr, err := proj.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	assert.Equal(t, "[[UINT64(6)] [UINT64(0)] [UINT64(2)]]", fmt.Sprintf("%v", qr.Rows))

	fp = &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("a|b", "uint64|uint64"),
			"3|2",
			"1|0",
			"1|2",
		)},
	}
	proj.Input = fp
	qr, err = wrapStreamExecute(proj, &noopVCursor{}, nil, true)
	require.NoError(t, err)
	assert.Equal(t, "[[UINT64(6)] [UINT64(0)] [UINT64(2)]]", fmt.Sprintf("%v", qr.Rows))
}

func TestProjectionStreaming(t *testing.T) {
	expr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     &sqlparser.Offset{V: 0},
		Right:    &sqlparser.Offset{V: 1},
	}
	evalExpr, err := evalengine.Translate(expr, &evalengine.Config{
		Environment: vtenv.NewTestEnv(),
		Collation:   collations.MySQL8().DefaultConnectionCharset(),
	})
	require.NoError(t, err)
	fp := &fakePrimitive{
		results: sqltypes.MakeTestStreamingResults(
			sqltypes.MakeTestFields("a|b", "uint64|uint64"),
			"3|2",
			"1|0",
			"6|2",
			"---",
			"3|2",
			"---",
			"1|0",
			"---",
			"1|2",
			"4|2",
			"---",
			"5|5",
			"4|10",
		),
		async: true,
	}
	proj := &Projection{
		Cols:  []string{"apa"},
		Exprs: []evalengine.Expr{evalExpr},
		Input: fp,
	}

	qr := &sqltypes.Result{}
	err = proj.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(result *sqltypes.Result) error {
		qr.Rows = append(qr.Rows, result.Rows...)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, sqltypes.RowsEqualsStr(`[[UINT64(25)] [UINT64(40)] [UINT64(6)] [UINT64(2)] [UINT64(8)] [UINT64(0)] [UINT64(6)] [UINT64(0)] [UINT64(12)]]`,
		qr.Rows))
}

func TestEmptyInput(t *testing.T) {
	expr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     &sqlparser.Offset{V: 0},
		Right:    &sqlparser.Offset{V: 1},
	}
	evalExpr, err := evalengine.Translate(expr, &evalengine.Config{
		Environment: vtenv.NewTestEnv(),
		Collation:   collations.MySQL8().DefaultConnectionCharset(),
	})
	require.NoError(t, err)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "uint64|uint64"))},
	}
	proj := &Projection{
		Cols:       []string{"count(*)"},
		Exprs:      []evalengine.Expr{evalExpr},
		Input:      fp,
		noTxNeeded: noTxNeeded{},
	}
	qr, err := proj.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	assert.Equal(t, "[]", fmt.Sprintf("%v", qr.Rows))

	// fp = &fakePrimitive{
	//	results: []*sqltypes.Result{sqltypes.MakeTestResult(
	//		sqltypes.MakeTestFields("a|b", "uint64|uint64"),
	//		"3|2",
	//		"1|0",
	//		"1|2",
	//	)},
	// }
	// proj.Input = fp
	// qr, err = wrapStreamExecute(proj, newNoopVCursor(t.Context()), nil, true)
	// require.NoError(t, err)
	// assert.Equal(t, "[[UINT64(6)] [UINT64(0)] [UINT64(2)]]", fmt.Sprintf("%v", qr.Rows))
}

func TestHexAndBinaryArgument(t *testing.T) {
	hexExpr, err := evalengine.Translate(sqlparser.NewArgument("vtg1"), &evalengine.Config{
		Environment: vtenv.NewTestEnv(),
		Collation:   collations.MySQL8().DefaultConnectionCharset(),
	})
	require.NoError(t, err)
	proj := &Projection{
		Cols:       []string{"hex"},
		Exprs:      []evalengine.Expr{hexExpr},
		Input:      &SingleRow{},
		noTxNeeded: noTxNeeded{},
	}
	qr, err := proj.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{
		"vtg1": sqltypes.HexNumBindVariable([]byte("0x9")),
	}, false)
	require.NoError(t, err)
	assert.Equal(t, `[[VARBINARY("\t")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestFields(t *testing.T) {
	testCases := []struct {
		name      string
		bindVar   *querypb.BindVariable
		typ       querypb.Type
		collation collations.ID
	}{
		{
			name:      `integer`,
			bindVar:   sqltypes.Int64BindVariable(10),
			typ:       querypb.Type_INT64,
			collation: collations.CollationBinaryID,
		},
		{
			name:      `string`,
			bindVar:   sqltypes.StringBindVariable("test"),
			typ:       querypb.Type_VARCHAR,
			collation: collations.MySQL8().DefaultConnectionCharset(),
		},
		{
			name:      `binary`,
			bindVar:   sqltypes.BytesBindVariable([]byte("test")),
			typ:       querypb.Type_VARBINARY,
			collation: collations.CollationBinaryID,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bindExpr, err := evalengine.Translate(sqlparser.NewArgument("vtg1"), &evalengine.Config{
				Environment: vtenv.NewTestEnv(),
				Collation:   collations.MySQL8().DefaultConnectionCharset(),
			})
			require.NoError(t, err)
			proj := &Projection{
				Cols:       []string{"col"},
				Exprs:      []evalengine.Expr{bindExpr},
				Input:      &SingleRow{},
				noTxNeeded: noTxNeeded{},
			}
			qr, err := proj.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{
				"vtg1": testCase.bindVar,
			}, true)
			require.NoError(t, err)
			assert.Equal(t, testCase.typ, qr.Fields[0].Type)
			assert.Equal(t, testCase.collation, collations.ID(qr.Fields[0].Charset))
		})
	}
}

func TestFieldConversion(t *testing.T) {
	testCases := []struct {
		name      string
		expr      string
		typ       querypb.Type
		collation collations.ID
	}{
		{
			name:      `convert different charset`,
			expr:      `_latin1 0xFF`,
			typ:       sqltypes.VarChar,
			collation: collations.MySQL8().DefaultConnectionCharset(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			arg, err := sqlparser.NewTestParser().ParseExpr(testCase.expr)
			require.NoError(t, err)
			bindExpr, err := evalengine.Translate(arg, &evalengine.Config{
				Environment: vtenv.NewTestEnv(),
				Collation:   collations.MySQL8().DefaultConnectionCharset(),
			})
			require.NoError(t, err)
			proj := &Projection{
				Cols:       []string{"col"},
				Exprs:      []evalengine.Expr{bindExpr},
				Input:      &SingleRow{},
				noTxNeeded: noTxNeeded{},
			}
			qr, err := proj.TryExecute(t.Context(), &noopVCursor{}, nil, true)
			require.NoError(t, err)
			assert.Equal(t, testCase.typ, qr.Fields[0].Type)
			assert.Equal(t, testCase.collation, collations.ID(qr.Fields[0].Charset))
		})
	}
}

// TestProjectionBindVarLength checks that a string function whose length
// depends on a bind variable has the same type in its field and in its
// values, and that both change with the value of the bind variable:
// REPEAT(col, :n) on a utf8mb4 VARCHAR(10) is a VARCHAR for n = 2 and a TEXT
// for n = 1639.
func TestProjectionBindVarLength(t *testing.T) {
	venv := vtenv.NewTestEnv()
	field := &querypb.Field{Name: "col", Type: sqltypes.VarChar, ColumnLength: 40, Charset: uint32(collations.CollationUtf8mb4ID)}
	translate := func(expression string, bindType sqltypes.Type) evalengine.Expr {
		expr, err := venv.Parser().ParseExpr(expression)
		require.NoError(t, err)
		// The normalizer gives the bind variable the type of its literal.
		expr = sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
			if arg, ok := cursor.Node().(*sqlparser.Argument); ok {
				cursor.Replace(sqlparser.NewTypedArgument(arg.Name, bindType))
			}
			return true
		}, nil).(sqlparser.Expr)
		e, err := evalengine.Translate(expr, &evalengine.Config{
			ResolveColumn: evalengine.FieldResolver{field}.Column,
			ResolveType: func(sqlparser.Expr) (evalengine.Type, bool) {
				return evalengine.NewTypeFromField(field), true
			},
			Collation:   collations.CollationUtf8mb4ID,
			Environment: venv,
		})
		require.NoError(t, err)
		return e
	}
	proj := &Projection{
		Cols:  []string{"r", "c"},
		Exprs: []evalengine.Expr{translate("REPEAT(col, :n)", sqltypes.Int64), translate("CONCAT(col, :v)", sqltypes.VarChar)},
	}
	executions := []struct {
		bindVars map[string]*querypb.BindVariable
		want     sqltypes.Type
	}{
		{map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2), "v": sqltypes.StringBindVariable("xyz")}, sqltypes.VarChar},
		{map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1639), "v": sqltypes.StringBindVariable(strings.Repeat("x", 16380))}, sqltypes.Text},
	}
	run := map[string]func(wantfields bool, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error){
		"TryExecute": func(wantfields bool, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
			return proj.TryExecute(t.Context(), &noopVCursor{}, bindVars, wantfields)
		},
		"TryStreamExecute": func(wantfields bool, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
			return wrapStreamExecute(proj, &noopVCursor{}, bindVars, wantfields)
		},
	}
	for name, execute := range run {
		for _, wantfields := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/wantfields=%v", name, wantfields), func(t *testing.T) {
				for _, exec := range executions {
					proj.Input = &fakePrimitive{results: []*sqltypes.Result{{
						Fields: []*querypb.Field{field},
						Rows:   []sqltypes.Row{{sqltypes.NewVarChar("abc")}},
					}}}
					qr, err := execute(wantfields, exec.bindVars)
					require.NoError(t, err)
					require.Len(t, qr.Rows, 1)
					for i := range proj.Exprs {
						assert.Equal(t, exec.want, qr.Rows[0][i].Type(), "type of the value of %s", proj.Cols[i])
						if wantfields {
							require.Len(t, qr.Fields, len(proj.Exprs))
							assert.Equal(t, exec.want, qr.Fields[i].Type, "type of the field %s", proj.Cols[i])
						}
					}
				}
			})
		}
	}
}
